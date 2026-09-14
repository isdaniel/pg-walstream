//! CopyBoth hot path — the critical performance piece.
//!
//! Once `START_REPLICATION` returns `CopyBothResponse`, the connection enters
//! COPY mode. This module provides the zero-copy, drain-loop optimized read
//! path for CopyData messages, and the write path for standby status updates.

use bytes::{Bytes, BytesMut};
use std::collections::VecDeque;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};

use super::wire;
use crate::error::ReplicationError;
use tokio_util::sync::CancellationToken;

/// Maximum messages to drain from the read buffer in a single batch.
/// Prevents unbounded queue growth under extreme throughput.
const MAX_DRAIN_BATCH: usize = 4096;

// Header framing and body-length bounds are shared with the query path (single source in `wire`).
use super::wire::{HEADER_LEN, MAX_MESSAGE_LEN};

/// Headroom reserved on `read_buf` before each socket read.
///
/// `read_buf` on a full `BytesMut` reserves only 64 bytes at a time, capping each `read()` to a tiny slice and multiplying syscalls under load. Reserving a large chunk lets one syscall pull far more TLS-decrypted data, so the drain loop slices more messages per read.
///
/// Note: 256 KiB is a common sweet spot; the win is syscall-count, which this repo has no IO benchmark to measure — tune here if a real workload shows read/syscall dominating a flamegraph.
const READ_CHUNK: usize = 256 * 1024;

/// Minimum free headroom that triggers a `reserve(READ_CHUNK)` top-up.
///
/// Using `READ_CHUNK` itself as the threshold would realloc on almost every
/// read: after each drain, frozen slices pin the buffer so `reserve` can't
/// reclaim, and the remaining headroom sits just below `READ_CHUNK` → reserve fires and reallocs every iteration. Checking against this smaller threshold lets one 256 KiB reservation serve many reads before the next top-up.
///
/// Pinned strictly greater than [`startup::TLS_BUF_SIZE`](super::startup::TLS_BUF_SIZE): at read time `read_buf` always has more free space than the TLS `BufReader`'s internal buffer, so tokio's `BufReader` bypasses its own buffer and decrypts
/// straight into `read_buf` — no intermediate copy on the streaming hot path.
/// The `const` assert below enforces the coupling at compile time (previously the two happened to be equal, which held only by coincidence).
const MIN_HEADROOM: usize = super::startup::TLS_BUF_SIZE + 4 * 1024;
const _: () = assert!(MIN_HEADROOM > super::startup::TLS_BUF_SIZE);

/// Read the next CopyData payload from the replication stream.
///
/// This implements a **drain-loop batch queue** optimization:
/// 1. First check the `pending` queue — return immediately if non-empty (zero syscall).
/// 2. Otherwise, read from the transport into `read_buf`.
/// 3. Parse ALL complete messages from `read_buf` into `pending` (drain loop).
/// 4. Return the first message.
///
/// The critical performance wins:
/// - `read_buf()` reads TLS-decrypted data directly into `BytesMut` — no copies.
/// - `split_to().freeze().slice(5..)` — zero-copy extraction of CopyData payload.
/// - Drain loop: one `read_buf()` → parse ALL complete messages → amortize syscall.
/// - Native `AsyncRead` on `TlsStream` — no `AsyncFd` wrapper around C socket fd.
pub async fn get_copy_data<R: AsyncRead + Unpin>(
    reader: &mut R,
    read_buf: &mut BytesMut,
    pending: &mut VecDeque<Bytes>,
    cancellation_token: &CancellationToken,
) -> Result<Bytes, ReplicationError> {
    loop {
        // Re-drain before blocking: the `MAX_DRAIN_BATCH` cap can leave complete
        // frames behind, and `drain_read_buffer` latches a terminal ('c'/'E') in
        // `read_buf` while frames are still queued. Without this they stay
        // invisible until the next byte arrives — up to `wal_sender_timeout/2`
        // (30 s) of latency on data already in hand, or a hang if the server has
        // stopped talking. Only when the queue is empty: a non-empty queue is the
        // zero-syscall fast path, and it is also the precondition the terminal
        // latch inside `drain_read_buffer` is written against.
        if pending.is_empty() {
            if let Some(err) = drain_read_buffer(read_buf, pending) {
                return Err(err);
            }
        }
        if let Some(payload) = pending.pop_front() {
            return Ok(payload);
        }

        // Ensure a large contiguous headroom BEFORE the read so one read() pulls as much as possible. Reserve only when free headroom drops below MIN_HEADROOM (not READ_CHUNK), so one 256 KiB reservation serves many reads instead of reallocating every iteration. `pending` is empty here (the pop above returned otherwise).
        if read_buf.capacity() - read_buf.len() < MIN_HEADROOM {
            read_buf.reserve(READ_CHUNK);
        }
        tokio::select! {
            biased;
            _ = cancellation_token.cancelled() => {
                // Check for remaining buffered data before returning
                if let Some(err) = drain_read_buffer(read_buf, pending) {
                    return Err(err);
                }
                if let Some(payload) = pending.pop_front() {
                    tracing::info!("Found buffered data after cancellation");
                    return Ok(payload);
                }
                return Err(ReplicationError::Cancelled(
                    "Operation cancelled".to_string(),
                ));
            }
            result = reader.read_buf(read_buf) => {
                let n = result.map_err(|e| {
                    ReplicationError::transient_connection(format!("read error: {e}"))
                })?;
                if n == 0 {
                    return Err(ReplicationError::transient_connection(
                        "connection closed by server".to_string(),
                    ));
                }

                // Drain all complete messages from the buffer
                if let Some(err) = drain_read_buffer(read_buf, pending) {
                    return Err(err);
                }

                // If we got messages, the next loop iteration will pop one
                if pending.is_empty() {
                    // Partial message — need more data, loop again
                    continue;
                }
            }
        }
    }
}

/// Parse and drain all complete PostgreSQL messages from `read_buf` into `pending`.
///
/// CopyData ('d') payloads are extracted zero-copy via `split_to().freeze().slice(5..)`.
/// Other message types (NoticeResponse, CopyDone, keepalive) are handled inline.
///
/// Returns `Some(error)` if an ErrorResponse is received during COPY mode,
/// indicating the server reported a protocol-level error.
fn drain_read_buffer(
    read_buf: &mut BytesMut,
    pending: &mut VecDeque<Bytes>,
) -> Option<ReplicationError> {
    let mut drained = 0;

    while read_buf.len() >= HEADER_LEN && drained < MAX_DRAIN_BATCH {
        let body_len = i32::from_be_bytes(read_buf[1..5].try_into().unwrap());
        if body_len < 4 {
            // The length field includes its own 4 bytes, so must be >= 4, a negative or too-small value indicates a corrupt/malicious message.
            return Some(ReplicationError::protocol(format!(
                "invalid message length {body_len} (must be >= 4)"
            )));
        }
        let body_len_usize = body_len as usize;
        if body_len_usize > MAX_MESSAGE_LEN {
            return Some(ReplicationError::protocol(format!(
                "message length {} exceeds maximum allowed {} bytes",
                body_len_usize, MAX_MESSAGE_LEN
            )));
        }
        let total_len = 1 + body_len_usize; // tag + body (body_len includes its own 4 bytes)

        if read_buf.len() < total_len {
            // Incomplete message — wait for more data
            break;
        }

        let tag = read_buf[0];

        match tag {
            b'd' => {
                // CopyData — the hot path
                // Zero-copy: split off the full message, freeze it, then slice past the header
                let frame = read_buf.split_to(total_len);
                let payload = frame.freeze().slice(HEADER_LEN..);
                pending.push_back(payload);
                drained += 1;
            }
            b'E' => {
                // ErrorResponse inside COPY mode — the server terminated the
                // stream. Classify on SQLSTATE so an invalidated slot surfaces
                // as a permanent error rather than a retryable protocol one.
                //
                // The frame is NOT consumed. That is what makes the terminal
                // sticky in both directions:
                //   - within a pass, anything already queued is delivered first
                //     (the `break` below), and the terminal is re-found once the
                //     queue drains;
                //   - across calls, every later `get_copy_data` re-finds it and
                //     reports the same reason, with no extra state.
                // Consuming it reported the error exactly once and destroyed the
                // evidence; the next call then awaited readability on a socket the
                // server was not going to write to again — a permanent 0%-CPU hang.
                // `start_replication` clears `read_buf`, so this cannot latch
                // across streams.
                if !pending.is_empty() {
                    break;
                }
                let fields = super::error::parse_error_fields(&read_buf[HEADER_LEN..total_len]);
                return Some(ReplicationError::from_sqlstate(
                    &fields.code,
                    format!("server error during replication: {fields}"),
                ));
            }
            b'c' => {
                // CopyDone — the server ended the replication stream.
                //
                // Previously this was skipped with only a debug log, leaving
                // `get_copy_data` with no exit condition: it looped back to await
                // a socket that would never produce another byte, so the consumer
                // hung. Reachable via `start_physical_replication` — a physical
                // walsender sends an unprompted CopyDone on a timeline switch
                // (`XLogSendPhysical`). A *logical* walsender only sends one in
                // reply to ours (`ProcessRepliesIfAny`); its shutdown path goes
                // through `WalSndDone` → CommandComplete → `proc_exit`, which the
                // existing `read() == 0` arm already handled. Report it as
                // transient so the stream layer reconnects — matching the libpq
                // backend, which latches the same condition in `copy_end`.
                //
                // Like 'E', the frame is left in `read_buf` so the terminal is
                // sticky across calls as well as within a pass. This one matters
                // most: unlike an EOF or a killed backend, a CopyDone leaves the
                // socket *open* (the server is waiting for our CopyDone in reply),
                // so there is no transport-level signal to re-derive the terminal
                // from. Consuming it made the very next `get_copy_data` block
                // forever at 0% CPU — measured against a real promoted server.
                if !pending.is_empty() {
                    break;
                }
                tracing::debug!("CopyDone received — server ended the replication stream");
                return Some(ReplicationError::transient_connection(
                    "replication stream ended by server (CopyDone)",
                ));
            }
            b'N' => {
                // NoticeResponse inside COPY
                let frame_data = read_buf.split_to(total_len);
                let fields = super::error::parse_error_fields(&frame_data[5..]);
                tracing::info!("Notice during replication: {}", fields);
            }
            _ => {
                // Unknown message type — skip it
                tracing::debug!(
                    "Skipping message type '{}' (0x{:02x}) in COPY mode",
                    tag as char,
                    tag
                );
                read_buf.advance(total_len);
            }
        }
    }

    None
}

/// Send a CopyData message containing the given payload.
///
/// Used for standby status updates and hot standby feedback.
pub async fn put_copy_data<W: AsyncWrite + Unpin>(
    writer: &mut W,
    data: &[u8],
) -> Result<(), ReplicationError> {
    let msg = wire::build_copy_data(data);
    wire::write_all(writer, &msg).await?;
    wire::flush(writer).await?;
    Ok(())
}

/// Drop a COPY terminal (`'c'` / `'E'`) left latched at the head of `read_buf`.
///
/// `drain_read_buffer` deliberately does not consume terminal frames — that is
/// what makes them sticky across `get_copy_data` calls. The latch must not
/// outlive the stream, though: the next command issued on this connection would
/// otherwise read the stale terminal as the first message of its own response.
/// Called before a new simple query goes out.
///
/// Only a well-formed leading terminal is dropped. Anything else is left alone,
/// so a genuine protocol desync still surfaces instead of being papered over.
pub(super) fn clear_latched_terminal(read_buf: &mut BytesMut) {
    use bytes::Buf;

    if read_buf.len() < HEADER_LEN || !matches!(read_buf[0], b'c' | b'E') {
        return;
    }
    let body_len = i32::from_be_bytes(read_buf[1..5].try_into().unwrap());
    if body_len < 4 {
        return;
    }
    let total_len = 1 + body_len as usize;
    if read_buf.len() >= total_len {
        read_buf.advance(total_len);
    }
}

/// Send a CopyDone message to end the COPY stream.
pub async fn send_copy_done<W: AsyncWrite + Unpin>(writer: &mut W) -> Result<(), ReplicationError> {
    let msg = wire::build_copy_done();
    wire::write_all(writer, &msg).await?;
    wire::flush(writer).await?;
    Ok(())
}

use bytes::Buf;

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    #[test]
    fn test_drain_single_copy_data() {
        let mut buf = BytesMut::new();
        let payload = b"hello";
        let body_len = (4 + payload.len()) as i32;
        buf.put_u8(b'd');
        buf.put_i32(body_len);
        buf.put_slice(payload);

        let mut pending = VecDeque::new();
        drain_read_buffer(&mut buf, &mut pending);

        assert_eq!(pending.len(), 1);
        assert_eq!(&pending[0][..], b"hello");
        assert!(buf.is_empty());
    }

    #[test]
    fn test_drain_multiple_messages() {
        let mut buf = BytesMut::new();

        // Message 1: CopyData "abc"
        buf.put_u8(b'd');
        buf.put_i32(4 + 3);
        buf.put_slice(b"abc");

        // Message 2: CopyData "defgh"
        buf.put_u8(b'd');
        buf.put_i32(4 + 5);
        buf.put_slice(b"defgh");

        let mut pending = VecDeque::new();
        drain_read_buffer(&mut buf, &mut pending);

        assert_eq!(pending.len(), 2);
        assert_eq!(&pending[0][..], b"abc");
        assert_eq!(&pending[1][..], b"defgh");
        assert!(buf.is_empty());
    }

    #[test]
    fn test_drain_partial_message() {
        let mut buf = BytesMut::new();

        // Complete message
        buf.put_u8(b'd');
        buf.put_i32(4 + 3);
        buf.put_slice(b"abc");

        // Incomplete message (header only, no payload)
        buf.put_u8(b'd');
        buf.put_i32(4 + 10); // claims 10 bytes payload
        buf.put_slice(b"part"); // only 4 bytes

        let mut pending = VecDeque::new();
        drain_read_buffer(&mut buf, &mut pending);

        assert_eq!(pending.len(), 1);
        assert_eq!(&pending[0][..], b"abc");
        // The incomplete message should remain in the buffer
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_drain_copy_done() {
        let mut buf = BytesMut::new();

        // CopyData
        buf.put_u8(b'd');
        buf.put_i32(4 + 3);
        buf.put_slice(b"abc");

        // CopyDone
        buf.put_u8(b'c');
        buf.put_i32(4);

        let mut pending = VecDeque::new();
        // Pass 1: the queued frame wins; the terminal is left in the buffer.
        assert!(drain_read_buffer(&mut buf, &mut pending).is_none());
        assert_eq!(pending.len(), 1);
        assert_eq!(&pending[0][..], b"abc");
        assert!(!buf.is_empty(), "CopyDone must stay latched in the buffer");

        // Pass 2, queue drained: the terminal surfaces. Sticky with no extra state.
        pending.clear();
        let err = drain_read_buffer(&mut buf, &mut pending)
            .expect("CopyDone must terminate once the queue is empty");
        assert!(
            matches!(err, ReplicationError::TransientConnection(_)),
            "{err:?}"
        );

        // Pass 3+: the terminal is NOT consumed, so every later call reports the
        // same reason. Consuming it made the next `get_copy_data` go back to
        // awaiting a socket the server had stopped writing to — a permanent hang.
        assert!(
            !buf.is_empty(),
            "CopyDone must stay latched after reporting"
        );
        for _ in 0..3 {
            let again = drain_read_buffer(&mut buf, &mut pending)
                .expect("terminal must be sticky across calls");
            assert!(
                matches!(again, ReplicationError::TransientConnection(_)),
                "{again:?}"
            );
        }
        assert!(pending.is_empty(), "a terminal must not queue data");
    }

    /// ErrorResponse is sticky for the same reason, and keeps its SQLSTATE on
    /// every repeat — the classification drives the reconnect decision.
    #[test]
    fn drain_error_response_is_sticky() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'E');
        let payload = b"C57P01\0Mterminating connection\0\0";
        buf.put_i32(4 + payload.len() as i32);
        buf.put_slice(payload);

        let mut pending = VecDeque::new();
        for _ in 0..3 {
            let err = drain_read_buffer(&mut buf, &mut pending).expect("ErrorResponse terminates");
            assert!(err.to_string().contains("57P01"), "{err}");
        }
        assert!(!buf.is_empty(), "ErrorResponse must stay latched");
    }

    /// The latch must not outlive the stream: a new command on the same
    /// connection would otherwise read the stale terminal as its own response.
    #[test]
    fn clear_latched_terminal_drops_only_a_terminal() {
        // CopyDone is dropped.
        let mut buf = BytesMut::new();
        buf.put_u8(b'c');
        buf.put_i32(4);
        clear_latched_terminal(&mut buf);
        assert!(buf.is_empty());

        // ErrorResponse is dropped.
        let mut buf = BytesMut::new();
        buf.put_u8(b'E');
        buf.put_i32(4 + 3);
        buf.put_slice(b"ab\0");
        clear_latched_terminal(&mut buf);
        assert!(buf.is_empty());

        // A CopyData frame is NOT dropped — a real desync must still surface.
        let mut buf = BytesMut::new();
        buf.put_u8(b'd');
        buf.put_i32(4 + 3);
        buf.put_slice(b"abc");
        clear_latched_terminal(&mut buf);
        assert_eq!(buf.len(), 8, "non-terminal data must be left alone");

        // An incomplete terminal is left alone rather than half-consumed.
        let mut buf = BytesMut::new();
        buf.put_u8(b'c');
        buf.put_i32(64);
        clear_latched_terminal(&mut buf);
        assert_eq!(buf.len(), 5);

        // A malformed terminal (length field below the 4-byte minimum) is left
        // alone rather than half-consumed — `drain_read_buffer` still has to be
        // the one that reports it as a protocol error.
        let mut buf = BytesMut::new();
        buf.put_u8(b'c');
        buf.put_i32(2);
        buf.put_slice(b"xx");
        clear_latched_terminal(&mut buf);
        assert_eq!(buf.len(), 7, "a bad length must not be trusted to advance");

        // Empty buffer is a no-op.
        let mut buf = BytesMut::new();
        clear_latched_terminal(&mut buf);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_drain_empty_buffer() {
        let mut buf = BytesMut::new();
        let mut pending = VecDeque::new();
        drain_read_buffer(&mut buf, &mut pending);
        assert!(pending.is_empty());
    }

    #[test]
    fn test_drain_header_only() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'd');
        buf.put_i32(4 + 100); // claims 100 byte payload, but we have none

        let mut pending = VecDeque::new();
        drain_read_buffer(&mut buf, &mut pending);
        assert!(pending.is_empty());
        assert_eq!(buf.len(), 5); // header remains
    }

    // === Additional drain_read_buffer tests ===

    #[test]
    fn test_drain_negative_body_len() {
        let mut buf = BytesMut::new();
        // A message with negative body_len (corrupt/malicious)
        buf.put_u8(b'd');
        buf.put_i32(-1); // negative length

        let mut pending = VecDeque::new();
        let result = drain_read_buffer(&mut buf, &mut pending);

        // Should return a protocol error, not loop forever
        assert!(result.is_some());
        let err = result.unwrap();
        assert!(
            err.to_string().contains("invalid message length"),
            "Expected invalid length error, got: {err}"
        );
        assert!(pending.is_empty());
    }

    #[test]
    fn test_drain_body_len_too_small() {
        let mut buf = BytesMut::new();
        // body_len = 3, which is < 4 (minimum valid)
        buf.put_u8(b'd');
        buf.put_i32(3);

        let mut pending = VecDeque::new();
        let result = drain_read_buffer(&mut buf, &mut pending);

        assert!(result.is_some());
        let err = result.unwrap();
        assert!(
            err.to_string().contains("invalid message length"),
            "Expected invalid length error, got: {err}"
        );
    }

    #[test]
    fn test_drain_exceeds_max_message_len() {
        let mut buf = BytesMut::new();
        // body_len exceeding MAX_MESSAGE_LEN (64 MiB)
        let huge_len: i32 = (MAX_MESSAGE_LEN as i32) + 1;
        buf.put_u8(b'd');
        buf.put_i32(huge_len);

        let mut pending = VecDeque::new();
        let result = drain_read_buffer(&mut buf, &mut pending);

        assert!(result.is_some());
        let err = result.unwrap();
        assert!(
            err.to_string().contains("exceeds maximum"),
            "Expected max length error, got: {err}"
        );
        assert!(pending.is_empty());
    }

    #[test]
    fn test_drain_error_response() {
        let mut buf = BytesMut::new();
        // Build a minimal ErrorResponse: 'E' + len + payload
        let payload = b"SFATAL\0C42P01\0Mrelation not found\0\0";
        let body_len = (4 + payload.len()) as i32;
        buf.put_u8(b'E');
        buf.put_i32(body_len);
        buf.put_slice(payload);

        let mut pending = VecDeque::new();
        let result = drain_read_buffer(&mut buf, &mut pending);

        // ErrorResponse should return an error, not be queued as data
        assert!(result.is_some());
        let err = result.unwrap();
        assert!(
            err.to_string().contains("server error during replication"),
            "Expected protocol error, got: {err}"
        );
        assert!(pending.is_empty());
    }

    #[test]
    fn test_drain_notice_response() {
        let mut buf = BytesMut::new();
        // NoticeResponse 'N': should be consumed but NOT queued
        let payload = b"SNOTICE\0C00000\0Mtest notice\0\0";
        let body_len = (4 + payload.len()) as i32;
        buf.put_u8(b'N');
        buf.put_i32(body_len);
        buf.put_slice(payload);

        let mut pending = VecDeque::new();
        drain_read_buffer(&mut buf, &mut pending);

        assert!(pending.is_empty()); // Notice not queued
        assert!(buf.is_empty()); // But consumed from buffer
    }

    #[test]
    fn test_drain_unknown_tag() {
        let mut buf = BytesMut::new();
        // Unknown tag 'X': should be skipped via advance()
        let payload = b"data";
        let body_len = (4 + payload.len()) as i32;
        buf.put_u8(b'X');
        buf.put_i32(body_len);
        buf.put_slice(payload);

        let mut pending = VecDeque::new();
        drain_read_buffer(&mut buf, &mut pending);

        assert!(pending.is_empty()); // Unknown not queued
        assert!(buf.is_empty()); // But consumed
    }

    #[test]
    fn test_drain_max_batch_limit() {
        let mut buf = BytesMut::new();

        // Insert MAX_DRAIN_BATCH + 10 CopyData messages
        for _ in 0..(MAX_DRAIN_BATCH + 10) {
            buf.put_u8(b'd');
            buf.put_i32(4 + 1); // 1 byte payload
            buf.put_u8(b'x');
        }

        let mut pending = VecDeque::new();
        drain_read_buffer(&mut buf, &mut pending);

        // Should have drained exactly MAX_DRAIN_BATCH messages
        assert_eq!(pending.len(), MAX_DRAIN_BATCH);
        // Remaining 10 messages should still be in the buffer
        assert!(!buf.is_empty());
        assert_eq!(buf.len(), 10 * 6); // 10 * (1 + 4 + 1) bytes
    }

    /// Notices are consumed inline and do not interrupt the drain; CopyDone does.
    /// (A conforming server never sends CopyData after CopyDone — it waits for the
    /// client's own CopyDone first — so the trailing frame here only exists to
    /// prove the drain stops at the terminal rather than reading past it.)
    #[test]
    fn test_drain_interleaved_types() {
        let mut buf = BytesMut::new();

        // CopyData "a"
        buf.put_u8(b'd');
        buf.put_i32(4 + 1);
        buf.put_u8(b'a');

        // NoticeResponse (consumed, not queued)
        let notice_payload = b"SINFO\0C00000\0Minfo\0\0";
        buf.put_u8(b'N');
        buf.put_i32((4 + notice_payload.len()) as i32);
        buf.put_slice(notice_payload);

        // CopyData "b"
        buf.put_u8(b'd');
        buf.put_i32(4 + 1);
        buf.put_u8(b'b');

        // CopyDone — terminal, stops the drain here
        buf.put_u8(b'c');
        buf.put_i32(4);

        // CopyData "c" — must NOT be read past the terminal
        buf.put_u8(b'd');
        buf.put_i32(4 + 1);
        buf.put_u8(b'c');

        let mut pending = VecDeque::new();
        assert!(drain_read_buffer(&mut buf, &mut pending).is_none());

        assert_eq!(pending.len(), 2, "drain must stop at CopyDone");
        assert_eq!(&pending[0][..], b"a");
        assert_eq!(&pending[1][..], b"b");
        assert!(
            !buf.is_empty(),
            "CopyDone and everything after it stay latched"
        );
    }

    // === Async tests ===

    #[tokio::test]
    async fn test_get_copy_data_returns_payload() {
        use tokio::io::AsyncWriteExt;
        let (mut client, mut server) = tokio::io::duplex(8192);
        let token = CancellationToken::new();

        tokio::spawn(async move {
            // Write a CopyData message
            let payload = b"test payload";
            let body_len = (4 + payload.len()) as i32;
            let mut msg = vec![b'd'];
            msg.extend_from_slice(&body_len.to_be_bytes());
            msg.extend_from_slice(payload);
            server.write_all(&msg).await.unwrap();
            server.flush().await.unwrap();
        });

        let mut read_buf = BytesMut::new();
        let mut pending = VecDeque::new();
        let result = get_copy_data(&mut client, &mut read_buf, &mut pending, &token).await;
        assert!(result.is_ok());
        assert_eq!(&result.unwrap()[..], b"test payload");
    }

    #[tokio::test]
    async fn test_get_copy_data_pre_queued() {
        let (mut client, _server) = tokio::io::duplex(8192);
        let token = CancellationToken::new();

        let mut read_buf = BytesMut::new();
        let mut pending = VecDeque::new();

        // Pre-load a message in pending queue
        pending.push_back(Bytes::from_static(b"pre-queued"));

        let result = get_copy_data(&mut client, &mut read_buf, &mut pending, &token).await;
        assert!(result.is_ok());
        assert_eq!(&result.unwrap()[..], b"pre-queued");
    }

    #[tokio::test]
    async fn test_get_copy_data_cancelled() {
        let (mut client, _server) = tokio::io::duplex(8192);
        let token = CancellationToken::new();

        // Cancel immediately
        token.cancel();

        let mut read_buf = BytesMut::new();
        let mut pending = VecDeque::new();

        let result = get_copy_data(&mut client, &mut read_buf, &mut pending, &token).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ReplicationError::Cancelled(_)));
    }

    #[tokio::test]
    async fn test_get_copy_data_cancelled_with_buffered_data() {
        // Cancelled, but a complete message is already sitting in read_buf → it
        // must be drained and returned rather than dropped.
        let (mut client, _server) = tokio::io::duplex(8192);
        let token = CancellationToken::new();
        token.cancel();

        let payload = b"buffered";
        let body_len = (4 + payload.len()) as i32;
        let mut read_buf = BytesMut::new();
        read_buf.extend_from_slice(b"d");
        read_buf.extend_from_slice(&body_len.to_be_bytes());
        read_buf.extend_from_slice(payload);

        let mut pending = VecDeque::new();
        let result = get_copy_data(&mut client, &mut read_buf, &mut pending, &token).await;
        assert_eq!(&result.unwrap()[..], b"buffered");
    }

    #[tokio::test]
    async fn test_get_copy_data_connection_closed() {
        // Server hangs up → read returns 0 → transient "connection closed" error.
        let (mut client, server) = tokio::io::duplex(8192);
        drop(server);
        let token = CancellationToken::new();
        let mut read_buf = BytesMut::new();
        let mut pending = VecDeque::new();
        let result = get_copy_data(&mut client, &mut read_buf, &mut pending, &token).await;
        let err = result.unwrap_err();
        assert!(
            format!("{err}").contains("closed"),
            "expected connection-closed error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_get_copy_data_server_error_response() {
        // An ErrorResponse ('E') during COPY surfaces as a protocol error.
        use tokio::io::AsyncWriteExt;
        let (mut client, mut server) = tokio::io::duplex(8192);
        let token = CancellationToken::new();
        tokio::spawn(async move {
            let fields = b"SERROR\0C28000\0Mnope\0\0";
            let body_len = (4 + fields.len()) as i32;
            let mut msg = vec![b'E'];
            msg.extend_from_slice(&body_len.to_be_bytes());
            msg.extend_from_slice(fields);
            server.write_all(&msg).await.unwrap();
            server.flush().await.unwrap();
        });
        let mut read_buf = BytesMut::new();
        let mut pending = VecDeque::new();
        let result = get_copy_data(&mut client, &mut read_buf, &mut pending, &token).await;
        assert!(result.is_err(), "server ErrorResponse should error");
    }

    #[tokio::test]
    async fn test_get_copy_data_partial_then_complete() {
        // First read delivers only part of a message → the loop must `continue`
        // and read again before returning the assembled payload.
        use tokio::io::AsyncWriteExt;
        let (mut client, mut server) = tokio::io::duplex(8192);
        let token = CancellationToken::new();
        tokio::spawn(async move {
            let payload = b"split payload";
            let body_len = (4 + payload.len()) as i32;
            let mut msg = vec![b'd'];
            msg.extend_from_slice(&body_len.to_be_bytes());
            msg.extend_from_slice(payload);
            server.write_all(&msg[..7]).await.unwrap();
            server.flush().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            server.write_all(&msg[7..]).await.unwrap();
            server.flush().await.unwrap();
        });
        let mut read_buf = BytesMut::new();
        let mut pending = VecDeque::new();
        let result = get_copy_data(&mut client, &mut read_buf, &mut pending, &token).await;
        assert_eq!(&result.unwrap()[..], b"split payload");
    }

    #[tokio::test]
    async fn test_put_copy_data_writes_framed_message() {
        use tokio::io::AsyncReadExt;
        let (mut client, mut server) = tokio::io::duplex(8192);

        let payload = b"hello world";
        put_copy_data(&mut client, payload).await.unwrap();

        // Read what was written and verify the frame
        let mut received = vec![0u8; 1024];
        let n = server.read(&mut received).await.unwrap();
        let received = &received[..n];

        assert_eq!(received[0], b'd'); // CopyData tag
        let len = i32::from_be_bytes(received[1..5].try_into().unwrap());
        assert_eq!(len as usize, 4 + payload.len());
        assert_eq!(&received[5..5 + payload.len()], payload);
    }

    #[tokio::test]
    async fn test_send_copy_done_writes_correct_bytes() {
        use tokio::io::AsyncReadExt;
        let (mut client, mut server) = tokio::io::duplex(8192);

        send_copy_done(&mut client).await.unwrap();

        let mut received = vec![0u8; 16];
        let n = server.read(&mut received).await.unwrap();
        let received = &received[..n];

        assert_eq!(received[0], b'c'); // CopyDone tag
        let len = i32::from_be_bytes(received[1..5].try_into().unwrap());
        assert_eq!(len, 4);
        assert_eq!(n, 5);
    }

    #[tokio::test]
    async fn test_put_copy_data_empty_payload() {
        use tokio::io::AsyncReadExt;
        let (mut client, mut server) = tokio::io::duplex(8192);

        put_copy_data(&mut client, &[]).await.unwrap();

        let mut received = vec![0u8; 16];
        let n = server.read(&mut received).await.unwrap();
        let received = &received[..n];

        assert_eq!(received[0], b'd');
        let len = i32::from_be_bytes(received[1..5].try_into().unwrap());
        assert_eq!(len, 4); // Just the length field, no payload
        assert_eq!(n, 5);
    }
}
