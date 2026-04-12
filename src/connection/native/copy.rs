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

/// Minimum header size: 1 (tag) + 4 (length) = 5 bytes.
const HEADER_LEN: usize = 5;

/// Maximum allowed message body length (128 MiB), matching wire.rs.
const MAX_MESSAGE_LEN: usize = 128 * 1024 * 1024;

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
        // ── Fast path: return from pre-drained queue (no syscall) ──
        if let Some(payload) = pending.pop_front() {
            return Ok(payload);
        }

        // ── Slow path: read from transport, drain all complete messages ──
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
                // ErrorResponse inside COPY mode — return as a protocol error
                let frame = read_buf.split_to(total_len);
                let fields = super::error::parse_error_fields(&frame[5..]);
                return Some(ReplicationError::protocol(format!(
                    "server error during replication: {fields}"
                )));
            }
            b'c' => {
                // CopyDone — replication stream ended
                read_buf.advance(total_len);
                tracing::debug!("CopyDone received");
                // Don't break — there might be more messages after CopyDone
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
        drain_read_buffer(&mut buf, &mut pending);

        assert_eq!(pending.len(), 1);
        assert_eq!(&pending[0][..], b"abc");
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

        // CopyDone (consumed, not queued)
        buf.put_u8(b'c');
        buf.put_i32(4);

        // CopyData "c"
        buf.put_u8(b'd');
        buf.put_i32(4 + 1);
        buf.put_u8(b'c');

        let mut pending = VecDeque::new();
        drain_read_buffer(&mut buf, &mut pending);

        assert_eq!(pending.len(), 3); // 3 CopyData messages
        assert_eq!(&pending[0][..], b"a");
        assert_eq!(&pending[1][..], b"b");
        assert_eq!(&pending[2][..], b"c");
        assert!(buf.is_empty());
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
