//! Streaming `COPY ... TO STDOUT` reader for ordinary (non-replication) queries.
//!
//! # Why this is not [`super::copy`]
//!
//! [`super::copy::get_copy_data`] serves the *replication* `CopyBoth` stream, and
//! two of its deliberate choices are wrong for a plain `COPY OUT`:
//!
//! 1. It reports `CopyDone ('c')` as
//!    [`ReplicationError::TransientConnection`] — for replication that means "the
//!    stream broke, reconnect", but for a `COPY OUT` it means **success, every row
//!    was delivered**. Detecting that by string-matching an error is not
//!    acceptable.
//! 2. Its catch-all arm silently discards unknown tags, which swallows the
//!    `CommandComplete ('C')` and `ReadyForQuery ('Z')` that PostgreSQL sends
//!    *after* `CopyDone`. Leaving those buffered desynchronises the connection:
//!    the next query would read the stale `'Z'` as its own response.
//!
//! So this module is a separate state machine over [`wire::read_message`], and
//! `copy.rs` is left untouched — which is also what keeps the WAL hot path out of
//! this change entirely.
//!
//! # Phases
//!
//! Phase 0 (issuing the `COPY` and consuming `CopyOutResponse ('H')`) is already
//! performed by [`super::query::simple_query`], which sets
//! [`NativeResultStatus::CopyOut`](super::result::NativeResultStatus::CopyOut) and
//! breaks *without* consuming `ReadyForQuery`, parking the transport at the first
//! `CopyData` frame. This module implements phases 1 and 2: the data loop and the
//! epilogue.
//!
//! The client never writes anything during a `COPY OUT` — in particular it must
//! not send `CopyDone`, which is only legal in `COPY IN` / `COPY BOTH`.

use bytes::{Bytes, BytesMut};
use tokio::io::AsyncRead;

use super::wire;
use crate::error::ReplicationError;
use crate::prelude::*;

/// One step of a `COPY OUT` stream.
#[derive(Debug)]
pub(super) enum CopyOutItem {
    /// A `CopyData` payload. Conventionally one row in TEXT mode, but the
    /// protocol does not guarantee it.
    Data(Bytes),
    /// The server sent `CopyDone` and the trailing `CommandComplete` /
    /// `ReadyForQuery` have been consumed. The connection is reusable.
    Done,
}

/// Read the next item of a `COPY OUT` stream.
///
/// `stream` must be parked immediately after the `CopyOutResponse ('H')` that
/// [`super::query::simple_query`] consumed.
///
/// A mid-stream `ErrorResponse` is surfaced as "server error during COPY OUT"
/// carrying the server's SQLSTATE. The libpq backend routes the same failure
/// through the terminal it shares with replication and words it "replication
/// stream terminated by server", so the prose is backend-dependent even though
/// the classification is not.
pub(super) async fn next_copy_out<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
) -> Result<CopyOutItem, ReplicationError> {
    loop {
        let msg = wire::read_message(stream, buf).await?;

        match msg.first().copied() {
            // CopyData — zero-copy slice past the tag and length.
            Some(b'd') => return Ok(CopyOutItem::Data(msg.slice(wire::HEADER_LEN..))),

            // CopyDone: the normal, successful end of a COPY OUT.
            Some(b'c') => {
                drain_epilogue(stream, buf).await?;
                return Ok(CopyOutItem::Done);
            }

            Some(b'E') => {
                let fields = super::error::parse_error_fields(&msg[wire::HEADER_LEN..]);
                let err = ReplicationError::from_sqlstate(
                    &fields.code,
                    format!("server error during COPY OUT: {fields}"),
                );
                let _ = super::query::drain_to_ready(stream, buf).await;
                return Err(err);
            }

            Some(b'N') => {
                tracing::info!(
                    "Server notice during COPY OUT: {}",
                    super::error::parse_error_fields(&msg[wire::HEADER_LEN..])
                );
            }

            // ParameterStatus and NotificationResponse are the only messages
            // PostgreSQL sanctions between CopyData frames: "It is possible for NoticeResponse and ParameterStatus messages to be interspersed between CopyData messages … Otherwise, any message type other than CopyData or CopyDone may be treated as  terminating copy-out mode."
            //
            // ('N' is handled above.) Everything else is a protocol violation, including `G`/`W`/`H` — entering a different COPY sub-protocol mid-stream — and `C`/`Z`, which would mean a second statement's response. Skipping those would silently swallow that statement's `CopyOutResponse` *and all of its rows*; the callers only ever issue single statements, so it is unreachable today, but silent data loss is not the failure mode to leave armed.
            Some(b'S' | b'A') => {}

            Some(tag) => {
                return Err(ReplicationError::protocol(format!(
                    "unexpected '{}' message inside COPY OUT",
                    tag as char
                )));
            }

            // Unreachable: `read_message` rejects `body_len < 4`, so every message it returns is at least 5 bytes. Erroring rather than looping keeps a future framing change from spinning here without consuming input.
            None => return Err(ReplicationError::protocol("empty message during COPY OUT")),
        }
    }
}

/// Consume the post-`CopyDone` epilogue: `CommandComplete ('C')` … `ReadyForQuery ('Z')`.
///
/// This **must** run, or the next query on this connection reads the stale `'Z'`
/// as its own first message.
///
/// An `ErrorResponse` here means the COPY itself failed after some rows were
/// already delivered (a mid-scan I/O or permission failure, for example). The
/// first such error is remembered and returned only once `ReadyForQuery` has been
/// reached, so the connection is still left on a message boundary.
///
/// If the read fails before `ReadyForQuery` arrives — a FATAL closes the socket
/// instead of sending one — the deferred error still wins over the read error, for
/// the same reason it does in [`next_copy_out`]: its SQLSTATE is what decides
/// whether the stream reconnects.
async fn drain_epilogue<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
) -> Result<(), ReplicationError> {
    let mut deferred: Option<ReplicationError> = None;

    loop {
        let msg = match wire::read_message(stream, buf).await {
            Ok(msg) => msg,
            Err(e) => return Err(deferred.unwrap_or(e)),
        };

        match msg.first().copied() {
            Some(b'Z') => break,
            Some(b'C') => {}
            Some(b'N') => {
                tracing::info!(
                    "Server notice after COPY OUT: {}",
                    super::error::parse_error_fields(&msg[wire::HEADER_LEN..])
                );
            }
            Some(b'E') => {
                let fields = super::error::parse_error_fields(&msg[wire::HEADER_LEN..]);
                deferred.get_or_insert_with(|| {
                    ReplicationError::from_sqlstate(
                        &fields.code,
                        format!("COPY OUT failed after CopyDone: {fields}"),
                    )
                });
            }
            // Same reasoning as the data loop: after `CopyDone` the only things
            // due are `CommandComplete`, `ReadyForQuery`, and the asynchronous
            // `N`/`S`/`A` trio. Anything else means the framing is off.
            Some(b'S' | b'A') => {}
            Some(tag) => {
                return Err(ReplicationError::protocol(format!(
                    "unexpected '{}' message after COPY OUT",
                    tag as char
                )))
            }
            None => return Err(ReplicationError::protocol("empty message after COPY OUT")),
        }
    }

    match deferred {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    /// Build a framed backend message: `[tag][len: i32 BE][payload]`.
    fn frame(tag: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(wire::HEADER_LEN + payload.len());
        out.push(tag);
        out.extend_from_slice(&((4 + payload.len()) as i32).to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    fn copy_data(payload: &[u8]) -> Vec<u8> {
        frame(b'd', payload)
    }

    fn copy_done() -> Vec<u8> {
        frame(b'c', b"")
    }

    fn command_complete() -> Vec<u8> {
        frame(b'C', b"COPY 2\0")
    }

    fn ready_for_query() -> Vec<u8> {
        frame(b'Z', b"I")
    }

    /// `ErrorResponse`: a sequence of `[field-type][value\0]` pairs, terminated by
    /// a zero byte. `C` is the SQLSTATE, `M` the message.
    fn error_response(code: &str, message: &str) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.push(b'C');
        payload.extend_from_slice(code.as_bytes());
        payload.push(0);
        payload.push(b'M');
        payload.extend_from_slice(message.as_bytes());
        payload.push(0);
        payload.push(0);
        frame(b'E', &payload)
    }

    fn notice(message: &str) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.push(b'M');
        payload.extend_from_slice(message.as_bytes());
        payload.push(0);
        payload.push(0);
        frame(b'N', &payload)
    }

    /// Script a fake server over an in-memory duplex.
    ///
    /// The server half is returned so the caller keeps it alive: dropping it would
    /// signal EOF, which several tests need to *not* happen.
    async fn scripted(script: Vec<u8>) -> (tokio::io::DuplexStream, tokio::io::DuplexStream) {
        let (client, mut server) = tokio::io::duplex(64 * 1024);
        server.write_all(&script).await.unwrap();
        server.flush().await.unwrap();
        (client, server)
    }

    async fn drive(script: Vec<u8>) -> (Vec<Bytes>, Result<(), ReplicationError>) {
        let (mut stream, _server) = scripted(script).await;
        let mut buf = BytesMut::new();
        let mut rows = Vec::new();

        loop {
            match next_copy_out(&mut stream, &mut buf).await {
                Ok(CopyOutItem::Data(b)) => rows.push(b),
                Ok(CopyOutItem::Done) => return (rows, Ok(())),
                Err(e) => return (rows, Err(e)),
            }
        }
    }

    #[tokio::test]
    async fn reads_data_frames_then_done() {
        let script = [
            copy_data(b"1\tAlice\n"),
            copy_data(b"2\tBob\n"),
            copy_done(),
            command_complete(),
            ready_for_query(),
        ]
        .concat();

        let (rows, result) = drive(script).await;
        result.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(&rows[0][..], b"1\tAlice\n");
        assert_eq!(&rows[1][..], b"2\tBob\n");
    }

    /// The regression test for the desync `copy.rs`'s catch-all arm leaves behind:
    /// after `Done`, the *next* message read must be whatever follows the
    /// epilogue, not the stale `CommandComplete`/`ReadyForQuery`.
    #[tokio::test]
    async fn epilogue_is_consumed_so_the_next_read_is_clean() {
        let sentinel = frame(b'T', b"sentinel");
        let script = [
            copy_data(b"x\n"),
            copy_done(),
            command_complete(),
            ready_for_query(),
            sentinel.clone(),
        ]
        .concat();

        let (mut stream, _server) = scripted(script).await;
        let mut buf = BytesMut::new();

        assert!(matches!(
            next_copy_out(&mut stream, &mut buf).await.unwrap(),
            CopyOutItem::Data(_)
        ));
        assert!(matches!(
            next_copy_out(&mut stream, &mut buf).await.unwrap(),
            CopyOutItem::Done
        ));

        let next = wire::read_message(&mut stream, &mut buf).await.unwrap();
        assert_eq!(next[0], b'T', "epilogue was not fully consumed");
    }

    #[tokio::test]
    async fn zero_row_table_completes() {
        let script = [copy_done(), command_complete(), ready_for_query()].concat();
        let (rows, result) = drive(script).await;
        result.unwrap();
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn error_response_maps_sqlstate() {
        let script = [
            error_response("42P01", "relation \"nope\" does not exist"),
            ready_for_query(),
        ]
        .concat();

        let (_, result) = drive(script).await;
        let err = result.unwrap_err();
        assert!(matches!(err, ReplicationError::Protocol(_)), "{err:?}");
        assert!(format!("{err}").contains("42P01"), "{err}");
    }

    #[tokio::test]
    async fn error_response_classified_as_transient() {
        // 57P01 = admin_shutdown, which the crate classifies as retryable.
        let script = [
            error_response("57P01", "terminating connection"),
            ready_for_query(),
        ]
        .concat();

        let (_, result) = drive(script).await;
        let err = result.unwrap_err();
        assert!(err.is_transient(), "{err:?}");
    }

    #[tokio::test]
    async fn error_response_drains_to_ready() {
        let sentinel = frame(b'T', b"after");
        let script = [
            error_response("42501", "permission denied"),
            ready_for_query(),
            sentinel,
        ]
        .concat();

        let (mut stream, _server) = scripted(script).await;
        let mut buf = BytesMut::new();

        assert!(next_copy_out(&mut stream, &mut buf).await.is_err());

        let next = wire::read_message(&mut stream, &mut buf).await.unwrap();
        assert_eq!(next[0], b'T', "did not drain to ReadyForQuery");
    }

    #[tokio::test]
    async fn notice_is_skipped() {
        let script = [
            notice("something happened"),
            copy_data(b"row\n"),
            copy_done(),
            command_complete(),
            ready_for_query(),
        ]
        .concat();

        let (rows, result) = drive(script).await;
        result.unwrap();
        assert_eq!(rows.len(), 1);
    }

    /// `ParameterStatus` is one of the two tags PostgreSQL may intersperse
    /// between `CopyData` frames, so it is skipped rather than rejected.
    #[tokio::test]
    async fn parameter_status_between_frames_is_skipped() {
        let script = [
            frame(b'S', b"param\0value\0"),
            copy_data(b"row\n"),
            frame(b'A', b"\0\0\0\0chan\0payload\0"),
            copy_done(),
            command_complete(),
            ready_for_query(),
        ]
        .concat();

        let (rows, result) = drive(script).await;
        result.unwrap();
        assert_eq!(rows.len(), 1);
    }

    /// Everything else terminates copy-out mode per the protocol spec, so
    /// skipping it would swallow a second statement's `CopyOutResponse` *and all
    /// of its rows*. Unreachable through the crate's own single-statement
    /// callers; rejected so it stays that way.
    #[tokio::test]
    async fn an_unsanctioned_tag_between_frames_is_rejected() {
        for tag in *b"CZTD" {
            let script = [copy_data(b"row\n"), frame(tag, b"x\0"), copy_done()].concat();

            let (rows, result) = drive(script).await;
            assert_eq!(rows.len(), 1);
            let err = result.unwrap_err();
            assert!(
                matches!(err, ReplicationError::Protocol(_)),
                "'{}' must not be silently skipped, got {err:?}",
                tag as char
            );
        }
    }

    #[tokio::test]
    async fn notice_in_epilogue_is_skipped() {
        let script = [
            copy_data(b"row\n"),
            copy_done(),
            notice("all done"),
            command_complete(),
            ready_for_query(),
        ]
        .concat();

        let (rows, result) = drive(script).await;
        result.unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn parameter_status_in_epilogue_is_skipped() {
        let script = [
            copy_done(),
            frame(b'S', b"k\0v\0"),
            command_complete(),
            ready_for_query(),
        ]
        .concat();

        let (_, result) = drive(script).await;
        result.unwrap();
    }

    #[tokio::test]
    async fn an_unsanctioned_tag_in_the_epilogue_is_rejected() {
        let script = [copy_done(), frame(b'd', b"row\n"), ready_for_query()].concat();

        let (_, result) = drive(script).await;
        let err = result.unwrap_err();
        assert!(matches!(err, ReplicationError::Protocol(_)), "{err:?}");
    }

    #[tokio::test]
    async fn error_in_epilogue_propagates_after_ready() {
        let script = [
            copy_data(b"partial\n"),
            copy_done(),
            error_response("58030", "could not read block"),
            ready_for_query(),
        ]
        .concat();

        let (rows, result) = drive(script).await;
        assert_eq!(rows.len(), 1, "rows before the failure are still delivered");
        let err = result.unwrap_err();
        assert!(format!("{err}").contains("after CopyDone"), "{err}");
    }

    #[tokio::test]
    async fn first_epilogue_error_wins() {
        let script = [
            copy_done(),
            error_response("58030", "first"),
            error_response("XX000", "second"),
            ready_for_query(),
        ]
        .concat();

        let (_, result) = drive(script).await;
        let err = result.unwrap_err();
        assert!(format!("{err}").contains("first"), "{err}");
    }

    #[tokio::test]
    async fn copy_in_response_is_rejected() {
        let (_, result) = drive(frame(b'G', b"\0\0\0")).await;
        let err = result.unwrap_err();
        assert!(matches!(err, ReplicationError::Protocol(_)), "{err:?}");
        assert!(format!("{err}").contains('G'), "{err}");
    }

    #[tokio::test]
    async fn copy_both_response_is_rejected() {
        let (_, result) = drive(frame(b'W', b"\0\0\0")).await;
        assert!(format!("{}", result.unwrap_err()).contains('W'));
    }

    #[tokio::test]
    async fn nested_copy_out_response_is_rejected() {
        let (_, result) = drive(frame(b'H', b"\0\0\0")).await;
        assert!(format!("{}", result.unwrap_err()).contains('H'));
    }

    #[tokio::test]
    async fn truncated_stream_errors() {
        // Drop the server half so the read hits EOF mid-stream.
        let (mut client, server) = tokio::io::duplex(1024);
        {
            let mut server = server;
            server.write_all(&copy_data(b"row\n")).await.unwrap();
            server.flush().await.unwrap();
        } // server dropped here -> EOF

        let mut buf = BytesMut::new();
        assert!(matches!(
            next_copy_out(&mut client, &mut buf).await.unwrap(),
            CopyOutItem::Data(_)
        ));
        assert!(
            next_copy_out(&mut client, &mut buf).await.is_err(),
            "EOF mid-COPY must be an error, not a silent Done"
        );
    }

    #[tokio::test]
    async fn truncated_epilogue_errors() {
        let (mut client, server) = tokio::io::duplex(1024);
        {
            let mut server = server;
            server.write_all(&copy_done()).await.unwrap();
            server.flush().await.unwrap();
        }

        let mut buf = BytesMut::new();
        assert!(
            next_copy_out(&mut client, &mut buf).await.is_err(),
            "EOF before ReadyForQuery must be an error"
        );
    }

    #[tokio::test]
    async fn data_payload_is_zero_copy_slice_of_the_frame() {
        let script = [copy_data(b"payload\n"), copy_done(), ready_for_query()].concat();
        let (mut stream, _server) = scripted(script).await;
        let mut buf = BytesMut::new();

        let CopyOutItem::Data(data) = next_copy_out(&mut stream, &mut buf).await.unwrap() else {
            panic!("expected data");
        };
        assert_eq!(&data[..], b"payload\n");
    }

    #[tokio::test]
    async fn multiple_rows_in_one_frame_are_passed_through() {
        // Framing is the caller's problem; this layer forwards payloads verbatim.
        let script = [
            copy_data(b"1\n2\n3\n"),
            copy_done(),
            command_complete(),
            ready_for_query(),
        ]
        .concat();

        let (rows, result) = drive(script).await;
        result.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(&rows[0][..], b"1\n2\n3\n");
    }

    /// A FATAL is followed by the server *closing*, not by `ReadyForQuery`, so the
    /// resync read fails. The SQLSTATE must survive that — it is what decides
    /// whether the stream reconnects, and `42501` is permanent while the read
    /// error ("connection closed by server") is transient. Letting the read error
    /// win would put a permanent failure into an endless reconnect loop.
    #[tokio::test]
    async fn a_fatal_without_ready_for_query_keeps_its_sqlstate() {
        let (mut client, server) = tokio::io::duplex(1024);
        {
            let mut server = server;
            server
                .write_all(
                    &[
                        copy_data(b"row\n"),
                        error_response("42501", "must be superuser"),
                    ]
                    .concat(),
                )
                .await
                .unwrap();
            server.flush().await.unwrap();
        } // dropped -> EOF instead of ReadyForQuery

        let mut buf = BytesMut::new();
        assert!(matches!(
            next_copy_out(&mut client, &mut buf).await.unwrap(),
            CopyOutItem::Data(_)
        ));

        let err = next_copy_out(&mut client, &mut buf).await.unwrap_err();
        assert!(format!("{err}").contains("42501"), "{err}");
        assert!(
            err.is_permanent(),
            "a permanent SQLSTATE must not be laundered into a retryable error: {err:?}"
        );
        assert!(!err.is_transient(), "{err:?}");
    }

    /// The same rule one phase later: an error after `CopyDone` is deferred until
    /// `ReadyForQuery`, and if the socket closes first the deferred error still
    /// wins over the EOF.
    #[tokio::test]
    async fn a_deferred_epilogue_error_survives_a_truncated_epilogue() {
        let (mut client, server) = tokio::io::duplex(1024);
        {
            let mut server = server;
            server
                .write_all(&[copy_done(), error_response("42501", "must be superuser")].concat())
                .await
                .unwrap();
            server.flush().await.unwrap();
        }

        let mut buf = BytesMut::new();
        let err = next_copy_out(&mut client, &mut buf).await.unwrap_err();
        assert!(format!("{err}").contains("42501"), "{err}");
        assert!(err.is_permanent(), "{err:?}");
    }

    /// A non-fatal error mid-COPY still resyncs to `ReadyForQuery`, so the
    /// connection is left on a message boundary and stays reusable.
    #[tokio::test]
    async fn an_error_with_ready_for_query_still_resyncs() {
        let sentinel = frame(b'T', b"next query");
        let script = [
            copy_data(b"row\n"),
            error_response("57014", "canceling statement"),
            ready_for_query(),
            sentinel,
        ]
        .concat();

        let (mut stream, _server) = scripted(script).await;
        let mut buf = BytesMut::new();

        assert!(matches!(
            next_copy_out(&mut stream, &mut buf).await.unwrap(),
            CopyOutItem::Data(_)
        ));
        let err = next_copy_out(&mut stream, &mut buf).await.unwrap_err();
        assert!(format!("{err}").contains("57014"), "{err}");

        let next = wire::read_message(&mut stream, &mut buf).await.unwrap();
        assert_eq!(next[0], b'T', "the epilogue must have been consumed");
    }
}
