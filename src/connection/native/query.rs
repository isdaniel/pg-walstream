//! Simple query protocol for PostgreSQL replication commands.
//!
//! Sends a `'Q'` (Query) message and collects the response sequence:
//! `RowDescription ('T') → DataRow ('D')* → CommandComplete ('C') → ReadyForQuery ('Z')`
//! or `ErrorResponse ('E')` or `CopyBothResponse ('W')`.

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite};

use super::result::{NativePgResult, NativeResultStatus};
use super::wire;
use crate::error::ReplicationError;

/// Execute a simple query and return the result.
///
/// This handles the standard simple-query flow used by replication commands:
/// IDENTIFY_SYSTEM, CREATE_REPLICATION_SLOT, DROP_REPLICATION_SLOT, etc.
///
/// For `START_REPLICATION`, the response ends with `CopyBothResponse ('W')`
/// instead of `CommandComplete + ReadyForQuery`, which signals that the
/// connection has entered COPY mode.
pub async fn simple_query<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
    sql: &str,
) -> Result<NativePgResult, ReplicationError> {
    let query_msg = wire::build_query_message(sql);
    wire::write_all(stream, &query_msg).await?;
    wire::flush(stream).await?;

    let mut result = NativePgResult::new();

    loop {
        let msg = wire::read_message(stream, buf).await?;
        if msg.is_empty() {
            continue;
        }

        match msg[0] {
            b'T' => {
                // RowDescription — parse column metadata
                result.parse_row_description(&msg[5..]);
            }
            b'D' => {
                // DataRow — parse row values
                result.parse_data_row(&msg[5..]);
            }
            b'C' => {
                // CommandComplete — query finished successfully
                if result.status == NativeResultStatus::Empty {
                    result.status = NativeResultStatus::CommandOk;
                }
            }
            b'Z' => {
                // ReadyForQuery — done
                break;
            }
            b'W' => {
                // CopyBothResponse — entering replication COPY mode
                result.status = NativeResultStatus::CopyBoth;
                break;
            }
            b'H' => {
                // CopyOutResponse — for physical replication / base backup
                result.status = NativeResultStatus::CopyOut;
                break;
            }
            b'E' => {
                // ErrorResponse
                let fields = super::error::parse_error_fields(&msg[5..]);
                result.status = NativeResultStatus::FatalError;
                result.error_code = Some(fields.code.clone());
                result.error_msg = Some(format!("{}", fields));
                // After an error, the server sends ReadyForQuery
                // We need to consume it
            }
            b'N' => {
                // NoticeResponse — log and continue
                let fields = super::error::parse_error_fields(&msg[5..]);
                tracing::info!("Server notice: {}", fields);
            }
            _ => {
                tracing::debug!("Skipping message type '{}' during query", msg[0] as char);
            }
        }
    }

    Ok(result)
}

/// Largest payload put in a single CopyData frame.
///
/// A backup manifest is a few hundred KiB; the protocol would allow one frame,
/// but chunking bounds the write buffer and keeps each message far below the
/// server's 1 GiB message limit.
const COPY_IN_CHUNK: usize = 64 * 1024;

/// Execute a simple query that answers with `CopyInResponse ('G')`, stream
/// `payload` into it, and collect the final result.
///
/// The only such replication command is `UPLOAD_MANIFEST`. The flow is
/// `Query → CopyInResponse → CopyData* → CopyDone → CommandComplete →
/// ReadyForQuery`, with `ErrorResponse` possible at either stage: the server
/// rejects a malformed manifest only after the whole CopyIn completes, so the
/// payload is always sent before the verdict is read.
pub async fn simple_query_copy_in<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
    sql: &str,
    payload: &[u8],
) -> Result<NativePgResult, ReplicationError> {
    let query_msg = wire::build_query_message(sql);
    wire::write_all(stream, &query_msg).await?;
    wire::flush(stream).await?;

    let mut result = NativePgResult::new();

    // Phase 1: wait for the server to open the CopyIn.
    loop {
        let msg = wire::read_message(stream, buf).await?;
        if msg.is_empty() {
            continue;
        }
        match msg[0] {
            b'G' => break, // CopyInResponse — the server is ready for data
            b'E' => {
                let fields = super::error::parse_error_fields(&msg[5..]);
                result.status = NativeResultStatus::FatalError;
                result.error_code = Some(fields.code.clone());
                result.error_msg = Some(format!("{fields}"));
                // The command never entered CopyIn; drain to ReadyForQuery so
                // the connection stays usable.
                drain_to_ready(stream, buf).await?;
                return Ok(result);
            }
            b'N' => {
                let fields = super::error::parse_error_fields(&msg[5..]);
                tracing::info!("Server notice: {fields}");
            }
            b'Z' => {
                return Err(ReplicationError::protocol(format!(
                    "{sql} completed without entering CopyIn mode"
                )));
            }
            other => {
                tracing::debug!("Skipping message type '{}' before CopyIn", other as char);
            }
        }
    }

    // Phase 2: stream the payload, then close the copy.
    for chunk in payload.chunks(COPY_IN_CHUNK) {
        let msg = wire::build_copy_data(chunk);
        wire::write_all(stream, &msg).await?;
    }
    let done = wire::build_copy_done();
    wire::write_all(stream, &done).await?;
    wire::flush(stream).await?;

    // Phase 3: read the verdict.
    loop {
        let msg = wire::read_message(stream, buf).await?;
        if msg.is_empty() {
            continue;
        }
        match msg[0] {
            b'C' => {
                if result.status == NativeResultStatus::Empty {
                    result.status = NativeResultStatus::CommandOk;
                }
            }
            b'Z' => break,
            b'E' => {
                let fields = super::error::parse_error_fields(&msg[5..]);
                result.status = NativeResultStatus::FatalError;
                result.error_code = Some(fields.code.clone());
                result.error_msg = Some(format!("{fields}"));
            }
            b'N' => {
                let fields = super::error::parse_error_fields(&msg[5..]);
                tracing::info!("Server notice: {fields}");
            }
            other => {
                tracing::debug!("Skipping message type '{}' after CopyIn", other as char);
            }
        }
    }

    Ok(result)
}

/// Consume messages until `ReadyForQuery ('Z')`.
async fn drain_to_ready<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
) -> Result<(), ReplicationError> {
    loop {
        let msg = wire::read_message(stream, buf).await?;
        if msg.first() == Some(&b'Z') {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn test_result_status_variants() {
        // Just verify the enum values exist and match expectations
        assert_ne!(NativeResultStatus::CommandOk, NativeResultStatus::TuplesOk);
        assert_ne!(NativeResultStatus::CopyBoth, NativeResultStatus::FatalError);
    }

    /// Helper: build a CommandComplete message ('C' + len + tag\0)
    fn build_command_complete(tag: &str) -> Vec<u8> {
        let mut payload = tag.as_bytes().to_vec();
        payload.push(0);
        let len = (4 + payload.len()) as i32;
        let mut msg = vec![b'C'];
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(&payload);
        msg
    }

    /// Helper: build a ReadyForQuery message ('Z' + len(5) + status)
    fn build_ready_for_query(status: u8) -> Vec<u8> {
        vec![b'Z', 0, 0, 0, 5, status]
    }

    /// Helper: build a CopyBothResponse message ('W' + len)
    fn build_copy_both_response() -> Vec<u8> {
        // Minimal: W + len(7) + format(1 byte) + ncols(2 bytes)
        let mut msg = vec![b'W'];
        msg.extend_from_slice(&7i32.to_be_bytes());
        msg.push(0); // overall format (text)
        msg.extend_from_slice(&0i16.to_be_bytes()); // 0 columns
        msg
    }

    /// Helper: build a CopyOutResponse message ('H' + len)
    fn build_copy_out_response() -> Vec<u8> {
        let mut msg = vec![b'H'];
        msg.extend_from_slice(&7i32.to_be_bytes());
        msg.push(0); // format
        msg.extend_from_slice(&0i16.to_be_bytes()); // 0 columns
        msg
    }

    /// Helper: build an ErrorResponse message
    fn build_error_response(severity: &str, code: &str, message: &str) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.push(b'S');
        payload.extend_from_slice(severity.as_bytes());
        payload.push(0);
        payload.push(b'C');
        payload.extend_from_slice(code.as_bytes());
        payload.push(0);
        payload.push(b'M');
        payload.extend_from_slice(message.as_bytes());
        payload.push(0);
        payload.push(0); // terminator

        let len = (4 + payload.len()) as i32;
        let mut msg = vec![b'E'];
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(&payload);
        msg
    }

    /// Helper: build a RowDescription message ('T' + len + nfields + field_descriptors)
    fn build_row_description(names: &[&str]) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(names.len() as i16).to_be_bytes());

        for name in names {
            payload.extend_from_slice(name.as_bytes());
            payload.push(0); // null-terminated name
            payload.extend_from_slice(&0i32.to_be_bytes()); // table OID
            payload.extend_from_slice(&0i16.to_be_bytes()); // column number
            payload.extend_from_slice(&25i32.to_be_bytes()); // type OID (text)
            payload.extend_from_slice(&(-1i16).to_be_bytes()); // type size
            payload.extend_from_slice(&0i32.to_be_bytes()); // type modifier
            payload.extend_from_slice(&0i16.to_be_bytes()); // format code (text)
        }

        let len = (4 + payload.len()) as i32;
        let mut msg = vec![b'T'];
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(&payload);
        msg
    }

    /// Helper: build a DataRow message ('D' + len + ncols + column values)
    fn build_data_row(values: &[&str]) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(values.len() as i16).to_be_bytes());

        for val in values {
            let bytes = val.as_bytes();
            payload.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            payload.extend_from_slice(bytes);
        }

        let len = (4 + payload.len()) as i32;
        let mut msg = vec![b'D'];
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(&payload);
        msg
    }

    /// Helper: build a CopyInResponse message ('G' + len + format + ncols)
    fn build_copy_in_response() -> Vec<u8> {
        let mut msg = vec![b'G'];
        msg.extend_from_slice(&7i32.to_be_bytes());
        msg.push(0); // overall format (text)
        msg.extend_from_slice(&0i16.to_be_bytes()); // 0 columns
        msg
    }

    /// Read everything the client sends until it goes quiet, so a test can
    /// assert on the CopyData/CopyDoneframing it produced.
    async fn drain_client<S: AsyncRead + AsyncWrite + Unpin>(
        server: &mut S,
        upto: usize,
    ) -> Vec<u8> {
        let mut got = Vec::new();
        let mut chunk = vec![0u8; 8192];
        while got.len() < upto {
            match server.read(&mut chunk).await {
                Ok(0) => break,
                Ok(n) => got.extend_from_slice(&chunk[..n]),
                Err(_) => break,
            }
        }
        got
    }

    /// Happy path: Query -> CopyInResponse -> CopyData -> CopyDone ->
    /// CommandComplete -> ReadyForQuery.
    #[tokio::test]
    async fn test_simple_query_copy_in_success() {
        let (mut client, mut server) = tokio::io::duplex(65536);

        let handle = tokio::spawn(async move {
            // Query message, then open the CopyIn.
            let mut discard = vec![0u8; 1024];
            let _ = server.read(&mut discard).await;
            server.write_all(&build_copy_in_response()).await.unwrap();
            server.flush().await.unwrap();

            // 'd' + len(4+7) + "payload", then 'c' + len(4).
            let sent = drain_client(&mut server, 5 + 7 + 5).await;

            server
                .write_all(&build_command_complete("UPLOAD_MANIFEST"))
                .await
                .unwrap();
            server
                .write_all(&build_ready_for_query(b'I'))
                .await
                .unwrap();
            server.flush().await.unwrap();
            sent
        });

        let mut buf = BytesMut::new();
        let result = simple_query_copy_in(&mut client, &mut buf, "UPLOAD_MANIFEST", b"payload")
            .await
            .unwrap();
        assert_eq!(result.status(), &NativeResultStatus::CommandOk);
        assert!(result.is_ok());

        let sent = handle.await.unwrap();
        // One CopyData frame carrying the payload, then CopyDone.
        assert_eq!(sent[0], b'd');
        assert_eq!(i32::from_be_bytes(sent[1..5].try_into().unwrap()), 4 + 7);
        assert_eq!(&sent[5..12], b"payload");
        assert_eq!(sent[12], b'c');
        assert_eq!(i32::from_be_bytes(sent[13..17].try_into().unwrap()), 4);
    }

    /// A payload larger than COPY_IN_CHUNK is split across frames, and every
    /// byte still arrives exactly once.
    #[tokio::test]
    async fn test_simple_query_copy_in_chunks_large_payload() {
        let payload = vec![b'x'; COPY_IN_CHUNK + 100];
        let expect_bytes = 2 * 5 + payload.len() + 5; // two CopyData headers + body + CopyDone
        let (mut client, mut server) = tokio::io::duplex(4 * 1024 * 1024);

        let handle = tokio::spawn(async move {
            let mut discard = vec![0u8; 1024];
            let _ = server.read(&mut discard).await;
            server.write_all(&build_copy_in_response()).await.unwrap();
            server.flush().await.unwrap();

            let sent = drain_client(&mut server, expect_bytes).await;

            server
                .write_all(&build_command_complete("UPLOAD_MANIFEST"))
                .await
                .unwrap();
            server
                .write_all(&build_ready_for_query(b'I'))
                .await
                .unwrap();
            server.flush().await.unwrap();
            sent
        });

        let mut buf = BytesMut::new();
        let result = simple_query_copy_in(&mut client, &mut buf, "UPLOAD_MANIFEST", &payload)
            .await
            .unwrap();
        assert!(result.is_ok());

        let sent = handle.await.unwrap();
        let first = i32::from_be_bytes(sent[1..5].try_into().unwrap()) as usize - 4;
        assert_eq!(first, COPY_IN_CHUNK, "first frame is a full chunk");
        let second_at = 5 + first;
        assert_eq!(sent[second_at], b'd');
        let second =
            i32::from_be_bytes(sent[second_at + 1..second_at + 5].try_into().unwrap()) as usize - 4;
        assert_eq!(
            first + second,
            payload.len(),
            "every byte sent exactly once"
        );
        assert_eq!(sent[second_at + 5 + second], b'c', "CopyDone follows");
    }

    /// The server rejects the command before opening the CopyIn (e.g.
    /// UPLOAD_MANIFEST on PostgreSQL 16). The error must surface with its
    /// SQLSTATE and the connection must be drained to ReadyForQuery.
    #[tokio::test]
    async fn test_simple_query_copy_in_error_before_copy_starts() {
        let (mut client, mut server) = tokio::io::duplex(8192);

        tokio::spawn(async move {
            let mut discard = vec![0u8; 1024];
            let _ = server.read(&mut discard).await;
            server
                .write_all(&build_error_response(
                    "ERROR",
                    "42601",
                    "syntax error at or near \"UPLOAD_MANIFEST\"",
                ))
                .await
                .unwrap();
            server
                .write_all(&build_ready_for_query(b'I'))
                .await
                .unwrap();
            server.flush().await.unwrap();
        });

        let mut buf = BytesMut::new();
        let result = simple_query_copy_in(&mut client, &mut buf, "UPLOAD_MANIFEST", b"x")
            .await
            .unwrap();
        assert_eq!(result.status(), &NativeResultStatus::FatalError);
        assert_eq!(result.error_sqlstate(), "42601");
        assert!(result.error_message().unwrap().contains("syntax error"));
    }

    /// The manifest is only validated once the CopyIn completes, so the whole
    /// payload goes out before the rejection arrives.
    #[tokio::test]
    async fn test_simple_query_copy_in_error_after_copy_completes() {
        let (mut client, mut server) = tokio::io::duplex(8192);

        tokio::spawn(async move {
            let mut discard = vec![0u8; 1024];
            let _ = server.read(&mut discard).await;
            server.write_all(&build_copy_in_response()).await.unwrap();
            server.flush().await.unwrap();

            let _ = drain_client(&mut server, 5 + 3 + 5).await;

            server
                .write_all(&build_error_response(
                    "ERROR",
                    "XX000",
                    "could not parse backup manifest",
                ))
                .await
                .unwrap();
            server
                .write_all(&build_ready_for_query(b'I'))
                .await
                .unwrap();
            server.flush().await.unwrap();
        });

        let mut buf = BytesMut::new();
        let result = simple_query_copy_in(&mut client, &mut buf, "UPLOAD_MANIFEST", b"bad")
            .await
            .unwrap();
        assert_eq!(result.status(), &NativeResultStatus::FatalError);
        assert_eq!(result.error_sqlstate(), "XX000");
        assert!(result.error_message().unwrap().contains("manifest"));
    }

    /// A NoticeResponse in either phase is logged and skipped, not mistaken for
    /// the CopyIn opening or the final verdict.
    #[tokio::test]
    async fn test_simple_query_copy_in_skips_notices() {
        let (mut client, mut server) = tokio::io::duplex(8192);

        tokio::spawn(async move {
            let mut discard = vec![0u8; 1024];
            let _ = server.read(&mut discard).await;
            let mut notice = build_error_response("NOTICE", "00000", "heads up");
            notice[0] = b'N';
            server.write_all(&notice).await.unwrap();
            server.write_all(&build_copy_in_response()).await.unwrap();
            server.flush().await.unwrap();

            let _ = drain_client(&mut server, 5 + 1 + 5).await;

            server.write_all(&notice).await.unwrap();
            server
                .write_all(&build_command_complete("UPLOAD_MANIFEST"))
                .await
                .unwrap();
            server
                .write_all(&build_ready_for_query(b'I'))
                .await
                .unwrap();
            server.flush().await.unwrap();
        });

        let mut buf = BytesMut::new();
        let result = simple_query_copy_in(&mut client, &mut buf, "UPLOAD_MANIFEST", b"m")
            .await
            .unwrap();
        assert!(result.is_ok());
    }

    /// A server that answers with ReadyForQuery instead of opening the CopyIn is
    /// a protocol violation, not a silent success.
    #[tokio::test]
    async fn test_simple_query_copy_in_never_enters_copy_mode() {
        let (mut client, mut server) = tokio::io::duplex(8192);

        tokio::spawn(async move {
            let mut discard = vec![0u8; 1024];
            let _ = server.read(&mut discard).await;
            server
                .write_all(&build_ready_for_query(b'I'))
                .await
                .unwrap();
            server.flush().await.unwrap();
        });

        let mut buf = BytesMut::new();
        let err = simple_query_copy_in(&mut client, &mut buf, "UPLOAD_MANIFEST", b"m")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("without entering CopyIn"), "{err}");
    }

    #[tokio::test]
    async fn test_simple_query_command_ok() {
        let (mut client, mut server) = tokio::io::duplex(8192);

        tokio::spawn(async move {
            // Read and discard the Query message
            let mut discard = vec![0u8; 1024];
            let _ = server.read(&mut discard).await;

            // Send CommandComplete + ReadyForQuery
            server
                .write_all(&build_command_complete("SELECT 0"))
                .await
                .unwrap();
            server
                .write_all(&build_ready_for_query(b'I'))
                .await
                .unwrap();
            server.flush().await.unwrap();
        });

        let mut buf = BytesMut::new();
        let result = simple_query(&mut client, &mut buf, "SELECT 1")
            .await
            .unwrap();
        assert_eq!(result.status(), &NativeResultStatus::CommandOk);
    }

    #[tokio::test]
    async fn test_simple_query_with_rows() {
        let (mut client, mut server) = tokio::io::duplex(8192);

        tokio::spawn(async move {
            let mut discard = vec![0u8; 1024];
            let _ = server.read(&mut discard).await;

            // RowDescription with 2 columns
            server
                .write_all(&build_row_description(&["systemid", "timeline"]))
                .await
                .unwrap();
            // Two DataRows
            server
                .write_all(&build_data_row(&["12345", "1"]))
                .await
                .unwrap();
            server
                .write_all(&build_data_row(&["67890", "2"]))
                .await
                .unwrap();
            // CommandComplete + ReadyForQuery
            server
                .write_all(&build_command_complete("SELECT 2"))
                .await
                .unwrap();
            server
                .write_all(&build_ready_for_query(b'I'))
                .await
                .unwrap();
            server.flush().await.unwrap();
        });

        let mut buf = BytesMut::new();
        let result = simple_query(&mut client, &mut buf, "IDENTIFY_SYSTEM")
            .await
            .unwrap();
        assert_eq!(result.ntuples(), 2);
        assert_eq!(result.nfields(), 2);
        assert_eq!(result.get_value(0, 0), Some("12345".to_string()));
        assert_eq!(result.get_value(0, 1), Some("1".to_string()));
        assert_eq!(result.get_value(1, 0), Some("67890".to_string()));
    }

    #[tokio::test]
    async fn test_simple_query_error_response() {
        let (mut client, mut server) = tokio::io::duplex(8192);

        tokio::spawn(async move {
            let mut discard = vec![0u8; 1024];
            let _ = server.read(&mut discard).await;

            // ErrorResponse + ReadyForQuery
            server
                .write_all(&build_error_response("ERROR", "42601", "syntax error"))
                .await
                .unwrap();
            server
                .write_all(&build_ready_for_query(b'I'))
                .await
                .unwrap();
            server.flush().await.unwrap();
        });

        let mut buf = BytesMut::new();
        let result = simple_query(&mut client, &mut buf, "INVALID SQL")
            .await
            .unwrap();
        assert_eq!(result.status(), &NativeResultStatus::FatalError);
        assert!(result.error_message().is_some());
        let err_msg = result.error_message().unwrap();
        assert!(err_msg.contains("syntax error"), "Got: {err_msg}");
    }

    #[tokio::test]
    async fn test_simple_query_copy_both() {
        let (mut client, mut server) = tokio::io::duplex(8192);

        tokio::spawn(async move {
            let mut discard = vec![0u8; 1024];
            let _ = server.read(&mut discard).await;

            server.write_all(&build_copy_both_response()).await.unwrap();
            server.flush().await.unwrap();
        });

        let mut buf = BytesMut::new();
        let result = simple_query(
            &mut client,
            &mut buf,
            "START_REPLICATION SLOT test LOGICAL 0/0",
        )
        .await
        .unwrap();
        assert_eq!(result.status(), &NativeResultStatus::CopyBoth);
    }

    #[tokio::test]
    async fn test_simple_query_copy_out() {
        let (mut client, mut server) = tokio::io::duplex(8192);

        tokio::spawn(async move {
            let mut discard = vec![0u8; 1024];
            let _ = server.read(&mut discard).await;

            server.write_all(&build_copy_out_response()).await.unwrap();
            server.flush().await.unwrap();
        });

        let mut buf = BytesMut::new();
        let result = simple_query(&mut client, &mut buf, "BASE_BACKUP")
            .await
            .unwrap();
        assert_eq!(result.status(), &NativeResultStatus::CopyOut);
    }
}
