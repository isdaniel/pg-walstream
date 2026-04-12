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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;
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
