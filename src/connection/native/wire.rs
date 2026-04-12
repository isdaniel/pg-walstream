//! PostgreSQL wire protocol message framing.
//!
//! All PostgreSQL messages (except the startup message) follow the format:
//! `[tag: u8][length: i32][payload: length-4 bytes]`

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::ReplicationError;

/// Minimum message size: 1 (tag) + 4 (length) = 5 bytes header.
const HEADER_LEN: usize = 5;

/// Read a single complete PostgreSQL backend message from the transport.
///
/// Returns the raw message bytes INCLUDING the tag byte and length.
/// The caller can inspect `msg[0]` for the tag and `msg[5..]` for the payload.
///
/// This function is cancel-safe: it only advances `buf` when a
/// complete message is available (`split_to`).
pub async fn read_message<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut BytesMut,
) -> Result<Bytes, ReplicationError> {
    loop {
        // Try to parse a complete message from buffered data
        if buf.len() >= HEADER_LEN {
            let body_len = i32::from_be_bytes(buf[1..5].try_into().unwrap()) as usize;

            // The length field includes its own 4 bytes, so body_len must be >= 4, value < 4 indicates a corrupt or malicious message.
            if body_len < 4 {
                return Err(ReplicationError::protocol(format!(
                    "invalid message length {body_len} (must be >= 4)"
                )));
            }

            let total_len = 1 + body_len; // tag + body_len (body_len includes its own 4 bytes)

            if buf.len() >= total_len {
                // Complete message available — split it off as frozen Bytes (zero-copy)
                return Ok(buf.split_to(total_len).freeze());
            }

            // Ensure capacity for the rest of the message
            buf.reserve(total_len - buf.len());
        }

        // Need more data from the network
        let n = reader
            .read_buf(buf)
            .await
            .map_err(|e| ReplicationError::transient_connection(format!("read error: {e}")))?;
        if n == 0 {
            return Err(ReplicationError::transient_connection(
                "connection closed by server".to_string(),
            ));
        }
    }
}

/// Read a single-byte response (used for SSLRequest response).
pub async fn read_byte<R: AsyncRead + Unpin>(reader: &mut R) -> Result<u8, ReplicationError> {
    let mut buf = [0u8; 1];
    reader
        .read_exact(&mut buf)
        .await
        .map_err(|e| ReplicationError::transient_connection(format!("read_byte error: {e}")))?;
    Ok(buf[0])
}

/// Write raw bytes to the transport.
pub async fn write_all<W: AsyncWrite + Unpin>(
    writer: &mut W,
    data: &[u8],
) -> Result<(), ReplicationError> {
    writer
        .write_all(data)
        .await
        .map_err(|e| ReplicationError::transient_connection(format!("write error: {e}")))?;
    Ok(())
}

/// Flush the transport.
pub async fn flush<W: AsyncWrite + Unpin>(writer: &mut W) -> Result<(), ReplicationError> {
    writer
        .flush()
        .await
        .map_err(|e| ReplicationError::transient_connection(format!("flush error: {e}")))?;
    Ok(())
}

/// Build a startup message (no tag byte, starts with length).
///
/// Format: `[Int32: length][Int32: protocol version 3.0][key\0value\0 pairs][\0]`
pub fn build_startup_message(params: &[(&str, &str)]) -> BytesMut {
    let mut body = BytesMut::new();
    body.put_i32(196608); // protocol version 3.0 = 3 << 16 | 0
    for (key, val) in params {
        body.put_slice(key.as_bytes());
        body.put_u8(0);
        body.put_slice(val.as_bytes());
        body.put_u8(0);
    }
    body.put_u8(0); // terminator

    let mut msg = BytesMut::with_capacity(4 + body.len());
    msg.put_i32((4 + body.len()) as i32);
    msg.put(body);
    msg
}

/// Build an SSLRequest message.
pub fn build_ssl_request() -> BytesMut {
    let mut buf = BytesMut::with_capacity(8);
    buf.put_i32(8); // message length
    buf.put_i32(80877103); // SSL request code = 1234 << 16 | 5679
    buf
}

/// Build a Query message ('Q').
pub fn build_query_message(sql: &str) -> BytesMut {
    let body_len = 4 + sql.len() + 1; // 4 (length) + sql + null terminator
    let mut buf = BytesMut::with_capacity(1 + body_len);
    buf.put_u8(b'Q');
    buf.put_i32(body_len as i32);
    buf.put_slice(sql.as_bytes());
    buf.put_u8(0);
    buf
}

/// Build a PasswordMessage ('p').
pub fn build_password_message(password: &str) -> BytesMut {
    let body_len = 4 + password.len() + 1;
    let mut buf = BytesMut::with_capacity(1 + body_len);
    buf.put_u8(b'p');
    buf.put_i32(body_len as i32);
    buf.put_slice(password.as_bytes());
    buf.put_u8(0);
    buf
}

/// Build a CopyData message ('d') wrapping a payload.
pub fn build_copy_data(payload: &[u8]) -> BytesMut {
    let body_len = 4 + payload.len();
    let mut buf = BytesMut::with_capacity(1 + body_len);
    buf.put_u8(b'd');
    buf.put_i32(body_len as i32);
    buf.put_slice(payload);
    buf
}

/// Build a CopyDone message ('c').
pub fn build_copy_done() -> BytesMut {
    let mut buf = BytesMut::with_capacity(5);
    buf.put_u8(b'c');
    buf.put_i32(4);
    buf
}

/// Build a Terminate message ('X').
pub fn build_terminate() -> BytesMut {
    let mut buf = BytesMut::with_capacity(5);
    buf.put_u8(b'X');
    buf.put_i32(4);
    buf
}

/// Parse a null-terminated C string from a byte slice.
/// Returns the string and the number of bytes consumed (including the null terminator).
/// If no null terminator exists, consumes the entire slice.
pub fn read_cstring(data: &[u8]) -> (&str, usize) {
    match data.iter().position(|&b| b == 0) {
        Some(null_pos) => {
            let s = std::str::from_utf8(&data[..null_pos]).unwrap_or("");
            (s, null_pos + 1) // consume string + null byte
        }
        None => {
            // No null terminator — consume everything
            let s = std::str::from_utf8(data).unwrap_or("");
            (s, data.len())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_query_message() {
        let msg = build_query_message("IDENTIFY_SYSTEM");
        assert_eq!(msg[0], b'Q');
        let len = i32::from_be_bytes(msg[1..5].try_into().unwrap());
        assert_eq!(len as usize, 4 + 15 + 1); // 4 + "IDENTIFY_SYSTEM" + \0
        assert_eq!(msg[msg.len() - 1], 0); // null terminator
    }

    #[test]
    fn test_build_ssl_request() {
        let msg = build_ssl_request();
        assert_eq!(msg.len(), 8);
        let len = i32::from_be_bytes(msg[0..4].try_into().unwrap());
        assert_eq!(len, 8);
        let code = i32::from_be_bytes(msg[4..8].try_into().unwrap());
        assert_eq!(code, 80877103);
    }

    #[test]
    fn test_build_startup_message() {
        let msg = build_startup_message(&[("user", "test"), ("database", "mydb")]);
        let len = i32::from_be_bytes(msg[0..4].try_into().unwrap());
        assert_eq!(len as usize, msg.len());
        let proto = i32::from_be_bytes(msg[4..8].try_into().unwrap());
        assert_eq!(proto, 196608); // v3.0
    }

    #[test]
    fn test_read_cstring() {
        let data = b"hello\0world";
        let (s, consumed) = read_cstring(data);
        assert_eq!(s, "hello");
        assert_eq!(consumed, 6);
    }

    // --- Sync message builder tests ---

    #[test]
    fn test_build_password_message() {
        let msg = build_password_message("secret");
        assert_eq!(msg[0], b'p');
        let len = i32::from_be_bytes(msg[1..5].try_into().unwrap());
        assert_eq!(len as usize, msg.len() - 1); // length includes self but not tag
                                                 // Payload is "secret\0"
        assert_eq!(&msg[5..11], b"secret");
        assert_eq!(msg[11], 0); // null terminator
    }

    #[test]
    fn test_build_copy_data() {
        let payload = b"hello world";
        let msg = build_copy_data(payload);
        assert_eq!(msg[0], b'd');
        let len = i32::from_be_bytes(msg[1..5].try_into().unwrap());
        assert_eq!(len as usize, 4 + payload.len());
        assert_eq!(&msg[5..], payload);
    }

    #[test]
    fn test_build_copy_done() {
        let msg = build_copy_done();
        assert_eq!(msg.len(), 5);
        assert_eq!(msg[0], b'c');
        let len = i32::from_be_bytes(msg[1..5].try_into().unwrap());
        assert_eq!(len, 4);
    }

    #[test]
    fn test_build_terminate() {
        let msg = build_terminate();
        assert_eq!(msg.len(), 5);
        assert_eq!(msg[0], b'X');
        let len = i32::from_be_bytes(msg[1..5].try_into().unwrap());
        assert_eq!(len, 4);
    }

    // --- Async read_message tests ---

    #[tokio::test]
    async fn test_read_message_single_complete() {
        use tokio::io::AsyncWriteExt;
        let (mut client, mut server) = tokio::io::duplex(8192);

        // Write a ReadyForQuery message: 'Z' + len(5) + 'I'
        let msg = vec![b'Z', 0, 0, 0, 5, b'I'];
        tokio::spawn(async move {
            server.write_all(&msg).await.unwrap();
            server.flush().await.unwrap();
        });

        let mut buf = BytesMut::new();
        let result = read_message(&mut client, &mut buf).await.unwrap();
        assert_eq!(result[0], b'Z');
        assert_eq!(result.len(), 6); // tag(1) + length(4) + status(1)
        assert_eq!(result[5], b'I');
    }

    #[tokio::test]
    async fn test_read_message_two_messages_sequentially() {
        use tokio::io::AsyncWriteExt;
        let (mut client, mut server) = tokio::io::duplex(8192);

        // Two messages: 'Z' + len(5) + 'I', then 'Z' + len(5) + 'T'
        let mut msgs = Vec::new();
        msgs.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);
        msgs.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'T']);
        tokio::spawn(async move {
            server.write_all(&msgs).await.unwrap();
            server.flush().await.unwrap();
        });

        let mut buf = BytesMut::new();
        let m1 = read_message(&mut client, &mut buf).await.unwrap();
        assert_eq!(m1[5], b'I');
        let m2 = read_message(&mut client, &mut buf).await.unwrap();
        assert_eq!(m2[5], b'T');
    }

    #[tokio::test]
    async fn test_read_message_partial_then_complete() {
        use tokio::io::AsyncWriteExt;
        let (mut client, mut server) = tokio::io::duplex(8192);

        // Pre-load first 3 bytes into buf
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[b'Z', 0, 0]);

        // Write remaining bytes via stream
        tokio::spawn(async move {
            server.write_all(&[0, 5, b'I']).await.unwrap();
            server.flush().await.unwrap();
        });

        let result = read_message(&mut client, &mut buf).await.unwrap();
        assert_eq!(result[0], b'Z');
        assert_eq!(result.len(), 6);
    }

    #[tokio::test]
    async fn test_read_message_connection_closed() {
        let (mut client, server) = tokio::io::duplex(8192);
        drop(server); // Close the server end immediately

        let mut buf = BytesMut::new();
        let result = read_message(&mut client, &mut buf).await;
        assert!(result.is_err());
    }

    // --- read_byte test ---

    #[tokio::test]
    async fn test_read_byte_ssl_response() {
        use tokio::io::AsyncWriteExt;
        let (mut client, mut server) = tokio::io::duplex(8192);

        tokio::spawn(async move {
            server.write_all(&[b'S']).await.unwrap();
            server.flush().await.unwrap();
        });

        let byte = read_byte(&mut client).await.unwrap();
        assert_eq!(byte, b'S');
    }

    // --- read_cstring edge case ---

    #[test]
    fn test_read_cstring_no_null_terminator() {
        // If no null terminator, should return the full data
        let data = b"no null here";
        let (s, _) = read_cstring(data);
        assert_eq!(s, "no null here");
    }
}
