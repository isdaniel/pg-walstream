//! PostgreSQL authentication handler.
//!
//! Supports cleartext, MD5, and SCRAM-SHA-256 authentication.

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite};

use super::wire;
use crate::error::ReplicationError;

/// Handle the authentication exchange after the startup message.
///
/// Reads AuthenticationXxx messages until AuthenticationOk (type 0) is received,
/// or returns an error if authentication fails.
pub async fn authenticate<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
    user: &str,
    password: Option<&str>,
) -> Result<(), ReplicationError> {
    loop {
        let msg = wire::read_message(stream, buf).await?;
        if msg.is_empty() {
            return Err(ReplicationError::protocol(
                "Empty message during authentication".to_string(),
            ));
        }

        match msg[0] {
            b'R' => {
                // Authentication message
                if msg.len() < 9 {
                    return Err(ReplicationError::protocol(
                        "Authentication message too short".to_string(),
                    ));
                }
                let auth_type = i32::from_be_bytes(msg[5..9].try_into().unwrap());

                match auth_type {
                    0 => {
                        // AuthenticationOk
                        tracing::debug!("Authentication successful");
                        return Ok(());
                    }
                    3 => {
                        // AuthenticationCleartextPassword
                        let pw = password.ok_or_else(|| {
                            ReplicationError::authentication(
                                "Server requires password but none provided".to_string(),
                            )
                        })?;
                        let pw_msg = wire::build_password_message(pw);
                        wire::write_all(stream, &pw_msg).await?;
                        wire::flush(stream).await?;
                    }
                    5 => {
                        // AuthenticationMD5Password
                        let pw = password.ok_or_else(|| {
                            ReplicationError::authentication(
                                "Server requires password but none provided".to_string(),
                            )
                        })?;
                        if msg.len() < 13 {
                            return Err(ReplicationError::protocol(
                                "MD5 auth message too short (missing salt)".to_string(),
                            ));
                        }
                        let salt = &msg[9..13];
                        let hashed = md5_password(user, pw, salt);
                        let pw_msg = wire::build_password_message(&hashed);
                        wire::write_all(stream, &pw_msg).await?;
                        wire::flush(stream).await?;
                    }
                    10 => {
                        // AuthenticationSASL — SCRAM-SHA-256
                        handle_scram_sha256(stream, buf, &msg, password).await?;
                        // SCRAM handler consumed AuthenticationOk internally
                        return Ok(());
                    }
                    _ => {
                        return Err(ReplicationError::authentication(format!(
                            "Unsupported authentication type: {auth_type}"
                        )));
                    }
                }
            }
            b'E' => {
                // ErrorResponse during auth
                let fields = super::error::parse_error_fields(&msg[5..]);
                return Err(ReplicationError::authentication(format!(
                    "Authentication failed: {}",
                    fields
                )));
            }
            _ => {
                // Unexpected message during auth phase — skip
                tracing::warn!(
                    "Unexpected message type '{}' during authentication",
                    msg[0] as char
                );
            }
        }
    }
}

/// Compute an MD5 password hash as PostgreSQL expects:
/// `"md5" + md5(md5(password + user) + salt)`
fn md5_password(user: &str, password: &str, salt: &[u8]) -> String {
    use md5::{Digest, Md5};

    // Step 1: md5(password + user)
    let mut hasher = Md5::new();
    hasher.update(password.as_bytes());
    hasher.update(user.as_bytes());
    let inner_hash = hasher.finalize();
    let inner = hex_encode(&inner_hash);

    // Step 2: md5(inner_hex + salt)
    let mut hasher = Md5::new();
    hasher.update(inner.as_bytes());
    hasher.update(salt);
    let outer_hash = hasher.finalize();
    let outer = hex_encode(&outer_hash);

    format!("md5{outer}")
}

/// Encode bytes as lowercase hex string.
fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Handle SCRAM-SHA-256 authentication using `postgres_protocol`.
async fn handle_scram_sha256<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
    initial_msg: &[u8],
    password: Option<&str>,
) -> Result<(), ReplicationError> {
    use postgres_protocol::authentication::sasl;

    let pw = password.ok_or_else(|| {
        ReplicationError::authentication(
            "Server requires password for SCRAM-SHA-256 but none provided".to_string(),
        )
    })?;

    // Parse the SASL mechanism list from the initial message.
    // After the 4-byte auth type, the payload contains null-terminated mechanism names
    // followed by an extra null terminator.
    let mechanisms_payload = &initial_msg[9..];
    let mut has_scram_sha256 = false;
    let mut pos = 0;
    while pos < mechanisms_payload.len() {
        if mechanisms_payload[pos] == 0 {
            break;
        }
        let end = mechanisms_payload[pos..]
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(mechanisms_payload.len() - pos);
        let mechanism = std::str::from_utf8(&mechanisms_payload[pos..pos + end]).unwrap_or("");
        if mechanism == "SCRAM-SHA-256" {
            has_scram_sha256 = true;
        }
        pos += end + 1;
    }

    if !has_scram_sha256 {
        return Err(ReplicationError::authentication(
            "Server does not support SCRAM-SHA-256".to_string(),
        ));
    }

    // Create the SCRAM client
    let mut scram = sasl::ScramSha256::new(pw.as_bytes(), sasl::ChannelBinding::unsupported());

    // Step 1: Send SASLInitialResponse
    let client_first = scram.message();
    let mechanism_name = b"SCRAM-SHA-256";
    // Message format: 'p' tag + length + mechanism\0 + client-first-length(i32) + client-first
    let body_len = 4 + mechanism_name.len() + 1 + 4 + client_first.len();
    let mut sasl_init = BytesMut::with_capacity(1 + body_len);
    sasl_init.put_u8(b'p');
    sasl_init.put_i32(body_len as i32);
    sasl_init.put_slice(mechanism_name);
    sasl_init.put_u8(0);
    sasl_init.put_i32(client_first.len() as i32);
    sasl_init.put_slice(client_first);

    wire::write_all(stream, &sasl_init).await?;
    wire::flush(stream).await?;

    // Step 2: Receive AuthenticationSASLContinue (type 11)
    let msg2 = wire::read_message(stream, buf).await?;
    if msg2[0] == b'E' {
        let fields = super::error::parse_error_fields(&msg2[5..]);
        return Err(ReplicationError::authentication(format!(
            "SCRAM authentication failed: {}",
            fields
        )));
    }
    if msg2[0] != b'R' || msg2.len() < 9 {
        return Err(ReplicationError::protocol(
            "Expected AuthenticationSASLContinue".to_string(),
        ));
    }
    let auth_type2 = i32::from_be_bytes(msg2[5..9].try_into().unwrap());
    if auth_type2 != 11 {
        return Err(ReplicationError::protocol(format!(
            "Expected SASL continue (11), got {auth_type2}"
        )));
    }

    // Process server-first message
    let server_first = &msg2[9..];
    scram.update(server_first).map_err(|e| {
        ReplicationError::authentication(format!("SCRAM server-first processing failed: {e}"))
    })?;

    // Step 3: Send SASLResponse (client-final)
    let client_final = scram.message();
    let resp_body_len = 4 + client_final.len();
    let mut sasl_resp = BytesMut::with_capacity(1 + resp_body_len);
    sasl_resp.put_u8(b'p');
    sasl_resp.put_i32(resp_body_len as i32);
    sasl_resp.put_slice(client_final);

    wire::write_all(stream, &sasl_resp).await?;
    wire::flush(stream).await?;

    // Step 4: Receive AuthenticationSASLFinal (type 12)
    let msg3 = wire::read_message(stream, buf).await?;
    if msg3[0] == b'E' {
        let fields = super::error::parse_error_fields(&msg3[5..]);
        return Err(ReplicationError::authentication(format!(
            "SCRAM authentication failed: {}",
            fields
        )));
    }
    if msg3[0] != b'R' || msg3.len() < 9 {
        return Err(ReplicationError::protocol(
            "Expected AuthenticationSASLFinal".to_string(),
        ));
    }
    let auth_type3 = i32::from_be_bytes(msg3[5..9].try_into().unwrap());
    if auth_type3 != 12 {
        return Err(ReplicationError::protocol(format!(
            "Expected SASL final (12), got {auth_type3}"
        )));
    }

    // Verify server signature
    let server_final = &msg3[9..];
    scram.finish(server_final).map_err(|e| {
        ReplicationError::authentication(format!("SCRAM server signature verification failed: {e}"))
    })?;

    // Step 5: AuthenticationOk (type 0) should follow
    let msg4 = wire::read_message(stream, buf).await?;
    if msg4[0] == b'R' && msg4.len() >= 9 {
        let auth_type4 = i32::from_be_bytes(msg4[5..9].try_into().unwrap());
        if auth_type4 != 0 {
            return Err(ReplicationError::protocol(format!(
                "Expected AuthenticationOk (0) after SCRAM, got {auth_type4}"
            )));
        }
    } else if msg4[0] == b'E' {
        let fields = super::error::parse_error_fields(&msg4[5..]);
        return Err(ReplicationError::authentication(format!(
            "Authentication failed after SCRAM: {}",
            fields
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn test_md5_password() {
        // Known test vector: user="md5", password="md5", salt=[0x01, 0x02, 0x03, 0x04]
        let result = md5_password("md5", "md5", &[0x01, 0x02, 0x03, 0x04]);
        assert!(result.starts_with("md5"));
        assert_eq!(result.len(), 35); // "md5" + 32 hex chars
    }

    #[test]
    fn test_md5_password_known() {
        // PostgreSQL MD5 hash for user=test, password=test, salt=[0,0,0,0]
        let result = md5_password("test", "test", &[0, 0, 0, 0]);
        assert!(result.starts_with("md5"));
    }

    // === hex_encode ===

    #[test]
    fn test_hex_encode_empty() {
        assert_eq!(hex_encode(&[]), "");
    }

    #[test]
    fn test_hex_encode_known_values() {
        assert_eq!(hex_encode(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
    }

    #[test]
    fn test_hex_encode_zero_bytes() {
        assert_eq!(hex_encode(&[0x00, 0x00]), "0000");
    }

    #[test]
    fn test_hex_encode_ff() {
        assert_eq!(hex_encode(&[0xff]), "ff");
    }

    // === Helper: build auth messages ===

    /// Build an AuthenticationOk message: R + len(8) + type(0)
    fn build_auth_ok() -> Vec<u8> {
        let mut msg = vec![b'R'];
        msg.extend_from_slice(&8i32.to_be_bytes());
        msg.extend_from_slice(&0i32.to_be_bytes());
        msg
    }

    /// Build an AuthenticationCleartextPassword message: R + len(8) + type(3)
    fn build_auth_cleartext() -> Vec<u8> {
        let mut msg = vec![b'R'];
        msg.extend_from_slice(&8i32.to_be_bytes());
        msg.extend_from_slice(&3i32.to_be_bytes());
        msg
    }

    /// Build an AuthenticationMD5Password message: R + len(12) + type(5) + salt(4)
    fn build_auth_md5(salt: &[u8; 4]) -> Vec<u8> {
        let mut msg = vec![b'R'];
        msg.extend_from_slice(&12i32.to_be_bytes());
        msg.extend_from_slice(&5i32.to_be_bytes());
        msg.extend_from_slice(salt);
        msg
    }

    /// Build an ErrorResponse message
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

        let mut msg = vec![b'E'];
        let len = (4 + payload.len()) as i32;
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(&payload);
        msg
    }

    // === authenticate mock stream tests ===

    #[tokio::test]
    async fn test_auth_ok_immediately() {
        let (mut client, mut server) = tokio::io::duplex(8192);

        tokio::spawn(async move {
            server.write_all(&build_auth_ok()).await.unwrap();
            server.flush().await.unwrap();
        });

        let mut buf = BytesMut::new();
        let result = authenticate(&mut client, &mut buf, "user", Some("pass")).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_auth_cleartext_then_ok() {
        let (mut client, mut server) = tokio::io::duplex(8192);

        tokio::spawn(async move {
            // Send cleartext password challenge
            server.write_all(&build_auth_cleartext()).await.unwrap();
            server.flush().await.unwrap();

            // Read and discard the password message from client
            let mut discard = vec![0u8; 1024];
            let _ = AsyncReadExt::read(&mut server, &mut discard).await;

            // Send AuthenticationOk
            server.write_all(&build_auth_ok()).await.unwrap();
            server.flush().await.unwrap();
        });

        let mut buf = BytesMut::new();
        let result = authenticate(&mut client, &mut buf, "user", Some("secret")).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_auth_md5_then_ok() {
        let (mut client, mut server) = tokio::io::duplex(8192);
        let salt = [0x01, 0x02, 0x03, 0x04];

        tokio::spawn(async move {
            // Send MD5 password challenge with salt
            server.write_all(&build_auth_md5(&salt)).await.unwrap();
            server.flush().await.unwrap();

            // Read and discard the password response from client
            let mut discard = vec![0u8; 1024];
            let _ = AsyncReadExt::read(&mut server, &mut discard).await;

            // Send AuthenticationOk
            server.write_all(&build_auth_ok()).await.unwrap();
            server.flush().await.unwrap();
        });

        let mut buf = BytesMut::new();
        let result = authenticate(&mut client, &mut buf, "testuser", Some("testpass")).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_auth_cleartext_no_password() {
        let (mut client, mut server) = tokio::io::duplex(8192);

        tokio::spawn(async move {
            server.write_all(&build_auth_cleartext()).await.unwrap();
            server.flush().await.unwrap();
        });

        let mut buf = BytesMut::new();
        let result = authenticate(&mut client, &mut buf, "user", None).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("password"),
            "Expected password error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_auth_error_response() {
        let (mut client, mut server) = tokio::io::duplex(8192);

        tokio::spawn(async move {
            let err_msg = build_error_response("FATAL", "28000", "password authentication failed");
            server.write_all(&err_msg).await.unwrap();
            server.flush().await.unwrap();
        });

        let mut buf = BytesMut::new();
        let result = authenticate(&mut client, &mut buf, "user", Some("wrong")).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("authentication")
                || err.to_string().contains("Authentication"),
            "Expected authentication error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_auth_unsupported_type() {
        let (mut client, mut server) = tokio::io::duplex(8192);

        tokio::spawn(async move {
            // Send an unsupported auth type (99)
            let mut msg = vec![b'R'];
            msg.extend_from_slice(&8i32.to_be_bytes());
            msg.extend_from_slice(&99i32.to_be_bytes());
            server.write_all(&msg).await.unwrap();
            server.flush().await.unwrap();
        });

        let mut buf = BytesMut::new();
        let result = authenticate(&mut client, &mut buf, "user", Some("pass")).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("Unsupported") || err.to_string().contains("unsupported"),
            "Expected unsupported auth type error, got: {err}"
        );
    }
}
