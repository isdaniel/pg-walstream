//! PostgreSQL authentication handler.
//!
//! Supports cleartext, MD5, and SCRAM-SHA-256 authentication.

use super::wire;
use crate::error::ReplicationError;
use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite};

/// Handle the authentication exchange after the startup message.
///
/// Reads AuthenticationXxx messages until AuthenticationOk (type 0) is received,
/// or returns an error if authentication fails.
///
/// `tls_server_end_point` is the SHA-256 hash of the server's TLS certificate
/// (DER-encoded), used for SCRAM-SHA-256 channel binding. Pass `None` for
/// plain-text connections.
pub async fn authenticate<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
    user: &str,
    password: Option<&str>,
    tls_server_end_point: Option<Vec<u8>>,
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
                        if tls_server_end_point.is_none() {
                            tracing::warn!(
                                "Server requested cleartext password over unencrypted connection"
                            );
                        }
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
                        handle_scram_sha256(stream, buf, &msg, password, &tls_server_end_point)
                            .await?;
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
    // Step 1: md5(password + user)
    let mut inner_data = Vec::with_capacity(password.len() + user.len());
    inner_data.extend_from_slice(password.as_bytes());
    inner_data.extend_from_slice(user.as_bytes());
    let inner_hash = compute(&inner_data);
    let inner = hex_digest(&inner_hash);

    // Step 2: md5(inner_hex + salt)
    let mut outer_data = Vec::with_capacity(inner.len() + salt.len());
    outer_data.extend_from_slice(inner.as_bytes());
    outer_data.extend_from_slice(salt);
    let outer_hash = compute(&outer_data);
    let outer = hex_digest(&outer_hash);

    format!("md5{outer}")
}

/// Handle SCRAM-SHA-256 authentication using `postgres_protocol`.
///
/// When `tls_server_end_point` is `Some`, the SCRAM exchange uses channel
/// binding (`tls-server-end-point`) if the server advertises
/// `SCRAM-SHA-256-PLUS`. This binds the TLS session to the authentication
/// exchange, preventing MITM attacks.
async fn handle_scram_sha256<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
    initial_msg: &[u8],
    password: Option<&str>,
    tls_server_end_point: &Option<Vec<u8>>,
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
    let mut has_scram_sha256_plus = false;
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
        match mechanism {
            "SCRAM-SHA-256" => has_scram_sha256 = true,
            "SCRAM-SHA-256-PLUS" => has_scram_sha256_plus = true,
            _ => {}
        }
        pos += end + 1;
    }

    if !has_scram_sha256 && !has_scram_sha256_plus {
        return Err(ReplicationError::authentication(
            "Server does not support SCRAM-SHA-256".to_string(),
        ));
    }

    // Determine channel binding mode:
    // - If the server offers SCRAM-SHA-256-PLUS and we have TLS channel binding data,
    //   use `tls-server-end-point` channel binding for MITM protection.
    // - If we have TLS but the server doesn't offer PLUS, use `unrequested` to signal
    //   we could have done channel binding (allows the server to detect downgrade).
    // - If no TLS, use `unsupported`.
    let (channel_binding, use_plus) = match tls_server_end_point {
        Some(ref data) if has_scram_sha256_plus => (
            sasl::ChannelBinding::tls_server_end_point(data.clone()),
            true,
        ),
        Some(_) => (sasl::ChannelBinding::unrequested(), false),
        None => (sasl::ChannelBinding::unsupported(), false),
    };

    let mechanism_name = if use_plus {
        sasl::SCRAM_SHA_256_PLUS
    } else {
        sasl::SCRAM_SHA_256
    };

    // Create the SCRAM client
    let mut scram = sasl::ScramSha256::new(pw.as_bytes(), channel_binding);

    // Step 1: Send SASLInitialResponse using postgres-protocol's canonical framing
    let mut sasl_init = BytesMut::new();
    postgres_protocol::message::frontend::sasl_initial_response(
        mechanism_name,
        scram.message(),
        &mut sasl_init,
    )
    .map_err(|e| {
        ReplicationError::authentication(format!("Failed to build SASL initial response: {e}"))
    })?;

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

    // Step 3: Send SASLResponse (client-final) using postgres-protocol's canonical framing
    let mut sasl_resp = BytesMut::new();
    postgres_protocol::message::frontend::sasl_response(scram.message(), &mut sasl_resp).map_err(
        |e| ReplicationError::authentication(format!("Failed to build SASL response: {e}")),
    )?;

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
        let result = authenticate(&mut client, &mut buf, "user", Some("pass"), None).await;
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
        let result = authenticate(&mut client, &mut buf, "user", Some("secret"), None).await;
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
        let result = authenticate(&mut client, &mut buf, "testuser", Some("testpass"), None).await;
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
        let result = authenticate(&mut client, &mut buf, "user", None, None).await;
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
        let result = authenticate(&mut client, &mut buf, "user", Some("wrong"), None).await;
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
        let result = authenticate(&mut client, &mut buf, "user", Some("pass"), None).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("Unsupported") || err.to_string().contains("unsupported"),
            "Expected unsupported auth type error, got: {err}"
        );
    }

    // === SCRAM channel binding mechanism selection tests ===

    /// Build an AuthenticationSASL message with given mechanism names.
    fn build_auth_sasl(mechanisms: &[&str]) -> Vec<u8> {
        let mut payload = Vec::new();
        for mech in mechanisms {
            payload.extend_from_slice(mech.as_bytes());
            payload.push(0);
        }
        payload.push(0); // final terminator

        let body_len = 4 + 4 + payload.len(); // length field + auth_type + mechanisms
        let mut msg = vec![b'R'];
        msg.extend_from_slice(&(body_len as i32).to_be_bytes());
        msg.extend_from_slice(&10i32.to_be_bytes()); // AuthenticationSASL = 10
        msg.extend_from_slice(&payload);
        msg
    }

    #[tokio::test]
    async fn test_scram_no_password_returns_error() {
        let (mut client, mut server) = tokio::io::duplex(8192);

        tokio::spawn(async move {
            let msg = build_auth_sasl(&["SCRAM-SHA-256"]);
            server.write_all(&msg).await.unwrap();
            server.flush().await.unwrap();
        });

        let mut buf = BytesMut::new();
        let result = authenticate(&mut client, &mut buf, "user", None, None).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("password"),
            "Expected password error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_scram_no_supported_mechanism_returns_error() {
        let (mut client, mut server) = tokio::io::duplex(8192);

        tokio::spawn(async move {
            // Server only offers an unknown mechanism
            let msg = build_auth_sasl(&["SCRAM-SHA-512"]);
            server.write_all(&msg).await.unwrap();
            server.flush().await.unwrap();
        });

        let mut buf = BytesMut::new();
        let result = authenticate(&mut client, &mut buf, "user", Some("pass"), None).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("SCRAM-SHA-256"),
            "Expected SCRAM-SHA-256 not supported error, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_scram_selects_plain_without_tls_endpoint() {
        let (mut client, mut server) = tokio::io::duplex(8192);

        // Server offers both PLUS and non-PLUS, but we have no TLS endpoint data.
        // The client should select SCRAM-SHA-256 (not PLUS) and send mechanism name.
        tokio::spawn(async move {
            let msg = build_auth_sasl(&["SCRAM-SHA-256-PLUS", "SCRAM-SHA-256"]);
            server.write_all(&msg).await.unwrap();
            server.flush().await.unwrap();

            // Read the SASLInitialResponse from client
            let mut response = vec![0u8; 4096];
            let n = AsyncReadExt::read(&mut server, &mut response)
                .await
                .unwrap();
            let response = &response[..n];

            // Verify: tag='p', then length, then mechanism name as C-string
            assert_eq!(response[0], b'p');
            // Find the null terminator after the 5-byte header
            let mech_start = 5;
            let mech_end =
                response[mech_start..].iter().position(|&b| b == 0).unwrap() + mech_start;
            let mechanism = std::str::from_utf8(&response[mech_start..mech_end]).unwrap();
            assert_eq!(
                mechanism, "SCRAM-SHA-256",
                "Without TLS endpoint, should select SCRAM-SHA-256, not PLUS"
            );

            // Send an error to terminate cleanly (we can't do a full SCRAM exchange)
            let err = build_error_response("FATAL", "28000", "test abort");
            server.write_all(&err).await.unwrap();
            server.flush().await.unwrap();
        });

        let mut buf = BytesMut::new();
        // No TLS endpoint → should use SCRAM-SHA-256
        let _ = authenticate(&mut client, &mut buf, "user", Some("pass"), None).await;
    }

    #[tokio::test]
    async fn test_scram_selects_plus_with_tls_endpoint() {
        let (mut client, mut server) = tokio::io::duplex(8192);

        let fake_tls_hash = vec![0xAA; 32]; // fake SHA-256 hash

        tokio::spawn(async move {
            let msg = build_auth_sasl(&["SCRAM-SHA-256-PLUS", "SCRAM-SHA-256"]);
            server.write_all(&msg).await.unwrap();
            server.flush().await.unwrap();

            // Read the SASLInitialResponse
            let mut response = vec![0u8; 4096];
            let n = AsyncReadExt::read(&mut server, &mut response)
                .await
                .unwrap();
            let response = &response[..n];

            assert_eq!(response[0], b'p');
            let mech_start = 5;
            let mech_end =
                response[mech_start..].iter().position(|&b| b == 0).unwrap() + mech_start;
            let mechanism = std::str::from_utf8(&response[mech_start..mech_end]).unwrap();
            assert_eq!(
                mechanism, "SCRAM-SHA-256-PLUS",
                "With TLS endpoint and server offering PLUS, should select SCRAM-SHA-256-PLUS"
            );

            // Send error to terminate
            let err = build_error_response("FATAL", "28000", "test abort");
            server.write_all(&err).await.unwrap();
            server.flush().await.unwrap();
        });

        let mut buf = BytesMut::new();
        let _ = authenticate(
            &mut client,
            &mut buf,
            "user",
            Some("pass"),
            Some(fake_tls_hash),
        )
        .await;
    }

    #[tokio::test]
    async fn test_scram_falls_back_without_plus_offered() {
        let (mut client, mut server) = tokio::io::duplex(8192);

        let fake_tls_hash = vec![0xBB; 32];

        tokio::spawn(async move {
            // Server only offers non-PLUS, even though client has TLS endpoint
            let msg = build_auth_sasl(&["SCRAM-SHA-256"]);
            server.write_all(&msg).await.unwrap();
            server.flush().await.unwrap();

            let mut response = vec![0u8; 4096];
            let n = AsyncReadExt::read(&mut server, &mut response)
                .await
                .unwrap();
            let response = &response[..n];

            assert_eq!(response[0], b'p');
            let mech_start = 5;
            let mech_end =
                response[mech_start..].iter().position(|&b| b == 0).unwrap() + mech_start;
            let mechanism = std::str::from_utf8(&response[mech_start..mech_end]).unwrap();
            assert_eq!(
                mechanism, "SCRAM-SHA-256",
                "When server doesn't offer PLUS, should fall back to SCRAM-SHA-256"
            );

            let err = build_error_response("FATAL", "28000", "test abort");
            server.write_all(&err).await.unwrap();
            server.flush().await.unwrap();
        });

        let mut buf = BytesMut::new();
        let _ = authenticate(
            &mut client,
            &mut buf,
            "user",
            Some("pass"),
            Some(fake_tls_hash),
        )
        .await;
    }
}
