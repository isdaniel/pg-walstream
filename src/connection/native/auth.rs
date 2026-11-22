//! PostgreSQL authentication handler.
//!
//! Supports cleartext, MD5, and SCRAM-SHA-256 authentication.

use super::error::error_frame;
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
pub(super) async fn authenticate<S: AsyncRead + AsyncWrite + Unpin>(
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
                        let pw = require_password(password)?;
                        let pw_msg = wire::build_password_message(pw);
                        wire::write_all(stream, &pw_msg).await?;
                        wire::flush(stream).await?;
                    }
                    5 => {
                        // AuthenticationMD5Password
                        let pw = require_password(password)?;
                        if msg.len() < 13 {
                            return Err(ReplicationError::protocol(
                                "MD5 auth message too short (missing salt)".to_string(),
                            ));
                        }
                        let salt: [u8; 4] = msg[9..13].try_into().unwrap();
                        let hashed = postgres_protocol::authentication::md5_hash(
                            user.as_bytes(),
                            pw.as_bytes(),
                            salt,
                        );
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
                let fields = error_frame(&msg);
                let message = format!("Authentication failed: {fields}");
                return Err(ReplicationError::from_server_startup(fields, message));
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

fn require_password(password: Option<&str>) -> Result<&str, ReplicationError> {
    password.ok_or_else(|| {
        ReplicationError::authentication("Server requires password but none provided".to_string())
    })
}

/// Check that `msg` is the Authentication message `want` and return its payload.
///
/// An ErrorResponse in its place is the server rejecting the SCRAM exchange.
fn expect_scram_step<'a>(
    msg: &'a [u8],
    want: i32,
    frame: &str,
    kind: &str,
) -> Result<&'a [u8], ReplicationError> {
    if msg[0] == b'E' {
        let fields = error_frame(msg);
        return Err(ReplicationError::Authentication(
            format!("SCRAM authentication failed: {fields}").into_boxed_str(),
            Some(Box::new(fields)),
        ));
    }
    if msg[0] != b'R' || msg.len() < 9 {
        return Err(ReplicationError::protocol(format!("Expected {frame}")));
    }
    let got = i32::from_be_bytes(msg[5..9].try_into().unwrap());
    if got != want {
        return Err(ReplicationError::protocol(format!(
            "Expected {kind} ({want}), got {got}"
        )));
    }
    Ok(&msg[9..])
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
    let offers = |name: &str| {
        initial_msg[9..]
            .split(|&b| b == 0)
            .take_while(|mechanism| !mechanism.is_empty())
            .any(|mechanism| mechanism == name.as_bytes())
    };
    let has_scram_sha256 = offers(sasl::SCRAM_SHA_256);
    let has_scram_sha256_plus = offers(sasl::SCRAM_SHA_256_PLUS);

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
    let server_first = expect_scram_step(&msg2, 11, "AuthenticationSASLContinue", "SASL continue")?;

    // Process server-first message
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
    let server_final = expect_scram_step(&msg3, 12, "AuthenticationSASLFinal", "SASL final")?;

    // Verify server signature
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
        let fields = error_frame(&msg4);
        return Err(ReplicationError::Authentication(
            format!("Authentication failed after SCRAM: {fields}").into_boxed_str(),
            Some(Box::new(fields)),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::wire::testing::{build_error_response, discard_request, frame};
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// `"md5" + md5hex(md5hex(password + user) + salt)`, the expected value computed
    /// outside this crate with `md5sum`.
    #[tokio::test]
    async fn test_md5_password_known_answer() {
        let (mut client, mut server) = tokio::io::duplex(8192);

        let peer = tokio::spawn(async move {
            server
                .write_all(&auth_request(5, &[0x01, 0x02, 0x03, 0x04]))
                .await
                .unwrap();
            let (tag, body) = read_frontend(&mut server).await;
            server.write_all(&auth_request(0, &[])).await.unwrap();
            (tag, body)
        });

        let mut buf = BytesMut::new();
        authenticate(&mut client, &mut buf, "md5_user", Some("password"), None)
            .await
            .unwrap();
        let (tag, body) = peer.await.unwrap();
        assert_eq!(tag, b'p');
        assert_eq!(body, b"md580a32c6f038b79de232354a2ee7060eb\0");
    }

    /// An Authentication message of `kind` carrying `data`.
    fn auth_request(kind: i32, data: &[u8]) -> Vec<u8> {
        frame(b'R', &[&kind.to_be_bytes()[..], data].concat())
    }

    // === authenticate mock stream tests ===

    #[tokio::test]
    async fn test_auth_ok_immediately() {
        let (mut client, mut server) = tokio::io::duplex(8192);

        tokio::spawn(async move {
            server.write_all(&auth_request(0, &[])).await.unwrap();
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
            server.write_all(&auth_request(3, &[])).await.unwrap();
            server.flush().await.unwrap();

            discard_request(&mut server).await;

            // Send AuthenticationOk
            server.write_all(&auth_request(0, &[])).await.unwrap();
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
            server.write_all(&auth_request(5, &salt)).await.unwrap();
            server.flush().await.unwrap();

            discard_request(&mut server).await;

            // Send AuthenticationOk
            server.write_all(&auth_request(0, &[])).await.unwrap();
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
            server.write_all(&auth_request(3, &[])).await.unwrap();
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
        assert_eq!(
            err.sqlstate(),
            Some(sqlstate::pg::INVALID_AUTHORIZATION_SPECIFICATION)
        );
    }

    #[tokio::test]
    async fn test_auth_unsupported_type() {
        let (mut client, mut server) = tokio::io::duplex(8192);

        tokio::spawn(async move {
            // Send an unsupported auth type (99)
            server.write_all(&auth_request(99, &[])).await.unwrap();
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
        auth_request(10, &payload) // AuthenticationSASL = 10
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
        let err = authenticate(&mut client, &mut buf, "user", Some("pass"), None)
            .await
            .unwrap_err();
        assert!(
            matches!(err, ReplicationError::Authentication(..)),
            "{err:?}"
        );
        assert_eq!(
            err.server_error().map(|server| server.message.as_str()),
            Some("test abort")
        );
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

    // === SCRAM exchange failures ===

    const SCRAM_PASSWORD: &str = "correct horse battery staple";
    const SCRAM_SALT: [u8; 16] = *b"pg-walstream-slt";

    /// Padded standard base64, the encoding SCRAM uses for salts and signatures.
    fn base64(bytes: &[u8]) -> String {
        const ALPHABET: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let bits = chunk
                .iter()
                .enumerate()
                .fold(0u32, |bits, (i, &b)| bits | u32::from(b) << (16 - 8 * i));
            for i in 0..4 {
                let sextet = usize::try_from((bits >> (18 - 6 * i)) & 63).unwrap();
                out.push(if i <= chunk.len() {
                    char::from(ALPHABET[sextet])
                } else {
                    '='
                });
            }
        }
        out
    }

    /// One frontend message read off the fake server's end: its type byte and body.
    async fn read_frontend(server: &mut tokio::io::DuplexStream) -> (u8, Vec<u8>) {
        let tag = server.read_u8().await.unwrap();
        let len = usize::try_from(server.read_u32().await.unwrap()).unwrap();
        let mut body = vec![0; len - 4];
        server.read_exact(&mut body).await.unwrap();
        (tag, body)
    }

    /// Plays a SCRAM-SHA-256 server for `SCRAM_PASSWORD`, then sends `failure`.
    ///
    /// With `verified` the client's proof is first answered by a valid
    /// AuthenticationSASLFinal, so `failure` stands where AuthenticationOk belongs.
    async fn scram_server(mut server: tokio::io::DuplexStream, verified: bool, failure: Vec<u8>) {
        use aws_lc_rs::{hmac, pbkdf2};

        server
            .write_all(&build_auth_sasl(&["SCRAM-SHA-256"]))
            .await
            .unwrap();
        let (tag, initial) = read_frontend(&mut server).await;
        assert_eq!(tag, b'p');
        // The mechanism name, then the length-prefixed client-first message `n,,n=,r=<nonce>`.
        let mechanism_end = initial.iter().position(|&b| b == 0).unwrap();
        let client_first = std::str::from_utf8(&initial[mechanism_end + 5..]).unwrap();
        let client_first_bare = client_first.strip_prefix("n,,").unwrap();
        let client_nonce = client_first_bare.split_once("r=").unwrap().1;
        let server_first = format!(
            "r={client_nonce}server-nonce,s={},i=4096",
            base64(&SCRAM_SALT)
        );
        server
            .write_all(&auth_request(11, server_first.as_bytes()))
            .await
            .unwrap();

        let (tag, client_final) = read_frontend(&mut server).await;
        assert_eq!(tag, b'p');
        if verified {
            let client_final = std::str::from_utf8(&client_final).unwrap();
            let without_proof = client_final.rsplit_once(",p=").unwrap().0;
            let mut salted = [0u8; 32];
            pbkdf2::derive(
                pbkdf2::PBKDF2_HMAC_SHA256,
                core::num::NonZeroU32::new(4096).unwrap(),
                &SCRAM_SALT,
                SCRAM_PASSWORD.as_bytes(),
                &mut salted,
            );
            let server_key = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, &salted), b"Server Key");
            let auth_message = format!("{client_first_bare},{server_first},{without_proof}");
            let signature = hmac::sign(
                &hmac::Key::new(hmac::HMAC_SHA256, server_key.as_ref()),
                auth_message.as_bytes(),
            );
            let server_final = format!("v={}", base64(signature.as_ref()));
            server
                .write_all(&auth_request(12, server_final.as_bytes()))
                .await
                .unwrap();
        }
        server.write_all(&failure).await.unwrap();
        server.flush().await.unwrap();
    }

    /// A server that rejects the client's proof answers client-final with an
    /// ErrorResponse, which reaches the caller with its SQLSTATE.
    #[tokio::test]
    async fn scram_rejected_proof_keeps_the_server_response() {
        let (mut client, server) = tokio::io::duplex(8192);
        let failure = build_error_response(
            "FATAL",
            "28P01",
            "password authentication failed for user \"user\"",
        );
        let peer = tokio::spawn(scram_server(server, false, failure));

        let mut buf = BytesMut::new();
        let err = authenticate(&mut client, &mut buf, "user", Some(SCRAM_PASSWORD), None)
            .await
            .unwrap_err();
        peer.await.unwrap();
        assert!(
            matches!(err, ReplicationError::Authentication(..)),
            "{err:?}"
        );
        assert_eq!(err.sqlstate(), Some(sqlstate::pg::INVALID_PASSWORD));
        assert_eq!(
            err.to_string(),
            "Authentication failed: SCRAM authentication failed: \
             FATAL: password authentication failed for user \"user\" (SQLSTATE 28P01)"
        );
    }

    /// A verified exchange can still end in an ErrorResponse where AuthenticationOk
    /// belongs, and that response reaches the caller too.
    #[tokio::test]
    async fn scram_error_after_server_final_keeps_the_server_response() {
        let (mut client, server) = tokio::io::duplex(8192);
        let failure =
            build_error_response("FATAL", "28000", "role \"user\" is not permitted to log in");
        let peer = tokio::spawn(scram_server(server, true, failure));

        let mut buf = BytesMut::new();
        let err = authenticate(&mut client, &mut buf, "user", Some(SCRAM_PASSWORD), None)
            .await
            .unwrap_err();
        peer.await.unwrap();
        assert!(
            matches!(err, ReplicationError::Authentication(..)),
            "{err:?}"
        );
        assert_eq!(
            err.sqlstate(),
            Some(sqlstate::pg::INVALID_AUTHORIZATION_SPECIFICATION)
        );
        assert_eq!(
            err.to_string(),
            "Authentication failed: Authentication failed after SCRAM: \
             FATAL: role \"user\" is not permitted to log in (SQLSTATE 28000)"
        );
    }
}
