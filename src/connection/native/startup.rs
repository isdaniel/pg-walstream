//! TLS negotiation and PostgreSQL startup sequence.
//!
//! Handles:
//! 1. TCP connection
//! 2. SSLRequest → TLS handshake (via rustls) if required
//! 3. StartupMessage with protocol v3.0
//! 4. Authentication delegation to `auth.rs`
//! 5. Backend parameter processing until ReadyForQuery

use bytes::BytesMut;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use super::conninfo::{ConnInfo, SslMode};
use super::wire;
use crate::error::ReplicationError;

/// Lazily-initialized, cached crypto provider.
///
/// `aws_lc_rs::default_provider()` constructs a new `CryptoProvider` on each call.
/// Caching it via `LazyLock` ensures one-time initialization and reuse across
/// all TLS connections.
static CRYPTO_PROVIDER: LazyLock<Arc<rustls::crypto::CryptoProvider>> =
    LazyLock::new(|| Arc::new(rustls::crypto::aws_lc_rs::default_provider()));

/// Return a reference to the cached aws-lc-rs crypto provider.
///
/// aws-lc-rs leverages AES-NI, AVX2, and SHA-NI instructions for
/// high-throughput AES-GCM decryption during WAL streaming.
fn crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
    CRYPTO_PROVIDER.clone()
}

/// The transport layer — either plain TCP or TLS-wrapped TCP.
pub enum Transport {
    Plain(TcpStream),
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
}

impl AsyncRead for Transport {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Transport::Plain(ref mut s) => std::pin::Pin::new(s).poll_read(cx, buf),
            Transport::Tls(ref mut s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Transport {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Transport::Plain(ref mut s) => std::pin::Pin::new(s).poll_write(cx, buf),
            Transport::Tls(ref mut s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Transport::Plain(ref mut s) => std::pin::Pin::new(s).poll_flush(cx),
            Transport::Tls(ref mut s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Transport::Plain(ref mut s) => std::pin::Pin::new(s).poll_shutdown(cx),
            Transport::Tls(ref mut s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// Establish a TCP connection, optionally upgrade to TLS, and perform the
/// PostgreSQL startup + authentication handshake.
///
/// Returns the established transport and server version.
pub async fn connect(info: &ConnInfo) -> Result<(Transport, i32, BytesMut), ReplicationError> {
    let addr = format!("{}:{}", info.host, info.port);

    let tcp = tcp_connect(&addr, info).await?;

    let mut transport = match info.sslmode {
        SslMode::Disable => Transport::Plain(tcp),
        SslMode::Prefer | SslMode::Allow => {
            // For prefer/allow: attempt TLS, but fall back to a new plaintext.
            match negotiate_tls(tcp, info).await {
                Ok(t) => t,
                Err(e) => {
                    tracing::debug!("TLS negotiation failed, falling back to plain: {e}");
                    let tcp = tcp_connect(&addr, info).await?;
                    Transport::Plain(tcp)
                }
            }
        }
        _ => negotiate_tls(tcp, info).await?,
    };

    // Perform startup + auth
    let mut buf = BytesMut::with_capacity(8192);
    let server_version = startup_and_auth(&mut transport, &mut buf, info).await?;

    Ok((transport, server_version, buf))
}

/// Open a TCP connection with optional timeout, TCP_NODELAY, and keepalive.
async fn tcp_connect(addr: &str, info: &ConnInfo) -> Result<TcpStream, ReplicationError> {
    let tcp = if info.connect_timeout > 0 {
        let timeout = Duration::from_secs(info.connect_timeout);
        tokio::time::timeout(timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| {
                ReplicationError::transient_connection(format!(
                    "Connection to {addr} timed out after {}s",
                    info.connect_timeout
                ))
            })?
            .map_err(|e| {
                ReplicationError::transient_connection(format!("Failed to connect to {addr}: {e}"))
            })?
    } else {
        TcpStream::connect(addr).await.map_err(|e| {
            ReplicationError::transient_connection(format!("Failed to connect to {addr}: {e}"))
        })?
    };

    tcp.set_nodelay(true).ok();

    if info.keepalives {
        configure_tcp_keepalive(&tcp, info);
    }

    Ok(tcp)
}

/// Configure TCP keepalive on the socket using socket2.
///
/// This is critical for long-running replication connections over cloud networks
/// where load balancers silently drop idle TCP connections.
fn configure_tcp_keepalive(tcp: &TcpStream, info: &ConnInfo) {
    use socket2::SockRef;

    let sock = SockRef::from(tcp);
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(info.keepalives_idle))
        .with_interval(Duration::from_secs(info.keepalives_interval));

    // retries/count is platform-specific
    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "netbsd",
    ))]
    let keepalive = keepalive.with_retries(info.keepalives_count);

    if let Err(e) = sock.set_tcp_keepalive(&keepalive) {
        tracing::warn!("Failed to set TCP keepalive: {e}");
    } else {
        tracing::debug!(
            "TCP keepalive configured: idle={}s, interval={}s",
            info.keepalives_idle,
            info.keepalives_interval
        );
    }
}

/// Perform TLS negotiation: send SSLRequest, check response, do handshake.
async fn negotiate_tls(mut tcp: TcpStream, info: &ConnInfo) -> Result<Transport, ReplicationError> {
    // Send SSLRequest
    let ssl_req = wire::build_ssl_request();
    wire::write_all(&mut tcp, &ssl_req).await?;
    wire::flush(&mut tcp).await?;

    let response = wire::read_byte(&mut tcp).await?;

    match response {
        b'S' => {
            // Server supports SSL — do the handshake
            let tls_config = build_tls_config(info)?;
            let connector = TlsConnector::from(Arc::new(tls_config));

            let server_name = rustls::pki_types::ServerName::try_from(info.host.as_str())
                .map_err(|e| {
                    ReplicationError::permanent_connection(format!(
                        "Invalid server name for TLS: {e}"
                    ))
                })?
                .to_owned();

            let tls_stream = connector.connect(server_name, tcp).await.map_err(|e| {
                ReplicationError::transient_connection(format!("TLS handshake failed: {e}"))
            })?;

            tracing::debug!("TLS connection established");
            Ok(Transport::Tls(tls_stream))
        }
        b'N' => {
            // Server does not support SSL
            match info.sslmode {
                SslMode::Require | SslMode::VerifyCa | SslMode::VerifyFull => {
                    Err(ReplicationError::permanent_connection(
                        "Server does not support SSL but sslmode=require".to_string(),
                    ))
                }
                _ => {
                    tracing::debug!("Server doesn't support SSL, falling back to plain");
                    Ok(Transport::Plain(tcp))
                }
            }
        }
        other => Err(ReplicationError::protocol(format!(
            "Unexpected SSLRequest response: 0x{other:02x}"
        ))),
    }
}

/// Build a rustls `ClientConfig` based on the sslmode.
fn build_tls_config(info: &ConnInfo) -> Result<rustls::ClientConfig, ReplicationError> {
    let provider = crypto_provider();

    match info.sslmode {
        SslMode::VerifyFull => {
            // Full verification: check cert chain + hostname
            let root_store = build_root_store(info.sslrootcert.as_deref())?;
            let config = rustls::ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map_err(|e| {
                    ReplicationError::permanent_connection(format!(
                        "Failed to configure TLS protocol versions: {e}"
                    ))
                })?
                .with_root_certificates(root_store)
                .with_no_client_auth();
            Ok(config)
        }
        SslMode::VerifyCa => {
            // Verify cert chain but NOT hostname.
            let root_store = build_root_store(info.sslrootcert.as_deref())?;
            let verifier = NoHostnameVerifier::new(root_store, provider.clone());

            let config = rustls::ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map_err(|e| {
                    ReplicationError::permanent_connection(format!(
                        "Failed to configure TLS protocol versions: {e}"
                    ))
                })?
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(verifier))
                .with_no_client_auth();
            Ok(config)
        }
        SslMode::Require | SslMode::Prefer | SslMode::Allow => {
            // Encrypt but skip certificate verification entirely.
            // This matches libpq's `sslmode=require` behavior.
            let config = rustls::ClientConfig::builder_with_provider(provider.clone())
                .with_safe_default_protocol_versions()
                .map_err(|e| {
                    ReplicationError::permanent_connection(format!(
                        "Failed to configure TLS protocol versions: {e}"
                    ))
                })?
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerification(provider)))
                .with_no_client_auth();
            Ok(config)
        }
        SslMode::Disable => {
            unreachable!("TLS config not needed when sslmode=disable")
        }
    }
}

/// Build a `RootCertStore` from available certificate sources.
///
/// Priority:
/// 1. If `sslrootcert` is provided, load ONLY those CAs (exclusive).
/// 2. Otherwise, load system CAs via `rustls-native-certs` + Mozilla bundle fallback.
fn build_root_store(sslrootcert: Option<&str>) -> Result<rustls::RootCertStore, ReplicationError> {
    let mut store = rustls::RootCertStore::empty();

    // 1. Custom CA file — if specified, use ONLY these CAs
    if let Some(path) = sslrootcert {
        let file = std::fs::File::open(path).map_err(|e| {
            ReplicationError::permanent_connection(format!(
                "Failed to open sslrootcert file '{path}': {e}"
            ))
        })?;
        let mut reader = std::io::BufReader::new(file);
        let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                ReplicationError::permanent_connection(format!(
                    "Failed to parse PEM certificates from '{path}': {e}"
                ))
            })?;
        if certs.is_empty() {
            return Err(ReplicationError::permanent_connection(format!(
                "No certificates found in sslrootcert file '{path}'"
            )));
        }
        for cert in certs {
            store.add(cert).map_err(|e| {
                ReplicationError::permanent_connection(format!(
                    "Failed to add certificate from '{path}': {e}"
                ))
            })?;
        }
        return Ok(store);
    }

    // 2. System CA store (best-effort, skip invalid certs)
    let native_result = rustls_native_certs::load_native_certs();
    let mut system_count = 0usize;
    for cert in native_result.certs {
        if store.add(cert).is_ok() {
            system_count += 1;
        }
    }
    if !native_result.errors.is_empty() {
        tracing::warn!(
            "Some system CA certificates could not be loaded: {:?}",
            native_result.errors
        );
    }
    tracing::debug!("Loaded {system_count} system CA certificates");

    // 3. Mozilla bundle (always add as fallback)
    store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    Ok(store)
}

/// A certificate verifier that accepts any certificate (no verification).
/// Used for `sslmode=require` which only requires encryption, not authentication.
#[derive(Debug)]
struct NoVerification(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for NoVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// A certificate verifier that checks the certificate chain against the root
/// store but does NOT verify the hostname. Used for `sslmode=verify-ca`.
#[derive(Debug)]
struct NoHostnameVerifier {
    inner: Arc<rustls::client::WebPkiServerVerifier>,
}

impl NoHostnameVerifier {
    fn new(roots: rustls::RootCertStore, provider: Arc<rustls::crypto::CryptoProvider>) -> Self {
        let inner =
            rustls::client::WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider)
                .build()
                .expect("failed to build WebPkiServerVerifier");
        Self { inner }
    }
}

impl rustls::client::danger::ServerCertVerifier for NoHostnameVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        ocsp_response: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        // Delegate to the inner WebPki verifier. If it succeeds, great.
        // If it fails ONLY because of hostname mismatch, that's OK for verify-ca.
        // All other errors (expired, untrusted CA, revoked, etc.) must propagate.
        match self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Ok(v) => Ok(v),
            Err(rustls::Error::InvalidCertificate(rustls::CertificateError::NotValidForName)) => {
                // Hostname mismatch is expected and OK for verify-ca
                Ok(rustls::client::danger::ServerCertVerified::assertion())
            }
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::NotValidForNameContext { .. },
            )) => {
                // Same as NotValidForName but with context info
                Ok(rustls::client::danger::ServerCertVerified::assertion())
            }
            Err(other) => Err(other), // Propagate all real errors
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// Send the StartupMessage and handle authentication + parameter negotiation.
///
/// Returns the server version number (e.g. 160001 for 16.1).
async fn startup_and_auth(
    transport: &mut Transport,
    buf: &mut BytesMut,
    info: &ConnInfo,
) -> Result<i32, ReplicationError> {
    // Build startup parameters
    let replication_str;
    let mut params: Vec<(&str, &str)> = vec![
        ("user", &info.user),
        ("database", &info.dbname),
        ("client_encoding", "UTF8"),
    ];

    match info.replication {
        super::conninfo::ReplicationMode::Database => {
            replication_str = "database".to_string();
            params.push(("replication", &replication_str));
        }
        super::conninfo::ReplicationMode::Physical => {
            replication_str = "true".to_string();
            params.push(("replication", &replication_str));
        }
        super::conninfo::ReplicationMode::None => {}
    }

    let startup_msg = wire::build_startup_message(&params);
    wire::write_all(transport, &startup_msg).await?;
    wire::flush(transport).await?;

    // Authenticate
    super::auth::authenticate(transport, buf, &info.user, info.password.as_deref()).await?;

    // Process post-auth messages: ParameterStatus, BackendKeyData, ReadyForQuery
    let mut server_version = 0i32;

    loop {
        let msg = wire::read_message(transport, buf).await?;
        if msg.is_empty() {
            continue;
        }

        match msg[0] {
            b'S' => {
                // ParameterStatus: key\0value\0
                let payload = &msg[5..];
                let (key, consumed) = wire::read_cstring(payload);
                let (value, _) = wire::read_cstring(&payload[consumed..]);
                tracing::debug!("ParameterStatus: {key}={value}");

                if key == "server_version" {
                    server_version = parse_server_version(value);
                }
            }
            b'K' => {
                // BackendKeyData: pid(4) + secret_key(4)
                tracing::debug!("Received BackendKeyData");
            }
            b'Z' => {
                // ReadyForQuery — startup complete
                tracing::debug!("Startup complete, server ready");
                break;
            }
            b'E' => {
                let fields = super::error::parse_error_fields(&msg[5..]);
                return Err(ReplicationError::permanent_connection(format!(
                    "Server error during startup: {}",
                    fields
                )));
            }
            b'N' => {
                // NoticeResponse — log and continue
                let fields = super::error::parse_error_fields(&msg[5..]);
                tracing::info!("Server notice: {}", fields);
            }
            _ => {
                tracing::debug!("Skipping message type '{}' during startup", msg[0] as char);
            }
        }
    }

    if server_version < 140000 {
        return Err(ReplicationError::permanent_connection(format!(
            "PostgreSQL version {} is not supported. Requires 14+",
            server_version
        )));
    }

    Ok(server_version)
}

/// Parse a server_version string like "16.1" or "16.1 (Debian 16.1-1)"
/// into the integer format used by libpq (e.g. 160001).
fn parse_server_version(version_str: &str) -> i32 {
    // Take the first word (before any space or parenthetical)
    let version = version_str.split_whitespace().next().unwrap_or("");

    let parts: Vec<&str> = version.split('.').collect();
    match parts.len() {
        1 => {
            // e.g. "16" → 160000
            parts[0].parse::<i32>().unwrap_or(0) * 10000
        }
        2 => {
            // e.g. "16.1" → 160001
            let major = parts[0].parse::<i32>().unwrap_or(0);
            let minor = parts[1].parse::<i32>().unwrap_or(0);
            major * 10000 + minor
        }
        _ => {
            // e.g. "14.2.1" → 140201
            let major = parts[0].parse::<i32>().unwrap_or(0);
            let minor = parts[1].parse::<i32>().unwrap_or(0);
            let patch = parts[2].parse::<i32>().unwrap_or(0);
            major * 10000 + minor * 100 + patch
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a ConnInfo with test defaults, allowing override of specific fields.
    fn test_conninfo(sslmode: SslMode, sslrootcert: Option<String>) -> ConnInfo {
        ConnInfo {
            host: "localhost".to_string(),
            port: 5432,
            user: "test".to_string(),
            password: None,
            dbname: "test".to_string(),
            sslmode,
            sslrootcert,
            replication: super::super::conninfo::ReplicationMode::None,
            connect_timeout: 0,
            keepalives: true,
            keepalives_idle: 120,
            keepalives_interval: 10,
            keepalives_count: 3,
        }
    }

    #[test]
    fn test_parse_server_version() {
        assert_eq!(parse_server_version("16.1"), 160001);
        assert_eq!(parse_server_version("14.2"), 140002);
        assert_eq!(parse_server_version("16.1 (Debian 16.1-1)"), 160001);
        assert_eq!(parse_server_version("15"), 150000);
    }

    #[test]
    fn test_parse_server_version_three_part() {
        assert_eq!(parse_server_version("14.2.1"), 140201);
        assert_eq!(parse_server_version("9.6.24"), 90624);
    }

    #[test]
    fn test_parse_server_version_empty() {
        assert_eq!(parse_server_version(""), 0);
    }

    #[test]
    fn test_parse_server_version_garbage() {
        assert_eq!(parse_server_version("abc"), 0);
        assert_eq!(parse_server_version("not.a.version"), 0);
    }

    #[test]
    fn test_parse_server_version_with_extra_text() {
        assert_eq!(
            parse_server_version("16.4 (Ubuntu 16.4-1.pgdg22.04+1)"),
            160004
        );
        assert_eq!(parse_server_version("16.12 - Azure"), 160012);
    }

    #[test]
    fn test_build_tls_config_require() {
        // Install crypto provider for test
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let info = test_conninfo(SslMode::Require, None);
        let config = build_tls_config(&info);
        assert!(config.is_ok());
    }

    #[test]
    fn test_build_tls_config_verify_full() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let info = test_conninfo(SslMode::VerifyFull, None);
        let config = build_tls_config(&info);
        assert!(config.is_ok());
    }

    #[test]
    fn test_build_tls_config_verify_ca() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let info = test_conninfo(SslMode::VerifyCa, None);
        let config = build_tls_config(&info);
        assert!(config.is_ok());
    }

    #[test]
    fn test_build_root_store_default() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        // No sslrootcert → should load system + Mozilla CAs
        let store = build_root_store(None).unwrap();
        // Mozilla bundle alone has 100+ certs, so we should have at least that
        assert!(store.len() > 50, "Expected many CAs, got {}", store.len());
    }

    #[test]
    fn test_build_root_store_custom_file() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        // Create a temp PEM file with a self-signed cert for testing
        let pem_content = include_str!("../../../load-tests/fixtures/test_ca.pem");
        let dir = std::env::temp_dir().join("pg_walstream_test");
        std::fs::create_dir_all(&dir).unwrap();
        let pem_path = dir.join("test_ca.pem");
        std::fs::write(&pem_path, pem_content).unwrap();

        let store = build_root_store(Some(pem_path.to_str().unwrap())).unwrap();
        // Should contain ONLY the certs from our file, not system/Mozilla
        assert!(
            store.len() >= 1,
            "Expected at least 1 CA from custom file, got {}",
            store.len()
        );

        // Clean up
        let _ = std::fs::remove_file(&pem_path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn test_build_root_store_missing_file() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let result = build_root_store(Some("/nonexistent/path/ca.pem"));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("sslrootcert"),
            "Error should mention sslrootcert: {err}"
        );
    }

    #[test]
    fn test_build_tls_config_verify_full_with_custom_ca() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let info = test_conninfo(SslMode::VerifyFull, Some("/nonexistent/ca.pem".to_string()));
        // Should fail because the file doesn't exist
        let config = build_tls_config(&info);
        assert!(config.is_err());
    }

    #[test]
    fn test_build_tls_config_require_ignores_sslrootcert() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let info = test_conninfo(SslMode::Require, Some("/nonexistent/ca.pem".to_string()));
        // sslrootcert is ignored for require mode — should succeed
        let config = build_tls_config(&info);
        assert!(config.is_ok());
    }
}
