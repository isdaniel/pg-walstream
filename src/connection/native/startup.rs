//! TLS negotiation and PostgreSQL startup sequence.
//!
//! Handles:
//! 1. TCP connection
//! 2. SSLRequest → TLS handshake (via rustls) if required
//! 3. StartupMessage with protocol v3.0
//! 4. Authentication delegation to `auth.rs`
//! 5. Backend parameter processing until ReadyForQuery

use super::conninfo::{ConnInfo, SslMode, SslNegotiation};
use super::wire;
use crate::error::ReplicationError;
use bytes::BytesMut;
use rustls_pki_types::pem::PemObject;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, BufReader};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

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

/// Size of the read buffer wrapping the TLS stream.
///
/// Wrapping the TLS stream in a `BufReader` reduces syscall overhead because
/// TLS record processing benefits from reading in larger chunks rather than
/// many small reads.
///
/// - A single TLS record payload is at most 16,384 bytes (2^14, per RFC 8446).
/// - `postgres-openssl` and `postgres-native-tls` use 8,192 (one typical record).
/// - For **replication streaming**, we use 16,384 so the buffer can hold a full
///   maximum-size TLS record, reducing the chance of partial-record reads during
///   sustained high-throughput WAL consumption.
const TLS_BUF_SIZE: usize = 16_384;

/// The transport layer — either plain TCP or TLS-wrapped TCP.
///
/// The TLS variant wraps the `TlsStream` in a `BufReader` to reduce read
/// syscall frequency during high-throughput WAL streaming.
pub enum Transport {
    Plain(TcpStream),
    Tls(BufReader<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl Transport {
    /// Get the TLS `tls-server-end-point` channel binding data, if this is a TLS connection.
    ///
    /// Per RFC 5929: hash the server's DER-encoded end-entity certificate.
    /// The hash algorithm depends on the certificate's signature algorithm:
    /// - MD5 or SHA-1 → use SHA-256
    /// - Otherwise → use the signature algorithm's hash (SHA-256, SHA-384, SHA-512)
    pub fn tls_server_end_point(&self) -> Option<Vec<u8>> {
        match self {
            Transport::Tls(buf_reader) => {
                let tls_stream = buf_reader.get_ref();
                let (_, conn) = tls_stream.get_ref();
                let certs = conn.peer_certificates()?;
                let ee_cert_der = certs.first()?;

                // Parse the certificate to determine the signature algorithm
                let hash = compute_tls_server_end_point_hash(ee_cert_der.as_ref())?;
                Some(hash)
            }
            Transport::Plain(_) => None,
        }
    }
}

/// Compute the `tls-server-end-point` hash per RFC 5929.
///
/// Parses the certificate's signature algorithm OID to select the correct
/// hash: MD5/SHA-1 → SHA-256, others → matching hash. Falls back to SHA-256
/// if the OID is unrecognized.
fn compute_tls_server_end_point_hash(cert_der: &[u8]) -> Option<Vec<u8>> {
    use aws_lc_rs::digest;

    // The signature algorithm OID is the second-to-last sequence in the
    // top-level certificate SEQUENCE. Rather than pulling in a full X.509
    // parser, we look for well-known OID byte patterns in the DER.
    //
    // Signature algorithm OIDs we care about:
    //   sha256WithRSAEncryption: 1.2.840.113549.1.1.11  → 06 09 2a 86 48 86 f7 0d 01 01 0b
    //   sha384WithRSAEncryption: 1.2.840.113549.1.1.12  → 06 09 2a 86 48 86 f7 0d 01 01 0c
    //   sha512WithRSAEncryption: 1.2.840.113549.1.1.13  → 06 09 2a 86 48 86 f7 0d 01 01 0d
    //   ecdsa-with-SHA256:       1.2.840.10045.4.3.2    → 06 08 2a 86 48 ce 3d 04 03 02
    //   ecdsa-with-SHA384:       1.2.840.10045.4.3.3    → 06 08 2a 86 48 ce 3d 04 03 03
    //   ecdsa-with-SHA512:       1.2.840.10045.4.3.4    → 06 08 2a 86 48 ce 3d 04 03 04
    //   md5WithRSAEncryption:    1.2.840.113549.1.1.4   → 06 09 2a 86 48 86 f7 0d 01 01 04
    //   sha1WithRSAEncryption:   1.2.840.113549.1.1.5   → 06 09 2a 86 48 86 f7 0d 01 01 05

    // RSA signature OID prefix: 2a 86 48 86 f7 0d 01 01
    let rsa_prefix = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01];
    // ECDSA signature OID prefix: 2a 86 48 ce 3d 04 03
    let ecdsa_prefix = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03];

    let algorithm = if let Some(pos) = find_subsequence(cert_der, rsa_prefix) {
        let suffix_pos = pos + rsa_prefix.len();
        if suffix_pos < cert_der.len() {
            match cert_der[suffix_pos] {
                0x04 | 0x05 => &digest::SHA256, // MD5 or SHA-1 → use SHA-256 per RFC 5929
                0x0b => &digest::SHA256,        // sha256WithRSA
                0x0c => &digest::SHA384,        // sha384WithRSA
                0x0d => &digest::SHA512,        // sha512WithRSA
                _ => &digest::SHA256,           // unknown → default to SHA-256
            }
        } else {
            &digest::SHA256
        }
    } else if let Some(pos) = find_subsequence(cert_der, ecdsa_prefix) {
        let suffix_pos = pos + ecdsa_prefix.len();
        if suffix_pos < cert_der.len() {
            match cert_der[suffix_pos] {
                0x02 => &digest::SHA256, // ecdsa-with-SHA256
                0x03 => &digest::SHA384, // ecdsa-with-SHA384
                0x04 => &digest::SHA512, // ecdsa-with-SHA512
                _ => &digest::SHA256,    // unknown → default to SHA-256
            }
        } else {
            &digest::SHA256
        }
    } else {
        // Unknown signature algorithm → default to SHA-256
        &digest::SHA256
    };

    let hash = digest::digest(algorithm, cert_der);
    Some(hash.as_ref().to_vec())
}

/// Find the first occurrence of `needle` in `haystack`.
fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
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
///
/// Supports two modes:
/// - **Postgres** (default): Send SSLRequest, wait for `'S'`/`'N'`, then handshake.
/// - **Direct** (PG 17+): Skip SSLRequest, send TLS ClientHello immediately with
///   ALPN `"postgresql"`. Falls back to standard negotiation on failure.
async fn negotiate_tls(tcp: TcpStream, info: &ConnInfo) -> Result<Transport, ReplicationError> {
    if info.sslnegotiation == SslNegotiation::Direct {
        // Direct mode: attempt TLS handshake immediately with ALPN.
        // On failure, fall back to standard SSLRequest negotiation with a fresh connection.
        match negotiate_tls_direct(tcp, info).await {
            Ok(transport) => return Ok(transport),
            Err(e) => {
                tracing::debug!(
                    "Direct SSL negotiation failed, falling back to standard SSLRequest: {e}"
                );
                // The TCP connection is consumed by the failed handshake attempt.
                // Open a new one for the standard SSLRequest fallback.
                let addr = format!("{}:{}", info.host, info.port);
                let tcp = tcp_connect(&addr, info).await?;
                return negotiate_tls_standard(tcp, info).await;
            }
        }
    }

    negotiate_tls_standard(tcp, info).await
}

/// Standard PostgreSQL TLS negotiation via SSLRequest.
async fn negotiate_tls_standard(
    mut tcp: TcpStream,
    info: &ConnInfo,
) -> Result<Transport, ReplicationError> {
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
            Ok(Transport::Tls(BufReader::with_capacity(
                TLS_BUF_SIZE,
                tls_stream,
            )))
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

/// Direct TLS negotiation (PostgreSQL 17+).
///
/// Skips the SSLRequest round-trip entirely: sends TLS ClientHello immediately
/// with ALPN protocol `"postgresql"` so the server knows this is a PostgreSQL
/// connection rather than HTTPS or another protocol.
///
/// This saves one full network round-trip compared to standard negotiation.
async fn negotiate_tls_direct(
    tcp: TcpStream,
    info: &ConnInfo,
) -> Result<Transport, ReplicationError> {
    let mut tls_config = build_tls_config(info)?;

    // Set ALPN to "postgresql" — this is how the server distinguishes a direct
    // TLS PostgreSQL connection from other protocols.
    tls_config.alpn_protocols = vec![b"postgresql".to_vec()];

    let connector = TlsConnector::from(Arc::new(tls_config));

    let server_name = rustls::pki_types::ServerName::try_from(info.host.as_str())
        .map_err(|e| {
            ReplicationError::permanent_connection(format!("Invalid server name for TLS: {e}"))
        })?
        .to_owned();

    let tls_stream = connector.connect(server_name, tcp).await.map_err(|e| {
        ReplicationError::transient_connection(format!("Direct TLS handshake failed: {e}"))
    })?;

    tracing::debug!("Direct TLS connection established (ALPN postgresql)");
    Ok(Transport::Tls(BufReader::with_capacity(
        TLS_BUF_SIZE,
        tls_stream,
    )))
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
/// 2. Otherwise, load system CAs from well-known OS paths + Mozilla bundle fallback.
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
        let certs: Vec<rustls_pki_types::CertificateDer<'static>> =
            rustls_pki_types::CertificateDer::pem_reader_iter(&mut reader)
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
    let native_result = certs::load_native_certs();
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

    // Authenticate (with channel binding if TLS is active)
    let tls_server_end_point = transport.tls_server_end_point();
    super::auth::authenticate(
        transport,
        buf,
        &info.user,
        info.password.as_deref(),
        tls_server_end_point,
    )
    .await?;

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
            sslnegotiation: super::super::conninfo::SslNegotiation::Postgres,
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

    // === Direct SSL negotiation ===

    #[test]
    fn test_build_tls_config_has_no_alpn_by_default() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let info = test_conninfo(SslMode::Require, None);
        let config = build_tls_config(&info).unwrap();
        assert!(
            config.alpn_protocols.is_empty(),
            "Standard TLS config should have no ALPN protocols set"
        );
    }

    #[test]
    fn test_direct_ssl_sets_alpn_postgresql() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let info = test_conninfo(SslMode::Require, None);
        let mut config = build_tls_config(&info).unwrap();
        // Simulate what negotiate_tls_direct does
        config.alpn_protocols = vec![b"postgresql".to_vec()];
        assert_eq!(config.alpn_protocols, vec![b"postgresql".to_vec()]);
    }

    #[test]
    fn test_sslnegotiation_default_is_postgres() {
        let info = test_conninfo(SslMode::Require, None);
        assert_eq!(
            info.sslnegotiation,
            super::super::conninfo::SslNegotiation::Postgres
        );
    }

    // === find_subsequence ===

    #[test]
    fn test_find_subsequence_found() {
        let haystack = &[0x01, 0x02, 0x03, 0x04, 0x05];
        assert_eq!(find_subsequence(haystack, &[0x02, 0x03]), Some(1));
    }

    #[test]
    fn test_find_subsequence_at_start() {
        let haystack = &[0x01, 0x02, 0x03];
        assert_eq!(find_subsequence(haystack, &[0x01, 0x02]), Some(0));
    }

    #[test]
    fn test_find_subsequence_at_end() {
        let haystack = &[0x01, 0x02, 0x03];
        assert_eq!(find_subsequence(haystack, &[0x02, 0x03]), Some(1));
    }

    #[test]
    fn test_find_subsequence_not_found() {
        let haystack = &[0x01, 0x02, 0x03];
        assert_eq!(find_subsequence(haystack, &[0x04, 0x05]), None);
    }

    #[test]
    fn test_find_subsequence_single_byte_needle() {
        let haystack = &[0x01, 0x02, 0x03];
        assert_eq!(find_subsequence(haystack, &[0x02]), Some(1));
    }

    #[test]
    fn test_find_subsequence_needle_longer_than_haystack() {
        let haystack = &[0x01];
        assert_eq!(find_subsequence(haystack, &[0x01, 0x02]), None);
    }

    // === compute_tls_server_end_point_hash ===

    /// Build a minimal DER certificate stub containing a known RSA signature algorithm OID.
    /// This is NOT a valid X.509 cert, but contains the OID bytes that
    /// `compute_tls_server_end_point_hash` scans for.
    fn cert_with_rsa_oid(suffix: u8) -> Vec<u8> {
        let mut data = vec![0x30, 0x82, 0x00, 0x10]; // SEQUENCE header (fake)
        data.extend_from_slice(&[0x06, 0x09]); // OID tag + length
                                               // RSA OID prefix: 2a 86 48 86 f7 0d 01 01
        data.extend_from_slice(&[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, suffix]);
        data.extend_from_slice(&[0x00; 20]); // padding
        data
    }

    /// Build a minimal DER certificate stub containing a known ECDSA signature algorithm OID.
    fn cert_with_ecdsa_oid(suffix: u8) -> Vec<u8> {
        let mut data = vec![0x30, 0x82, 0x00, 0x10]; // SEQUENCE header (fake)
        data.extend_from_slice(&[0x06, 0x08]); // OID tag + length
                                               // ECDSA OID prefix: 2a 86 48 ce 3d 04 03
        data.extend_from_slice(&[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, suffix]);
        data.extend_from_slice(&[0x00; 20]); // padding
        data
    }

    #[test]
    fn test_channel_binding_hash_sha256_with_rsa() {
        use aws_lc_rs::digest;
        let cert = cert_with_rsa_oid(0x0b); // sha256WithRSAEncryption
        let hash = compute_tls_server_end_point_hash(&cert).unwrap();
        let expected = digest::digest(&digest::SHA256, &cert);
        assert_eq!(hash, expected.as_ref());
    }

    #[test]
    fn test_channel_binding_hash_sha384_with_rsa() {
        use aws_lc_rs::digest;
        let cert = cert_with_rsa_oid(0x0c); // sha384WithRSAEncryption
        let hash = compute_tls_server_end_point_hash(&cert).unwrap();
        let expected = digest::digest(&digest::SHA384, &cert);
        assert_eq!(hash, expected.as_ref());
    }

    #[test]
    fn test_channel_binding_hash_sha512_with_rsa() {
        use aws_lc_rs::digest;
        let cert = cert_with_rsa_oid(0x0d); // sha512WithRSAEncryption
        let hash = compute_tls_server_end_point_hash(&cert).unwrap();
        let expected = digest::digest(&digest::SHA512, &cert);
        assert_eq!(hash, expected.as_ref());
    }

    #[test]
    fn test_channel_binding_hash_md5_falls_back_to_sha256() {
        use aws_lc_rs::digest;
        let cert = cert_with_rsa_oid(0x04); // md5WithRSAEncryption
        let hash = compute_tls_server_end_point_hash(&cert).unwrap();
        // RFC 5929: MD5 → use SHA-256
        let expected = digest::digest(&digest::SHA256, &cert);
        assert_eq!(hash, expected.as_ref());
    }

    #[test]
    fn test_channel_binding_hash_sha1_falls_back_to_sha256() {
        use aws_lc_rs::digest;
        let cert = cert_with_rsa_oid(0x05); // sha1WithRSAEncryption
        let hash = compute_tls_server_end_point_hash(&cert).unwrap();
        // RFC 5929: SHA-1 → use SHA-256
        let expected = digest::digest(&digest::SHA256, &cert);
        assert_eq!(hash, expected.as_ref());
    }

    #[test]
    fn test_channel_binding_hash_ecdsa_sha256() {
        use aws_lc_rs::digest;
        let cert = cert_with_ecdsa_oid(0x02); // ecdsa-with-SHA256
        let hash = compute_tls_server_end_point_hash(&cert).unwrap();
        let expected = digest::digest(&digest::SHA256, &cert);
        assert_eq!(hash, expected.as_ref());
    }

    #[test]
    fn test_channel_binding_hash_ecdsa_sha384() {
        use aws_lc_rs::digest;
        let cert = cert_with_ecdsa_oid(0x03); // ecdsa-with-SHA384
        let hash = compute_tls_server_end_point_hash(&cert).unwrap();
        let expected = digest::digest(&digest::SHA384, &cert);
        assert_eq!(hash, expected.as_ref());
    }

    #[test]
    fn test_channel_binding_hash_ecdsa_sha512() {
        use aws_lc_rs::digest;
        let cert = cert_with_ecdsa_oid(0x04); // ecdsa-with-SHA512
        let hash = compute_tls_server_end_point_hash(&cert).unwrap();
        let expected = digest::digest(&digest::SHA512, &cert);
        assert_eq!(hash, expected.as_ref());
    }

    #[test]
    fn test_channel_binding_hash_unknown_oid_defaults_sha256() {
        use aws_lc_rs::digest;
        // No known OID present → should default to SHA-256
        let cert = vec![0x30, 0x82, 0x00, 0x10, 0xAA, 0xBB, 0xCC, 0xDD];
        let hash = compute_tls_server_end_point_hash(&cert).unwrap();
        let expected = digest::digest(&digest::SHA256, &cert);
        assert_eq!(hash, expected.as_ref());
    }

    #[test]
    fn test_channel_binding_hash_unknown_rsa_suffix_defaults_sha256() {
        use aws_lc_rs::digest;
        let cert = cert_with_rsa_oid(0xFF); // unknown RSA suffix
        let hash = compute_tls_server_end_point_hash(&cert).unwrap();
        let expected = digest::digest(&digest::SHA256, &cert);
        assert_eq!(hash, expected.as_ref());
    }

    // === Transport::tls_server_end_point on plain connection ===

    #[test]
    fn test_transport_plain_returns_none_for_tls_endpoint() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let std_tcp = std::net::TcpStream::connect(addr).unwrap();
        std_tcp.set_nonblocking(true).unwrap();
        let _peer = listener.accept().unwrap();

        let tcp = match tokio::runtime::Handle::try_current() {
            Ok(_) => tokio::net::TcpStream::from_std(std_tcp).unwrap(),
            Err(_) => {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .build()
                    .unwrap();
                let _guard = rt.enter();
                tokio::net::TcpStream::from_std(std_tcp).unwrap()
            }
        };

        let transport = Transport::Plain(tcp);
        assert!(transport.tls_server_end_point().is_none());
    }
}
