//! TLS negotiation and PostgreSQL startup sequence.
//!
//! Handles:
//! 1. TCP connection
//! 2. SSLRequest → TLS handshake (via rustls) if required
//! 3. StartupMessage with protocol v3.0
//! 4. Authentication delegation to `auth.rs`
//! 5. Backend parameter processing until ReadyForQuery

use super::conninfo::{ConnInfo, ReplicationMode, SslMode, SslNegotiation, SSLROOTCERT_SYSTEM};
use super::error::error_frame;
use super::wire;
use crate::error::ReplicationError;
use bytes::BytesMut;
use memchr::memmem;
use rustls::pki_types::pem::PemObject;
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
/// - For **replication streaming**, we use 65,536 so one `read()` can pull several maximum-size TLS records at once. The CopyData drain loop (`copy::drain_read_buffer`) then parses more WAL messages per wakeup,  cutting syscalls and — on the worker-thread driver — cross-thread batch handoffs during sustained high-throughput WAL consumption.
///
/// Exposed `pub(super)` so `copy.rs` can pin its read-headroom invariant
/// (`MIN_HEADROOM > TLS_BUF_SIZE`) to this value at compile time, guaranteeing tokio's `BufReader` always bypasses its buffer on the CopyData hot path.
pub(super) const TLS_BUF_SIZE: usize = 65_536;

/// The transport layer — either plain TCP or TLS-wrapped TCP.
///
/// The TLS variant wraps the `TlsStream` in a `BufReader` to reduce read
/// syscall frequency during high-throughput WAL streaming.
// One Transport per connection on the I/O hot path. Boxing the Tls variant to
// satisfy large_enum_variant would add a pointer indirection to every read and
// write, so the size gap is an intentional tradeoff.
#[allow(clippy::large_enum_variant)]
pub(super) enum Transport {
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
    fn tls_server_end_point(&self) -> Option<Vec<u8>> {
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

    // Each prefix is followed by one byte naming the hash. The listed bytes are
    // the ones that pick something other than SHA-256: RSA's 0x04 (MD5) and 0x05
    // (SHA-1) fall back to it per RFC 5929, like any unknown byte or OID.
    type Wider<'a> = [(u8, &'a digest::Algorithm); 2];
    let oids: [(&[u8], Wider); 2] = [
        (
            &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01],
            [(0x0c, &digest::SHA384), (0x0d, &digest::SHA512)],
        ),
        (
            &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03],
            [(0x03, &digest::SHA384), (0x04, &digest::SHA512)],
        ),
    ];

    let mut algorithm = &digest::SHA256;
    for (prefix, wider) in oids {
        if let Some(pos) = memmem::find(cert_der, prefix) {
            let suffix = cert_der.get(pos + prefix.len());
            if let Some((_, wide)) = wider.iter().find(|(byte, _)| Some(byte) == suffix) {
                algorithm = wide;
            }
            // The first OID found decides, even when it names nothing wider.
            break;
        }
    }

    let hash = digest::digest(algorithm, cert_der);
    Some(hash.as_ref().to_vec())
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
///
/// `connect_timeout` bounds the **whole** establishment, not just the TCP handshake.
/// libpq documents it as "Maximum time to wait while connecting" — i.e. through
/// `CONNECTION_OK` — and the phases after the SYN are where a connection actually
/// parks: a stalled proxy, a half-dead load balancer or a `DROP`-after-accept
/// firewall completes the TCP handshake and then goes silent, leaving the TLS
/// negotiation or the auth exchange waiting on a read that never arrives.
/// `tcp_user_timeout` does not rescue that case — it is Linux-only and only fires on
/// *unacknowledged* writes, so a peer that ACKs and says nothing defeats it.
///
/// [`tcp_connect`] keeps its own inner timeout: it fires first for the common
/// SYN-timeout case and names the address, which is a better diagnostic than the
/// generic deadline below. The outer budget covers the phases that have none.
pub(super) async fn connect(
    info: &ConnInfo,
) -> Result<(Transport, i32, BytesMut), ReplicationError> {
    if info.connect_timeout == 0 {
        return connect_inner(info).await;
    }

    let budget = Duration::from_secs(info.connect_timeout);
    tokio::time::timeout(budget, connect_inner(info))
        .await
        .map_err(|_| {
            ReplicationError::transient_connection(format!(
                "Connection to {}:{} timed out after {}s before the startup handshake completed",
                info.host, info.port, info.connect_timeout
            ))
        })?
}

async fn connect_inner(info: &ConnInfo) -> Result<(Transport, i32, BytesMut), ReplicationError> {
    let addr = format!("{}:{}", info.host, info.port);

    // Whatever is wrong locally with the TLS setup (an unusable client identity, unreadable
    // root certificates) is a configuration error, settled before a socket is opened: the
    // plaintext fallback below is for a server or network that cannot do TLS, and must not
    // answer a bad `sslcert`/`sslkey` by quietly dropping encryption.
    let tls = match info.sslmode {
        SslMode::Disable => None,
        _ => Some(Arc::new(build_tls_config(info)?)),
    };

    let tcp = tcp_connect(&addr, info).await?;

    let mut transport = match tls {
        None => Transport::Plain(tcp),
        Some(tls) if matches!(info.sslmode, SslMode::Prefer | SslMode::Allow) => {
            // For prefer/allow: attempt TLS, but fall back to a new plaintext.
            match negotiate_tls(tcp, info, tls).await {
                Ok(t) => t,
                Err(e) => {
                    tracing::debug!("TLS negotiation failed, falling back to plain: {e}");
                    let tcp = tcp_connect(&addr, info).await?;
                    Transport::Plain(tcp)
                }
            }
        }
        Some(tls) => negotiate_tls(tcp, info, tls).await?,
    };

    // Perform startup + auth
    let mut buf = BytesMut::with_capacity(8192);
    let server_version = startup_and_auth(&mut transport, &mut buf, info).await?;

    Ok((transport, server_version, buf))
}

/// Open a TCP connection with optional timeout, TCP_NODELAY, and keepalive.
async fn tcp_connect(addr: &str, info: &ConnInfo) -> Result<TcpStream, ReplicationError> {
    let connect = TcpStream::connect(addr);
    let tcp = if info.connect_timeout > 0 {
        let timeout = Duration::from_secs(info.connect_timeout);
        tokio::time::timeout(timeout, connect).await.map_err(|_| {
            ReplicationError::transient_connection(format!(
                "Connection to {addr} timed out after {}s",
                info.connect_timeout
            ))
        })?
    } else {
        connect.await
    }
    .map_err(|e| {
        ReplicationError::transient_connection(format!("Failed to connect to {addr}: {e}"))
    })?;

    tcp.set_nodelay(true).ok();

    if info.keepalives {
        configure_tcp_keepalive(&tcp, info);
    }

    if info.tcp_user_timeout > 0 {
        configure_tcp_user_timeout(&tcp, info);
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

/// Configure `TCP_USER_TIMEOUT` on the socket using socket2.
///
/// The only knob that bounds a blocking control-plane round-trip. Keepalives
/// bound the *idle* case; they never fire while a reply is outstanding, because
/// the connection is not idle. So a `DROP_REPLICATION_SLOT` issued from snapshot
/// cleanup in `Drop` against a peer that has silently gone away waits for the
/// keepalive cycle only because the socket eventually goes idle — this sets a
/// deadline on the unacknowledged write itself.
///
/// Best-effort on purpose: a sandbox that refuses the `setsockopt` must not fail
/// an otherwise-healthy connection, so this warns rather than returning `Err`.
#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "fuchsia",
    target_os = "cygwin",
))]
fn configure_tcp_user_timeout(tcp: &TcpStream, info: &ConnInfo) {
    use socket2::SockRef;

    let sock = SockRef::from(tcp);
    // Milliseconds — libpq's unit for this option, unlike every other timeout
    // in `ConnInfo`, which is seconds.
    if let Err(e) = sock.set_tcp_user_timeout(Some(Duration::from_millis(info.tcp_user_timeout))) {
        tracing::warn!("Failed to set TCP_USER_TIMEOUT: {e}");
    } else {
        tracing::debug!("TCP_USER_TIMEOUT configured: {}ms", info.tcp_user_timeout);
    }
}

/// No-op fallback: `TCP_USER_TIMEOUT` is Linux-family only.
///
/// Warns rather than staying silent — the whole reason this option was wired up
/// is that it used to be accepted and then discarded with no diagnostic.
#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "fuchsia",
    target_os = "cygwin",
)))]
fn configure_tcp_user_timeout(_tcp: &TcpStream, info: &ConnInfo) {
    tracing::warn!(
        "tcp_user_timeout={}ms ignored: TCP_USER_TIMEOUT is not available on this platform",
        info.tcp_user_timeout
    );
}

/// Perform TLS negotiation: send SSLRequest, check response, do handshake.
///
/// Supports two modes:
/// - **Postgres** (default): Send SSLRequest, wait for `'S'`/`'N'`, then handshake.
/// - **Direct** (PG 17+): Skip SSLRequest, send TLS ClientHello immediately with
///   ALPN `"postgresql"`. Falls back to standard negotiation on failure.
async fn negotiate_tls(
    tcp: TcpStream,
    info: &ConnInfo,
    tls: Arc<rustls::ClientConfig>,
) -> Result<Transport, ReplicationError> {
    if info.sslnegotiation == SslNegotiation::Direct {
        // Direct mode: attempt TLS handshake immediately with ALPN.
        // On failure, fall back to standard SSLRequest negotiation with a fresh connection.
        match negotiate_tls_direct(tcp, info, &tls).await {
            Ok(transport) => return Ok(transport),
            Err(e) => {
                tracing::debug!(
                    "Direct SSL negotiation failed, falling back to standard SSLRequest: {e}"
                );
                // The TCP connection is consumed by the failed handshake attempt.
                // Open a new one for the standard SSLRequest fallback.
                let addr = format!("{}:{}", info.host, info.port);
                let tcp = tcp_connect(&addr, info).await?;
                return negotiate_tls_standard(tcp, info, tls).await;
            }
        }
    }

    negotiate_tls_standard(tcp, info, tls).await
}

/// Standard PostgreSQL TLS negotiation via SSLRequest.
async fn negotiate_tls_standard(
    mut tcp: TcpStream,
    info: &ConnInfo,
    tls: Arc<rustls::ClientConfig>,
) -> Result<Transport, ReplicationError> {
    // Send SSLRequest
    let ssl_req = wire::build_ssl_request();
    wire::write_all(&mut tcp, &ssl_req).await?;
    wire::flush(&mut tcp).await?;

    let response = wire::read_byte(&mut tcp).await?;

    match response {
        b'S' => {
            // Server supports SSL — do the handshake
            tls_handshake(
                tcp,
                &info.host,
                TlsConnector::from(tls),
                "TLS",
                "TLS connection established",
            )
            .await
        }
        b'N' => {
            // Server does not support SSL
            if info.sslmode.requires_tls() {
                Err(ReplicationError::permanent_connection(
                    "Server does not support SSL but sslmode=require".to_string(),
                ))
            } else {
                tracing::debug!("Server doesn't support SSL, falling back to plain");
                Ok(Transport::Plain(tcp))
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
    tls: &rustls::ClientConfig,
) -> Result<Transport, ReplicationError> {
    let mut tls_config = tls.clone();

    // Set ALPN to "postgresql" — this is how the server distinguishes a direct
    // TLS PostgreSQL connection from other protocols.
    tls_config.alpn_protocols = vec![b"postgresql".to_vec()];

    tls_handshake(
        tcp,
        &info.host,
        TlsConnector::from(Arc::new(tls_config)),
        "Direct TLS",
        "Direct TLS connection established (ALPN postgresql)",
    )
    .await
}

/// Run the TLS handshake with `host` over `tcp` and wrap the stream for reading.
///
/// `what` names the handshake in its error, `established` is the debug line logged once it succeeds.
async fn tls_handshake(
    tcp: TcpStream,
    host: &str,
    connector: TlsConnector,
    what: &str,
    established: &str,
) -> Result<Transport, ReplicationError> {
    let server_name = rustls::pki_types::ServerName::try_from(host)
        .map_err(|e| {
            ReplicationError::permanent_connection(format!("Invalid server name for TLS: {e}"))
        })?
        .to_owned();

    let tls_stream = connector.connect(server_name, tcp).await.map_err(|e| {
        ReplicationError::transient_connection(format!("{what} handshake failed: {e}"))
    })?;

    tracing::debug!("{established}");
    log_negotiated_suite(&tls_stream);
    Ok(Transport::Tls(BufReader::with_capacity(
        TLS_BUF_SIZE,
        tls_stream,
    )))
}

/// Log the negotiated TLS cipher suite for verification that hardware-accelerated
/// AES-GCM (via aws-lc-rs) was selected rather than an unexpected fallback.
fn log_negotiated_suite(tls_stream: &tokio_rustls::client::TlsStream<TcpStream>) {
    if let Some(suite) = tls_stream.get_ref().1.negotiated_cipher_suite() {
        tracing::info!("TLS negotiated cipher suite: {:?}", suite.suite());
    }
}

/// Build a rustls `ClientConfig` based on the sslmode.
fn build_tls_config(info: &ConnInfo) -> Result<rustls::ClientConfig, ReplicationError> {
    let provider = crypto_provider();

    match info.sslmode {
        SslMode::VerifyFull => {
            // Full verification: check cert chain + hostname
            let root_store = build_root_store(info.sslrootcert.as_deref())?;
            let config = tls_builder(provider)?.with_root_certificates(root_store);
            with_client_identity(config, info)
        }
        SslMode::VerifyCa => {
            // Verify cert chain but NOT hostname.
            let root_store = build_root_store(info.sslrootcert.as_deref())?;
            let verifier = NoHostnameVerifier::new(root_store, provider.clone());

            let config = tls_builder(provider)?
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(verifier));
            with_client_identity(config, info)
        }
        SslMode::Require | SslMode::Prefer | SslMode::Allow => {
            // Encrypt but skip certificate verification entirely.
            // This matches libpq's `sslmode=require` behavior.
            let config = tls_builder(provider.clone())?
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerification(provider)));
            with_client_identity(config, info)
        }
        SslMode::Disable => {
            unreachable!("TLS config not needed when sslmode=disable")
        }
    }
}

fn tls_builder(
    provider: Arc<rustls::crypto::CryptoProvider>,
) -> Result<rustls::ConfigBuilder<rustls::ClientConfig, rustls::WantsVerifier>, ReplicationError> {
    rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| {
            ReplicationError::permanent_connection(format!(
                "Failed to configure TLS protocol versions: {e}"
            ))
        })
}

/// Finish a `ClientConfig`, presenting the `sslcert`/`sslkey` pair if there is one.
///
/// rustls sends it only when the server asks for a certificate, so it is harmless against a server that does not.
fn with_client_identity(
    builder: rustls::ConfigBuilder<rustls::ClientConfig, rustls::client::WantsClientCert>,
    info: &ConnInfo,
) -> Result<rustls::ClientConfig, ReplicationError> {
    let (Some(cert_path), Some(key_path)) = (info.sslcert.as_deref(), info.sslkey.as_deref())
    else {
        return Ok(builder.with_no_client_auth());
    };
    let Some((chain, key)) = load_client_identity(cert_path, key_path)? else {
        return Ok(builder.with_no_client_auth());
    };
    builder.with_client_auth_cert(chain, key).map_err(|e| {
        ReplicationError::permanent_connection(format!(
            "sslcert '{cert_path}' and sslkey '{key_path}' cannot be used as a client identity: {e}"
        ))
    })
}

/// The certificate chain and private key of a client identity.
type ClientIdentity = (
    Vec<rustls::pki_types::CertificateDer<'static>>,
    rustls::pki_types::PrivateKeyDer<'static>,
);

/// Read the client certificate chain and private key, at every connect so a rotated pair is picked up on reconnect.
///
/// `Ok(None)` when `sslcert` names a file that does not exist: libpq carries on without a client certificate then (`initialize_SSL`), so a connection string with a stale path keeps working as it did before the pair was used. A certificate that exists but cannot be used, or a missing key, is an error.
///
/// The key may be PKCS#8, PKCS#1 (`RSA PRIVATE KEY`) or SEC1 (`EC PRIVATE KEY`). An encrypted one is refused by name: decrypting it needs `sslpassword`, which this backend does not support. Errors name the path and say what is wrong with the file, never what is in it.
fn load_client_identity(
    cert_path: &str,
    key_path: &str,
) -> Result<Option<ClientIdentity>, ReplicationError> {
    let unreadable = |what: &str, path: &str, why: &dyn std::fmt::Display| {
        ReplicationError::permanent_connection(format!("Failed to read {what} '{path}': {why}"))
    };

    let cert_pem = match std::fs::read(cert_path) {
        Ok(pem) => pem,
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
            ) =>
        {
            tracing::warn!("sslcert '{cert_path}' does not exist: no client certificate is sent");
            return Ok(None);
        }
        Err(e) => return Err(unreadable("sslcert file", cert_path, &e)),
    };
    let chain = rustls::pki_types::CertificateDer::pem_slice_iter(&cert_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            unreadable(
                "PEM certificates from sslcert file",
                cert_path,
                &pem_problem(&e),
            )
        })?;
    if chain.is_empty() {
        return Err(ReplicationError::permanent_connection(format!(
            "No certificates found in sslcert file '{cert_path}'"
        )));
    }

    let key_pem = std::fs::read(key_path).map_err(|e| unreadable("sslkey file", key_path, &e))?;
    // The markers of an encrypted key, not the bare word: a comment may say it.
    const ENCRYPTED_MARKERS: [&[u8]; 2] =
        [b"BEGIN ENCRYPTED PRIVATE KEY", b"Proc-Type: 4,ENCRYPTED"];
    if ENCRYPTED_MARKERS
        .iter()
        .any(|marker| memmem::find(&key_pem, marker).is_some())
    {
        return Err(ReplicationError::permanent_connection(format!(
            "sslkey '{key_path}' is encrypted, and the native backend cannot decrypt it \
             (sslpassword is not supported): decrypt it, or use the `libpq` backend"
        )));
    }
    let key = rustls::pki_types::PrivateKeyDer::from_pem_slice(&key_pem).map_err(|e| {
        unreadable(
            "a PEM private key from sslkey file",
            key_path,
            &pem_problem(&e),
        )
    })?;
    Ok(Some((chain, key)))
}

/// What is wrong with a PEM file, without any of its content.
///
/// pki-types puts the offending line into its error text, and for a file that lost its newlines (a pasted or environment-variable secret) the parser takes the whole key for the section label, so its message would carry the key as a list of byte values into a log line written on every connect attempt.
fn pem_problem(e: &rustls::pki_types::pem::Error) -> String {
    use rustls::pki_types::pem::Error;
    match e {
        Error::Io(io) => format!("I/O error: {io}"),
        Error::NoItemsFound => "it holds no PEM section of the expected kind".to_string(),
        Error::Base64Decode(_) => "a section's base64 body is invalid".to_string(),
        Error::MissingSectionEnd { .. } => "a section has no matching END line".to_string(),
        Error::IllegalSectionStart { .. } => "a BEGIN line is malformed".to_string(),
        _ => "it is not valid PEM".to_string(),
    }
}

/// Build a `RootCertStore` from available certificate sources.
///
/// Priority:
/// 1. If `sslrootcert` is provided, load ONLY those CAs (exclusive).
/// 2. Otherwise, load the bundled Mozilla CA set via `webpki-roots`.
fn build_root_store(sslrootcert: Option<&str>) -> Result<rustls::RootCertStore, ReplicationError> {
    let mut store = rustls::RootCertStore::empty();

    // 1. `system` is libpq's reserved word for "the trusted CA roots from the SSL
    //    implementation" (PG16+), not a file path. Opening it as one would look
    //    for a file literally named `system`. Fall through to the bundled roots.
    if sslrootcert == Some(SSLROOTCERT_SYSTEM) {
        store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        return Ok(store);
    }

    // 2. Custom CA file — if specified, use ONLY these CAs
    if let Some(path) = sslrootcert {
        let file = std::fs::File::open(path).map_err(|e| {
            ReplicationError::permanent_connection(format!(
                "Failed to open sslrootcert file '{path}': {e}"
            ))
        })?;
        let mut reader = std::io::BufReader::new(file);
        let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
            rustls::pki_types::CertificateDer::pem_reader_iter(&mut reader)
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

    // 3. Mozilla CA bundle via webpki-roots
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

/// The StartupMessage for `info`.
///
/// `options` goes out verbatim and `application_name` already resolved against
/// `fallback_application_name`, as libpq sends them; neither is sent when unset.
///
/// `client_encoding` must stay: the server applies `options` first and the other
/// parameters after it, so this is what keeps a `-c client_encoding=...` in
/// `options` from making the stream's UTF-8 decoding read mojibake.
fn build_startup(info: &ConnInfo) -> Result<BytesMut, ReplicationError> {
    let mut params: Vec<(&str, &str)> = vec![
        ("user", &info.user),
        ("database", &info.dbname),
        ("client_encoding", "UTF8"),
    ];
    match info.replication {
        ReplicationMode::Database => params.push(("replication", "database")),
        ReplicationMode::Physical => params.push(("replication", "true")),
        ReplicationMode::None => {}
    }
    if let Some(options) = &info.options {
        params.push(("options", options));
    }
    if let Some(name) = &info.application_name {
        params.push(("application_name", name));
    }
    wire::build_startup_message(&params)
}

/// Send the StartupMessage and handle authentication + parameter negotiation.
///
/// Returns the server version number (e.g. 160001 for 16.1).
async fn startup_and_auth(
    transport: &mut Transport,
    buf: &mut BytesMut,
    info: &ConnInfo,
) -> Result<i32, ReplicationError> {
    let startup_msg = build_startup(info)?;
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
                let fields = error_frame(&msg);
                let message = format!("Server error during startup: {fields}");
                return Err(ReplicationError::from_server_startup(fields, message));
            }
            b'N' => {
                // NoticeResponse — log and continue
                let fields = error_frame(&msg);
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

/// Leading ASCII-digit run of `s`, or 0. Pre-release servers report
/// "19beta3"/"19rc1"/"19devel"; a plain `parse` yields 0 for those, which the
/// caller's `< 140000` floor then rejects as "version 0".
fn leading_num(s: &str) -> i32 {
    s.split(|c: char| !c.is_ascii_digit())
        .next()
        .unwrap_or("")
        .parse()
        .unwrap_or(0)
}

/// Parse a server_version string like "16.1" or "16.1 (Debian 16.1-1)"
/// into the integer format used by libpq (e.g. 160001).
fn parse_server_version(version_str: &str) -> i32 {
    // Take the first word (before any space or parenthetical)
    let version = version_str.split_whitespace().next().unwrap_or("");

    let parts: Vec<&str> = version.split('.').collect();
    match parts.len() {
        1 => {
            // e.g. "16" → 160000, "19beta3" → 190000
            leading_num(parts[0]) * 10000
        }
        2 => {
            // e.g. "16.1" → 160001
            leading_num(parts[0]) * 10000 + leading_num(parts[1])
        }
        _ => {
            // e.g. "14.2.1" → 140201
            leading_num(parts[0]) * 10000 + leading_num(parts[1]) * 100 + leading_num(parts[2])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::wire::testing::frame;
    use super::*;

    /// `sslrootcert=system` is libpq's reserved word for the platform trust
    /// store, not a path. Treating it as a path was the silent-downgrade bug:
    /// `build_root_store` is only reached from the verifying modes, so the bogus
    /// path never even produced a file-open error.
    #[test]
    fn build_root_store_maps_system_to_bundled_roots() {
        let store = build_root_store(Some(SSLROOTCERT_SYSTEM)).expect("system must resolve");
        assert!(!store.is_empty(), "system must load real CA roots");
        assert_eq!(
            store.len(),
            webpki_roots::TLS_SERVER_ROOTS.len(),
            "system must be exactly the bundled Mozilla set"
        );
    }

    /// No `sslrootcert` also uses the bundled roots — same outcome, different
    /// branch, so pin both to keep them from drifting apart.
    #[test]
    fn build_root_store_defaults_to_bundled_roots() {
        let store = build_root_store(None).expect("default must resolve");
        assert_eq!(store.len(), webpki_roots::TLS_SERVER_ROOTS.len());
    }

    /// An ordinary path is still opened as a file; `system` must not have turned
    /// every value into "use the bundled roots".
    #[test]
    fn build_root_store_still_reads_a_real_path() {
        let err = build_root_store(Some("/nonexistent/ca.pem"))
            .expect_err("a missing CA file must fail loudly");
        assert!(err.to_string().contains("/nonexistent/ca.pem"), "{err}");
    }

    const TESTDATA: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/connection/native/testdata"
    );

    fn fixture(name: &str) -> String {
        format!("{TESTDATA}/{name}")
    }

    fn info_with_identity(sslmode: SslMode, cert: &str, key: &str) -> ConnInfo {
        let mut info = test_conninfo(sslmode, None);
        info.sslcert = Some(fixture(cert));
        info.sslkey = Some(fixture(key));
        info
    }

    const VERIFYING_AND_NOT: [SslMode; 3] =
        [SslMode::Require, SslMode::VerifyCa, SslMode::VerifyFull];

    /// Every `ClientConfig` builder presents the pair, for each key encoding rustls reads.
    #[test]
    fn client_identity_is_configured_in_every_tls_mode() {
        for (cert, key) in [
            ("client_ec.crt", "client_ec_pkcs8.key"),
            ("client_ec.crt", "client_ec_sec1.key"),
            ("client_rsa.crt", "client_rsa_pkcs1.key"),
        ] {
            for mode in VERIFYING_AND_NOT {
                let config = build_tls_config(&info_with_identity(mode, cert, key))
                    .unwrap_or_else(|e| panic!("{mode:?} {key}: {e}"));
                assert!(
                    config.client_auth_cert_resolver.has_certs(),
                    "{mode:?} {key}"
                );
            }
        }
    }

    #[test]
    fn no_client_identity_means_no_client_auth() {
        for mode in VERIFYING_AND_NOT {
            let config = build_tls_config(&test_conninfo(mode, None)).unwrap();
            assert!(!config.client_auth_cert_resolver.has_certs(), "{mode:?}");
        }
    }

    #[test]
    fn the_whole_certificate_chain_is_loaded() {
        let bundle = std::env::temp_dir().join(format!("pgws-chain-{}.pem", std::process::id()));
        let both = [
            std::fs::read_to_string(fixture("client_ec.crt")).unwrap(),
            std::fs::read_to_string(fixture("client_rsa.crt")).unwrap(),
        ]
        .concat();
        std::fs::write(&bundle, both).unwrap();
        let loaded =
            load_client_identity(bundle.to_str().unwrap(), &fixture("client_ec_pkcs8.key"));
        std::fs::remove_file(&bundle).unwrap();
        assert_eq!(
            loaded
                .unwrap()
                .expect("a certificate file that exists")
                .0
                .len(),
            2
        );
    }

    /// Each failure is permanent (retrying a bad file cannot fix it) and names the file;
    /// none echoes any of the key material.
    #[test]
    fn unusable_client_identities_fail_permanently_naming_the_file() {
        let key_body = std::fs::read_to_string(fixture("client_ec_pkcs8.key"))
            .unwrap()
            .lines()
            .nth(1)
            .unwrap()
            .to_string();
        for (cert, key, expect) in [
            ("client_ec.crt", "missing.key", "missing.key"),
            (
                "client_ec_pkcs8.key",
                "client_ec_pkcs8.key",
                "No certificates found",
            ),
            ("client_ec.crt", "client_ec.crt", "PEM private key"),
            ("client_ec.crt", "client_ec_encrypted.key", "is encrypted"),
            (
                "client_ec.crt",
                "other_ec_pkcs8.key",
                "cannot be used as a client identity",
            ),
            (
                "client_rsa.crt",
                "client_ec_pkcs8.key",
                "cannot be used as a client identity",
            ),
        ] {
            let err = build_tls_config(&info_with_identity(SslMode::VerifyFull, cert, key))
                .expect_err(&format!("{cert} + {key} must be refused"));
            let shown = err.to_string();
            assert!(err.is_permanent(), "{cert} + {key}: {shown}");
            assert!(shown.contains(expect), "{cert} + {key}: {shown}");
            assert!(
                !shown.contains(&key_body),
                "{cert} + {key} echoed key material"
            );
        }
    }

    /// A scratch file that removes itself, so a failing assertion cannot leave it behind.
    struct TempFile(std::path::PathBuf);
    impl TempFile {
        fn new(name: &str, contents: &str) -> Self {
            let path = std::env::temp_dir().join(format!("pgws-{}-{name}", std::process::id()));
            std::fs::write(&path, contents).unwrap();
            Self(path)
        }
        fn path(&self) -> String {
            self.0.to_str().unwrap().to_string()
        }
    }
    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    /// A key file that lost its newlines (a pasted or environment-variable secret) makes the
    /// PEM parser fold the whole content into its error text. That text is logged on every
    /// connect attempt, so none of it may reach our message, whichever of `sslcert` and
    /// `sslkey` the file is given as.
    #[test]
    fn a_key_file_without_newlines_never_reaches_the_error_message() {
        let original = std::fs::read_to_string(fixture("client_ec_pkcs8.key")).unwrap();
        let body: String = original
            .lines()
            .filter(|l| !l.starts_with("-----"))
            .collect();
        // The same bytes as pki-types renders them: a decimal list.
        let decimal = format!("{:?}", &body.as_bytes()[..24]);
        let decimal = decimal.trim_matches(['[', ']']);
        let one_line = TempFile::new("oneline.key", &original.lines().collect::<String>());
        for (cert, key) in [
            (one_line.path(), fixture("client_ec_pkcs8.key")),
            (fixture("client_ec.crt"), one_line.path()),
        ] {
            let mut info = test_conninfo(SslMode::VerifyFull, None);
            info.sslcert = Some(cert.clone());
            info.sslkey = Some(key.clone());
            let shown = build_tls_config(&info)
                .expect_err("a mangled file must be refused")
                .to_string();
            assert!(
                !shown.contains(&body[..24]),
                "{cert} + {key} leaked the key: {shown}"
            );
            assert!(
                !shown.contains(decimal),
                "{cert} + {key} leaked the key: {shown}"
            );
        }
    }

    /// A certificate file that is PEM in name only is refused, naming the file and what is
    /// wrong, not what it holds.
    #[test]
    fn a_corrupt_certificate_file_is_refused_without_echoing_it() {
        let junk = "notbase64-0123456789-SECRETMARKER";
        let corrupt = TempFile::new(
            "corrupt.crt",
            &format!("-----BEGIN CERTIFICATE-----\n!!{junk}!!\n-----END CERTIFICATE-----\n"),
        );
        let mut info = test_conninfo(SslMode::VerifyFull, None);
        info.sslcert = Some(corrupt.path());
        info.sslkey = Some(fixture("client_ec_pkcs8.key"));
        let err = build_tls_config(&info).expect_err("a corrupt certificate must be refused");
        let shown = err.to_string();
        assert!(err.is_permanent(), "{shown}");
        assert!(shown.contains(&corrupt.path()), "{shown}");
        assert!(shown.contains("base64 body is invalid"), "{shown}");
        assert!(!shown.contains(junk), "{shown}");
    }

    /// Only a real encrypted-key marker refuses a key, not the word anywhere in the file.
    #[test]
    fn encrypted_keys_are_recognised_by_their_markers_not_by_the_word() {
        let plain = std::fs::read_to_string(fixture("client_ec_pkcs8.key")).unwrap();
        let commented = TempFile::new("commented.key", &format!("# not ENCRYPTED\n{plain}"));
        let loaded = load_client_identity(&fixture("client_ec.crt"), &commented.path());
        assert!(
            loaded.is_ok(),
            "a plain key with a comment was refused: {:?}",
            loaded.err()
        );

        let legacy = TempFile::new(
            "legacy.key",
            "-----BEGIN RSA PRIVATE KEY-----\nProc-Type: 4,ENCRYPTED\nDEK-Info: AES-128-CBC,00\n\nAAAA\n-----END RSA PRIVATE KEY-----\n",
        );
        for key in [fixture("client_ec_encrypted.key"), legacy.path()] {
            let err = load_client_identity(&fixture("client_ec.crt"), &key).unwrap_err();
            assert!(err.to_string().contains("is encrypted"), "{key}: {err}");
        }
    }

    /// A broken client identity is a configuration error. Under `sslmode=prefer` and `allow`,
    /// where any TLS-negotiation failure used to be answered by retrying in plaintext, it must
    /// not be: the connection stays an error, and nothing is sent in the clear. It is settled
    /// before a socket is opened, so the server sees no connection at all.
    #[tokio::test]
    async fn a_broken_client_identity_is_an_error_not_a_plaintext_fallback() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        for mode in [SslMode::Prefer, SslMode::Allow, SslMode::Require] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let seen = Arc::new(std::sync::Mutex::new(Vec::<[u8; 8]>::new()));
            let log = seen.clone();
            let server = tokio::spawn(async move {
                // Answers an SSLRequest with 'S', as a TLS-capable server does.
                while let Ok((mut peer, _)) = listener.accept().await {
                    let mut first = [0u8; 8];
                    if peer.read_exact(&mut first).await.is_ok() {
                        log.lock().unwrap().push(first);
                        if first[4..] == 80877103u32.to_be_bytes() {
                            let _ = peer.write_all(b"S").await;
                        }
                    }
                }
            });
            for (cert, key) in [
                ("client_ec.crt", "missing.key"),
                ("client_ec.crt", "other_ec_pkcs8.key"),
                ("client_ec.crt", "client_ec_encrypted.key"),
            ] {
                let mut info = info_with_identity(mode, cert, key);
                info.host = addr.ip().to_string();
                info.port = addr.port();
                let Err(err) = connect(&info).await else {
                    panic!("{mode:?} {key}: a broken identity must refuse the connection");
                };
                assert!(err.to_string().contains(key), "{mode:?} {key}: {err}");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            server.abort();
            let seen = seen.lock().unwrap().clone();
            assert!(
                seen.is_empty(),
                "{mode:?}: a socket was opened for a configuration error: {seen:?}"
            );
        }
    }

    /// The other half of the same rule: a good identity must not stop `prefer` from doing what
    /// it is for, so a server without TLS still gets the plaintext startup.
    #[tokio::test]
    async fn prefer_with_a_good_identity_still_falls_back_when_the_server_has_no_tls() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut peer, _) = listener.accept().await.unwrap();
            let mut ssl_request = [0u8; 8];
            peer.read_exact(&mut ssl_request).await.unwrap();
            peer.write_all(b"N").await.unwrap();
            let mut len = [0u8; 4];
            peer.read_exact(&mut len).await.unwrap();
            let mut msg = len.to_vec();
            msg.resize(i32::from_be_bytes(len) as usize, 0);
            peer.read_exact(&mut msg[4..]).await.unwrap();
            msg
        });
        let mut info = info_with_identity(SslMode::Prefer, "client_ec.crt", "client_ec_pkcs8.key");
        info.host = addr.ip().to_string();
        info.port = addr.port();
        assert!(
            connect(&info).await.is_err(),
            "the peer hangs up after the startup"
        );
        let pairs = startup_pairs(&server.await.unwrap());
        assert!(
            pairs.contains(&("user".to_string(), "test".to_string())),
            "{pairs:?}"
        );
    }

    /// libpq carries on without a client certificate when `sslcert` names a file that does not
    /// exist (`initialize_SSL` in fe-secure-openssl.c), and so did this backend before it
    /// started sending certificates: a connection string with a stale path must keep working.
    /// A certificate that is there but unusable, or a missing key, is still an error.
    #[test]
    fn a_missing_sslcert_sends_no_client_certificate() {
        for mode in VERIFYING_AND_NOT {
            let config = build_tls_config(&info_with_identity(
                mode,
                "missing.crt",
                "client_ec_pkcs8.key",
            ))
            .unwrap_or_else(|e| panic!("{mode:?}: {e}"));
            assert!(!config.client_auth_cert_resolver.has_certs(), "{mode:?}");
        }
        assert!(
            build_tls_config(&info_with_identity(
                SslMode::Require,
                "client_ec.crt",
                "missing.key"
            ))
            .is_err(),
            "a certificate without its key stays an error"
        );
    }

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
            sslcert: None,
            sslkey: None,
            sslnegotiation: super::super::conninfo::SslNegotiation::Postgres,
            replication: super::super::conninfo::ReplicationMode::None,
            application_name: None,
            options: None,
            connect_timeout: 0,
            keepalives: true,
            keepalives_idle: 120,
            keepalives_interval: 10,
            keepalives_count: 3,
            tcp_user_timeout: 0,
        }
    }

    /// A listener on a free local port, and a `ConnInfo` that connects to it.
    async fn listener_and_info(sslmode: SslMode) -> (tokio::net::TcpListener, ConnInfo) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut info = test_conninfo(sslmode, None);
        info.host = addr.ip().to_string();
        info.port = addr.port();
        (listener, info)
    }

    /// The `(key, value)` pairs a server reads out of a StartupMessage.
    fn startup_pairs(msg: &[u8]) -> Vec<(String, String)> {
        let mut rest = &msg[8..]; // length, then protocol version
        let mut pairs = Vec::new();
        while rest.first().is_some_and(|b| *b != 0) {
            let (key, n) = wire::read_cstring(rest);
            rest = &rest[n..];
            let (value, n) = wire::read_cstring(rest);
            rest = &rest[n..];
            pairs.push((key.to_string(), value.to_string()));
        }
        pairs
    }

    /// `application_name` and `options` are session parameters the libpq backend
    /// has always sent. The native backend parsed them and dropped them, so the
    /// walsender showed no name in `pg_stat_activity` and a pinned `TimeZone`
    /// never applied.
    #[test]
    fn the_startup_message_carries_application_name_and_options() {
        let mut info = test_conninfo(SslMode::Disable, None);
        info.replication = ReplicationMode::Database;
        info.application_name = Some("pgws cdc".to_string());
        info.options = Some("-c TimeZone=Asia/Tokyo -c lock_timeout=2s".to_string());

        let mut pairs = startup_pairs(&build_startup(&info).unwrap());
        pairs.sort();

        let want = [
            ("application_name", "pgws cdc"),
            ("client_encoding", "UTF8"),
            ("database", "test"),
            ("options", "-c TimeZone=Asia/Tokyo -c lock_timeout=2s"),
            ("replication", "database"),
            ("user", "test"),
        ];
        assert_eq!(
            pairs,
            want.map(|(k, v)| (k.to_string(), v.to_string())),
            "the server must read exactly these parameters"
        );
    }

    /// The replication mode is still the one the connection string asked for: `database`
    /// for a logical session, `true` for a physical one.
    #[test]
    fn the_startup_message_keeps_the_replication_mode() {
        for (mode, want) in [
            (ReplicationMode::Database, Some("database")),
            (ReplicationMode::Physical, Some("true")),
            (ReplicationMode::None, None),
        ] {
            let mut info = test_conninfo(SslMode::Disable, None);
            info.replication = mode;
            let pairs = startup_pairs(&build_startup(&info).unwrap());
            let sent = pairs.iter().find(|(k, _)| k == "replication");
            assert_eq!(sent.map(|(_, v)| v.as_str()), want, "{mode:?}");
        }
    }

    /// Nothing is sent for what was not asked for, so a plain connection string
    /// still produces the startup message it always did.
    #[test]
    fn unset_session_parameters_are_not_sent() {
        let pairs = startup_pairs(&build_startup(&test_conninfo(SslMode::Disable, None)).unwrap());
        let mut keys: Vec<&str> = pairs.iter().map(|(k, _)| k.as_str()).collect();
        keys.sort_unstable();
        assert_eq!(keys, ["client_encoding", "database", "user"]);
    }

    /// `%00` decodes to a real NUL (libpq refuses it, `url_decode` passes it
    /// through), and a quoted key=value string can hold one too. The startup
    /// message is where a NUL does harm: it ends a value early and the rest is read
    /// as further parameters. These switched the session to replication mode, or
    /// set `options`, from a string that asked for neither.
    #[test]
    fn a_nul_in_the_connection_string_cannot_inject_startup_parameters() {
        for dsn in [
            "postgresql://u%00replication%00database@h/db",
            "postgresql://u@h/db?application_name=x%00options%00-c%20search_path%3Devil",
            "host=h options='-c a=b\0application_name\0evil'",
        ] {
            let info = ConnInfo::parse(dsn).unwrap();
            let err = build_startup(&info).unwrap_err();
            assert!(
                matches!(err, ReplicationError::Config(_)),
                "{dsn:?}: expected a Config error, got {err:?}"
            );
        }
    }

    /// The parameters reach the socket, not only the builder: this is what a
    /// server reads off a real connection.
    #[tokio::test]
    async fn connect_sends_the_session_parameters_to_the_server() {
        use tokio::io::AsyncReadExt;

        let (listener, mut info) = listener_and_info(SslMode::Disable).await;
        let server = tokio::spawn(async move {
            let (mut peer, _) = listener.accept().await.unwrap();
            let mut len = [0u8; 4];
            peer.read_exact(&mut len).await.unwrap();
            let mut msg = len.to_vec();
            msg.resize(i32::from_be_bytes(len) as usize, 0);
            peer.read_exact(&mut msg[4..]).await.unwrap();
            msg // dropping `peer` here ends the handshake
        });

        info.application_name = Some("pgws-wire".to_string());
        info.options = Some("-c TimeZone=Asia/Tokyo".to_string());

        // The peer hangs up instead of authenticating: only what it received matters.
        assert!(connect(&info).await.is_err());
        let pairs = startup_pairs(&server.await.unwrap());
        for want in [
            ("application_name", "pgws-wire"),
            ("options", "-c TimeZone=Asia/Tokyo"),
        ] {
            assert!(
                pairs.contains(&(want.0.to_string(), want.1.to_string())),
                "{want:?} missing from {pairs:?}"
            );
        }
    }

    /// Reads the option back off a real socket, so it fails on the two traps the
    /// implementation actually has: treating the value as seconds (libpq's unit
    /// is milliseconds, unlike every other timeout in `ConnInfo`), and nesting
    /// the call inside the `keepalives` branch, which would make
    /// `keepalives=0 tcp_user_timeout=..` a silent no-op.
    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "fuchsia",
        target_os = "cygwin",
    ))]
    #[tokio::test]
    async fn tcp_user_timeout_reaches_the_socket_in_milliseconds() {
        use socket2::SockRef;
        use std::net::{TcpListener, TcpStream as StdTcpStream};

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let std_stream = StdTcpStream::connect(listener.local_addr().unwrap()).unwrap();
        std_stream.set_nonblocking(true).unwrap();
        let tcp = TcpStream::from_std(std_stream).unwrap();

        let mut info = test_conninfo(SslMode::Disable, None);
        info.tcp_user_timeout = 15000;
        // Off, to prove the setting does not ride on the keepalive branch.
        info.keepalives = false;

        configure_tcp_user_timeout(&tcp, &info);

        assert_eq!(
            SockRef::from(&tcp).tcp_user_timeout().unwrap(),
            Some(Duration::from_millis(15000)),
            "15000 must land as 15s, not 15000s — libpq's unit here is milliseconds"
        );
    }

    /// The exact gap the outer deadline closes: the peer completes the TCP
    /// handshake and then says nothing. `connect_timeout` used to wrap only
    /// `TcpStream::connect`, so the auth exchange below waited forever — on the
    /// caller's thread, because `connect()` is reached through a blocking
    /// `ready_rx.recv()`. A stalled proxy or `DROP`-after-accept firewall is the
    /// real-world shape.
    #[tokio::test]
    async fn connect_timeout_bounds_the_handshake_not_just_the_syn() {
        let (listener, mut info) = listener_and_info(SslMode::Disable).await;

        // Accept, hold the socket open, and never answer the StartupMessage.
        let _silent_peer = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
        });

        info.connect_timeout = 1;

        let started = std::time::Instant::now();
        // `Transport` is not `Debug`, so `expect_err` is unavailable here.
        let err = match connect(&info).await {
            Ok(_) => panic!("a silent peer must not hang the connect"),
            Err(e) => e,
        };

        assert!(
            err.to_string().contains("timed out"),
            "expected a timeout, got: {err}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "deadline did not fire promptly: {:?}",
            started.elapsed()
        );
        // Transient: the peer may well be healthy on the next attempt.
        assert!(
            !err.is_permanent(),
            "a handshake timeout must stay retryable"
        );
    }

    /// An ErrorResponse after authentication and before ReadyForQuery reaches the
    /// caller with the server's response. `55000` here is the database refusing
    /// connections, so it stays a connection error instead of naming a slot.
    #[tokio::test]
    async fn startup_error_response_keeps_the_server_response() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (listener, mut info) = listener_and_info(SslMode::Disable).await;
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let len = usize::try_from(stream.read_u32().await.unwrap()).unwrap();
            let mut startup = vec![0; len - 4];
            stream.read_exact(&mut startup).await.unwrap();
            let fields = b"SFATAL\0VFATAL\0C55000\0\
                Mdatabase \"test\" is not currently accepting connections\0\
                RCheckMyDatabase\0\0";
            // AuthenticationOk, then the ErrorResponse.
            let mut reply = frame(b'R', &[0; 4]);
            reply.extend_from_slice(&frame(b'E', fields));
            stream.write_all(&reply).await.unwrap();
            stream
        });

        info.connect_timeout = 5;
        // `Transport` is not `Debug`, so `expect_err` is unavailable here.
        let err = connect(&info)
            .await
            .err()
            .expect("the server refused the startup");
        drop(peer.await.unwrap());

        assert!(
            matches!(err, ReplicationError::PermanentConnection(..)),
            "{err:?}"
        );
        assert_eq!(
            err.sqlstate(),
            Some(sqlstate::pg::OBJECT_NOT_IN_PREREQUISITE_STATE)
        );
        assert_eq!(
            err.server_error()
                .and_then(|server| server.routine.as_deref()),
            Some("CheckMyDatabase")
        );
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

    /// Pre-release servers report a non-numeric major ("19beta3"). These must
    /// clear the 140000 floor, not parse to 0 and be rejected as unsupported.
    #[test]
    fn test_parse_server_version_prerelease() {
        assert_eq!(parse_server_version("19beta3"), 190000);
        assert_eq!(parse_server_version("19rc1"), 190000);
        assert_eq!(parse_server_version("19devel"), 190000);
        assert_eq!(parse_server_version("18beta1 (Debian 18~beta1-1)"), 180000);
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

        // No sslrootcert → should load Mozilla CA bundle
        let store = build_root_store(None).unwrap();
        // Mozilla bundle has 100+ certs
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
        // Should contain ONLY the certs from our file, not the Mozilla bundle
        assert!(
            !store.is_empty(),
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
    fn test_build_root_store_empty_pem_file() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let dir = std::env::temp_dir().join("pg_walstream_test_empty");
        std::fs::create_dir_all(&dir).unwrap();
        let pem_path = dir.join("empty.pem");
        std::fs::write(&pem_path, b"").unwrap();

        let result = build_root_store(Some(pem_path.to_str().unwrap()));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("No certificates found"),
            "Error should mention no certs: {err}"
        );

        let _ = std::fs::remove_file(&pem_path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn test_build_root_store_malformed_pem_file() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let dir = std::env::temp_dir().join("pg_walstream_test_bad");
        std::fs::create_dir_all(&dir).unwrap();
        let pem_path = dir.join("bad.pem");
        // Valid PEM frame around garbage base64 payload — tickles the parse-error branch.
        std::fs::write(
            &pem_path,
            b"-----BEGIN CERTIFICATE-----\nnot-valid-base64-or-der!!!\n-----END CERTIFICATE-----\n",
        )
        .unwrap();

        let result = build_root_store(Some(pem_path.to_str().unwrap()));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.to_lowercase().contains("pem") || err.contains("sslrootcert"),
            "Error should mention PEM/sslrootcert: {err}"
        );

        let _ = std::fs::remove_file(&pem_path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn test_build_root_store_default_uses_webpki_roots() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        // The default path must return exactly the Mozilla bundle size — this guards
        // against accidentally reintroducing a system-CA loader.
        let store = build_root_store(None).unwrap();
        assert_eq!(
            store.len(),
            webpki_roots::TLS_SERVER_ROOTS.len(),
            "Default root store should contain exactly the Mozilla bundle"
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

    /// The handshake error names which handshake failed, and a host that is not a
    /// valid TLS server name is refused before any byte is sent.
    #[tokio::test]
    async fn tls_handshake_errors_name_the_handshake() {
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut peer, _)) = listener.accept().await {
                let _ = peer.write_all(b"not a TLS server hello at all").await;
            }
        });
        let connector = || {
            let config = build_tls_config(&test_conninfo(SslMode::Require, None)).unwrap();
            TlsConnector::from(Arc::new(config))
        };

        let tcp = TcpStream::connect(addr).await.unwrap();
        let Err(err) = tls_handshake(tcp, "localhost", connector(), "Direct TLS", "up").await
        else {
            panic!("a peer that does not speak TLS must fail the handshake");
        };
        assert!(
            err.to_string().contains("Direct TLS handshake failed: "),
            "{err}"
        );
        assert!(err.is_transient(), "{err:?}");

        let tcp = TcpStream::connect(addr).await.unwrap();
        let Err(err) = tls_handshake(tcp, "not a host name", connector(), "TLS", "up").await else {
            panic!("an invalid server name must be refused");
        };
        assert!(
            err.to_string().contains("Invalid server name for TLS: "),
            "{err}"
        );
        assert!(err.is_permanent(), "{err:?}");
    }

    // === memmem::find, which locates the OIDs and PEM markers ===

    #[test]
    fn test_memmem_find() {
        let cases: [(&[u8], &[u8], Option<usize>); 6] = [
            // found
            (&[0x01, 0x02, 0x03, 0x04, 0x05], &[0x02, 0x03], Some(1)),
            // at start
            (&[0x01, 0x02, 0x03], &[0x01, 0x02], Some(0)),
            // at end
            (&[0x01, 0x02, 0x03], &[0x02, 0x03], Some(1)),
            // not found
            (&[0x01, 0x02, 0x03], &[0x04, 0x05], None),
            // single byte needle
            (&[0x01, 0x02, 0x03], &[0x02], Some(1)),
            // needle longer than haystack
            (&[0x01], &[0x01, 0x02], None),
        ];
        for (haystack, needle, want) in cases {
            assert_eq!(
                memmem::find(haystack, needle),
                want,
                "{haystack:?} {needle:?}"
            );
        }
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
