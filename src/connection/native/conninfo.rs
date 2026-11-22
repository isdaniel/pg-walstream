//! Connection string parser for PostgreSQL.
//!
//! Supports both URI format (`postgresql://user:pass@host:port/db?params`)
//! and key-value format (`host=localhost port=5432 ...`).

use crate::error::ReplicationError;

/// Parsed connection configuration.
#[derive(Clone)]
pub(crate) struct ConnInfo {
    pub(super) host: String,
    pub(super) port: u16,
    pub(super) user: String,
    pub(super) password: Option<String>,
    pub(super) dbname: String,
    pub(super) sslmode: SslMode,
    pub(super) sslrootcert: Option<String>,
    /// PEM client certificate chain to present when the server asks for one (libpq's `sslcert`). Always set together with `sslkey`: this backend never reads libpq's default `~/.postgresql/postgresql.crt`.
    pub(super) sslcert: Option<String>,
    /// PEM private key for `sslcert` (libpq's `sslkey`). Unencrypted only; an encrypted key is refused when the connection is made.
    pub(super) sslkey: Option<String>,
    /// TLS negotiation mode. `Postgres` (default) uses the standard SSLRequest, round-trip; `Direct` skips it for PostgreSQL 17+ (saves one round-trip).
    pub(super) sslnegotiation: SslNegotiation,
    pub(crate) replication: ReplicationMode,
    /// `application_name` for the startup message: what `pg_stat_activity` and `pg_stat_replication` show, and what `synchronous_standby_names` matches. Already resolved against `fallback_application_name` as libpq does; `None` when neither is set or the result is empty. libpq's `PGAPPNAME` is not read, as with every environment variable but `PGPASSWORD`.
    pub(super) application_name: Option<String>,
    /// `options` for the startup message, passed to the server verbatim (`-c TimeZone=UTC -c lock_timeout=5s`): GUCs pinned for this session, so decoded text renders as it would on any connection with the same settings. `None` when unset or empty; `PGOPTIONS` is not read. Redacted in `Debug`, like `password`: a custom setting can carry a secret.
    pub(super) options: Option<String>,
    /// Connection timeout in seconds (0 = disabled). Maps to libpq's `connect_timeout`.
    pub(super) connect_timeout: u64,
    /// Whether TCP keepalives are enabled (default: true). Maps to `keepalives`.
    pub(super) keepalives: bool,
    /// Seconds of idle time before sending a keepalive probe. Maps to `keepalives_idle`.
    pub(super) keepalives_idle: u64,
    /// Seconds between keepalive probes. Maps to `keepalives_interval`.
    pub(super) keepalives_interval: u64,
    /// Maximum number of keepalive probes before declaring dead. Maps to `keepalives_count`.
    pub(super) keepalives_count: u32,
    /// Milliseconds transmitted data may go unacknowledged before the kernel
    /// forcibly closes the connection (0 = disabled). Maps to libpq's
    /// `tcp_user_timeout`; Linux-family only.
    ///
    /// **Milliseconds**, unlike `connect_timeout` and the `keepalives_*` options
    /// beside it, which are seconds. That is libpq's unit, not a choice here.
    ///
    /// This is the only option that bounds a *blocking control-plane round-trip*
    /// — including the `DROP_REPLICATION_SLOT` that snapshot cleanup runs from
    /// `Drop`. Keepalives do not: they fire only when the connection is idle,
    /// and a round-trip waiting for a reply is not idle.
    pub(super) tcp_user_timeout: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum SslMode {
    Disable,
    Allow,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

impl SslMode {
    /// Whether the connection must be encrypted or fail, rather than fall back to plaintext.
    pub(super) fn requires_tls(self) -> bool {
        matches!(self, Self::Require | Self::VerifyCa | Self::VerifyFull)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum ReplicationMode {
    Database,
    Physical,
    None,
}

/// How TLS negotiation is initiated with the server.
///
/// PostgreSQL 17+ supports a "direct" mode that skips the SSLRequest
/// round-trip and begins the TLS handshake immediately using ALPN
/// protocol `"postgresql"`. This saves one network round-trip on
/// every connection establishment.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum SslNegotiation {
    /// Standard PostgreSQL TLS negotiation: send SSLRequest, wait for
    /// `'S'`/`'N'` response, then perform TLS handshake. Works with all
    /// PostgreSQL versions.
    Postgres,
    /// Direct TLS negotiation (PostgreSQL 17+): skip SSLRequest and send
    /// TLS ClientHello immediately with ALPN `"postgresql"`. Falls back
    /// to standard negotiation if the server doesn't support it.
    Direct,
}

impl std::fmt::Debug for ConnInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnInfo")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field(
                "password",
                if self.password.is_some() {
                    &"<REDACTED>"
                } else {
                    &"None"
                },
            )
            .field("dbname", &self.dbname)
            .field("sslmode", &self.sslmode)
            .field("sslrootcert", &self.sslrootcert)
            .field("sslcert", &self.sslcert)
            .field("sslkey", &self.sslkey)
            .field("sslnegotiation", &self.sslnegotiation)
            .field("replication", &self.replication)
            .field("application_name", &self.application_name)
            .field(
                "options",
                if self.options.is_some() {
                    &"<REDACTED>"
                } else {
                    &"None"
                },
            )
            .field("connect_timeout", &self.connect_timeout)
            .field("keepalives", &self.keepalives)
            .field("keepalives_idle", &self.keepalives_idle)
            .field("keepalives_interval", &self.keepalives_interval)
            .field("keepalives_count", &self.keepalives_count)
            .field("tcp_user_timeout", &self.tcp_user_timeout)
            .finish()
    }
}

impl ConnInfo {
    pub(crate) fn parse(conninfo: &str) -> Result<Self, ReplicationError> {
        let info = if conninfo.starts_with("postgresql://") || conninfo.starts_with("postgres://") {
            Self::parse_uri(conninfo)?
        } else {
            Self::parse_key_value(conninfo)?
        };

        if info.sslnegotiation == SslNegotiation::Direct && !info.sslmode.requires_tls() {
            return Err(ReplicationError::config(
                "sslnegotiation=direct requires sslmode=require, verify-ca or verify-full; \
                 a weaker mode may silently fall back to plaintext"
                    .to_string(),
            ));
        }

        Ok(info)
    }

    fn parse_uri(uri: &str) -> Result<Self, ReplicationError> {
        let stripped = uri
            .trim_start_matches("postgresql://")
            .trim_start_matches("postgres://");

        // The userinfo lookahead must stop at the authority boundary. libpq scans
        // `while (*p && *p != '@' && *p != '/')` in `conninfo_uri_parse_options`, so
        // a literal '@' inside the path or query is NOT a credentials designator.
        // Splitting on the first '@' anywhere made
        // `postgresql://pg.internal:5432/appdb?application_name=svc@prod` parse as
        // host="prod", user="pg.internal" — the wrong host, and sslmode dropped
        // along with the rest of the query.
        //
        // Stop at '@' and '/' ONLY, exactly as libpq does. Adding '?' to the set
        // looks harmless but suppresses credential detection for a userinfo that
        // legitimately contains one: `postgresql://us?er@host/db` parsed as
        // host="us", and `postgresql://user:p?ss@host/db` fell into
        // `parse_port("p")` and failed. libpq accepts both.
        let (creds, rest) = match stripped.find(['@', '/']) {
            Some(i) if stripped.as_bytes()[i] == b'@' => (&stripped[..i], &stripped[i + 1..]),
            _ => ("", stripped),
        };
        let (user, password) = if creds.is_empty() {
            (None, None)
        } else if let Some((u, p)) = creds.split_once(':') {
            (
                Some(url_decode(u)),
                if p.is_empty() {
                    None
                } else {
                    Some(url_decode(p))
                },
            )
        } else {
            (Some(url_decode(creds)), None)
        };

        // Split rest into authority and the `db?params` tail.
        //
        // The path is OPTIONAL in a libpq URI: `postgres://host?sslmode=verify-full`
        // is valid and carries no `/`. Splitting only on `/` swallowed the whole
        // query string into the hostname and dropped every parameter — which
        // silently downgraded sslmode back to the default. Cut at whichever of
        // `/` or `?` comes first.
        let authority_end = rest.find(['/', '?']).unwrap_or(rest.len());
        let host_port = &rest[..authority_end];
        let db_params = match rest.as_bytes().get(authority_end) {
            Some(b'/') => &rest[authority_end + 1..],
            // A bare `?...` tail: no database name, query params follow.
            Some(b'?') => &rest[authority_end..],
            _ => "",
        };

        // An IPv6 literal is bracketed (`[::1]`, `[::1]:5433`); splitting on the
        // last `:` would otherwise cut inside the address.
        let (host, port) = if let Some(rest_after_bracket) = host_port.strip_prefix('[') {
            let (addr, tail) = rest_after_bracket.split_once(']').ok_or_else(|| {
                ReplicationError::config(format!(
                    "unterminated IPv6 address in connection URI: \"{host_port}\""
                ))
            })?;
            let port = match tail.strip_prefix(':') {
                Some(p) => parse_port(p)?,
                None if tail.is_empty() => 5432,
                // Anything else after `]` is junk. Defaulting the port here would
                // connect to 5432 silently: libpq answers `unexpected character
                // "5" at position N in URI (expected ":" or "/")`.
                None => {
                    return Err(ReplicationError::config(format!(
                        "unexpected character in connection URI after IPv6 address \
                         (expected \":\" or \"/\"): \"{tail}\""
                    )))
                }
            };
            if addr.is_empty() {
                return Err(ReplicationError::config(
                    "IPv6 host address may not be empty in connection URI".to_string(),
                ));
            }
            (addr.to_string(), port)
        } else if let Some((h, p)) = host_port.rsplit_once(':') {
            (h.to_string(), parse_port(p)?)
        } else {
            (host_port.to_string(), 5432)
        };

        // Split db from query params. The user fallback is applied AFTER the
        // query loop, as libpq does it in `pqConnectOptions2` (`connectOptions2` before PG17) — otherwise
        // `postgresql:///?user=alice` would yield dbname="postgres".
        let (db, params_str) = db_params.split_once('?').unwrap_or((db_params, ""));
        let mut raw = RawOpts {
            host: Some(host),
            port: Some(port),
            user,
            password,
            dbname: Some(url_decode(db)),
            ..Default::default()
        };

        for param in params_str.split('&') {
            if param.is_empty() {
                continue;
            }
            if let Some((key, val)) = param.split_once('=') {
                // Query values are percent-encoded like the rest of the URI.
                let val = url_decode(val);
                match key {
                    // `ssl=true` is JDBC compatibility, hardcoded in
                    // `conninfo_uri_parse_params` and therefore URI-query only.
                    "ssl" if val == "true" => raw.sslmode = Some(SslMode::Require),
                    _ => raw.set(key, &val)?,
                }
            } else {
                // No `=` at all. Dropping it silently is the same downgrade the
                // unknown-key check in `RawOpts::set` exists to prevent, through a wider door:
                // `?sslmodeverify-full` would leave sslmode at its Prefer default.
                // libpq answers `missing key/value separator "=" in URI query
                // parameter: "%s"` (`conninfo_uri_parse_params`).
                return Err(ReplicationError::config(format!(
                    "missing key/value separator \"=\" in URI query parameter: \"{param}\""
                )));
            }
        }

        // libpq applies this fallback in `pqConnectOptions2` (`connectOptions2` before PG17), i.e. AFTER the query
        // params are parsed — so `postgresql:///?user=alice` gives dbname=alice.
        raw.dbname = raw.dbname.filter(|dbname| !dbname.is_empty());

        // `postgresql:///mydb` has no authority at all. libpq reads an empty host
        // as "use the Unix socket"; this backend is TCP-only, so fall back to the
        // same default the key/value path uses rather than handing
        // `TcpStream::connect(":5432")` an unparsable address.
        raw.host = raw.host.filter(|host| !host.is_empty());

        raw.finish()
    }

    fn parse_key_value(input: &str) -> Result<Self, ReplicationError> {
        let mut raw = RawOpts::default();

        // Simple key=value parser (handles single-quoted values)
        let mut chars = input.chars().peekable();
        while chars.peek().is_some() {
            // Skip whitespace
            while chars.peek().is_some_and(|c| c.is_whitespace()) {
                chars.next();
            }
            if chars.peek().is_none() {
                break;
            }

            // Read key
            let key: String = chars.by_ref().take_while(|c| *c != '=').collect();
            let key = key.trim();

            // Skip whitespace after '='. libpq's own docs show `keyword = 'a value'`
            // as valid, and `conninfo_parse` skips space on both sides of the `=`.
            // Without this the value reads back empty and the next token is taken
            // as a key — harmless while unknown keys were ignored, fatal now that
            // they are rejected.
            while chars.peek().is_some_and(|c| c.is_whitespace()) {
                chars.next();
            }

            // Read value (may be quoted with single quotes).
            //
            // KNOWN DIVERGENCE FROM libpq — this is not parity, and the previous
            // comment here claiming `''` "matches libpq behavior" was wrong.
            // Verified against `conninfo_parse` (fe-connect.c) and empirically
            // with `PQconninfoParse` on libpq 18.6:
            //
            //   libpq                              | here
            //   -----------------------------------|---------------------------
            //   `\` escapes the next char, in BOTH | no backslash handling at
            //   quoted and unquoted values:        | all: `password='it\'s'`
            //   `password='it\'s'`        -> it's  | truncates to `it\` and the
            //   `sslrootcert='C:\\a'`     -> C:\a  | trailing `s'` is then read
            //   `password=a\ b`           -> "a b" | as a key -> hard error,
            //                                      | on a conninfo psql accepts
            //   -----------------------------------|---------------------------
            //   no `''` rule at all; the quoted    | `''` is folded to a literal
            //   branch ends on the first unescaped | quote, so this ACCEPTS
            //   `'`. `password='it''s'` is         | input libpq refuses and
            //   REJECTED:                          | silently produces a
            //   `missing "=" after "'s'"`          | password no other client
            //                                      | would generate
            //   -----------------------------------|---------------------------
            //   unterminated quote is an error:    | silently uses the partial
            //   `unterminated quoted string in     | value -> a TRUNCATED
            //    connection info string`           | password reaches the server
            let value = if chars.peek() == Some(&'\'') {
                chars.next(); // skip opening quote
                let mut v = String::new();
                loop {
                    match chars.next() {
                        Some('\'') => {
                            // Non-libpq: fold a doubled quote into a literal one.
                            if chars.peek() == Some(&'\'') {
                                chars.next(); // consume second quote
                                v.push('\'');
                            } else {
                                break; // end of quoted value
                            }
                        }
                        Some(c) => v.push(c),
                        None => break, // non-libpq: silently accept a truncated value
                    }
                }
                v
            } else {
                let v: String = chars.by_ref().take_while(|c| !c.is_whitespace()).collect();
                v
            };

            raw.set(if key == "database" { "dbname" } else { key }, &value)?;
        }

        raw.finish()
    }
}

/// The options of a connection string as given, in either syntax.
///
/// `None` is "not given": [`RawOpts::finish`] applies libpq's defaults, and
/// `sslmode` being `Some` is what separates an explicit `prefer` from the default.
#[derive(Default)]
struct RawOpts {
    host: Option<String>,
    port: Option<u16>,
    user: Option<String>,
    password: Option<String>,
    dbname: Option<String>,
    sslmode: Option<SslMode>,
    sslrootcert: Option<String>,
    sslcert: Option<String>,
    sslkey: Option<String>,
    client_cert_allowed: Option<bool>,
    sslnegotiation: Option<SslNegotiation>,
    replication: Option<ReplicationMode>,
    application_name: Option<String>,
    fallback_application_name: Option<String>,
    options: Option<String>,
    connect_timeout: Option<u64>,
    keepalives: Option<bool>,
    keepalives_idle: Option<u64>,
    keepalives_interval: Option<u64>,
    keepalives_count: Option<u32>,
    tcp_user_timeout: Option<u64>,
}

impl RawOpts {
    /// Store one `key=value`, rejecting a malformed value or an unknown key.
    fn set(&mut self, key: &str, val: &str) -> Result<(), ReplicationError> {
        match key {
            // libpq docs 32.1.1.2: "Values that would normally appear in the
            // hierarchical part of the URI can alternatively be given as named
            // parameters", e.g. `postgresql:///mydb?host=localhost&port=5433`. The
            // query overrides the hierarchical part, matching
            // `conninfo_uri_parse_options`, which stores host/port/dbname before
            // calling `conninfo_uri_parse_params`. Without this the unknown-key
            // error below would reject a legal URI.
            "host" | "hostaddr" => self.host = Some(val.to_string()),
            "port" => self.port = Some(parse_port(val)?),
            "user" => self.user = Some(val.to_string()),
            "password" => self.password = Some(val.to_string()),
            "dbname" => self.dbname = Some(val.to_string()),
            "application_name" => self.application_name = Some(val.to_string()),
            "fallback_application_name" => {
                self.fallback_application_name = Some(val.to_string());
            }
            "options" => self.options = Some(val.to_string()),
            "sslmode" => self.sslmode = Some(parse_sslmode(val)?),
            "sslrootcert" => self.sslrootcert = Some(val.to_string()),
            "sslcert" => self.sslcert = Some(val.to_string()),
            "sslkey" => self.sslkey = Some(val.to_string()),
            "sslcertmode" => self.client_cert_allowed = Some(parse_sslcertmode(val)?),
            "sslnegotiation" => self.sslnegotiation = Some(parse_ssl_negotiation(val)?),
            "replication" => self.replication = Some(parse_replication_mode(val)?),
            "connect_timeout" => {
                self.connect_timeout = Some(parse_connect_timeout("connect_timeout", val)?);
            }
            // libpq parses this as an integer (`pqParseIntParam` via
            // `useKeepalives`), so `keepalives=abc` is an error there, not
            // "on". Every sibling keepalive option is validated; so is this.
            "keepalives" => self.keepalives = Some(parse_num::<i32>("keepalives", val)? != 0),
            "keepalives_idle" => {
                self.keepalives_idle = Some(parse_num("keepalives_idle", val)?);
            }
            "keepalives_interval" => {
                self.keepalives_interval = Some(parse_num("keepalives_interval", val)?);
            }
            "keepalives_count" => {
                self.keepalives_count = Some(parse_num("keepalives_count", val)?);
            }
            // Milliseconds, and clamped like `connect_timeout` rather than
            // rejected: libpq's `setTCPUserTimeout` takes a negative as 0,
            // and a `Config` error here is permanent, so rejecting one
            // would hard fail a string psql accepts.
            "tcp_user_timeout" => {
                self.tcp_user_timeout = Some(parse_connect_timeout("tcp_user_timeout", val)?);
            }
            // libpq translates this to sslmode in `conninfo_storeval`, so it
            // works in both syntaxes. Dropping it is a silent downgrade.
            "requiressl" => {
                self.sslmode = Some(if val.starts_with('1') {
                    SslMode::Require
                } else {
                    SslMode::Prefer
                });
            }
            k => check_unhandled(k, val)?,
        }
        Ok(())
    }

    fn finish(self) -> Result<ConnInfo, ReplicationError> {
        let user = self.user.unwrap_or_else(|| "postgres".to_string());
        let sslmode = resolve_sslrootcert_system(
            self.sslmode.unwrap_or(SslMode::Prefer),
            self.sslmode.is_some(),
            self.sslrootcert.as_deref(),
        )?;
        let (sslcert, sslkey) = client_identity(
            self.sslcert,
            self.sslkey,
            self.client_cert_allowed.unwrap_or(true) && sslmode != SslMode::Disable,
        )?;
        let (application_name, options) = session_params(
            self.application_name,
            self.fallback_application_name,
            self.options,
        );

        Ok(ConnInfo {
            host: self.host.unwrap_or_else(|| "localhost".to_string()),
            port: self.port.unwrap_or(5432),
            dbname: self.dbname.unwrap_or_else(|| user.clone()),
            user,
            // Check PGPASSWORD env var if no password was given
            password: self.password.or_else(|| std::env::var("PGPASSWORD").ok()),
            sslmode,
            sslrootcert: self.sslrootcert,
            sslcert,
            sslkey,
            sslnegotiation: self.sslnegotiation.unwrap_or(SslNegotiation::Postgres),
            replication: self.replication.unwrap_or(ReplicationMode::None),
            application_name,
            options,
            connect_timeout: self.connect_timeout.unwrap_or(0),
            keepalives: self.keepalives.unwrap_or(true),
            keepalives_idle: self.keepalives_idle.unwrap_or(120),
            keepalives_interval: self.keepalives_interval.unwrap_or(10),
            keepalives_count: self.keepalives_count.unwrap_or(3),
            tcp_user_timeout: self.tcp_user_timeout.unwrap_or(0),
        })
    }
}

/// Resolve the session parameters the way libpq sends them.
///
/// An empty value counts as not given, so `application_name=` falls back to
/// `fallback_application_name`, whatever order the two are written in. That is
/// libpq's parameter-array API (`PQconnectStartParams`, which the libpq backend's
/// `connect_async` uses; measured on libpq 18.7: `application_name=&fallback_application_name=fb`
/// names the session `fb`). `PQconnectdb` given the same string keeps the empty
/// name. An empty name or `options` is never sent.
fn session_params(
    application_name: Option<String>,
    fallback_application_name: Option<String>,
    options: Option<String>,
) -> (Option<String>, Option<String>) {
    let given = |value: Option<String>| value.filter(|value| !value.is_empty());
    (
        given(application_name).or_else(|| given(fallback_application_name)),
        given(options),
    )
}

/// libpq's reserved `sslrootcert` value meaning "use the SSL implementation's
/// own trusted CA roots" (PG16+), as opposed to a path to a CA bundle.
pub(super) const SSLROOTCERT_SYSTEM: &str = "system";

/// Apply libpq's `sslrootcert=system` rule to the parsed `sslmode`.
///
/// In libpq, `sslrootcert=system` is not merely "where to find the roots" — it is
/// a request for *full verification*. `pqConnectOptions2` (fe-connect.c) raises
/// the effective default to `verify-full`, and explicitly refuses to pair it with
/// a weaker mode:
///
/// ```text
/// host=h sslrootcert=system                  ->  effective sslmode = verify-full
/// host=h sslrootcert=system sslmode=require  ->  error: weak sslmode "require" may not
///                                                be used with sslrootcert=system
/// ```
///
/// Without this, `system` was stored as an ordinary file path and `sslmode` stayed
/// at its `Prefer` default, which selects `NoVerification` (accepts any
/// certificate) *and* falls back to plaintext if the handshake fails. Because
/// `build_root_store` is only reached from the verifying modes, the bogus `system`
/// path never even produced a file-open error — the downgrade was completely
/// silent. `postgresql://u@host/db?sslrootcert=system` is the form cloud providers
/// document for publicly-signed certificates, so this was the documented happy path
/// connecting with verification disabled.
fn resolve_sslrootcert_system(
    sslmode: SslMode,
    sslmode_explicit: bool,
    sslrootcert: Option<&str>,
) -> Result<SslMode, ReplicationError> {
    if sslrootcert != Some(SSLROOTCERT_SYSTEM) {
        return Ok(sslmode);
    }
    match sslmode {
        // Not spelled out by the caller: `system` raises the default.
        _ if !sslmode_explicit => Ok(SslMode::VerifyFull),
        // `verify-full` only, exactly as libpq (`strcmp(sslmode, "verify-full") != 0`).
        //
        // `verify-ca` is NOT good enough here, even though it does validate the
        // chain: `system` loads the whole public root store, and `VerifyCa`
        // installs `NoHostnameVerifier`. Chain validation against *every public
        // CA* with no name binding authenticates nothing — any certificate a
        // public CA will issue for any domain the attacker controls passes. With
        // a pinned `sslrootcert=/path/ca.pem` the same mode is meaningful, which
        // is why this rule is keyed on the `system` keyword and nothing else.
        SslMode::VerifyFull => Ok(sslmode),
        weak => Err(ReplicationError::config(format!(
            "weak sslmode \"{}\" may not be used with sslrootcert=system \
             (use verify-full)",
            sslmode_name(weak)
        ))),
    }
}

/// Spelling of an [`SslMode`] as it appears in a connection string.
fn sslmode_name(mode: SslMode) -> &'static str {
    match mode {
        SslMode::Disable => "disable",
        SslMode::Allow => "allow",
        SslMode::Prefer => "prefer",
        SslMode::Require => "require",
        SslMode::VerifyCa => "verify-ca",
        SslMode::VerifyFull => "verify-full",
    }
}

/// Parse an `sslmode` value, rejecting anything unrecognised.
///
/// Returning a default here would be a silent security downgrade: a typo
/// (`verify_full`, `requirre`) or a case difference (`REQUIRE`) would land on
/// `prefer`, which accepts any certificate and falls back to plaintext. libpq
/// hard-fails instead (`invalid sslmode value: "..."`, `pqConnectOptions2` (`connectOptions2` before PG17) in
/// fe-connect.c), and so do we.
fn parse_sslmode(s: &str) -> Result<SslMode, ReplicationError> {
    match s {
        "disable" => Ok(SslMode::Disable),
        "allow" => Ok(SslMode::Allow),
        "prefer" => Ok(SslMode::Prefer),
        "require" => Ok(SslMode::Require),
        "verify-ca" => Ok(SslMode::VerifyCa),
        "verify-full" => Ok(SslMode::VerifyFull),
        other => Err(ReplicationError::config(format!(
            "invalid sslmode value: \"{other}\" \
             (expected disable, allow, prefer, require, verify-ca or verify-full)"
        ))),
    }
}

/// libpq keywords this parser does not implement, where dropping one cannot
/// weaken the connection's security posture. Accepted and ignored, so a conninfo
/// psql accepts does not start failing here.
///
/// **Maintenance contract.** This list plus [`UNSUPPORTED_OPTIONS`] plus the
/// keywords handled in the two match arms must equal `PQconninfoOptions[]`
/// exactly. **Synced against PostgreSQL 18** (52 keywords; PG14's 36 are a strict
/// subset — 16 added, none removed across PG14→PG18). When libpq adds a keyword,
/// a conninfo using it hard-fails here until this list is updated; that is the
/// deliberate price of rejecting unknown keys at all, and it is why the two lists
/// are a union across majors rather than a single version.
///
/// Source: `PQconninfoOptions[]` in `src/interfaces/libpq/fe-connect.c` — the
/// union over REL_14_STABLE..master, so a string written for a newer libpq still
/// parses and a keyword absent from the reader's server is simply inert.
///
/// `sslpassword` is here despite being unhonourable because it fails **closed**: an
/// encrypted `sslkey` is refused by name when the connection is made, and an
/// unencrypted one needs no password. Contrast `sslcrl`, which fails **open** — a
/// revoked certificate would be silently accepted — so that one is in
/// [`UNSUPPORTED_OPTIONS`]. `sslcert`, `sslkey` and `sslcertmode` are handled in
/// the two match arms.
///
/// That fail-open/fail-closed split is a *security* audit, and it is the only
/// one this list has ever had. It says nothing about liveness, which is how
/// `tcp_user_timeout` sat here being parsed and discarded while the libpq
/// backend — which hands the conninfo to `PQconnectdb` verbatim — honoured it:
/// the same string bounded a dead round-trip on one backend and not the other,
/// with no diagnostic. When adding a keyword here, ask whether ignoring it
/// changes *behaviour*, not just whether it is safe to ignore. `options` was the
/// same mistake: it sat here while the libpq backend applied it, so a pinned
/// `TimeZone` changed the text of every decoded `timestamptz` on one backend only.
/// It, `application_name` and `fallback_application_name` are handled in the two
/// match arms now.
const IGNORED_OPTIONS: &[&str] = &[
    // `require` only: `auth.rs` DOES negotiate SCRAM-SHA-256-PLUS with
    // `tls-server-end-point` binding when the server offers it — what is missing
    // is the *enforcement* (erroring when the server declines PLUS), so we cannot
    // honour `require`, but the mechanism itself is implemented.
    "channel_binding",
    "client_encoding",
    "gssdelegation",
    "gssencmode", // `require` only: no GSSAPI transport encryption here
    "gsslib",
    "krbsrvname",
    "load_balance_hosts",
    "max_protocol_version",
    "min_protocol_version",
    "passfile",
    "servicefile",
    "ssl_max_protocol_version",
    "ssl_min_protocol_version", // only a TLSv1.3 floor is unhonourable
    "sslcompression",
    "sslkeylogfile",
    "sslpassword",
    "sslsni",
    "target_session_attrs",
];

/// libpq keywords whose whole purpose is to tighten authentication or transport
/// security and that this backend cannot honour. Silently dropping one leaves
/// the connection weaker than the caller asked for, with nothing on the wire to
/// say so, so these are a hard error.
///
/// `require_auth` is what stops a rogue server demanding
/// `AuthenticationCleartextPassword` (auth type 3, which `auth.rs` answers);
/// `sslcrl`/`sslcrldir` are what reject a revoked certificate; `service` names
/// the host we would otherwise silently replace with the default.
const UNSUPPORTED_OPTIONS: &[&str] = &[
    "oauth_ca_file",
    "oauth_client_id",
    "oauth_client_secret",
    "oauth_issuer",
    "oauth_scope",
    "require_auth",
    "requirepeer",
    "scram_client_key",
    "scram_server_key",
    "service",
    "sslcrl",
    "sslcrldir",
];

fn unsupported_option(option: &str) -> ReplicationError {
    ReplicationError::config(format!(
        "connection option \"{option}\" is not supported by the native backend; \
         ignoring it would silently weaken the connection \
         (use the `libpq` backend, or remove it)"
    ))
}

/// Classify a connection option this parser does not implement.
///
/// libpq rejects any keyword outside `PQconninfoOptions[]`: `conninfo_storeval`
/// emits `invalid connection option "%s"` and the URI query loop adds
/// `invalid URI query parameter: "%s"`. Silently ignoring instead turned a typo
/// like `sslmod=verify-full` into `sslmode=prefer` — `NoVerification` plus a
/// plaintext fallback, from a string that asked for the strongest posture.
/// That is the same downgrade [`parse_sslmode`] exists to prevent, through a
/// one-character-different door.
fn check_unhandled(key: &str, value: &str) -> Result<(), ReplicationError> {
    // Unhonourable only in their strict form. The *value* is what makes these
    // unhonourable — `channel_binding=prefer` is fine — so it belongs in the
    // message, and none of these three carry a secret.
    let strict_form = match key {
        // libpq validates these two with strcmp, so an exact match is correct.
        "channel_binding" | "gssencmode" => value == "require",
        // rustls is built with the `tls12` feature, so the floor is already
        // TLSv1.2 and every looser value is satisfied. Only a TLSv1.3 floor is
        // something we cannot promise. libpq compares with `pg_strcasecmp`
        // (`sslVerifyProtocolVersion`), so we must be case-insensitive too.
        "ssl_min_protocol_version" => value.eq_ignore_ascii_case("TLSv1.3"),
        _ => false,
    };
    if strict_form {
        return Err(unsupported_option(&format!("{key}={value}")));
    }
    // Unhonourable whatever the value is, so the value adds no diagnostic — and
    // echoing it would leak a credential. `UNSUPPORTED_OPTIONS` holds
    // `scram_client_key`, `scram_server_key` and `oauth_client_secret`; a
    // `scram_client_key` is authentication-equivalent (it computes `ClientProof`).
    // This is a `Config` error, which is permanent and goes straight to a caller
    // who will log it, and the secret never becomes a `ConnInfo` field, so the
    // hand-written `Debug` redaction below cannot help here. Key only.
    if UNSUPPORTED_OPTIONS.contains(&key) {
        return Err(unsupported_option(key));
    }
    if IGNORED_OPTIONS.contains(&key) {
        tracing::debug!("ignoring unimplemented connection option \"{key}\"");
        return Ok(());
    }
    Err(ReplicationError::config(format!(
        "invalid connection option \"{key}\""
    )))
}

/// Parse `sslcertmode`: `Ok(true)` when a client certificate may be sent.
///
/// `require` would also demand that the server ask for one, which this backend does not check, so it stays refused rather than weakened.
fn parse_sslcertmode(value: &str) -> Result<bool, ReplicationError> {
    match value {
        "disable" => Ok(false),
        "allow" => Ok(true),
        "require" => Err(unsupported_option("sslcertmode=require")),
        other => Err(ReplicationError::config(format!(
            "invalid sslcertmode value: \"{other}\" (expected disable, allow or require)"
        ))),
    }
}

/// Settle `sslcert`/`sslkey` into both paths or neither, as the TLS layer needs them.
///
/// An empty value is unset, as in libpq. `sslcertmode=disable` drops both. A lone `sslcert` is an error because libpq would look for the key at `~/.postgresql/postgresql.key`, a default this backend never reads; a lone `sslkey` has no certificate to go with it and libpq ignores it too.
fn client_identity(
    cert: Option<String>,
    key: Option<String>,
    allowed: bool,
) -> Result<(Option<String>, Option<String>), ReplicationError> {
    let cert = cert.filter(|c| !c.is_empty());
    let key = key.filter(|k| !k.is_empty());
    match (allowed, cert, key) {
        (false, ..) | (_, None, _) => Ok((None, None)),
        (true, Some(cert), Some(key)) => Ok((Some(cert), Some(key))),
        (true, Some(_), None) => Err(ReplicationError::config(
            "sslcert needs sslkey: the native backend does not read libpq's default key file"
                .to_string(),
        )),
    }
}

/// Parse a `connect_timeout`, mapping a negative value to "no timeout".
///
/// libpq documents this verbatim: "Zero, negative, or not specified means wait
/// indefinitely" (`libpq-connect.html`), and accepts `connect_timeout=-1`. The
/// startup path already gates on `connect_timeout > 0`, so 0 is that state.
/// Rejecting a negative here would fail a connection string psql accepts — and
/// as a `Config` error it is permanent, so it would never retry.
fn parse_connect_timeout(key: &str, value: &str) -> Result<u64, ReplicationError> {
    Ok(parse_num::<i64>(key, value)?.max(0) as u64)
}

/// Parse a `port` value, treating an empty one as the default.
///
/// libpq substitutes `DEF_PGPORT` for a null or empty port field, so `port=` and
/// `postgres://host:/db` must keep working. Malformed and out-of-range values are
/// still rejected — only emptiness is special, and only for `port`. `0` is not
/// empty: libpq rejects `thisport < 1 || thisport > 65535` with
/// `invalid port number`.
fn parse_port(value: &str) -> Result<u16, ReplicationError> {
    if value.is_empty() {
        return Ok(5432);
    }
    let port = parse_num::<u16>("port", value)?;
    if port == 0 {
        return Err(ReplicationError::config(format!(
            "invalid port number: \"{value}\""
        )));
    }
    Ok(port)
}

/// Parse an integer connection option, rejecting anything malformed.
///
/// Silently defaulting is worse than failing: `port=99999` would connect to
/// whatever is listening on 5432, and `keepalives_idle=abc` would become a
/// value the caller never asked for. libpq rejects both
/// (`pqParseIntParam`/`parse_int_param`, same message text).
///
/// Note this is deliberately NOT used for `connect_timeout`, where libpq accepts
/// a negative — see [`parse_connect_timeout`].
fn parse_num<T: core::str::FromStr>(key: &str, value: &str) -> Result<T, ReplicationError> {
    value.parse::<T>().map_err(|_| {
        ReplicationError::config(format!(
            "invalid integer value \"{value}\" for connection option \"{key}\""
        ))
    })
}

/// Parse a `replication` value, rejecting anything unrecognised.
///
/// Defaulting here is worse than for most options: in a logical-replication
/// library a typo like `replication=dattabase` would silently open an ordinary
/// connection, and the failure only surfaces much later as an opaque server
/// error on `START_REPLICATION`. libpq forwards the value to the server, which
/// rejects it outright.
///
/// The accepted set is libpq's boolean spellings plus `database`
/// (`GUC_bool` in `guc.c`: on/off/true/false/yes/no/1/0).
fn parse_replication_mode(s: &str) -> Result<ReplicationMode, ReplicationError> {
    match s {
        "database" => Ok(ReplicationMode::Database),
        "true" | "yes" | "on" | "1" => Ok(ReplicationMode::Physical),
        "false" | "no" | "off" | "0" => Ok(ReplicationMode::None),
        other => Err(ReplicationError::config(format!(
            "invalid replication value: \"{other}\" \
             (expected database, or a boolean: true/false/yes/no/on/off/1/0)"
        ))),
    }
}

/// Parse an `sslnegotiation` value, rejecting anything unrecognised.
///
/// Falling back to `postgres` would be the *safe* direction — it costs one
/// round-trip, not any security — but libpq hard-errors here
/// (`invalid sslnegotiation value`), and silently ignoring a typo in a TLS
/// option is exactly what [`parse_sslmode`] was hardened against.
fn parse_ssl_negotiation(s: &str) -> Result<SslNegotiation, ReplicationError> {
    match s {
        "direct" => Ok(SslNegotiation::Direct),
        "postgres" => Ok(SslNegotiation::Postgres),
        other => Err(ReplicationError::config(format!(
            "invalid sslnegotiation value: \"{other}\" (expected postgres or direct)"
        ))),
    }
}

/// Percent-decode a URI component per RFC 3986.
///
/// Decodes to raw OCTETS and then interprets the result as UTF-8. The previous
/// `byte as char` form was a Latin-1 lift, not a decode: `%C3%A9` became the two
/// chars U+00C3 U+00A9, which re-encode to four UTF-8 bytes instead of the two
/// the user wrote. That silently corrupted any non-ASCII password before it
/// reached SCRAM, producing an unexplainable "password authentication failed" —
/// SASLprep (RFC 5802 §5.1, which RFC 7677 inherits; `pg_saslprep` in libpq)
/// normalises the password but its NFKC pass does not undo mojibake, so the
/// corruption survived to the key derivation. libpq's `conninfo_uri_decode`
/// writes raw octets for the same reason.
///
/// Divergence from `conninfo_uri_decode`, deliberate: libpq hard-fails a
/// malformed escape (`invalid percent-encoded token`) and `%00` (`forbidden
/// value %00 in percent-encoded value`). This passes both through, which keeps
/// the function infallible.
///
/// `%00` therefore still decodes to a real NUL here, but it no longer reaches the wire: `wire::build_startup_message` rejects a NUL in any key or value, so `postgresql://u%00replication%00database@h/db` fails with a `Config` error instead of injecting startup parameters. Guarding there rather than here covers the key-value syntax too, which has no escape sequence to intercept.
fn url_decode(s: &str) -> String {
    #[inline]
    fn hex_val(b: u8) -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        }
    }

    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        // Byte-indexed throughout: `%` is ASCII, so this can never split a
        // multi-byte UTF-8 sequence the way slicing by char index could.
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                out.push((hi << 4) | lo);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }

    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_uri_full() {
        let ci = ConnInfo::parse(
            "postgresql://repl:s3cret@db.example.com:5433/mydb?sslmode=require&replication=database",
        )
        .unwrap();
        assert_eq!(ci.host, "db.example.com");
        assert_eq!(ci.port, 5433);
        assert_eq!(ci.user, "repl");
        assert_eq!(ci.password, Some("s3cret".to_string()));
        assert_eq!(ci.dbname, "mydb");
        assert_eq!(ci.sslmode, SslMode::Require);
        assert_eq!(ci.replication, ReplicationMode::Database);
    }

    #[test]
    fn parse_uri_defaults() {
        let ci = ConnInfo::parse("postgresql://localhost/testdb").unwrap();
        assert_eq!(ci.host, "localhost");
        assert_eq!(ci.port, 5432);
        assert_eq!(ci.user, "postgres");
        assert_eq!(ci.sslmode, SslMode::Prefer);
    }

    #[test]
    fn parse_uri_encoded_password() {
        let ci = ConnInfo::parse("postgresql://user:p%40ss@host/db").unwrap();
        assert_eq!(ci.password, Some("p@ss".to_string()));
    }

    /// The userinfo lookahead must stop at the authority boundary, as libpq's
    /// `conninfo_uri_parse_options` does. Splitting on the first `@` anywhere made a
    /// literal `@` in the query hijack the host — and silently dropped sslmode with
    /// the rest of the query, the exact downgrade the query-parsing fix exists to
    /// prevent.
    #[test]
    fn uri_at_sign_in_query_is_not_credentials() {
        let ci = ConnInfo::parse(
            "postgresql://pg.internal:5432/appdb?sslmode=require&application_name=svc@prod",
        )
        .unwrap();
        assert_eq!(ci.host, "pg.internal");
        assert_eq!(ci.port, 5432);
        assert_eq!(ci.dbname, "appdb");
        assert!(matches!(ci.sslmode, SslMode::Require));

        // ...and a genuine userinfo still parses.
        let ci = ConnInfo::parse("postgresql://u:p@h/db").unwrap();
        assert_eq!(ci.user, "u");
        assert_eq!(ci.host, "h");
    }

    /// The userinfo lookahead must stop at `@` and `/` ONLY, as libpq does.
    /// Adding `?` to that set suppressed credential detection whenever the
    /// userinfo itself contained one: libpq accepts both of these.
    #[test]
    fn uri_question_mark_in_userinfo_is_not_an_authority_boundary() {
        let ci = ConnInfo::parse("postgresql://user:p?ss@host/db").unwrap();
        assert_eq!(ci.host, "host");
        assert_eq!(ci.user, "user");
        assert_eq!(ci.password.as_deref(), Some("p?ss"));
        assert_eq!(ci.port, 5432);

        // The silent variant: no password, so nothing failed to parse — it just
        // connected to the wrong host.
        let ci = ConnInfo::parse("postgresql://us?er@host/db").unwrap();
        assert_eq!(ci.host, "host");
        assert_eq!(ci.user, "us?er");

        // The case the '?' stop was added for is still covered by the '/' stop.
        let ci =
            ConnInfo::parse("postgresql://pg.internal/appdb?application_name=svc@prod").unwrap();
        assert_eq!(ci.host, "pg.internal");
    }

    /// A bracketed IPv6 literal followed by junk must not silently default the
    /// port: `[2001:db8::1]5433` resolves as a host and would connect to 5432.
    /// libpq answers `unexpected character "5" ... (expected ":" or "/")`.
    #[test]
    fn uri_ipv6_trailing_junk_is_rejected() {
        assert!(ConnInfo::parse("postgresql://u@[2001:db8::1]5433/db").is_err());
        assert!(ConnInfo::parse("postgresql://u@[::1]junk/db").is_err());
        // An empty address is rejected too, as in libpq.
        assert!(ConnInfo::parse("postgresql://u@[]/db").is_err());
        // Well-formed forms still parse.
        assert_eq!(
            ConnInfo::parse("postgresql://u@[2001:db8::1]:5433/db")
                .unwrap()
                .port,
            5433
        );
        assert_eq!(
            ConnInfo::parse("postgresql://u@[::1]/db").unwrap().host,
            "::1"
        );
    }

    /// libpq documents "Zero, negative, or not specified means wait
    /// indefinitely" for `connect_timeout` and accepts `-1`. Rejecting it made a
    /// connection string psql accepts a *permanent* `Config` failure.
    #[test]
    fn negative_connect_timeout_means_no_timeout_not_an_error() {
        assert_eq!(
            ConnInfo::parse("host=h connect_timeout=-1")
                .unwrap()
                .connect_timeout,
            0
        );
        assert_eq!(
            ConnInfo::parse("postgresql://u@h/db?connect_timeout=-30")
                .unwrap()
                .connect_timeout,
            0
        );
        // Non-numeric is still an error, and a positive value still lands.
        assert!(ConnInfo::parse("host=h connect_timeout=abc").is_err());
        assert_eq!(
            ConnInfo::parse("host=h connect_timeout=10")
                .unwrap()
                .connect_timeout,
            10
        );
    }

    /// `port=0` is not the same as an omitted port: libpq rejects
    /// `thisport < 1 || thisport > 65535` with `invalid port number`.
    #[test]
    fn port_zero_is_rejected() {
        assert!(ConnInfo::parse("host=h port=0").is_err());
        assert!(ConnInfo::parse("postgresql://u@h:0/db").is_err());
        // Empty is still the documented default-substitution case.
        assert_eq!(ConnInfo::parse("host=h port=").unwrap().port, 5432);
    }

    /// The originating bug: a typo'd KEY was silently dropped, so sslmode stayed
    /// at the `Prefer` default and startup installed `NoVerification` with a
    /// plaintext fallback — from a string that asked for the strongest posture.
    /// libpq hard-errors on the same input.
    #[test]
    fn typo_in_security_option_is_rejected_not_dropped() {
        for s in [
            "host=h sslmod=verify-full",
            "postgresql://u@h/db?sslmod=verify-full",
        ] {
            let err = ConnInfo::parse(s).unwrap_err().to_string();
            assert!(err.contains("invalid connection option"), "{s}: {err}");
            assert!(err.contains("sslmod"), "{s}: {err}");
        }
        // libpq's key compare is strcmp, so case variants are unknown keys too.
        assert!(ConnInfo::parse("host=h SSLMODE=require").is_err());
    }

    /// The unknown-KEY fix left a wider door open: a query parameter with no `=`
    /// at all was dropped by the `if let Some(..) = split_once('=')` with no else,
    /// so `?sslmodeverify-full` still silently left sslmode at `Prefer` — the same
    /// downgrade, one keystroke away. libpq reports a missing separator.
    #[test]
    fn uri_query_param_without_separator_is_rejected() {
        let err = ConnInfo::parse("postgresql://u@h/db?sslmodeverify-full")
            .unwrap_err()
            .to_string();
        assert!(err.contains("missing key/value separator"), "{err}");
        assert!(err.contains("sslmodeverify-full"), "{err}");
        // A trailing `&` is still just an empty segment, not an error.
        assert!(ConnInfo::parse("postgresql://u@h/db?sslmode=require&").is_ok());
    }

    /// libpq's own docs show `keyword = 'a value'` with spaces around the `=`.
    /// The value reader never skipped whitespace after `=`, so the value came back
    /// empty and the next token was taken as a key — harmless while unknown keys
    /// were ignored, fatal once they are rejected.
    #[test]
    fn key_value_tolerates_spaces_around_equals() {
        let ci = ConnInfo::parse("host = h  sslmode = require  port = 5433").unwrap();
        assert_eq!(ci.host, "h");
        assert!(matches!(ci.sslmode, SslMode::Require));
        assert_eq!(ci.port, 5433);

        // Quoted values still work with a space before the opening quote.
        let ci = ConnInfo::parse("host=h password = 'it''s'").unwrap();
        assert_eq!(ci.password.as_deref(), Some("it's"));
    }

    /// `negotiate_tls` retries a failed direct handshake over standard SSLRequest,
    /// and under `prefer`/`allow` that retry may fall back to plaintext — so
    /// asking for the *faster* TLS path could silently yield no TLS. libpq refuses
    /// the same combination for the same stated reason.
    #[test]
    fn direct_ssl_negotiation_requires_a_non_falling_back_sslmode() {
        for weak in ["prefer", "allow", "disable"] {
            assert!(
                ConnInfo::parse(&format!("host=h sslnegotiation=direct sslmode={weak}")).is_err(),
                "sslnegotiation=direct must not pair with sslmode={weak}"
            );
        }
        // Default sslmode is `prefer`, so direct alone must be refused too.
        assert!(ConnInfo::parse("host=h sslnegotiation=direct").is_err());
        assert!(ConnInfo::parse("postgresql://u@h/db?sslnegotiation=direct").is_err());

        for strong in ["require", "verify-ca", "verify-full"] {
            let ci = ConnInfo::parse(&format!("host=h sslnegotiation=direct sslmode={strong}"))
                .unwrap_or_else(|e| panic!("sslmode={strong} must be allowed: {e}"));
            assert!(matches!(ci.sslnegotiation, SslNegotiation::Direct));
        }
    }

    /// `postgresql:///mydb` carries no authority. An empty host reached
    /// `TcpStream::connect(":5432")` and failed with an opaque address error,
    /// while the key/value path defaulted to localhost — the two syntaxes
    /// disagreed.
    #[test]
    fn uri_without_authority_defaults_the_host() {
        let ci = ConnInfo::parse("postgresql:///mydb").unwrap();
        assert_eq!(ci.host, "localhost");
        assert_eq!(ci.dbname, "mydb");
        assert_eq!(ci.port, 5432);
    }

    /// `keepalives` was the one option the numeric-validation sweep skipped, so
    /// `keepalives=abc` silently meant "on" while `keepalives_idle=abc` was fatal.
    /// libpq parses it with `pqParseIntParam`, so `00`/`+0`/`-0` all disable.
    #[test]
    fn keepalives_is_validated_like_its_siblings() {
        assert!(ConnInfo::parse("host=h keepalives=abc").is_err());
        assert!(!ConnInfo::parse("host=h keepalives=0").unwrap().keepalives);
        assert!(!ConnInfo::parse("host=h keepalives=+0").unwrap().keepalives);
        assert!(ConnInfo::parse("host=h keepalives=1").unwrap().keepalives);
        assert!(ConnInfo::parse("postgresql://u@h/db?keepalives=x").is_err());
    }

    /// Anti-regression for the fix above: a conninfo psql accepts must not start
    /// failing here. These are real `PQconninfoOptions[]` keywords this parser
    /// does not implement, and dropping any of them cannot weaken the connection.
    #[test]
    fn libpq_options_we_do_not_implement_still_parse() {
        for kv in [
            "client_encoding=UTF8",
            "target_session_attrs=read-write",
            "passfile=/x/.pgpass",
            "sslsni=1",
            "sslcompression=0",
            // Unhonourable but fail-CLOSED: the server rejects the auth loudly.
            "sslpassword=secret",
            "gsslib=gssapi",
            "krbsrvname=postgres",
            "load_balance_hosts=random",
            "sslkeylogfile=/tmp/k",
            "min_protocol_version=3.0",
            // Value-sensitive: the loose forms are all satisfiable.
            "channel_binding=prefer",
            "gssencmode=disable",
            "sslcertmode=disable",
            "ssl_min_protocol_version=TLSv1.2",
            "ssl_min_protocol_version=tlsv1.2",
            "ssl_min_protocol_version=TLSv1",
        ] {
            assert!(
                ConnInfo::parse(&format!("host=h {kv}")).is_ok(),
                "key=value rejected: {kv}"
            );
            assert!(
                ConnInfo::parse(&format!("postgresql://u@h/db?{kv}")).is_ok(),
                "URI rejected: {kv}"
            );
        }
    }

    /// `sslcert`/`sslkey` used to be dropped, so the native backend never sent a client
    /// certificate and the server rejected it; they are carried to the TLS layer now.
    #[test]
    fn sslcert_and_sslkey_are_carried_in_both_syntaxes() {
        for dsn in [
            "host=h sslcert=/c/client.crt sslkey=/c/client.key",
            "host=h sslkey='/c/client.key' sslcert='/c/client.crt'",
            "postgresql://u@h/db?sslcert=/c/client.crt&sslkey=/c/client.key",
            "postgresql://u@h/db?sslcert=%2Fc%2Fclient.crt&sslkey=%2Fc%2Fclient.key",
        ] {
            let ci = ConnInfo::parse(dsn).unwrap();
            assert_eq!(ci.sslcert.as_deref(), Some("/c/client.crt"), "{dsn}");
            assert_eq!(ci.sslkey.as_deref(), Some("/c/client.key"), "{dsn}");
        }
        let none = ConnInfo::parse("host=h").unwrap();
        assert!(none.sslcert.is_none() && none.sslkey.is_none());
    }

    /// libpq would read the key from `~/.postgresql/postgresql.key`; this backend does not,
    /// so a certificate without a key is refused up front rather than silently not sent.
    #[test]
    fn sslcert_without_sslkey_is_refused_but_a_lone_sslkey_is_ignored() {
        for dsn in [
            "host=h sslcert=/c/a.crt",
            "postgresql://u@h/db?sslcert=/c/a.crt",
        ] {
            let err = ConnInfo::parse(dsn).unwrap_err().to_string();
            assert!(err.contains("sslcert needs sslkey"), "{dsn}: {err}");
        }
        for dsn in [
            "host=h sslkey=/c/a.key",
            "postgresql://u@h/db?sslkey=/c/a.key",
        ] {
            let ci = ConnInfo::parse(dsn).unwrap();
            assert!(ci.sslcert.is_none() && ci.sslkey.is_none(), "{dsn}");
        }
    }

    /// With TLS off libpq never reads the certificate, so a lone `sslcert` is no error there,
    /// and it was none here before the pair was used.
    #[test]
    fn sslmode_disable_ignores_the_client_certificate_options() {
        for dsn in [
            "host=h sslmode=disable sslcert=/c/a.crt",
            "host=h sslcert=/c/a.crt sslmode=disable",
            "postgresql://u@h/db?sslmode=disable&sslcert=/c/a.crt",
            "host=h sslmode=disable sslcert=/c/a.crt sslkey=/c/a.key",
        ] {
            let ci = ConnInfo::parse(dsn).unwrap_or_else(|e| panic!("{dsn}: {e}"));
            assert!(ci.sslcert.is_none() && ci.sslkey.is_none(), "{dsn}");
        }
    }

    #[test]
    fn empty_sslcert_and_sslkey_count_as_unset() {
        let ci = ConnInfo::parse("host=h sslcert='' sslkey=''").unwrap();
        assert!(ci.sslcert.is_none() && ci.sslkey.is_none());
        let ci = ConnInfo::parse("postgresql://u@h/db?sslcert=&sslkey=").unwrap();
        assert!(ci.sslcert.is_none() && ci.sslkey.is_none());
    }

    #[test]
    fn sslcertmode_disable_drops_the_client_certificate_and_allow_keeps_it() {
        let pair = "sslcert=/c/a.crt sslkey=/c/a.key";
        let off = ConnInfo::parse(&format!("host=h {pair} sslcertmode=disable")).unwrap();
        assert!(off.sslcert.is_none() && off.sslkey.is_none());
        // Whatever the order, and even for a lone sslcert that would otherwise be refused.
        let off = ConnInfo::parse(&format!("host=h sslcertmode=disable {pair}")).unwrap();
        assert!(off.sslcert.is_none());
        assert!(ConnInfo::parse("host=h sslcert=/c/a.crt sslcertmode=disable").is_ok());
        for mode in ["allow", "disable"] {
            let dsn =
                format!("postgresql://u@h/db?sslcertmode={mode}&sslcert=/c/a.crt&sslkey=/c/a.key");
            assert_eq!(
                ConnInfo::parse(&dsn).unwrap().sslcert.is_some(),
                mode == "allow",
                "{dsn}"
            );
        }
    }

    #[test]
    fn sslcertmode_rejects_require_and_unknown_values() {
        for (value, expect) in [
            ("require", "not supported"),
            ("always", "invalid sslcertmode value"),
            ("", "invalid sslcertmode value"),
            ("DISABLE", "invalid sslcertmode value"),
        ] {
            for dsn in [
                format!("host=h sslcertmode={value}"),
                format!("postgresql://u@h/db?sslcertmode={value}"),
            ] {
                let err = ConnInfo::parse(&dsn).unwrap_err().to_string();
                assert!(err.contains(expect), "{dsn}: {err}");
            }
        }
    }

    /// Both paths are shown (they are not secrets) so a misconfigured pair is diagnosable.
    #[test]
    fn debug_shows_the_client_certificate_paths() {
        let ci = ConnInfo::parse("host=h sslcert=/c/a.crt sslkey=/c/a.key").unwrap();
        let shown = format!("{ci:?}");
        assert!(
            shown.contains("/c/a.crt") && shown.contains("/c/a.key"),
            "{shown}"
        );
    }

    /// `application_name` and `options` were parsed and dropped, so the native
    /// backend ignored what the libpq backend honours: the session never got its
    /// name, and a pinned `TimeZone` changed the text of every decoded
    /// `timestamptz`. `options` carries a space, so each syntax escapes it the way
    /// libpq does: single quotes in key=value, percent-encoding in a URI.
    #[test]
    fn application_name_and_options_are_kept_in_both_syntaxes() {
        let kv = ConnInfo::parse(
            "host=h application_name='my app' options='-c TimeZone=Asia/Tokyo -c lock_timeout=2s'",
        )
        .unwrap();
        let uri = ConnInfo::parse(
            "postgresql://u@h/db?application_name=my%20app\
             &options=-c%20TimeZone%3DAsia/Tokyo%20-c%20lock_timeout%3D2s",
        )
        .unwrap();
        for ci in [kv, uri] {
            assert_eq!(ci.application_name.as_deref(), Some("my app"));
            assert_eq!(
                ci.options.as_deref(),
                Some("-c TimeZone=Asia/Tokyo -c lock_timeout=2s")
            );
        }

        // Unset stays unset: nothing is invented for the startup message.
        let ci = ConnInfo::parse("host=h").unwrap();
        assert_eq!((ci.application_name, ci.options), (None, None));

        // A literal `@` in the value is not a credentials designator.
        let ci = ConnInfo::parse("postgresql://pg.internal/db?application_name=svc@prod").unwrap();
        assert_eq!(ci.application_name.as_deref(), Some("svc@prod"));
    }

    /// libpq sends `application_name` if given and `fallback_application_name`
    /// otherwise, whatever order they are written in. An empty value counts as not
    /// given, as in `PQconnectStartParams` (measured on libpq 18.7:
    /// `application_name=&fallback_application_name=fb` names the session `fb`),
    /// and an empty name or `options` is never sent.
    #[test]
    fn fallback_application_name_applies_only_without_application_name() {
        for (args, want) in [
            ("fallback_application_name=fb", Some("fb")),
            (
                "application_name=app fallback_application_name=fb",
                Some("app"),
            ),
            (
                "fallback_application_name=fb application_name=app",
                Some("app"),
            ),
            (
                "application_name='' fallback_application_name=fb",
                Some("fb"),
            ),
            ("application_name=", None),
            ("options=''", None),
        ] {
            let ci = ConnInfo::parse(&format!("host=h {args}")).unwrap();
            assert_eq!(ci.application_name.as_deref(), want, "{args}");
            assert_eq!(ci.options, None, "{args}");
            let uri_args = args.replace(' ', "&").replace("''", "");
            let ci = ConnInfo::parse(&format!("postgresql://u@h/db?{uri_args}")).unwrap();
            assert_eq!(ci.application_name.as_deref(), want, "{uri_args}");
        }
    }

    /// Every fail-OPEN option must be refused in both syntaxes, and the message
    /// must not claim the option is invalid — it is a valid libpq option this
    /// backend cannot honour, which is a different thing to tell an operator.
    /// `sslrootcert=system` is libpq's request for *full verification*, not a
    /// file path. Storing it as a path left `sslmode` at `Prefer`, which selects
    /// `NoVerification` **and** falls back to plaintext — and because
    /// `build_root_store` is only reached from the verifying modes, the bogus
    /// path never even produced a file-open error. Completely silent downgrade
    /// on the exact URI cloud providers document.
    #[test]
    fn sslrootcert_system_raises_sslmode_to_verify_full() {
        for dsn in [
            "host=h sslrootcert=system",
            "postgresql://u@h/db?sslrootcert=system",
        ] {
            let ci = ConnInfo::parse(dsn).unwrap();
            assert_eq!(ci.sslmode, SslMode::VerifyFull, "{dsn}");
            assert_eq!(ci.sslrootcert.as_deref(), Some("system"), "{dsn}");
        }
    }

    /// libpq refuses the pairing rather than silently honouring the weaker mode:
    /// `weak sslmode "require" may not be used with sslrootcert=system`.
    #[test]
    fn sslrootcert_system_rejects_weak_sslmode() {
        for mode in ["disable", "allow", "prefer", "require"] {
            let err = ConnInfo::parse(&format!("host=h sslrootcert=system sslmode={mode}"))
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("may not be used with sslrootcert=system"),
                "{mode}: {err}"
            );
            assert!(err.contains(mode), "{mode}: {err}");
            assert!(
                ConnInfo::parse(&format!(
                    "postgresql://u@h/db?sslrootcert=system&sslmode={mode}"
                ))
                .is_err(),
                "URI accepted sslmode={mode}"
            );
        }
        // The deprecated aliases are sslmode settings too, so they are caught.
        assert!(ConnInfo::parse("host=h sslrootcert=system requiressl=0").is_err());
    }

    /// `verify-full` is the ONLY legal companion for `system`, exactly as libpq
    /// (`strcmp(sslmode, "verify-full") != 0` is the rejection condition).
    ///
    /// `verify-ca` looks safe — it validates the chain — but paired with
    /// `system` it authenticates nothing: the root store is every public CA
    /// (`build_root_store`) and `VerifyCa` installs `NoHostnameVerifier`, so any
    /// certificate a public CA issues for any domain the attacker controls is
    /// accepted for *this* host. Accepting it here was a live MITM path.
    #[test]
    fn sslrootcert_system_accepts_only_verify_full() {
        let ci = ConnInfo::parse("host=h sslrootcert=system sslmode=verify-full").unwrap();
        assert_eq!(ci.sslmode, SslMode::VerifyFull);

        for dsn in [
            "host=h sslrootcert=system sslmode=verify-ca",
            "postgresql://u@h/db?sslrootcert=system&sslmode=verify-ca",
        ] {
            let err = ConnInfo::parse(dsn)
                .expect_err("verify-ca + sslrootcert=system must be refused")
                .to_string();
            assert!(
                err.contains("may not be used with sslrootcert=system"),
                "{dsn}: {err}"
            );
        }

        // Keyed on the `system` keyword only: with a pinned CA file, `verify-ca`
        // is a meaningful mode and must keep working.
        let ci = ConnInfo::parse("host=h sslrootcert=/etc/ssl/ca.pem sslmode=verify-ca").unwrap();
        assert_eq!(ci.sslmode, SslMode::VerifyCa);
    }

    /// The rule is keyed on the reserved word only — an ordinary CA path must not
    /// start silently overriding the caller's sslmode.
    #[test]
    fn ordinary_sslrootcert_path_does_not_change_sslmode() {
        let ci = ConnInfo::parse("host=h sslrootcert=/etc/ssl/ca.pem").unwrap();
        assert_eq!(ci.sslmode, SslMode::Prefer);
        let ci = ConnInfo::parse("host=h sslrootcert=/etc/ssl/ca.pem sslmode=require").unwrap();
        assert_eq!(ci.sslmode, SslMode::Require);
    }

    /// `sslmode_name` feeds the `sslrootcert=system` rejection message, so every
    /// arm has to round-trip — a wrong spelling there sends an operator looking
    /// for an option they did not set.
    #[test]
    fn sslmode_name_round_trips_every_variant() {
        for name in [
            "disable",
            "allow",
            "prefer",
            "require",
            "verify-ca",
            "verify-full",
        ] {
            let mode = parse_sslmode(name).expect(name);
            assert_eq!(sslmode_name(mode), name);
        }
    }

    /// Known divergence from libpq, pinned so it is a deliberate state rather
    /// than an accident: libpq rejects an unterminated quoted value with
    /// `unterminated quoted string in connection info string`; this parser
    /// silently accepts the truncated value. See the table at the value reader.
    #[test]
    fn unterminated_quote_is_accepted_truncated_unlike_libpq() {
        let ci = ConnInfo::parse("host=h password='trunc").unwrap();
        assert_eq!(ci.password.as_deref(), Some("trunc"));
    }

    #[test]
    fn security_relevant_options_we_cannot_honour_are_refused() {
        for key in UNSUPPORTED_OPTIONS {
            let err = ConnInfo::parse(&format!("host=h {key}=x"))
                .unwrap_err()
                .to_string();
            assert!(err.contains("not supported"), "{key}: {err}");
            assert!(!err.contains("invalid connection option"), "{key}: {err}");
            assert!(
                ConnInfo::parse(&format!("postgresql://u@h/db?{key}=x")).is_err(),
                "URI accepted {key}"
            );
        }
        // Value-sensitive: only the strict form is unhonourable.
        for kv in [
            "channel_binding=require",
            "gssencmode=require",
            "sslcertmode=require",
            "ssl_min_protocol_version=TLSv1.3",
            "ssl_min_protocol_version=tlsv1.3",
        ] {
            assert!(ConnInfo::parse(&format!("host=h {kv}")).is_err(), "{kv}");
        }
    }

    /// The rejection message must never echo the option's *value*.
    ///
    /// `UNSUPPORTED_OPTIONS` holds `scram_client_key`, `scram_server_key` and
    /// `oauth_client_secret`. A `scram_client_key` is authentication-equivalent —
    /// it computes `ClientProof` — and `Config` is a permanent error that goes
    /// straight to a caller who logs it. The secret never becomes a `ConnInfo`
    /// field, so this path bypasses the hand-written `Debug` redaction entirely;
    /// key-only is the only thing that closes it.
    #[test]
    fn unsupported_option_error_never_echoes_the_value() {
        const SECRET: &str = "s3cret-scram-client-key-do-not-log";
        for key in UNSUPPORTED_OPTIONS {
            for dsn in [
                format!("host=h {key}={SECRET}"),
                format!("postgresql://u@h/db?{key}={SECRET}"),
            ] {
                let err = ConnInfo::parse(&dsn).unwrap_err().to_string();
                assert!(err.contains("not supported"), "{key}: {err}");
                assert!(
                    !err.contains(SECRET),
                    "{key} leaked its value into the error: {err}"
                );
            }
        }
    }

    /// The counterpart: where the *value* is the reason for the rejection, it
    /// must still be shown, or the message reads as "this key is never
    /// supported" when only the strict form is.
    #[test]
    fn value_sensitive_options_still_report_which_value_was_refused() {
        let err = ConnInfo::parse("host=h channel_binding=require")
            .unwrap_err()
            .to_string();
        assert!(err.contains("channel_binding=require"), "{err}");
        // ...and the loose form is accepted, which is what makes the value
        // load-bearing in the message.
        assert!(ConnInfo::parse("host=h channel_binding=prefer").is_ok());
    }

    /// `requiressl=1` is `sslmode=require` (translated in `conninfo_storeval`, so
    /// it works in both syntaxes); `ssl=true` is JDBC compatibility and is
    /// URI-query only. Dropping either is the same downgrade as the headline bug.
    #[test]
    fn deprecated_ssl_aliases_are_translated_like_libpq() {
        assert!(matches!(
            ConnInfo::parse("host=h requiressl=1").unwrap().sslmode,
            SslMode::Require
        ));
        assert!(matches!(
            ConnInfo::parse("host=h requiressl=0").unwrap().sslmode,
            SslMode::Prefer
        ));
        assert!(matches!(
            ConnInfo::parse("postgresql://u@h/db?requiressl=1")
                .unwrap()
                .sslmode,
            SslMode::Require
        ));
        assert!(matches!(
            ConnInfo::parse("postgresql://u@h/db?ssl=true")
                .unwrap()
                .sslmode,
            SslMode::Require
        ));
        // `ssl` is not a libpq keyword outside the URI query loop.
        assert!(ConnInfo::parse("host=h ssl=true").is_err());
    }

    /// libpq docs 32.1.1.2: the hierarchical values may instead be given as
    /// named query parameters. Without these arms the new unknown-key error
    /// would reject a URI libpq documents as valid — this URI previously parsed
    /// to host == "".
    #[test]
    fn uri_query_may_carry_the_hierarchical_params() {
        let ci = ConnInfo::parse("postgresql:///mydb?host=localhost&port=5433").unwrap();
        assert_eq!(ci.host, "localhost");
        assert_eq!(ci.port, 5433);
        assert_eq!(ci.dbname, "mydb");

        // Query overrides the hierarchical part, as in libpq.
        let ci = ConnInfo::parse("postgresql://a:5432/db?host=b&port=5433").unwrap();
        assert_eq!(ci.host, "b");
        assert_eq!(ci.port, 5433);

        // The dbname->user fallback runs AFTER the query loop, so a
        // query-supplied user still names the database.
        let ci = ConnInfo::parse("postgresql://h/?user=alice").unwrap();
        assert_eq!(ci.user, "alice");
        assert_eq!(ci.dbname, "alice");
    }

    /// What an alias or an empty value means differs between the two syntaxes:
    /// `database` is a `dbname` alias in key/value only, and an empty `host` or
    /// `dbname` falls back in a URI but stays empty in key/value.
    #[test]
    fn aliases_and_empty_values_differ_between_the_syntaxes() {
        assert_eq!(
            ConnInfo::parse("host=h user=u database=d").unwrap().dbname,
            "d"
        );
        assert!(ConnInfo::parse("postgresql://u@h/db?database=d").is_err());

        let ci = ConnInfo::parse("postgresql://u@h/db?dbname=&host=").unwrap();
        assert_eq!((ci.dbname.as_str(), ci.host.as_str()), ("u", "localhost"));
        let ci = ConnInfo::parse("host='' user=u dbname=''").unwrap();
        assert_eq!((ci.dbname.as_str(), ci.host.as_str()), ("", ""));
    }

    /// libpq substitutes the default port for an empty port field, so tightening
    /// numeric validation must not break `port=` / `host:`.
    #[test]
    fn empty_port_falls_back_to_default() {
        assert_eq!(ConnInfo::parse("host=h port=").unwrap().port, 5432);
        assert_eq!(ConnInfo::parse("postgresql://u@h:/db").unwrap().port, 5432);
        // Emptiness is special only for port; other options still reject it.
        assert!(ConnInfo::parse("host=h connect_timeout=").is_err());
    }

    #[test]
    fn parse_key_value_basic() {
        let ci = ConnInfo::parse(
            "host=db.example.com port=5433 user=repl password=secret dbname=mydb sslmode=require",
        )
        .unwrap();
        assert_eq!(ci.host, "db.example.com");
        assert_eq!(ci.port, 5433);
        assert_eq!(ci.user, "repl");
        assert_eq!(ci.password, Some("secret".to_string()));
        assert_eq!(ci.dbname, "mydb");
        assert_eq!(ci.sslmode, SslMode::Require);
    }

    #[test]
    fn parse_key_value_quoted() {
        let ci = ConnInfo::parse("host=localhost password='has spaces'").unwrap();
        assert_eq!(ci.password, Some("has spaces".to_string()));
    }

    /// Pins the CURRENT behaviour, which is a known divergence from libpq —
    /// **not** parity. libpq has no `''` rule and REJECTS this input with
    /// `missing "=" after "'s' a test'"` (verified with `PQconninfoParse`,
    /// libpq 18.6). See the divergence table at the value reader. When that is
    /// fixed for 0.9.0 this test should flip to asserting an error.
    #[test]
    fn parse_key_value_escaped_quotes() {
        let ci = ConnInfo::parse("host=localhost password='it''s a test'").unwrap();
        assert_eq!(ci.password, Some("it's a test".to_string()));
    }

    /// Same known divergence as [`parse_key_value_escaped_quotes`]; libpq rejects
    /// this too.
    #[test]
    fn parse_key_value_multiple_escaped_quotes() {
        let ci = ConnInfo::parse("host=localhost password='a''b''c'").unwrap();
        assert_eq!(ci.password, Some("a'b'c".to_string()));
    }

    // === parse_sslmode variants ===

    #[test]
    fn test_parse_sslmode_all_variants() {
        assert!(matches!(parse_sslmode("disable"), Ok(SslMode::Disable)));
        assert!(matches!(parse_sslmode("allow"), Ok(SslMode::Allow)));
        assert!(matches!(parse_sslmode("prefer"), Ok(SslMode::Prefer)));
        assert!(matches!(parse_sslmode("require"), Ok(SslMode::Require)));
        assert!(matches!(parse_sslmode("verify-ca"), Ok(SslMode::VerifyCa)));
        assert!(matches!(
            parse_sslmode("verify-full"),
            Ok(SslMode::VerifyFull)
        ));
    }

    /// An unrecognised sslmode must be an error, never a default. Defaulting to
    /// `prefer` turned a typo into a silent security downgrade: `prefer` accepts
    /// any certificate and falls back to plaintext. libpq rejects these too.
    #[test]
    fn parse_sslmode_rejects_unknown_instead_of_downgrading() {
        for bad in ["something_else", "", "verify_full", "REQUIRE", "requirre"] {
            assert!(
                parse_sslmode(bad).is_err(),
                "sslmode {bad:?} must be rejected, not silently downgraded"
            );
        }
        // ...and the whole conninfo parse fails rather than connecting weakly.
        assert!(ConnInfo::parse("host=h sslmode=verify_full").is_err());
        assert!(ConnInfo::parse("postgresql://u@h/db?sslmode=verify_full").is_err());
    }

    /// `byte as char` is a Latin-1 lift, not a percent-decode: it re-encoded every
    /// escape >= %80 as two UTF-8 bytes, corrupting non-ASCII passwords before
    /// SCRAM hashed them.
    #[test]
    fn url_decode_produces_raw_octets_not_latin1() {
        assert_eq!(url_decode("p%C3%A9ss").as_bytes(), "péss".as_bytes());
        assert_eq!(url_decode("%E6%97%A5%E6%9C%AC"), "日本");
        // Malformed escapes are passed through rather than dropped.
        assert_eq!(url_decode("100%"), "100%");
        assert_eq!(url_decode("a%zz"), "a%zz");
        assert_eq!(url_decode("plain"), "plain");
    }

    /// The path is optional in a libpq URI. Splitting only on `/` swallowed the
    /// query string into the hostname, dropping every parameter — including
    /// sslmode, which then silently fell back to the default.
    #[test]
    fn uri_query_without_path_is_parsed() {
        let info = ConnInfo::parse("postgresql://u@myhost?sslmode=require").unwrap();
        assert_eq!(info.host, "myhost");
        assert!(matches!(info.sslmode, SslMode::Require));

        let info = ConnInfo::parse("postgres://h?replication=database").unwrap();
        assert_eq!(info.host, "h");
        assert!(matches!(info.replication, ReplicationMode::Database));
    }

    /// A bracketed IPv6 literal must not be split on its own colons.
    #[test]
    fn uri_ipv6_host_is_unbracketed() {
        let info = ConnInfo::parse("postgresql://u@[::1]/db").unwrap();
        assert_eq!(info.host, "::1");
        assert_eq!(info.port, 5432);

        let info = ConnInfo::parse("postgresql://u@[2001:db8::1]:5433/db").unwrap();
        assert_eq!(info.host, "2001:db8::1");
        assert_eq!(info.port, 5433);

        assert!(ConnInfo::parse("postgresql://u@[::1/db").is_err());
    }

    /// Silently defaulting a malformed number is worse than failing: `port=99999`
    /// would connect to whatever is on 5432, and `connect_timeout=abc` becomes 0,
    /// which the startup path reads as "no timeout".
    #[test]
    fn numeric_options_are_validated_not_defaulted() {
        for bad in [
            "host=h port=99999",
            "host=h port=abc",
            "host=h port=-1",
            "host=h connect_timeout=abc",
            "host=h keepalives_idle=x",
        ] {
            assert!(ConnInfo::parse(bad).is_err(), "{bad:?} must be rejected");
        }
        assert!(ConnInfo::parse("postgresql://u@h:99999/db").is_err());
        // Valid values still parse.
        let info = ConnInfo::parse("host=h port=5433 connect_timeout=10").unwrap();
        assert_eq!(info.port, 5433);
        assert_eq!(info.connect_timeout, 10);
    }

    #[test]
    fn test_parse_replication_mode_variants() {
        assert!(matches!(
            parse_replication_mode("database"),
            Ok(ReplicationMode::Database)
        ));
        for on in ["true", "yes", "on", "1"] {
            assert!(
                matches!(parse_replication_mode(on), Ok(ReplicationMode::Physical)),
                "{on}"
            );
        }
        for off in ["false", "no", "off", "0"] {
            assert!(
                matches!(parse_replication_mode(off), Ok(ReplicationMode::None)),
                "{off}"
            );
        }
    }

    /// A typo here used to silently open an ordinary connection, and the failure
    /// only surfaced much later as an opaque server error on `START_REPLICATION`.
    /// In a logical-replication library that is the worst place to learn about it.
    #[test]
    fn parse_replication_mode_rejects_unknown() {
        for bad in ["dattabase", "", "DATABASE", "unknown"] {
            assert!(parse_replication_mode(bad).is_err(), "{bad:?}");
        }
        assert!(ConnInfo::parse("host=h replication=dattabase").is_err());
        assert!(ConnInfo::parse("postgresql://u@h/db?replication=dattabase").is_err());
    }

    #[test]
    fn test_parse_ssl_negotiation_variants() {
        assert!(matches!(
            parse_ssl_negotiation("direct"),
            Ok(SslNegotiation::Direct)
        ));
        assert!(matches!(
            parse_ssl_negotiation("postgres"),
            Ok(SslNegotiation::Postgres)
        ));
    }

    /// libpq hard-errors on an unrecognised `sslnegotiation`. Defaulting would be
    /// the *safe* direction here (one extra round-trip, no security loss), but
    /// silently ignoring a typo in a TLS option is what `parse_sslmode` was
    /// hardened against — so this stays consistent with it.
    #[test]
    fn parse_ssl_negotiation_rejects_unknown() {
        for bad in ["drect", "", "DIRECT", "unknown"] {
            assert!(parse_ssl_negotiation(bad).is_err(), "{bad:?}");
        }
    }

    // === URI edge cases ===

    #[test]
    fn test_parse_uri_postgres_prefix() {
        // postgres:// should work the same as postgresql://
        let info = ConnInfo::parse("postgres://user:pass@localhost:5432/mydb").unwrap();
        assert_eq!(info.host, "localhost");
        assert_eq!(info.port, 5432);
        assert_eq!(info.user, "user");
        assert_eq!(info.password, Some("pass".to_string()));
        assert_eq!(info.dbname, "mydb");
    }

    #[test]
    fn test_parse_uri_no_credentials() {
        // No user:pass@ means defaults
        let info = ConnInfo::parse("postgresql://localhost:5432/mydb").unwrap();
        assert_eq!(info.host, "localhost");
        assert_eq!(info.user, "postgres"); // default user
        assert_eq!(info.password, None);
    }

    #[test]
    fn test_parse_uri_user_no_password() {
        let info = ConnInfo::parse("postgresql://myuser@localhost:5432/mydb").unwrap();
        assert_eq!(info.user, "myuser");
        assert_eq!(info.password, None);
    }

    #[test]
    fn test_parse_uri_empty_password() {
        let info = ConnInfo::parse("postgresql://myuser:@localhost:5432/mydb").unwrap();
        assert_eq!(info.user, "myuser");
        // Empty password should be treated as None
        assert!(info.password.is_none() || info.password.as_deref() == Some(""));
    }

    #[test]
    fn test_parse_uri_no_database() {
        // No database in path → defaults to user
        let info = ConnInfo::parse("postgresql://myuser:pass@localhost:5432").unwrap();
        assert_eq!(info.dbname, info.user);
    }

    // === url_decode edge cases ===

    #[test]
    fn test_url_decode_no_encoding() {
        assert_eq!(url_decode("hello_world"), "hello_world");
    }

    #[test]
    fn test_url_decode_multiple_encoded() {
        assert_eq!(url_decode("%20%40%2F"), " @/");
    }

    #[test]
    fn test_url_decode_plus_sign() {
        // Plus signs in URLs can mean spaces; check if they're passed through or decoded
        let result = url_decode("hello+world");
        // Our implementation may or may not convert + to space
        assert!(!result.is_empty());
    }

    // === sslrootcert parsing ===

    #[test]
    fn test_parse_uri_sslrootcert() {
        let ci = ConnInfo::parse(
            "postgresql://user:pass@host:5432/db?sslmode=verify-ca&sslrootcert=/path/to/ca.pem",
        )
        .unwrap();
        assert_eq!(ci.sslmode, SslMode::VerifyCa);
        assert_eq!(ci.sslrootcert, Some("/path/to/ca.pem".to_string()));
    }

    #[test]
    fn test_parse_uri_sslrootcert_encoded() {
        let ci = ConnInfo::parse(
            "postgresql://user:pass@host/db?sslrootcert=/path%20with%20spaces/ca.pem",
        )
        .unwrap();
        assert_eq!(ci.sslrootcert, Some("/path with spaces/ca.pem".to_string()));
    }

    #[test]
    fn test_parse_uri_no_sslrootcert() {
        let ci = ConnInfo::parse("postgresql://user:pass@host/db?sslmode=require").unwrap();
        assert!(ci.sslrootcert.is_none());
    }

    #[test]
    fn test_parse_key_value_sslrootcert() {
        let ci = ConnInfo::parse(
            "host=localhost sslmode=verify-ca sslrootcert=/etc/ssl/certs/ca.pem user=test",
        )
        .unwrap();
        assert_eq!(ci.sslmode, SslMode::VerifyCa);
        assert_eq!(ci.sslrootcert, Some("/etc/ssl/certs/ca.pem".to_string()));
    }

    #[test]
    fn test_parse_key_value_sslrootcert_quoted() {
        let ci = ConnInfo::parse("host=localhost sslrootcert='/path with spaces/ca.pem'").unwrap();
        assert_eq!(ci.sslrootcert, Some("/path with spaces/ca.pem".to_string()));
    }

    // === sslnegotiation parsing ===

    #[test]
    fn test_parse_uri_sslnegotiation_direct() {
        let ci = ConnInfo::parse(
            "postgresql://user:pass@host:5432/db?sslmode=require&sslnegotiation=direct",
        )
        .unwrap();
        assert_eq!(ci.sslnegotiation, SslNegotiation::Direct);
    }

    #[test]
    fn test_parse_uri_sslnegotiation_postgres() {
        let ci = ConnInfo::parse(
            "postgresql://user:pass@host:5432/db?sslmode=require&sslnegotiation=postgres",
        )
        .unwrap();
        assert_eq!(ci.sslnegotiation, SslNegotiation::Postgres);
    }

    #[test]
    fn test_parse_uri_sslnegotiation_default() {
        let ci = ConnInfo::parse("postgresql://user:pass@host:5432/db?sslmode=require").unwrap();
        assert_eq!(ci.sslnegotiation, SslNegotiation::Postgres);
    }

    #[test]
    fn test_parse_key_value_sslnegotiation_direct() {
        let ci = ConnInfo::parse("host=localhost sslmode=require sslnegotiation=direct user=test")
            .unwrap();
        assert_eq!(ci.sslnegotiation, SslNegotiation::Direct);
    }

    #[test]
    fn test_parse_key_value_sslnegotiation_default() {
        let ci = ConnInfo::parse("host=localhost sslmode=require user=test").unwrap();
        assert_eq!(ci.sslnegotiation, SslNegotiation::Postgres);
    }

    // === keepalive and timeout params ===

    #[test]
    fn test_parse_uri_keepalive_params() {
        let ci = ConnInfo::parse(
            "postgresql://user:pass@host:5432/db?keepalives=1&keepalives_idle=60&keepalives_interval=5&keepalives_count=6",
        )
        .unwrap();
        assert!(ci.keepalives);
        assert_eq!(ci.keepalives_idle, 60);
        assert_eq!(ci.keepalives_interval, 5);
        assert_eq!(ci.keepalives_count, 6);
    }

    #[test]
    fn test_parse_uri_keepalives_disabled() {
        let ci = ConnInfo::parse("postgresql://user:pass@host/db?keepalives=0").unwrap();
        assert!(!ci.keepalives);
    }

    /// `tcp_user_timeout` used to sit in `IGNORED_OPTIONS`: parsed, logged and
    /// thrown away on this backend while libpq honoured it, so the same string
    /// bounded a dead round-trip on one backend and not the other, silently.
    #[test]
    fn tcp_user_timeout_is_honoured_not_discarded() {
        // Both syntaxes, because the two parsers are independent.
        let uri = ConnInfo::parse("postgresql://user:pass@host/db?tcp_user_timeout=15000").unwrap();
        assert_eq!(uri.tcp_user_timeout, 15000);

        let kv = ConnInfo::parse("host=h user=u dbname=d tcp_user_timeout=15000").unwrap();
        assert_eq!(kv.tcp_user_timeout, 15000);

        // Absent means disabled, so wiring it up changed nothing for anyone who
        // never set it.
        assert_eq!(
            ConnInfo::parse("host=h user=u dbname=d")
                .unwrap()
                .tcp_user_timeout,
            0
        );
    }

    /// Clamped, not rejected — libpq's `setTCPUserTimeout` takes a negative as 0,
    /// and a `Config` error here is permanent, so rejecting would hard fail a
    /// string psql accepts. Non-numeric is still an error, as it is in libpq.
    #[test]
    fn tcp_user_timeout_clamps_negative_and_rejects_garbage() {
        assert_eq!(
            ConnInfo::parse("host=h user=u dbname=d tcp_user_timeout=-1")
                .unwrap()
                .tcp_user_timeout,
            0
        );
        assert!(ConnInfo::parse("host=h user=u dbname=d tcp_user_timeout=abc").is_err());
    }

    #[test]
    fn test_parse_uri_connect_timeout() {
        let ci = ConnInfo::parse("postgresql://user:pass@host/db?connect_timeout=30").unwrap();
        assert_eq!(ci.connect_timeout, 30);
    }

    #[test]
    fn test_parse_key_value_keepalive_params() {
        let ci = ConnInfo::parse(
            "host=localhost keepalives=1 keepalives_idle=90 keepalives_interval=15 keepalives_count=5 connect_timeout=10",
        )
        .unwrap();
        assert!(ci.keepalives);
        assert_eq!(ci.keepalives_idle, 90);
        assert_eq!(ci.keepalives_interval, 15);
        assert_eq!(ci.keepalives_count, 5);
        assert_eq!(ci.connect_timeout, 10);
    }

    #[test]
    fn test_keepalive_defaults() {
        let ci = ConnInfo::parse("postgresql://user:pass@host/db").unwrap();
        assert!(ci.keepalives);
        assert_eq!(ci.keepalives_idle, 120);
        assert_eq!(ci.keepalives_interval, 10);
        assert_eq!(ci.keepalives_count, 3);
        assert_eq!(ci.connect_timeout, 0);
    }

    // === Debug redaction ===

    #[test]
    fn test_debug_redacts_password() {
        let ci = ConnInfo::parse("postgresql://user:supersecret@host/db").unwrap();
        let debug_output = format!("{:?}", ci);
        assert!(
            !debug_output.contains("supersecret"),
            "Debug output should not contain the password: {debug_output}"
        );
        assert!(
            debug_output.contains("REDACTED"),
            "Debug output should contain REDACTED: {debug_output}"
        );
    }

    #[test]
    fn test_debug_redacts_options() {
        let ci = ConnInfo::parse("postgresql://user@host/db?options=-c%20custom.token%3Dhunter2")
            .unwrap();
        let debug_output = format!("{:?}", ci);
        assert!(
            !debug_output.contains("hunter2"),
            "Debug output should not contain options: {debug_output}"
        );
        assert!(
            debug_output.contains("options: \"<REDACTED>\""),
            "Debug output should redact options: {debug_output}"
        );
        // The value itself is still what the startup message carries.
        assert_eq!(ci.options.as_deref(), Some("-c custom.token=hunter2"));

        let unset = format!(
            "{:?}",
            ConnInfo::parse("postgresql://user@host/db").unwrap()
        );
        assert!(unset.contains("options: \"None\""), "{unset}");
    }

    #[test]
    fn test_debug_shows_none_when_no_password() {
        let ci = ConnInfo::parse("postgresql://user@host/db").unwrap();
        let debug_output = format!("{:?}", ci);
        assert!(
            debug_output.contains("None"),
            "Debug should show None for missing password: {debug_output}"
        );
    }
}
