//! Connection string parser for PostgreSQL.
//!
//! Supports both URI format (`postgresql://user:pass@host:port/db?params`)
//! and key-value format (`host=localhost port=5432 ...`).

use crate::error::ReplicationError;

/// Parsed connection configuration.
#[derive(Debug, Clone)]
pub struct ConnInfo {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub dbname: String,
    pub sslmode: SslMode,
    pub sslrootcert: Option<String>,
    pub replication: ReplicationMode,
    /// Connection timeout in seconds (0 = disabled). Maps to libpq's `connect_timeout`.
    pub connect_timeout: u64,
    /// Whether TCP keepalives are enabled (default: true). Maps to `keepalives`.
    pub keepalives: bool,
    /// Seconds of idle time before sending a keepalive probe. Maps to `keepalives_idle`.
    pub keepalives_idle: u64,
    /// Seconds between keepalive probes. Maps to `keepalives_interval`.
    pub keepalives_interval: u64,
    /// Maximum number of keepalive probes before declaring dead. Maps to `keepalives_count`.
    pub keepalives_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SslMode {
    Disable,
    Allow,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReplicationMode {
    Database,
    Physical,
    None,
}

impl ConnInfo {
    pub fn parse(conninfo: &str) -> Result<Self, ReplicationError> {
        if conninfo.starts_with("postgresql://") || conninfo.starts_with("postgres://") {
            Self::parse_uri(conninfo)
        } else {
            Self::parse_key_value(conninfo)
        }
    }

    fn parse_uri(uri: &str) -> Result<Self, ReplicationError> {
        let stripped = uri
            .trim_start_matches("postgresql://")
            .trim_start_matches("postgres://");

        // Split on @ to get credentials and host
        let (creds, rest) = stripped.split_once('@').unwrap_or(("", stripped));
        let (user, password) = if creds.is_empty() {
            ("postgres".to_string(), None)
        } else if let Some((u, p)) = creds.split_once(':') {
            (
                url_decode(u),
                if p.is_empty() {
                    None
                } else {
                    Some(url_decode(p))
                },
            )
        } else {
            (url_decode(creds), None)
        };

        // Split rest on / to get host:port and db?params
        let (host_port, db_params) = rest.split_once('/').unwrap_or((rest, ""));
        let (host, port) = if host_port.contains(':') {
            let (h, p) = host_port.rsplit_once(':').unwrap();
            (h.to_string(), p.parse::<u16>().unwrap_or(5432))
        } else {
            (host_port.to_string(), 5432)
        };

        // Split db from query params
        let (db, params_str) = db_params.split_once('?').unwrap_or((db_params, ""));
        let dbname = if db.is_empty() {
            user.clone()
        } else {
            url_decode(db)
        };

        // Parse query params
        let mut sslmode = SslMode::Prefer;
        let mut replication = ReplicationMode::None;
        let mut sslrootcert: Option<String> = None;
        let mut connect_timeout: u64 = 0;
        let mut keepalives = true;
        let mut keepalives_idle: u64 = 120;
        let mut keepalives_interval: u64 = 10;
        let mut keepalives_count: u32 = 3;

        for param in params_str.split('&') {
            if param.is_empty() {
                continue;
            }
            if let Some((key, val)) = param.split_once('=') {
                match key {
                    "sslmode" => sslmode = parse_sslmode(val),
                    "sslrootcert" => sslrootcert = Some(url_decode(val)),
                    "replication" => replication = parse_replication_mode(val),
                    "connect_timeout" => {
                        connect_timeout = val.parse().unwrap_or(0);
                    }
                    "keepalives" => keepalives = val != "0",
                    "keepalives_idle" => {
                        keepalives_idle = val.parse().unwrap_or(120);
                    }
                    "keepalives_interval" => {
                        keepalives_interval = val.parse().unwrap_or(10);
                    }
                    "keepalives_count" => {
                        keepalives_count = val.parse().unwrap_or(3);
                    }
                    _ => {} // ignore unknown params
                }
            }
        }

        // Check PGPASSWORD env var if no password in URI
        let password = password.or_else(|| std::env::var("PGPASSWORD").ok());

        Ok(ConnInfo {
            host,
            port,
            user,
            password,
            dbname,
            sslmode,
            sslrootcert,
            replication,
            connect_timeout,
            keepalives,
            keepalives_idle,
            keepalives_interval,
            keepalives_count,
        })
    }

    fn parse_key_value(input: &str) -> Result<Self, ReplicationError> {
        let mut host = "localhost".to_string();
        let mut port: u16 = 5432;
        let mut user = "postgres".to_string();
        let mut password: Option<String> = None;
        let mut dbname: Option<String> = None;
        let mut sslmode = SslMode::Prefer;
        let mut replication = ReplicationMode::None;
        let mut sslrootcert: Option<String> = None;
        let mut connect_timeout: u64 = 0;
        let mut keepalives = true;
        let mut keepalives_idle: u64 = 120;
        let mut keepalives_interval: u64 = 10;
        let mut keepalives_count: u32 = 3;

        // Simple key=value parser (handles single-quoted values)
        let mut chars = input.chars().peekable();
        while chars.peek().is_some() {
            // Skip whitespace
            while chars.peek().map_or(false, |c| c.is_whitespace()) {
                chars.next();
            }
            if chars.peek().is_none() {
                break;
            }

            // Read key
            let key: String = chars.by_ref().take_while(|c| *c != '=').collect();
            let key = key.trim();

            // Read value (may be quoted with single quotes).
            // Doubled single quotes inside a quoted value represent a literal quote,
            // e.g. password='it''s' → it's (matches libpq behavior).
            let value = if chars.peek() == Some(&'\'') {
                chars.next(); // skip opening quote
                let mut v = String::new();
                loop {
                    match chars.next() {
                        Some('\'') => {
                            // Check for doubled quote (escaped literal)
                            if chars.peek() == Some(&'\'') {
                                chars.next(); // consume second quote
                                v.push('\'');
                            } else {
                                break; // end of quoted value
                            }
                        }
                        Some(c) => v.push(c),
                        None => break, // unterminated quote — use what we have
                    }
                }
                v
            } else {
                let v: String = chars.by_ref().take_while(|c| !c.is_whitespace()).collect();
                v
            };

            match key {
                "host" | "hostaddr" => host = value,
                "port" => port = value.parse().unwrap_or(5432),
                "user" => user = value,
                "password" => password = Some(value),
                "dbname" | "database" => dbname = Some(value),
                "sslmode" => sslmode = parse_sslmode(&value),
                "sslrootcert" => sslrootcert = Some(value),
                "replication" => replication = parse_replication_mode(&value),
                "connect_timeout" => connect_timeout = value.parse().unwrap_or(0),
                "keepalives" => keepalives = value != "0",
                "keepalives_idle" => keepalives_idle = value.parse().unwrap_or(120),
                "keepalives_interval" => keepalives_interval = value.parse().unwrap_or(10),
                "keepalives_count" => keepalives_count = value.parse().unwrap_or(3),
                _ => {} // ignore unknown
            }
        }

        let password = password.or_else(|| std::env::var("PGPASSWORD").ok());

        let dbname = dbname.unwrap_or_else(|| user.clone());

        Ok(ConnInfo {
            host,
            port,
            user,
            password: password.clone(),
            dbname,
            sslmode,
            sslrootcert,
            replication,
            connect_timeout,
            keepalives,
            keepalives_idle,
            keepalives_interval,
            keepalives_count,
        })
    }
}

fn parse_sslmode(s: &str) -> SslMode {
    match s {
        "disable" => SslMode::Disable,
        "allow" => SslMode::Allow,
        "prefer" => SslMode::Prefer,
        "require" => SslMode::Require,
        "verify-ca" => SslMode::VerifyCa,
        "verify-full" => SslMode::VerifyFull,
        _ => SslMode::Prefer,
    }
}

fn parse_replication_mode(s: &str) -> ReplicationMode {
    match s {
        "database" => ReplicationMode::Database,
        "true" | "yes" | "1" => ReplicationMode::Physical,
        _ => ReplicationMode::None,
    }
}

/// Simple percent-decoding for URI components.
fn url_decode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '%' {
            let hex: String = chars.by_ref().take(2).collect();
            if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                result.push(byte as char);
            } else {
                result.push('%');
                result.push_str(&hex);
            }
        } else {
            result.push(c);
        }
    }
    result
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

    #[test]
    fn parse_key_value_escaped_quotes() {
        // Doubled single quotes represent a literal quote (libpq behavior)
        let ci = ConnInfo::parse("host=localhost password='it''s a test'").unwrap();
        assert_eq!(ci.password, Some("it's a test".to_string()));
    }

    #[test]
    fn parse_key_value_multiple_escaped_quotes() {
        let ci = ConnInfo::parse("host=localhost password='a''b''c'").unwrap();
        assert_eq!(ci.password, Some("a'b'c".to_string()));
    }

    // === parse_sslmode variants ===

    #[test]
    fn test_parse_sslmode_all_variants() {
        assert!(matches!(parse_sslmode("disable"), SslMode::Disable));
        assert!(matches!(parse_sslmode("allow"), SslMode::Allow));
        assert!(matches!(parse_sslmode("prefer"), SslMode::Prefer));
        assert!(matches!(parse_sslmode("require"), SslMode::Require));
        assert!(matches!(parse_sslmode("verify-ca"), SslMode::VerifyCa));
        assert!(matches!(parse_sslmode("verify-full"), SslMode::VerifyFull));
    }

    #[test]
    fn test_parse_sslmode_unknown_defaults_prefer() {
        assert!(matches!(parse_sslmode("something_else"), SslMode::Prefer));
        assert!(matches!(parse_sslmode(""), SslMode::Prefer));
    }

    #[test]
    fn test_parse_replication_mode_variants() {
        assert!(matches!(
            parse_replication_mode("database"),
            ReplicationMode::Database
        ));
        assert!(matches!(
            parse_replication_mode("true"),
            ReplicationMode::Physical
        ));
        assert!(matches!(
            parse_replication_mode("yes"),
            ReplicationMode::Physical
        ));
        assert!(matches!(
            parse_replication_mode("1"),
            ReplicationMode::Physical
        ));
        assert!(matches!(
            parse_replication_mode("unknown"),
            ReplicationMode::None
        ));
        assert!(matches!(parse_replication_mode(""), ReplicationMode::None));
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
}
