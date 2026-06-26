#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! Integration tests for `PgResult::get_bytes` (issue #77).
//!
//! Verifies zero-copy, lossless access to a `BYTEA` column against a live
//! PostgreSQL server, on whichever backend is compiled in.
//!
//! ## Running
//!
//! ```bash
//! export DATABASE_URL_REGULAR="postgresql://postgres:postgres@localhost:5432/test_walstream"
//! cargo test --test binary_columns -- --ignored --nocapture
//! cargo test --test binary_columns --no-default-features --features rustls-tls -- --ignored --nocapture
//! ```

use pg_walstream::PgReplicationConnection;

/// Regular (non-replication) connection string.
fn regular_conn_string() -> String {
    std::env::var("DATABASE_URL_REGULAR").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/test_walstream".to_string()
    })
}

#[test]
#[ignore = "requires live PostgreSQL"]
fn test_get_bytes_bytea_lossless() {
    let mut conn = PgReplicationConnection::connect(&regular_conn_string()).expect("connection");

    // BYTEA literal: 0xDEADBEEF. In text protocol Postgres returns the ASCII
    // hex-escaped form "\xdeadbeef".
    let result = conn
        .exec("SELECT '\\xDEADBEEF'::bytea")
        .expect("SELECT bytea");
    assert_eq!(result.ntuples(), 1);

    let text = result.get_value(0, 0).expect("get_value");
    let raw = result.get_bytes(0, 0).expect("get_bytes");

    // get_bytes returns exactly what the backend stored (hex-escaped text here),
    // losslessly and with no UTF-8 substitution.
    assert_eq!(raw, text.as_bytes());
    assert_eq!(raw, b"\\xdeadbeef");
}

#[test]
#[ignore = "requires live PostgreSQL"]
fn test_get_bytes_null_is_none() {
    let mut conn = PgReplicationConnection::connect(&regular_conn_string()).expect("connection");

    let result = conn.exec("SELECT NULL::bytea").expect("SELECT NULL");
    assert_eq!(result.ntuples(), 1);
    assert_eq!(result.get_bytes(0, 0), None);
}

#[test]
#[ignore = "requires live PostgreSQL"]
fn test_get_bytes_empty_bytea() {
    let mut conn = PgReplicationConnection::connect(&regular_conn_string()).expect("connection");

    // An empty BYTEA is rendered in the text protocol as the 2-char string "\x".
    let result = conn.exec("SELECT ''::bytea").expect("SELECT empty bytea");
    assert_eq!(result.ntuples(), 1);

    let text = result.get_value(0, 0).expect("get_value");
    let raw = result.get_bytes(0, 0).expect("get_bytes");

    assert_eq!(raw, text.as_bytes());
    assert_eq!(result.get_bytes(0, 0), Some(b"\\x".as_ref()));
}

#[test]
#[ignore = "requires live PostgreSQL"]
fn test_get_bytes_matches_get_value_for_text() {
    let mut conn = PgReplicationConnection::connect(&regular_conn_string()).expect("connection");

    let result = conn.exec("SELECT 'hello'::text").expect("SELECT text");
    assert_eq!(result.ntuples(), 1);

    let text = result.get_value(0, 0).expect("get_value");

    // get_bytes is a faithful byte view of plain text too.
    assert_eq!(result.get_bytes(0, 0), Some("hello".as_bytes()));
    assert_eq!(result.get_bytes(0, 0), Some(text.as_bytes()));
}

#[test]
#[ignore = "requires live PostgreSQL"]
fn test_get_bytes_multi_row() {
    let mut conn = PgReplicationConnection::connect(&regular_conn_string()).expect("connection");

    let result = conn
        .exec("SELECT * FROM (VALUES ('\\xAA'::bytea), ('\\xBB'::bytea)) AS t(b)")
        .expect("SELECT multi-row bytea");
    assert_eq!(result.ntuples(), 2);

    assert_eq!(result.get_bytes(0, 0), Some(b"\\xaa".as_ref()));
    assert_eq!(result.get_bytes(1, 0), Some(b"\\xbb".as_ref()));
}

#[test]
#[ignore = "requires live PostgreSQL"]
fn test_get_bytes_out_of_bounds_live() {
    let mut conn = PgReplicationConnection::connect(&regular_conn_string()).expect("connection");

    let result = conn.exec("SELECT 'x'::bytea").expect("SELECT bytea");
    assert_eq!(result.ntuples(), 1);

    // Out-of-bounds row and column indices both yield None.
    assert_eq!(result.get_bytes(5, 0), None);
    assert_eq!(result.get_bytes(0, 5), None);
}
