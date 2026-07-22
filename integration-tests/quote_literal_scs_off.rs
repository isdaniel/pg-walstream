#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! Live proof that `quote_literal` is injection-safe under either value of the
//! server's `standard_conforming_strings` setting.
//!
//! With `standard_conforming_strings = off` a backslash is an escape character
//! inside a plain `'…'` string, so quote-doubling alone would let a crafted
//! value terminate the literal early. `quote_literal` defends against this by
//! switching to the escape-string form ` E'…'` (backslashes doubled) whenever
//! the input contains a backslash. This test feeds a payload combining a
//! backslash, a single quote, and trailing SQL through `quote_literal` into a
//! `SELECT`, then asserts the value roundtrips byte-for-byte — proving the
//! string never terminated early and the trailing SQL never executed.
//!
//! ## Prerequisites
//!
//! - PostgreSQL (any supported version)
//! - `DATABASE_URL_REGULAR` — a regular (non-replication) connection, e.g.
//!   `postgresql://postgres:postgres@localhost:5432/test_walstream`
//!   (falls back to `DATABASE_URL` with `replication=database` stripped)
//!
//! ## Running Locally
//!
//! ```bash
//! export DATABASE_URL_REGULAR="postgresql://postgres:postgres@localhost:5432/test_walstream"
//! cargo test --test quote_literal_scs_off -- --ignored --nocapture
//! ```

use pg_walstream::{quote_literal, PgReplicationConnection};

fn regular_conn_string() -> String {
    std::env::var("DATABASE_URL_REGULAR").unwrap_or_else(|_| {
        std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| {
                "postgresql://postgres:postgres@localhost:5432/test_walstream".to_string()
            })
            .replace("?replication=database", "")
            .replace("&replication=database", "")
    })
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();
}

/// `SELECT <quote_literal(payload)>` and return the single scalar the server
/// echoes back.
fn roundtrip(conn: &mut PgReplicationConnection, payload: &str) -> String {
    let sql = format!(
        "SELECT {} AS v",
        quote_literal(payload).expect("no null bytes")
    );
    conn.exec(&sql)
        .expect("SELECT of quoted literal must succeed")
        .get_value(0, 0)
        .expect("one row, one column")
}

#[test]
#[ignore = "requires live PostgreSQL"]
fn quote_literal_survives_standard_conforming_strings_off() {
    init_tracing();

    let mut conn =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");

    // Combines both literal-breakout vectors: a backslash (the scs=off escape
    // character) and a single quote, followed by SQL that must NOT run.
    let payload = r"weird \ value ' with; DROP TABLE x; --";

    for scs in ["off", "on"] {
        conn.exec(&format!("SET standard_conforming_strings = {scs}"))
            .unwrap_or_else(|e| panic!("SET standard_conforming_strings = {scs}: {e}"));

        let got = roundtrip(&mut conn, payload);
        assert_eq!(
            got, payload,
            "payload must roundtrip intact under standard_conforming_strings = {scs}; \
             early string termination would change, truncate, or drop it"
        );
    }
}
