#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! Live smoke test for `BASE_BACKUP` option generation (`build_base_backup_sql`).
//!
//! An unrecognized base-backup option is rejected by the server at command start
//! (in `parse_basebackup_options`), *before* any data streams — so `base_backup()`
//! returns `Err` for a bad option and `Ok` (COPY_OUT started) when every option is
//! accepted. This test drives a wide option combination and asserts the server
//! accepts it, then drops the connection to abort the backup (the crate does not
//! consume the tar stream, and the server handles a mid-backup client disconnect).
//! This is the base-backup analogue of the FAILOVER slot-grammar regression guard.
//!
//! ## Compatibility
//!
//! `build_base_backup_sql` emits the PostgreSQL 15+ parenthesized generic-option
//! form, so this test is gated to `server_version_num >= 150000` and skips with a
//! `warn!` on PostgreSQL 14. The `INCREMENTAL` option (PG17+) is intentionally NOT
//! exercised here: an incremental backup requires a prior `UPLOAD_MANIFEST`
//! handshake that this crate does not implement. `INCREMENTAL` is covered at the
//! unit level (`build_base_backup_sql` golden test) instead.
//!
//! ## Prerequisites
//!
//! - PostgreSQL 15+ (physical or `replication=database` connection)
//! - `DATABASE_URL` — replication connection
//!   (e.g. `postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database`)
//! - `DATABASE_URL_REGULAR` — regular connection to the same database
//!
//! ## Running Locally
//!
//! ```bash
//! export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
//! export DATABASE_URL_REGULAR="postgresql://postgres:postgres@localhost:5432/test_walstream"
//! cargo test --test base_backup_options -- --ignored --nocapture
//! ```

use pg_walstream::{BaseBackupOptions, PgReplicationConnection};
use tracing::warn;

fn replication_conn_string() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
            .to_string()
    })
}

fn regular_conn_string() -> String {
    std::env::var("DATABASE_URL_REGULAR").unwrap_or_else(|_| {
        let repl = replication_conn_string();
        repl.replace("?replication=database", "")
            .replace("&replication=database", "")
    })
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();
}

fn server_version_num(conn: &mut PgReplicationConnection) -> i64 {
    conn.exec("SHOW server_version_num")
        .expect("SHOW server_version_num")
        .get_value(0, 0)
        .expect("server_version_num value present")
        .parse()
        .expect("server_version_num is numeric")
}

#[test]
#[ignore = "requires live PostgreSQL 15+"]
fn test_base_backup_options_accepted() {
    init_tracing();

    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    let version = server_version_num(&mut regular);
    if version < 150000 {
        warn!(
            "skip BASE_BACKUP options test: server_version_num {version} < 150000 \
             (the parenthesized BASE_BACKUP grammar is PG15+)"
        );
        return;
    }

    // Widest combo that does not require extra handshakes. `CHECKPOINT 'fast'` keeps
    // the forced checkpoint quick; `MANIFEST 'no'` avoids manifest generation. All of
    // these option names are validated by the server before streaming begins, so an
    // Ok result proves every generated option name/spelling was accepted.
    let opts = BaseBackupOptions {
        label: Some("walstream_it".to_string()),
        progress: true,
        checkpoint: Some("fast".to_string()),
        max_rate: Some(1024),
        wal: true,
        wait: false,
        verify_checksums: true,
        manifest: Some("no".to_string()),
        ..Default::default()
    };

    let mut repl = PgReplicationConnection::connect(&replication_conn_string())
        .expect("replication connection");

    match repl.base_backup(&opts) {
        Ok(_) => { /* server accepted the options and entered COPY_OUT */ }
        Err(e) => panic!("server must accept the generated BASE_BACKUP options: {e}"),
    }

    // Drop the connection to abort the backup without draining the tar stream; the
    // server handles a mid-backup client disconnect cleanly.
    drop(repl);
}
