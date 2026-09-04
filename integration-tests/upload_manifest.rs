#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! Integration test for `UPLOAD_MANIFEST` + `BASE_BACKUP (INCREMENTAL)`.
//!
//! An incremental backup is a two-step conversation on one connection:
//! `UPLOAD_MANIFEST` (Query → CopyInResponse → CopyData* → CopyDone →
//! CommandComplete) carrying the `backup_manifest` of a prior full backup, then
//! `BASE_BACKUP (... INCREMENTAL)`. Both steps are exercised here; the manifest
//! is only validated by the server once the CopyIn completes, so the negative
//! case also proves the whole round-trip is wired up.
//!
//! ## Prerequisites
//!
//! - PostgreSQL 17+ (the command does not exist earlier) — skips gracefully below
//! - `summarize_wal = on` for the positive case — skips gracefully when off
//! - `DATABASE_URL` — replication connection
//! - `DATABASE_URL_REGULAR` — regular connection to the same database
//! - `BACKUP_MANIFEST_PATH` — path to a real `backup_manifest` from a prior full
//!   backup of *this* cluster. Without it the positive case is skipped; the
//!   negative cases still run.
//!
//! ## Running Locally
//!
//! ```bash
//! export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
//! export DATABASE_URL_REGULAR="postgresql://postgres:postgres@localhost:5432/test_walstream"
//! pg_basebackup -h localhost -U postgres -D /tmp/bb -Fp -X stream --no-sync
//! export BACKUP_MANIFEST_PATH=/tmp/bb/backup_manifest
//! cargo test --test upload_manifest -- --ignored --nocapture --test-threads=1
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

fn show(conn: &mut PgReplicationConnection, name: &str) -> String {
    conn.exec(&format!("SHOW {name}"))
        .unwrap_or_else(|e| panic!("SHOW {name}: {e}"))
        .get_value(0, 0)
        .unwrap_or_else(|| panic!("SHOW {name} returned no value"))
}

/// `true` when the server is PostgreSQL 17+, i.e. `UPLOAD_MANIFEST` exists.
fn server_supports_upload_manifest() -> bool {
    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    let version: i64 = show(&mut regular, "server_version_num")
        .parse()
        .expect("server_version_num is numeric");
    if version < 170000 {
        warn!("skipping: UPLOAD_MANIFEST requires PostgreSQL 17+, server is {version}");
        return false;
    }
    true
}

/// The full round-trip: a real manifest is accepted, and the incremental
/// `BASE_BACKUP` that depends on it is then accepted too.
///
/// Needs `BACKUP_MANIFEST_PATH` (CI runs `pg_basebackup` first) and
/// `summarize_wal = on`; skips gracefully otherwise.
#[tokio::test]
#[ignore = "requires live PostgreSQL 17+ and a backup manifest"]
async fn real_manifest_enables_incremental_base_backup() {
    init_tracing();
    if !server_supports_upload_manifest() {
        return;
    }

    let Ok(path) = std::env::var("BACKUP_MANIFEST_PATH") else {
        warn!("skipping: BACKUP_MANIFEST_PATH is not set");
        return;
    };
    let manifest = match std::fs::read(&path) {
        Ok(m) => m,
        Err(e) => {
            warn!("skipping: cannot read BACKUP_MANIFEST_PATH={path}: {e}");
            return;
        }
    };
    assert!(!manifest.is_empty(), "manifest at {path} is empty");

    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    if show(&mut regular, "summarize_wal") != "on" {
        warn!("skipping: incremental backup requires summarize_wal = on");
        return;
    }

    let mut repl = PgReplicationConnection::connect(&replication_conn_string())
        .expect("replication connection");

    repl.upload_manifest(&manifest)
        .expect("a real backup_manifest must be accepted");

    // The server keeps the manifest for the life of the connection, so the
    // incremental backup must run on this same connection. Entering the COPY
    // stream is the assertion — the payload is not drained; the connection is
    // dropped immediately after, which aborts the backup server-side.
    let opts = BaseBackupOptions {
        incremental: true,
        label: Some("it_upload_manifest".to_string()),
        ..Default::default()
    };
    repl.base_backup(&opts)
        .expect("INCREMENTAL base backup must be accepted after UPLOAD_MANIFEST");
}

/// Without a prior `UPLOAD_MANIFEST` the server rejects an incremental backup.
///
/// This is the failure the option used to produce unconditionally, when the
/// crate exposed `incremental` with no way to upload a manifest at all.
#[tokio::test]
#[ignore = "requires live PostgreSQL 17+"]
async fn incremental_without_manifest_is_rejected() {
    init_tracing();
    if !server_supports_upload_manifest() {
        return;
    }

    let mut repl = PgReplicationConnection::connect(&replication_conn_string())
        .expect("replication connection");

    let opts = BaseBackupOptions {
        incremental: true,
        ..Default::default()
    };
    let err = match repl.base_backup(&opts) {
        Ok(_) => panic!("INCREMENTAL without UPLOAD_MANIFEST must fail"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("UPLOAD_MANIFEST"),
        "expected the server's must-UPLOAD_MANIFEST error, got: {err}"
    );
}

/// A malformed manifest must surface the server's diagnostics rather than
/// hanging or reporting success.
///
/// The server only validates after the CopyIn completes, so reaching this error
/// proves the full `Query → CopyInResponse → CopyData → CopyDone → ErrorResponse`
/// exchange works on this backend.
#[test]
#[ignore = "requires live PostgreSQL 17+"]
fn malformed_manifest_is_rejected_after_the_copy_completes() {
    init_tracing();
    if !server_supports_upload_manifest() {
        return;
    }

    let mut repl = PgReplicationConnection::connect(&replication_conn_string())
        .expect("replication connection");

    let err = repl
        .upload_manifest(b"this is not a backup manifest")
        .expect_err("a malformed manifest must be rejected");
    assert!(
        err.to_string().to_lowercase().contains("manifest"),
        "expected a manifest-specific error, got: {err}"
    );

    // A rejected upload must leave the connection usable, not wedged mid-CopyIn.
    let after = repl
        .exec("IDENTIFY_SYSTEM")
        .expect("connection must still be usable after a rejected manifest");
    assert!(after.ntuples() > 0, "IDENTIFY_SYSTEM returned no rows");
}

/// A manifest that parses as JSON but carries a wrong `Manifest-Checksum`
/// must be rejected — the checksum is what makes the upload trustworthy.
///
/// Manifest version 2 is required: version 1 predates incremental backup and is
/// refused before the checksum is ever examined. Version 2 also carries a
/// `System-Identifier`, which must match this cluster, so it is read back from
/// `IDENTIFY_SYSTEM` rather than hard-coded.
#[test]
#[ignore = "requires live PostgreSQL 17+"]
fn manifest_with_bad_checksum_is_rejected() {
    init_tracing();
    if !server_supports_upload_manifest() {
        return;
    }

    let mut repl = PgReplicationConnection::connect(&replication_conn_string())
        .expect("replication connection");

    let system_id = repl
        .exec("IDENTIFY_SYSTEM")
        .expect("IDENTIFY_SYSTEM")
        .get_value(0, 0)
        .expect("systemid column");

    let manifest = format!(
        r#"{{ "PostgreSQL-Backup-Manifest-Version": 2,
"System-Identifier": {system_id},
"Files": [],
"WAL-Ranges": [],
"Manifest-Checksum": "{}"}}
"#,
        "0".repeat(64)
    );

    let err = repl
        .upload_manifest(manifest.as_bytes())
        .expect_err("a wrong Manifest-Checksum must be rejected");
    assert!(
        err.to_string().to_lowercase().contains("checksum"),
        "expected a checksum error, got: {err}"
    );
}
