//! Managed initial snapshot: copy what is already there, then stream what changes.
//!
//! A CDC stream only tells you what *changed*. Without a baseline, a downstream
//! sink can never be brought to a consistent state — you would be applying
//! `UPDATE users SET name='B' WHERE id=7` to a table that has no row 7.
//!
//! The two obvious ways to get that baseline are both wrong:
//!
//! ```text
//! snapshot, then start streaming   ->  changes in between fall in the gap
//! start streaming, then snapshot   ->  the snapshot contains already-streamed rows
//! ```
//!
//! PostgreSQL's answer is the *exported snapshot*:
//! `CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT` produces a snapshot taken at
//! exactly the slot's `consistent_point`. Snapshot + stream-from-that-point is
//! complete, with no gap and no duplicate window.
//!
//! This example proves that property rather than asserting it: it seeds two rows
//! *before* the snapshot and inserts a third *after* the snapshot is exported but
//! *before* streaming starts — the exact window where a naive implementation
//! loses or duplicates data. The third row must appear on the stream exactly
//! once, and must not be in the snapshot.
//!
//! ## What to notice
//!
//! - One `WalRouter` with one set of handlers serves **both** phases. Snapshot
//!   rows arrive as ordinary [`ChangeEvent`]s, so nothing downstream needs to
//!   care which phase produced them.
//! - The replication stream is *moved into* the snapshot handle. `start()` during
//!   the snapshot would destroy the very snapshot being read, so the API makes it
//!   impossible to write — try uncommenting the marked line and it will not
//!   compile.
//!
//! ## Prerequisites
//!
//! PostgreSQL 15+ with logical replication enabled:
//!
//! ```text
//! wal_level = logical
//! max_replication_slots = 4
//! max_wal_senders = 4
//! ```
//!
//! ## Usage
//!
//! ```bash
//! export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/postgres?replication=database"
//! export DATABASE_URL_REGULAR="postgresql://postgres:postgres@localhost:5432/postgres"
//! cargo run
//! ```
//!
//! The example creates its own table, publication and slot, and drops them again
//! on the way out, so it is safe to re-run.

use pg_walstream::snapshot::SnapshotOutcome;
use pg_walstream::{
    CancellationToken, LogicalReplicationStream, PgReplicationConnection,
    ReplicationStreamConfig, RetryConfig, StreamingMode, WalRouter,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, Level};

const SLOT: &str = "example_initial_snapshot";
const TABLE: &str = "example_snapshot_users";
const PUBLICATION: &str = "example_snapshot_pub";

/// The row shape handlers receive. `#[derive(WalTable)]` binds it to the table,
/// so the router can infer it — and the SAME struct is used for snapshot rows and
/// live rows, because they are the same type by construction.
#[derive(Debug, serde::Deserialize, pg_walstream::WalTable)]
#[wal(table = "example_snapshot_users")]
struct User {
    id: i32,
    name: String,
}

fn replication_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/postgres?replication=database".to_string()
    })
}

/// A plain (non-replication) connection, used here only to set up demo data.
fn regular_url() -> String {
    std::env::var("DATABASE_URL_REGULAR").unwrap_or_else(|_| {
        replication_url()
            .replace("?replication=database", "")
            .replace("&replication=database", "")
    })
}

fn sql(conn: &mut PgReplicationConnection, statement: &str) {
    conn.exec(statement)
        .unwrap_or_else(|e| panic!("{statement}: {e}"));
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .init();

    // ── Setup: a table with two rows already in it ──────────────────────────
    let mut admin = PgReplicationConnection::connect(&regular_url())?;
    let _ = admin.exec(&format!("DROP PUBLICATION IF EXISTS {PUBLICATION}"));
    let _ = admin.exec(&format!("DROP TABLE IF EXISTS {TABLE} CASCADE"));
    sql(
        &mut admin,
        &format!("CREATE TABLE {TABLE} (id SERIAL PRIMARY KEY, name TEXT NOT NULL)"),
    );
    sql(
        &mut admin,
        &format!("INSERT INTO {TABLE} (name) VALUES ('alice'), ('bob')"),
    );
    sql(
        &mut admin,
        &format!("CREATE PUBLICATION {PUBLICATION} FOR TABLE {TABLE}"),
    );
    drop_slot();
    info!("seeded {TABLE} with 2 rows that exist BEFORE replication starts");

    // ── One router, one set of handlers, used for both phases ───────────────
    let snapshot_rows = Arc::new(AtomicUsize::new(0));
    let live_rows = Arc::new(AtomicUsize::new(0));
    let phase = Arc::new(AtomicUsize::new(0)); // 0 = snapshot, 1 = live

    let mut router = WalRouter::new();
    {
        let (snap, live, phase) = (
            Arc::clone(&snapshot_rows),
            Arc::clone(&live_rows),
            Arc::clone(&phase),
        );
        router.on_insert_of::<User, _>(move |user: User| {
            let (snap, live, phase) = (Arc::clone(&snap), Arc::clone(&live), Arc::clone(&phase));
            async move {
                // Which phase we are in is known from the CALL SITE, not from a
                // field on the event — the type-state already forces them apart.
                if phase.load(Ordering::Relaxed) == 0 {
                    snap.fetch_add(1, Ordering::Relaxed);
                    info!("  [snapshot] id={} name={}", user.id, user.name);
                } else {
                    live.fetch_add(1, Ordering::Relaxed);
                    info!("  [stream]   id={} name={}", user.id, user.name);
                }
                Ok(())
            }
        });
    }

    // ── Connect with the snapshot enabled ───────────────────────────────────
    let config = ReplicationStreamConfig::builder(SLOT, PUBLICATION)
        .with_protocol_version(2)
        .with_streaming_mode(StreamingMode::On)
        .with_connection_timeout(Duration::from_secs(30))
        .with_retry_config(RetryConfig::default())
        // The only new knob: create the slot with SNAPSHOT 'export'.
        .with_initial_snapshot(true);

    let stream = LogicalReplicationStream::new(&replication_url(), config).await?;

    // ── Phase 1: the snapshot ───────────────────────────────────────────────
    let mut stream = match stream.snapshot().await? {
        // Not an error: the slot already existed, so there is nothing to copy.
        // This is the ordinary "restarting an existing subscription" path.
        SnapshotOutcome::Unavailable(stream) => {
            info!("no snapshot available (slot already existed) — resuming the stream only");
            stream
        }

        SnapshotOutcome::Available(snapshot) => {
            info!(
                "snapshot ready: {} table(s) at LSN {}",
                snapshot.tables().len(),
                snapshot.consistent_point()
            );

            // This row is committed AFTER the snapshot was exported but BEFORE
            // streaming starts. It is the row a naive implementation loses or
            // duplicates, so it is the whole point of the example.
            sql(
                &mut admin,
                &format!("INSERT INTO {TABLE} (name) VALUES ('carol-inserted-mid-handoff')"),
            );
            info!("inserted 'carol' during the handoff window");

            let mut events = snapshot.events();

            // The stream lives INSIDE `events`. Uncommenting the next line does
            // not compile, which is the point — `start()` here would run a
            // replication command and destroy the snapshot being read:
            //
            //     stream.start(None).await?;

            router.run_snapshot(&mut events).await?;
            events.finish().await?
        }
    };

    // ── Phase 2: the live stream, same handlers ─────────────────────────────
    phase.store(1, Ordering::Relaxed);
    // Resumes exactly at the snapshot's consistent point.
    stream.start(None).await?;
    info!("streaming from the snapshot's consistent point");

    let token = CancellationToken::new();
    {
        let token = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            token.cancel();
        });
    }

    let mut event_stream = stream.into_stream(token);
    // Drive until the auto-cancel fires. `run` treats Cancelled as a clean exit.
    router.run(&mut event_stream).await?;

    // ── What the two phases saw ─────────────────────────────────────────────
    let snapshot_count = snapshot_rows.load(Ordering::Relaxed);
    let live_count = live_rows.load(Ordering::Relaxed);
    info!("──────────────────────────────────────────────");
    info!("snapshot delivered {snapshot_count} row(s)  (expected 2: alice, bob)");
    info!("stream   delivered {live_count} row(s)  (expected 1: carol)");

    if snapshot_count == 2 && live_count == 1 {
        info!("OK — no gap, no duplicate: every row arrived exactly once");
    } else {
        info!("UNEXPECTED — see above");
    }

    cleanup();
    Ok(())
}

fn drop_slot() {
    if let Ok(mut conn) = PgReplicationConnection::connect(&replication_url()) {
        let _ = conn.exec(&format!(
            "SELECT pg_drop_replication_slot('{SLOT}') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{SLOT}')"
        ));
    }
}

fn cleanup() {
    drop_slot();
    if let Ok(mut conn) = PgReplicationConnection::connect(&regular_url()) {
        let _ = conn.exec(&format!("DROP PUBLICATION IF EXISTS {PUBLICATION}"));
        let _ = conn.exec(&format!("DROP TABLE IF EXISTS {TABLE} CASCADE"));
    }
}
