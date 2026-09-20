#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! Live matrix for `CREATE_REPLICATION_SLOT`: every option combination the
//! builder emits, executed against a real server so a grammar/version rejection
//! (the FAILOVER-class bug that unit string-assertions cannot catch) fails CI.
//!
//! Each case is created on a fresh replication connection, asserted to succeed
//! (or to fail client-side for the guard cases), then dropped. Version-gated
//! cases are skipped with a greppable `SKIP:` line, and each test ends with a
//! `SKIPPED n/total` summary, so a server that runs only part of the matrix is
//! visible instead of reporting a bare `ok`.
//!
//! ## Prerequisites
//!
//! - PostgreSQL 14+ with `wal_level = logical`
//! - `DATABASE_URL` — replication connection
//!   (e.g. `postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database`)
//! - `DATABASE_URL_REGULAR` — regular connection to the same database
//!
//! ## Running Locally
//!
//! ```bash
//! export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
//! export DATABASE_URL_REGULAR="postgresql://postgres:postgres@localhost:5432/test_walstream"
//! cargo test --test slot_matrix -- --ignored --nocapture
//! ```

use pg_walstream::{PgReplicationConnection, ReplicationSlotOptions, SlotType};
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

/// Best-effort tracing init so `warn!` surfaces under `--nocapture`.
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

fn drop_slot(slot_name: &str) {
    if let Ok(mut conn) = PgReplicationConnection::connect(&replication_conn_string()) {
        let _ = conn.exec(&format!(
            "SELECT pg_drop_replication_slot('{}') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{}')",
            slot_name, slot_name
        ));
    }
}

struct Case {
    name: &'static str,
    slot_type: SlotType,
    plugin: Option<&'static str>,
    opts: ReplicationSlotOptions,
    /// Minimum `server_version_num` for the server to accept it (0 = all).
    min_version: i64,
    /// Guard cases fail in the builder before any server round-trip.
    expect_client_err: bool,
    /// Drop this case's slot with `DROP_REPLICATION_SLOT … WAIT` rather than the
    /// plain grammar. Both are exercised live; deriving it from the loop index
    /// silently re-targeted which cases cover WAIT whenever a case was inserted.
    /// Ignored by temporary and client-error cases, which never reach the DROP.
    wait: bool,
}

#[test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
fn test_create_slot_option_matrix() {
    init_tracing();

    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    let version = server_version_num(&mut regular);
    warn!("slot matrix running against server_version_num {version}");

    // `snapshot: use` is intentionally omitted — USE_SNAPSHOT requires an open
    // transaction with a snapshot, a runtime precondition unrelated to grammar;
    // it is covered by the unit tests. Everything the builder can emit otherwise
    // is exercised here.
    let cases = vec![
        Case {
            name: "logical default",
            slot_type: SlotType::Logical,
            plugin: Some("pgoutput"),
            opts: ReplicationSlotOptions::default(),
            min_version: 0,
            expect_client_err: false,
            wait: true,
        },
        Case {
            name: "logical temporary",
            slot_type: SlotType::Logical,
            plugin: Some("pgoutput"),
            opts: ReplicationSlotOptions {
                temporary: true,
                ..Default::default()
            },
            min_version: 0,
            expect_client_err: false,
            wait: false,
        },
        Case {
            name: "snapshot nothing",
            slot_type: SlotType::Logical,
            plugin: Some("pgoutput"),
            opts: ReplicationSlotOptions {
                snapshot: Some("nothing".into()),
                ..Default::default()
            },
            min_version: 0,
            expect_client_err: false,
            wait: true,
        },
        Case {
            name: "snapshot export",
            slot_type: SlotType::Logical,
            plugin: Some("pgoutput"),
            opts: ReplicationSlotOptions {
                snapshot: Some("export".into()),
                ..Default::default()
            },
            min_version: 0,
            expect_client_err: false,
            wait: false,
        },
        Case {
            name: "two_phase",
            slot_type: SlotType::Logical,
            plugin: Some("pgoutput"),
            opts: ReplicationSlotOptions {
                two_phase: true,
                ..Default::default()
            },
            min_version: 150000,
            expect_client_err: false,
            wait: true,
        },
        Case {
            name: "two_phase + snapshot nothing",
            slot_type: SlotType::Logical,
            plugin: Some("pgoutput"),
            opts: ReplicationSlotOptions {
                two_phase: true,
                snapshot: Some("nothing".into()),
                ..Default::default()
            },
            min_version: 150000,
            expect_client_err: false,
            wait: false,
        },
        Case {
            name: "physical default",
            slot_type: SlotType::Physical,
            plugin: None,
            opts: ReplicationSlotOptions::default(),
            min_version: 0,
            expect_client_err: false,
            wait: true,
        },
        Case {
            name: "physical reserve_wal",
            slot_type: SlotType::Physical,
            plugin: None,
            opts: ReplicationSlotOptions {
                reserve_wal: true,
                ..Default::default()
            },
            min_version: 0,
            expect_client_err: false,
            wait: false,
        },
        Case {
            name: "physical temporary",
            slot_type: SlotType::Physical,
            plugin: None,
            opts: ReplicationSlotOptions {
                temporary: true,
                ..Default::default()
            },
            min_version: 0,
            expect_client_err: false,
            wait: false,
        },
        Case {
            name: "failover",
            slot_type: SlotType::Logical,
            plugin: Some("pgoutput"),
            opts: ReplicationSlotOptions {
                failover: true,
                ..Default::default()
            },
            min_version: 170000,
            expect_client_err: false,
            wait: false,
        },
        Case {
            name: "failover + snapshot nothing",
            slot_type: SlotType::Logical,
            plugin: Some("pgoutput"),
            opts: ReplicationSlotOptions {
                failover: true,
                snapshot: Some("nothing".into()),
                ..Default::default()
            },
            min_version: 170000,
            expect_client_err: false,
            wait: true,
        },
        Case {
            name: "failover + two_phase",
            slot_type: SlotType::Logical,
            plugin: Some("pgoutput"),
            opts: ReplicationSlotOptions {
                failover: true,
                two_phase: true,
                ..Default::default()
            },
            min_version: 170000,
            expect_client_err: false,
            wait: false,
        },
        Case {
            name: "guard: physical + failover",
            slot_type: SlotType::Physical,
            plugin: None,
            opts: ReplicationSlotOptions {
                failover: true,
                ..Default::default()
            },
            min_version: 0,
            expect_client_err: true,
            wait: false,
        },
        Case {
            name: "guard: temporary + failover",
            slot_type: SlotType::Logical,
            plugin: Some("pgoutput"),
            opts: ReplicationSlotOptions {
                temporary: true,
                failover: true,
                ..Default::default()
            },
            min_version: 0,
            expect_client_err: true,
            wait: false,
        },
        Case {
            name: "guard: missing plugin",
            slot_type: SlotType::Logical,
            plugin: None,
            opts: ReplicationSlotOptions::default(),
            min_version: 0,
            expect_client_err: true,
            wait: false,
        },
    ];

    let total = cases.len();
    let mut skipped = 0usize;

    for (i, case) in cases.iter().enumerate() {
        if case.min_version > version {
            // Greppable: `warn!` goes through a best-effort try_init that can no-op,
            // so a version-gated case would otherwise skip with no visible trace.
            println!(
                "SKIP: case '{}' needs server_version_num {}, have {version}",
                case.name, case.min_version
            );
            skipped += 1;
            continue;
        }

        let slot = format!("it_matrix_{i}");
        drop_slot(&slot);
        // These are PERSISTENT WAL-pinning slots, so a panicking assertion below
        // must not leak one. Declared before `repl` so `repl` drops first and the
        // slot is inactive by the time the guard drops it.
        let _guard = SlotGuard(slot.clone());

        let mut repl = PgReplicationConnection::connect(&replication_conn_string())
            .expect("replication connection");
        let result = repl.create_replication_slot_with_options(
            &slot,
            case.slot_type,
            case.plugin,
            &case.opts,
        );

        if case.expect_client_err {
            assert!(
                result.is_err(),
                "case '{}' must fail client-side, but succeeded",
                case.name
            );
        } else {
            result.unwrap_or_else(|e| {
                panic!("case '{}' must be accepted by the server: {e}", case.name)
            });
            // Temporary slots stay owned+active for the session's lifetime, so an
            // explicit `DROP … WAIT` would block on our own session: they are left
            // to die with `repl` at the end of this iteration.
            if !case.opts.temporary {
                // Persistent + inactive: exercise BOTH DROP grammars live via the
                // typed method, per the case's `wait` flag.
                let wait = case.wait;
                repl.drop_replication_slot(&slot, wait).unwrap_or_else(|e| {
                    panic!("case '{}' DROP (wait={wait}) must succeed: {e}", case.name)
                });
            }
        }
    }

    println!("SKIPPED {skipped}/{total} slot-matrix cases on server_version_num {version}");
}

/// Drops its slot on `Drop`, so a panicking assertion never leaks a slot (an
/// orphaned slot pins WAL and can exhaust server disk).
struct SlotGuard(String);
impl Drop for SlotGuard {
    fn drop(&mut self) {
        drop_slot(&self.0);
    }
}

/// `READ_REPLICATION_SLOT` round-trip (PG15+, physical slots only).
///
/// The command does not exist on PG14 and, per the protocol, is only meaningful
/// for physical slots — so this creates a physical slot with `reserve_wal` (which
/// populates `restart_lsn`) and asserts the read returns the expected shape.
#[test]
#[ignore = "requires live PostgreSQL 15+ with wal_level=logical"]
fn test_read_replication_slot_physical() {
    init_tracing();

    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    let version = server_version_num(&mut regular);
    if version < 150000 {
        println!("SKIP: READ_REPLICATION_SLOT needs server_version_num 150000, have {version}");
        return;
    }

    let slot = "it_read_phys";
    drop_slot(slot);
    let _guard = SlotGuard(slot.to_string());

    let mut repl = PgReplicationConnection::connect(&replication_conn_string())
        .expect("replication connection");
    let opts = ReplicationSlotOptions {
        reserve_wal: true,
        ..Default::default()
    };
    repl.create_replication_slot_with_options(slot, SlotType::Physical, None, &opts)
        .expect("physical slot creation must succeed");

    let info = repl
        .read_replication_slot(slot)
        .expect("READ_REPLICATION_SLOT must succeed on PG15+");
    assert_eq!(
        info.slot_type.as_deref(),
        Some("physical"),
        "READ_REPLICATION_SLOT must report slot_type physical"
    );
    assert!(
        info.restart_lsn.is_some(),
        "a reserve_wal physical slot must have a restart_lsn"
    );

    let _ = repl.drop_replication_slot(slot, false);
}

/// `ALTER_REPLICATION_SLOT` matrix (PG17+ for FAILOVER, PG18+ for TWO_PHASE).
///
/// Creates a plain persistent logical slot, alters it, and verifies the effect via
/// `pg_replication_slots`. Version-gated cases skip with a `SKIP:` line; below PG17
/// the whole test is a no-op and says so.
#[test]
#[ignore = "requires live PostgreSQL 17+ with wal_level=logical"]
fn test_alter_replication_slot_matrix() {
    init_tracing();

    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    let version = server_version_num(&mut regular);
    if version < 170000 {
        // The whole test is a no-op below PG17; say so rather than reporting `ok`.
        println!("SKIP: ALTER_REPLICATION_SLOT needs server_version_num 170000, have {version}");
        println!("SKIPPED 4/4 alter cases on server_version_num {version}");
        return;
    }

    struct AlterCase {
        name: &'static str,
        two_phase: Option<bool>,
        failover: Option<bool>,
        min_version: i64,
        /// Column in `pg_replication_slots` to verify, and its expected `t`/`f`.
        verify_col: &'static str,
        expect: &'static str,
    }

    let cases = [
        AlterCase {
            name: "failover true",
            two_phase: None,
            failover: Some(true),
            min_version: 170000,
            verify_col: "failover",
            expect: "t",
        },
        AlterCase {
            name: "failover false",
            two_phase: None,
            failover: Some(false),
            min_version: 170000,
            verify_col: "failover",
            expect: "f",
        },
        AlterCase {
            name: "two_phase true",
            two_phase: Some(true),
            failover: None,
            min_version: 180000,
            verify_col: "two_phase",
            expect: "t",
        },
        AlterCase {
            name: "two_phase + failover",
            two_phase: Some(true),
            failover: Some(true),
            min_version: 180000,
            verify_col: "failover",
            expect: "t",
        },
    ];

    let total = cases.len();
    let mut skipped = 0usize;

    for (i, case) in cases.iter().enumerate() {
        if case.min_version > version {
            println!(
                "SKIP: alter case '{}' needs server_version_num {}, have {version}",
                case.name, case.min_version
            );
            skipped += 1;
            continue;
        }

        let slot = format!("it_alter_{i}");
        drop_slot(&slot);
        let _guard = SlotGuard(slot.clone());

        let mut repl = PgReplicationConnection::connect(&replication_conn_string())
            .expect("replication connection");
        repl.create_replication_slot_with_options(
            &slot,
            SlotType::Logical,
            Some("pgoutput"),
            &ReplicationSlotOptions::default(),
        )
        .unwrap_or_else(|e| panic!("alter '{}' slot setup must succeed: {e}", case.name));

        repl.alter_replication_slot(&slot, case.two_phase, case.failover)
            .unwrap_or_else(|e| panic!("alter '{}' must succeed: {e}", case.name));

        let query = format!(
            "SELECT {} FROM pg_replication_slots WHERE slot_name = '{}'",
            case.verify_col, slot
        );
        let res = regular
            .exec(&query)
            .unwrap_or_else(|e| panic!("alter '{}' verify query failed: {e}", case.name));
        assert_eq!(res.ntuples(), 1, "alter '{}': slot must exist", case.name);
        assert_eq!(
            res.get_value(0, 0).as_deref(),
            Some(case.expect),
            "alter '{}': pg_replication_slots.{} must be {}",
            case.name,
            case.verify_col,
            case.expect
        );

        let _ = repl.drop_replication_slot(&slot, false);
    }

    println!("SKIPPED {skipped}/{total} alter cases on server_version_num {version}");
}
