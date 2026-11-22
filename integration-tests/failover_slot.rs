#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! Integration test for FAILOVER replication-slot creation (issue #103).
//!
//! `FAILOVER` is valid only in the PostgreSQL 17+ parenthesized option grammar,
//! so on older servers this test skips gracefully with a `warn!`.
//!
//! ## Prerequisites
//!
//! - PostgreSQL 17+ with `wal_level = logical`
//! - `DATABASE_URL` — replication connection
//!   (e.g. `postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database`)
//! - `DATABASE_URL_REGULAR` — regular connection to the same database
//!
//! ## Running Locally
//!
//! ```bash
//! export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
//! export DATABASE_URL_REGULAR="postgresql://postgres:postgres@localhost:5432/test_walstream"
//! cargo test --test failover_slot -- --ignored --nocapture
//! ```

mod common;

use common::{
    drop_slot_best_effort as drop_slot, init_tracing, regular_conn_string, replication_conn_string,
    SlotGuard,
};
use pg_walstream::{PgReplicationConnection, ReplicationSlotOptions, SlotType};
use tracing::{error, warn};

/// Read `server_version_num` (e.g. 170004) via the given connection.
fn server_version_num(conn: &mut PgReplicationConnection) -> i64 {
    let result = conn
        .exec("SHOW server_version_num")
        .expect("SHOW server_version_num");
    result
        .get_value(0, 0)
        .expect("server_version_num value present")
        .parse()
        .expect("server_version_num is numeric")
}

#[test]
#[ignore = "requires live PostgreSQL 17+ with wal_level=logical"]
fn test_failover_slot_created_on_pg17plus() {
    init_tracing();
    let slot = "it_failover_slot";
    drop_slot(slot);

    // Guarantee cleanup even if an assertion below panics: an orphaned
    // replication slot blocks WAL recycling and can exhaust disk on the server.
    let _guard = SlotGuard(slot);

    // Read the server version over a regular connection.
    let mut regular =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    let version = server_version_num(&mut regular);
    if version < 170000 {
        warn!(
            "skipping failover integration test: server_version_num {} < 170000 \
             (FAILOVER requires PostgreSQL 17+)",
            version
        );
        return;
    }

    // Create a persistent logical failover slot over the replication connection.
    // `snapshot: nothing` avoids exporting a snapshot. This is the exact call
    // that produced SQLSTATE 42601 before the fix.
    let mut repl = PgReplicationConnection::connect(&replication_conn_string())
        .expect("replication connection");
    let opts = ReplicationSlotOptions {
        failover: true,
        snapshot: Some("nothing".to_string()),
        ..Default::default()
    };
    repl.create_replication_slot_with_options(slot, SlotType::Logical, Some("pgoutput"), &opts)
        .expect("failover slot creation must succeed on PG17+");

    // Verify the slot exists and is marked as a failover slot.
    let result = match regular.exec(&format!(
        "SELECT failover FROM pg_replication_slots WHERE slot_name = '{}'",
        slot
    )) {
        Ok(r) => r,
        Err(e) => {
            error!("failed to query pg_replication_slots: {}", e);
            panic!("verification query failed: {}", e);
        }
    };
    assert_eq!(result.ntuples(), 1, "slot '{}' must exist", slot);
    assert_eq!(
        result.get_value(0, 0).as_deref(),
        Some("t"),
        "pg_replication_slots.failover must be true"
    );
}
