#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! Requires PostgreSQL with `wal_level=logical` and `DATABASE_URL` in replication mode.

use core::time::Duration;
use pg_walstream::{
    EventType, LogicalReplicationStream, Lsn, PgReplicationConnection, ReplicationError,
    ReplicationStreamConfig,
};
use std::time::Instant;
use tokio_util::sync::CancellationToken;

fn replication_conn_string() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
            .to_string()
    })
}

struct Fixture {
    regular: PgReplicationConnection,
    slot: &'static str,
    activity_sql: String,
    drop_sql: String,
    cleanup_sql: String,
}

impl Fixture {
    fn new(slot: &'static str) -> Self {
        let regular_url = std::env::var("DATABASE_URL_REGULAR").unwrap_or_else(|_| {
            replication_conn_string()
                .replace("?replication=database", "")
                .replace("&replication=database", "")
        });
        let regular = PgReplicationConnection::connect(&regular_url).expect("regular connection");
        let mut fixture = Self {
            regular,
            slot,
            activity_sql: format!(
                "SELECT active FROM pg_replication_slots WHERE slot_name = '{slot}'"
            ),
            drop_sql: format!(
                "SELECT pg_drop_replication_slot('{slot}') WHERE EXISTS \
                 (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot}')"
            ),
            cleanup_sql: format!(
                "DROP PUBLICATION IF EXISTS {slot}_pub; DROP TABLE IF EXISTS {slot}"
            ),
        };
        fixture.drop_slot().expect("drop stale slot");
        fixture
            .regular
            .exec(&format!(
                "DROP PUBLICATION IF EXISTS {slot}_pub; \
                 DROP TABLE IF EXISTS {slot}; \
                 CREATE TABLE {slot} (id integer PRIMARY KEY); \
                 CREATE PUBLICATION {slot}_pub FOR TABLE {slot}"
            ))
            .expect("create published table");
        fixture
    }

    fn config(&self, required: bool) -> ReplicationStreamConfig {
        ReplicationStreamConfig::builder(self.slot, format!("{}_pub", self.slot))
            .with_existing_slot_required(required)
    }

    fn drop_slot(&mut self) -> Result<(), ReplicationError> {
        let started = Instant::now();
        loop {
            let active = self.regular.exec(&self.activity_sql)?.get_value(0, 0);
            if active.as_deref() != Some("t") {
                break;
            }
            if started.elapsed() >= Duration::from_secs(10) {
                return Err(ReplicationError::timeout("slot stayed active"));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        self.regular.exec(&self.drop_sql)?;
        Ok(())
    }

    fn insert(&mut self, id: i32) {
        self.regular
            .exec(&format!("INSERT INTO {} VALUES ({id})", self.slot))
            .expect("insert row");
    }

    fn assert_slot_absent(&mut self) {
        let count = self
            .regular
            .exec(&format!(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{}'",
                self.slot
            ))
            .expect("query slot catalog")
            .get_value(0, 0);
        assert_eq!(count.as_deref(), Some("0"));
    }

    async fn checkpoint(&mut self) -> Lsn {
        let mut stream =
            LogicalReplicationStream::new(&replication_conn_string(), self.config(false))
                .await
                .expect("initial stream");
        stream.start(None).await.expect("initial start");
        self.insert(1);
        let (end_lsn, rows) = read_commit(&mut stream).await;
        assert_eq!(rows, [1]);
        end_lsn
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.drop_slot();
        let _ = self.regular.exec(&self.cleanup_sql);
    }
}

async fn read_commit(stream: &mut LogicalReplicationStream) -> (Lsn, Vec<i32>) {
    tokio::time::timeout(Duration::from_secs(15), async {
        let cancel = CancellationToken::new();
        let mut rows = Vec::new();
        loop {
            match stream
                .next_event(&cancel)
                .await
                .expect("read event")
                .event_type
            {
                EventType::Insert { data, .. } => rows.push(
                    data.get("id")
                        .and_then(|value| value.as_str())
                        .expect("id column")
                        .parse()
                        .expect("integer id"),
                ),
                EventType::Commit { end_lsn, .. } => return (end_lsn, rows),
                _ => {}
            }
        }
    })
    .await
    .expect("commit did not arrive")
}

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn required_slot_errors_after_the_slot_is_dropped() {
    let mut fixture = Fixture::new("it_required_dropped");
    let stored = fixture.checkpoint().await;
    fixture.drop_slot().expect("drop checkpoint slot");
    fixture.insert(2);
    let mut resumed =
        LogicalReplicationStream::new(&replication_conn_string(), fixture.config(true))
            .await
            .expect("resuming stream");
    let error = resumed
        .start(Some(stored.value()))
        .await
        .expect_err("a missing slot must not be recreated");
    assert!(
        matches!(error, ReplicationError::ReplicationSlot(..)),
        "{error:?}"
    );
    assert!(error.is_permanent());
    fixture.assert_slot_absent();
}

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn required_slot_errors_without_an_explicit_start_position() {
    let mut fixture = Fixture::new("it_required_no_lsn");
    let mut stream =
        LogicalReplicationStream::new(&replication_conn_string(), fixture.config(true))
            .await
            .expect("stream");
    let error = stream
        .start(None)
        .await
        .expect_err("slot must already exist");
    assert!(
        matches!(error, ReplicationError::ReplicationSlot(..)),
        "{error:?}"
    );
    fixture.assert_slot_absent();
}

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn required_existing_slot_replays_commits_after_the_stored_position() {
    let mut fixture = Fixture::new("it_required_replay");
    let stored = fixture.checkpoint().await;
    fixture.insert(2);
    let mut resumed =
        LogicalReplicationStream::new(&replication_conn_string(), fixture.config(true))
            .await
            .expect("resuming stream");
    resumed
        .start(Some(stored.value()))
        .await
        .expect("resume existing slot");
    let (_, rows) = read_commit(&mut resumed).await;
    assert_eq!(rows, [2]);
}

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn default_start_recreates_a_dropped_slot() {
    let mut fixture = Fixture::new("it_default_recreate");
    let stored = fixture.checkpoint().await;
    fixture.drop_slot().expect("drop checkpoint slot");
    fixture.insert(2);
    let mut resumed =
        LogicalReplicationStream::new(&replication_conn_string(), fixture.config(false))
            .await
            .expect("resuming stream");
    resumed
        .start(Some(stored.value()))
        .await
        .expect("default start");
    fixture.insert(3);
    let (_, rows) = read_commit(&mut resumed).await;
    assert_eq!(rows, [3]);
}

#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn required_slot_is_not_recreated_during_recovery() {
    for (slot, temporary) in [("it_required_recovery", false), ("it_required_temp", true)] {
        let mut fixture = Fixture::new(slot);
        let config = fixture
            .config(true)
            .with_slot_options(pg_walstream::ReplicationSlotOptions {
                temporary,
                ..Default::default()
            });
        let mut stream = LogicalReplicationStream::new(&replication_conn_string(), config)
            .await
            .expect("stream");
        stream
            .ensure_replication_slot()
            .await
            .expect("explicit slot creation");
        stream
            .start(None)
            .await
            .expect("start explicitly created slot");
        fixture.insert(1);
        read_commit(&mut stream).await;
        fixture.regular.exec(&format!(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = '{slot}'"
        )).expect("terminate walsender");
        fixture.drop_slot().expect("drop disconnected slot");
        let error = tokio::time::timeout(Duration::from_secs(20), async {
            let cancel = CancellationToken::new();
            loop {
                if let Err(error) = stream.next_event_with_retry(&cancel).await {
                    break error;
                }
            }
        })
        .await
        .expect("recovery did not terminate");
        assert!(
            matches!(error, ReplicationError::ReplicationSlot(..)),
            "{error:?}"
        );
        assert!(error.is_permanent());
        fixture.assert_slot_absent();
    }
}
