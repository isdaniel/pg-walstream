#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! A consume-loop handler that outlasts `wal_sender_timeout` does not get the walsender
//! killed.
//!
//! `for_each_event` and `WalRouter::run` send a status update every `feedback_interval`
//! while a handler is pending. Without it the stream reads nothing during the handler, so
//! the server's ping goes unanswered and the walsender is terminated after
//! `wal_sender_timeout`. Each test holds the first event's handler for 12 s with a 4 s
//! `wal_sender_timeout` (set on the replication connection through `options`) and checks
//! from a second connection that the same walsender is still attached to the slot the
//! whole time. Each backend writes the status update through its own code.
//!
//! Requires PostgreSQL with `wal_level=logical`, `DATABASE_URL` in replication mode and
//! `DATABASE_URL_REGULAR` for the same database.
//!
//! ```bash
//! export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
//! export DATABASE_URL_REGULAR="postgresql://postgres:postgres@localhost:5432/test_walstream"
//! cargo test --test handler_feedback -- --ignored --nocapture --test-threads=1
//! cargo test --test handler_feedback --no-default-features --features libpq -- --ignored --nocapture --test-threads=1
//! ```

use core::time::Duration;
use pg_walstream::{
    CancellationToken, EventStream, LogicalReplicationStream, PgReplicationConnection,
    ReplicationSlotOptions, ReplicationStreamConfig, Result, RetryConfig, WalRouter,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinHandle;

const WAL_SENDER_TIMEOUT: &str = "options=-c%20wal_sender_timeout%3D4s";
const HANDLER: Duration = Duration::from_secs(12);

fn replication_conn_string() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
            .to_string()
    })
}

fn regular_conn_string() -> String {
    std::env::var("DATABASE_URL_REGULAR").unwrap_or_else(|_| {
        replication_conn_string()
            .replace("?replication=database", "")
            .replace("&replication=database", "")
    })
}

fn with_params(url: &str, extra: &str) -> String {
    let sep = if url.contains('?') { '&' } else { '?' };
    format!("{url}{sep}{extra}")
}

/// The pid of the walsender attached to `slot`, `None` once it is gone.
fn walsender_pid(admin: &mut PgReplicationConnection, slot: &str) -> Option<String> {
    admin
        .exec(&format!(
            "SELECT active_pid FROM pg_replication_slots WHERE slot_name = '{slot}'"
        ))
        .expect("query active_pid")
        .get_value(0, 0)
}

/// Holds the first event's handler for [`HANDLER`], and lets every later one through.
#[derive(Clone, Default)]
struct SlowOnce {
    started: Arc<AtomicBool>,
    done: Arc<AtomicBool>,
}

impl SlowOnce {
    async fn handle(self) -> Result<()> {
        if !self.started.swap(true, Ordering::SeqCst) {
            tokio::time::sleep(HANDLER).await;
            self.done.store(true, Ordering::SeqCst);
        }
        Ok(())
    }
}

/// Runs a consume loop whose first handler takes 12 s against a 4 s `wal_sender_timeout`
/// and asserts the walsender attached to the slot stays the same one throughout.
async fn the_walsender_survives_a_slow_handler(
    name: &str,
    run: impl FnOnce(EventStream, SlowOnce) -> JoinHandle<Result<()>>,
) {
    let slot = format!("it_hf_{name}");
    let table = format!("it_hf_{name}");
    let mut admin =
        PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
    for sql in [
        format!("DROP PUBLICATION IF EXISTS {slot}"),
        format!("DROP TABLE IF EXISTS {table}"),
        format!("CREATE TABLE {table} (id integer PRIMARY KEY)"),
        format!("CREATE PUBLICATION {slot} FOR TABLE {table}"),
    ] {
        admin.exec(&sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    let config = ReplicationStreamConfig::builder(&slot, &slot)
        .with_feedback_interval(Duration::from_secs(1))
        .with_retry_config(RetryConfig {
            max_attempts: 1,
            ..RetryConfig::default()
        })
        .with_slot_options(ReplicationSlotOptions {
            temporary: true,
            snapshot: Some("nothing".into()),
            ..Default::default()
        });
    let url = with_params(&replication_conn_string(), WAL_SENDER_TIMEOUT);
    let mut stream = LogicalReplicationStream::new(&url, config)
        .await
        .expect("stream");
    stream.start(None).await.expect("start");

    let attached = Instant::now();
    let pid = loop {
        if let Some(pid) = walsender_pid(&mut admin, &slot) {
            break pid;
        }
        assert!(
            attached.elapsed() < Duration::from_secs(5),
            "no walsender attached to {slot}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    let cancel = CancellationToken::new();
    let slow = SlowOnce::default();
    let consumer = run(stream.into_stream(cancel.clone()), slow.clone());
    admin
        .exec(&format!("INSERT INTO {table} VALUES (1)"))
        .expect("insert");

    let deadline = Instant::now() + HANDLER + Duration::from_secs(15);
    let mut samples = 0;
    let mut started_at = None;
    while !slow.done.load(Ordering::SeqCst) {
        assert!(Instant::now() < deadline, "the handler never finished");
        if started_at.is_none() && slow.started.load(Ordering::SeqCst) {
            started_at = Some(Instant::now());
        }
        if let Some(at) = started_at {
            samples += 1;
            assert_eq!(
                walsender_pid(&mut admin, &slot).as_deref(),
                Some(pid.as_str()),
                "the walsender was disconnected {:?} into a {HANDLER:?} handler \
                 with a 4 s wal_sender_timeout",
                at.elapsed()
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    println!(
        "{name}: walsender {pid} stayed attached for the whole {HANDLER:?} handler ({samples} samples)"
    );
    assert!(
        samples >= 20,
        "only {samples} samples fell inside the handler"
    );

    cancel.cancel();
    consumer
        .await
        .expect("consumer task")
        .expect("the consume loop ended with an error");
    let _ = admin.exec(&format!(
        "DROP PUBLICATION IF EXISTS {slot}; DROP TABLE IF EXISTS {table}"
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn for_each_event_keeps_the_walsender_alive_through_a_slow_handler() {
    the_walsender_survives_a_slow_handler("for_each", |mut events, slow| {
        tokio::spawn(async move { events.for_each_event(move |_| slow.clone().handle()).await })
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn wal_router_run_keeps_the_walsender_alive_through_a_slow_handler() {
    the_walsender_survives_a_slow_handler("router", |mut events, slow| {
        tokio::spawn(async move {
            let mut router = WalRouter::new();
            router.on_default(move |_| slow.clone().handle());
            router.run(&mut events).await
        })
    })
    .await;
}
