#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! A stream whose walsender is terminated reconnects and carries on, with no row lost
//! and none repeated.
//!
//! `require_existing_slot` covers recovery that fails. This is the only test of one that
//! succeeds, through both read entry points: `LogicalReplicationStream::next_event_with_retry`
//! and `EventStream::next_event` under `tokio::time::timeout`, the way a consumer drives it,
//! including a timeout shorter than the reconnect itself.
//!
//! Requires PostgreSQL with `wal_level=logical`, `DATABASE_URL` in replication mode and,
//! optionally, `DATABASE_URL_REGULAR` for the same database.

use core::time::Duration;
use pg_walstream::{
    EventStream, EventType, LogicalReplicationStream, PgReplicationConnection,
    ReplicationStreamConfig,
};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

/// Generous: a reconnect sits out a 1 s backoff, and a loaded CI runner is slower still.
const READ_TIMEOUT: Duration = Duration::from_secs(30);

fn replication_conn_string() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
            .to_string()
    })
}

struct Fixture {
    regular: PgReplicationConnection,
    slot: &'static str,
}

impl Fixture {
    fn new(slot: &'static str) -> Self {
        let regular_url = std::env::var("DATABASE_URL_REGULAR").unwrap_or_else(|_| {
            replication_conn_string()
                .replace("?replication=database", "")
                .replace("&replication=database", "")
        });
        let regular = PgReplicationConnection::connect(&regular_url).expect("regular connection");
        let mut fixture = Self { regular, slot };
        fixture.drop_slot();
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

    /// A persistent slot: a temporary one is dropped with its connection, and the
    /// reconnect would recreate it at a later point and lose the rows under test.
    fn config(&self) -> ReplicationStreamConfig {
        ReplicationStreamConfig::builder(self.slot, format!("{}_pub", self.slot))
    }

    fn drop_slot(&mut self) {
        let started = Instant::now();
        let active_sql = format!(
            "SELECT active FROM pg_replication_slots WHERE slot_name = '{}'",
            self.slot
        );
        while started.elapsed() < Duration::from_secs(10)
            && self
                .regular
                .exec(&active_sql)
                .ok()
                .and_then(|r| r.get_value(0, 0))
                .as_deref()
                == Some("t")
        {
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.regular.exec(&format!(
            "SELECT pg_drop_replication_slot('{0}') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{0}')",
            self.slot
        ));
    }

    /// Wait until the slot exists on the server: a command the client cancelled still runs there.
    fn wait_for_slot(&mut self) {
        let sql = format!(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = '{}'",
            self.slot
        );
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(10) {
            if self
                .regular
                .exec(&sql)
                .ok()
                .and_then(|r| r.get_value(0, 0))
                .is_some()
            {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!("the slot was never created");
    }

    fn insert(&mut self, id: i32) {
        self.regular
            .exec(&format!("INSERT INTO {} VALUES ({id})", self.slot))
            .expect("insert row");
    }

    fn walsender_pid(&mut self) -> String {
        self.regular
            .exec(&format!(
                "SELECT active_pid FROM pg_replication_slots WHERE slot_name = '{}'",
                self.slot
            ))
            .expect("query the slot")
            .get_value(0, 0)
            .expect("the slot has a walsender attached")
    }

    /// Kill the walsender, and insist it was there to kill: a `NULL` `active_pid` would
    /// terminate nothing and let the test pass without any recovery.
    fn terminate_walsender(&mut self) {
        let terminated = self
            .regular
            .exec(&format!(
                "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
                 WHERE slot_name = '{}'",
                self.slot
            ))
            .expect("terminate the walsender")
            .get_value(0, 0);
        assert_eq!(
            terminated.as_deref(),
            Some("t"),
            "no live walsender was terminated"
        );
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.drop_slot();
        let _ = self.regular.exec(&format!(
            "DROP PUBLICATION IF EXISTS {0}_pub; DROP TABLE IF EXISTS {0}",
            self.slot
        ));
    }
}

fn inserted_id(data: &pg_walstream::RowData) -> i32 {
    data.get("id")
        .and_then(|value| value.as_str())
        .expect("id column")
        .parse()
        .expect("integer id")
}

/// Read to the commit that carries `until`, acknowledging each commit, and return the ids
/// seen. Acknowledging matters: a reconnect resumes from the applied position, so an
/// unacknowledged row would be delivered again, which is correct but not what this asserts.
async fn read_until(stream: &mut LogicalReplicationStream, until: i32) -> Vec<i32> {
    tokio::time::timeout(READ_TIMEOUT, async {
        let cancel = CancellationToken::new();
        let mut ids = Vec::new();
        loop {
            let event = stream
                .next_event_with_retry(&cancel)
                .await
                .expect("the stream must recover, not give up");
            match event.event_type {
                EventType::Insert { data, .. } => ids.push(inserted_id(&data)),
                EventType::Commit { end_lsn, .. } => {
                    stream
                        .shared_lsn_feedback
                        .update_applied_lsn(end_lsn.value());
                    if ids.last() == Some(&until) {
                        return ids;
                    }
                }
                _ => {}
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("row {until} never arrived after the reconnect"))
}

/// [`read_until`] through `EventStream::next_event`, each read cut off after `slice`, the shape
/// a consumer uses and the one whose future is moved on every event.
async fn read_until_timed(events: &mut EventStream, until: i32, slice: Duration) -> Vec<i32> {
    let mut ids = Vec::new();
    let deadline = Instant::now() + READ_TIMEOUT;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        assert!(
            !left.is_zero(),
            "row {until} never arrived after the reconnect"
        );
        let Ok(event) = tokio::time::timeout(left.min(slice), events.next_event()).await else {
            continue;
        };
        let event = event.expect("the stream must recover, not give up");
        match event.event_type {
            EventType::Insert { data, .. } => ids.push(inserted_id(&data)),
            EventType::Commit { end_lsn, .. } => {
                events.update_applied_lsn(end_lsn.value());
                if ids.last() == Some(&until) {
                    return ids;
                }
            }
            _ => {}
        }
    }
}

/// A TCP proxy to PostgreSQL that holds every reply for `latency`, as a slow link would. A
/// connect, `IDENTIFY_SYSTEM` and `START_REPLICATION` are several round trips, so a
/// reconnect through it takes a good part of a second.
struct SlowLink {
    port: u16,
    accept_loop: tokio::task::JoinHandle<()>,
}

impl SlowLink {
    async fn to(server: String, latency: Duration) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let port = listener.local_addr().expect("proxy address").port();
        let accept_loop = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let server = server.clone();
                tokio::spawn(async move {
                    let Ok(upstream) = TcpStream::connect(server).await else {
                        return;
                    };
                    let (mut from_client, mut to_client) = client.into_split();
                    let (mut from_server, mut to_server) = upstream.into_split();
                    let forward = tokio::spawn(async move {
                        let _ = tokio::io::copy(&mut from_client, &mut to_server).await;
                    });
                    // Stamp each reply on arrival and release it `latency` later, in order.
                    let (queue, mut delayed) = tokio::sync::mpsc::unbounded_channel();
                    let read = async move {
                        let mut buf = [0u8; 8192];
                        while let Ok(n) = from_server.read(&mut buf).await {
                            if n == 0
                                || queue
                                    .send((
                                        tokio::time::Instant::now() + latency,
                                        buf[..n].to_vec(),
                                    ))
                                    .is_err()
                            {
                                break;
                            }
                        }
                    };
                    let write = async move {
                        while let Some((due, chunk)) = delayed.recv().await {
                            tokio::time::sleep_until(due).await;
                            if to_client.write_all(&chunk).await.is_err() {
                                break;
                            }
                        }
                        let _ = to_client.shutdown().await;
                    };
                    tokio::join!(read, write);
                    forward.abort();
                });
            }
        });
        Self { port, accept_loop }
    }
}

impl Drop for SlowLink {
    fn drop(&mut self) {
        self.accept_loop.abort();
    }
}

/// The `host[:port]` of a connection URL, as written.
fn authority_of(url: &str) -> &str {
    let after_scheme = url.split_once("://").map_or(url, |(_, rest)| rest);
    let authority = after_scheme.split('/').next().unwrap_or(after_scheme);
    authority.rsplit('@').next().unwrap_or(authority)
}

/// `host:port` to dial for a connection URL: its default port when it names none.
fn server_of(url: &str) -> String {
    let server = authority_of(url);
    if server.contains(':') {
        server.to_string()
    } else {
        format!("{server}:5432")
    }
}

/// One terminated walsender: the row inserted around it arrives exactly once, from a new
/// walsender, and the earlier row is not delivered again.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn a_stream_resumes_after_its_walsender_is_terminated() {
    let mut fixture = Fixture::new("it_reconnect_basic");
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), fixture.config())
        .await
        .expect("stream");
    stream.start(None).await.expect("start");

    fixture.insert(1);
    assert_eq!(read_until(&mut stream, 1).await, [1]);
    let before = fixture.walsender_pid();

    fixture.terminate_walsender();
    fixture.insert(2);

    assert_eq!(
        read_until(&mut stream, 2).await,
        [2],
        "row 2 exactly once, and row 1 not again"
    );
    assert_ne!(
        fixture.walsender_pid(),
        before,
        "the rows must come from a new walsender"
    );
}

/// Two terminations in a row through `EventStream::next_event` under `timeout`: the loop
/// recovers more than once, and each recovery leaves the next read working.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn an_event_stream_under_timeout_survives_repeated_terminations() {
    let mut fixture = Fixture::new("it_reconnect_timed");
    let mut stream = LogicalReplicationStream::new(&replication_conn_string(), fixture.config())
        .await
        .expect("stream");
    stream.start(None).await.expect("start");
    let mut events = stream.into_stream(CancellationToken::new());

    fixture.insert(1);
    assert_eq!(read_until_timed(&mut events, 1, READ_TIMEOUT).await, [1]);
    let mut previous = fixture.walsender_pid();

    for id in [2, 3] {
        fixture.terminate_walsender();
        fixture.insert(id);
        assert_eq!(
            read_until_timed(&mut events, id, READ_TIMEOUT).await,
            [id],
            "row {id} exactly once"
        );
        let current = fixture.walsender_pid();
        assert_ne!(current, previous, "row {id} must come from a new walsender");
        previous = current;
    }
}

/// A reconnect that takes longer than the consumer's read timeout still completes. The link
/// holds every reply for 120 ms, so a reconnect takes several hundred milliseconds and each
/// read is cut off at 200 ms. A reconnect that every cut-off read restarts from its first
/// connect attempt never gets there, and row 2 never arrives.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn a_reconnect_slower_than_the_read_timeout_still_completes() {
    let mut fixture = Fixture::new("it_reconnect_slow_link");
    let url = replication_conn_string();
    let link = SlowLink::to(server_of(&url), Duration::from_millis(120)).await;
    let url = url.replacen(authority_of(&url), &format!("127.0.0.1:{}", link.port), 1);
    let mut stream = LogicalReplicationStream::new(&url, fixture.config())
        .await
        .expect("stream");
    stream.start(None).await.expect("start");
    let mut events = stream.into_stream(CancellationToken::new());
    let slice = Duration::from_millis(200);

    fixture.insert(1);
    assert_eq!(read_until_timed(&mut events, 1, slice).await, [1]);
    let before = fixture.walsender_pid();

    fixture.terminate_walsender();
    fixture.insert(2);

    assert_eq!(
        read_until_timed(&mut events, 2, slice).await,
        [2],
        "row 2 exactly once, and row 1 not again"
    );
    assert_ne!(
        fixture.walsender_pid(),
        before,
        "the rows must come from a new walsender"
    );
}

/// A `start` that a timeout cancelled leaves the connection refusing every command, with an
/// error that reads as retryable. The stream must say it is to be rebuilt (a permanent error,
/// so a caller's retry loop stops), while a read still reconnects it and delivers the row.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn a_cancelled_start_is_permanent_but_a_read_still_reconnects() {
    let mut fixture = Fixture::new("it_reconnect_cancelled_start");
    let url = replication_conn_string();
    let link = SlowLink::to(server_of(&url), Duration::from_millis(120)).await;
    let url = url.replacen(authority_of(&url), &format!("127.0.0.1:{}", link.port), 1);
    let mut stream = LogicalReplicationStream::new(&url, fixture.config())
        .await
        .expect("stream");

    // A start takes several round trips of 120 ms each.
    let started = tokio::time::timeout(Duration::from_millis(50), stream.start(None)).await;
    assert!(started.is_err(), "start must outlast the 50 ms timeout");

    let err = stream.start(None).await.unwrap_err();
    assert!(
        err.is_permanent(),
        "a retry loop must learn to rebuild the stream, got {err:?}"
    );

    fixture.wait_for_slot();
    fixture.insert(1);
    assert_eq!(read_until(&mut stream, 1).await, [1]);
}

/// A reconnect that a timeout cut off, resumed by the next call, still lets a keepalive at or
/// past the boundary end `next_event_before`. The parked read must stay bounded: unbounded, it
/// ignores the keepalive and waits for a row, which an idle database never sends.
#[tokio::test]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn a_parked_reconnect_still_ends_a_bounded_read_on_a_keepalive() {
    let mut fixture = Fixture::new("it_reconnect_bounded");
    let url = replication_conn_string();
    let server = server_of(&url);
    let link = SlowLink::to(server.clone(), Duration::from_millis(120)).await;
    let url = url.replacen(&server, &format!("127.0.0.1:{}", link.port), 1);
    let mut stream = LogicalReplicationStream::new(&url, fixture.config())
        .await
        .expect("stream");
    stream.start(None).await.expect("start");
    let mut events = stream.into_stream(CancellationToken::new());
    fixture.insert(1);
    assert_eq!(read_until_timed(&mut events, 1, READ_TIMEOUT).await, [1]);

    fixture.terminate_walsender();
    // Written after the kill: only the new walsender can report a position at or past it.
    fixture
        .regular
        .exec(&format!(
            "DROP TABLE IF EXISTS {0}_other; \
             CREATE TABLE {0}_other (id integer); INSERT INTO {0}_other VALUES (1)",
            fixture.slot
        ))
        .expect("unpublished write");
    let boundary = fixture
        .regular
        .exec("SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint")
        .expect("wal position")
        .get_value(0, 0)
        .expect("a position")
        .parse::<u64>()
        .expect("integer position");
    let boundary = pg_walstream::Lsn::new(boundary);

    // The reconnect takes several 120 ms round trips: the first call is cut off inside it,
    // so the second resumes it from the parked read.
    let cut = tokio::time::timeout(
        Duration::from_millis(500),
        events.next_event_before(boundary),
    )
    .await;
    assert!(cut.is_err(), "the reconnect must outlast the timeout");

    let ended = tokio::time::timeout(READ_TIMEOUT, events.next_event_before(boundary))
        .await
        .expect("a keepalive past the boundary must end the read; it waited for a row instead")
        .expect("read");
    assert!(ended.is_none(), "nothing is committed past the boundary");
    drop(events);
    let _ = fixture
        .regular
        .exec(&format!("DROP TABLE IF EXISTS {}_other", fixture.slot));
}
