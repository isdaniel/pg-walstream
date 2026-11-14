#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! Session parameters in the connection string: `application_name`,
//! `fallback_application_name` and `options`.
//!
//! The native backend parsed these and dropped them, while the libpq backend sent
//! them in the startup message, so the same string named the walsender and pinned
//! its GUCs on one backend and not the other. Both backends must agree.
//!
//! Requires PostgreSQL with `wal_level=logical`, `DATABASE_URL` in replication
//! mode and `DATABASE_URL_REGULAR` for the same database.
//!
//! ```bash
//! export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
//! export DATABASE_URL_REGULAR="postgresql://postgres:postgres@localhost:5432/test_walstream"
//! cargo test --test startup_params -- --ignored --nocapture --test-threads=1
//! cargo test --test startup_params --no-default-features --features libpq -- --ignored --nocapture --test-threads=1
//! ```

use core::time::Duration;
use pg_walstream::{
    CancellationToken, EventType, LogicalReplicationStream, PgReplicationConnection,
    ReplicationError, ReplicationStreamConfig, RetryConfig,
};

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

/// `url` with `extra` appended as further query parameters.
fn with_params(url: &str, extra: &str) -> String {
    let sep = if url.contains('?') { '&' } else { '?' };
    format!("{url}{sep}{extra}")
}

fn value(conn: &mut PgReplicationConnection, sql: &str) -> Option<String> {
    conn.exec(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .get_value(0, 0)
}

/// One slot, publication and table (named after `tag`), dropped again by `finish`.
struct Fixture {
    admin: PgReplicationConnection,
    slot: String,
    table: String,
}

impl Fixture {
    fn new(tag: &str, columns: &str) -> Self {
        let mut admin =
            PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection");
        let slot = format!("it_params_{tag}");
        let table = format!("it_params_{tag}_t");
        for sql in [
            format!(
                "SELECT pg_drop_replication_slot('{slot}') WHERE EXISTS \
                 (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot}')"
            ),
            format!("DROP PUBLICATION IF EXISTS {slot}"),
            format!("DROP TABLE IF EXISTS {table}"),
            format!("CREATE TABLE {table} ({columns})"),
            format!("CREATE PUBLICATION {slot} FOR TABLE {table}"),
        ] {
            value(&mut admin, &sql);
        }
        Self { admin, slot, table }
    }

    /// A started stream on `url`, with the slot created by the library.
    async fn start(&self, url: &str) -> LogicalReplicationStream {
        let config = ReplicationStreamConfig::builder(&self.slot, &self.slot).with_retry_config(
            RetryConfig {
                max_attempts: 1,
                ..RetryConfig::default()
            },
        );
        let mut stream = LogicalReplicationStream::new(url, config)
            .await
            .expect("stream");
        stream.ensure_replication_slot().await.expect("slot");
        stream.start(None).await.expect("start");
        stream
    }

    /// `application_name` of the walsender serving this fixture's slot.
    fn walsender_application_name(&mut self) -> Option<String> {
        value(
            &mut self.admin,
            &format!(
                "SELECT a.application_name FROM pg_stat_activity a \
                 JOIN pg_replication_slots s ON s.active_pid = a.pid \
                 WHERE s.slot_name = '{}' AND a.backend_type = 'walsender'",
                self.slot
            ),
        )
    }

    /// Insert `values` into the table, then read events until its `Insert` arrives
    /// and return `column` of it as text.
    async fn decoded(
        &mut self,
        stream: &mut LogicalReplicationStream,
        values: &str,
        column: &str,
    ) -> Option<String> {
        let [text] = self.decoded_columns(stream, values, [column]).await;
        text
    }

    /// [`decoded`](Self::decoded) for several columns of the same row.
    async fn decoded_columns<const N: usize>(
        &mut self,
        stream: &mut LogicalReplicationStream,
        values: &str,
        columns: [&str; N],
    ) -> [Option<String>; N] {
        value(
            &mut self.admin,
            &format!("INSERT INTO {} VALUES ({values})", self.table),
        );
        let cancel = CancellationToken::new();
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let event = stream.next_event_with_retry(&cancel).await.expect("event");
                if let EventType::Insert { data, .. } = &event.event_type {
                    return columns.map(|c| data.get(c).and_then(|v| v.as_str()).map(String::from));
                }
            }
        })
        .await
        .expect("the Insert never arrived")
    }

    async fn finish(self, mut stream: LogicalReplicationStream) {
        stream.stop().await.expect("stop");
        drop(stream);
    }
}

/// Also runs when a test panics, so a failed assertion leaves no slot, publication
/// or table behind. The slot cannot be dropped while its walsender is still
/// shutting down, hence the wait.
impl Drop for Fixture {
    fn drop(&mut self) {
        for _ in 0..200 {
            let dropped = self.admin.exec(&format!(
                "SELECT pg_drop_replication_slot('{0}') WHERE EXISTS \
                 (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{0}')",
                self.slot
            ));
            if dropped.is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let _ = self.admin.exec(&format!(
            "DROP PUBLICATION IF EXISTS {0}; DROP TABLE IF EXISTS {1}",
            self.slot, self.table
        ));
    }
}

/// `application_name` names the session: it is what `pg_stat_activity`,
/// `pg_stat_replication` and `synchronous_standby_names` see.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn application_name_names_the_walsender() {
    let mut fixture = Fixture::new("appname", "id integer PRIMARY KEY");
    let url = with_params(
        &replication_conn_string(),
        "application_name=pgws%20cdc%20consumer",
    );
    let stream = fixture.start(&url).await;

    assert_eq!(
        fixture.walsender_application_name().as_deref(),
        Some("pgws cdc consumer")
    );
    fixture.finish(stream).await;
}

/// `fallback_application_name` is used only when `application_name` is absent
/// (an empty one counts as absent), whichever is written first, as in libpq's
/// parameter-array API (`PQconnectStartParams`), which the stream's connect uses.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn fallback_application_name_applies_only_without_application_name() {
    let mut fixture = Fixture::new("fallback", "id integer PRIMARY KEY");
    let base = replication_conn_string();

    let only_fallback = with_params(&base, "fallback_application_name=pgws_fallback");
    let stream = fixture.start(&only_fallback).await;
    assert_eq!(
        fixture.walsender_application_name().as_deref(),
        Some("pgws_fallback")
    );
    fixture.finish(stream).await;

    let mut fixture = Fixture::new("fallback", "id integer PRIMARY KEY");
    let both = with_params(
        &base,
        "fallback_application_name=pgws_fallback&application_name=pgws_real",
    );
    let stream = fixture.start(&both).await;
    assert_eq!(
        fixture.walsender_application_name().as_deref(),
        Some("pgws_real")
    );
    fixture.finish(stream).await;

    let mut fixture = Fixture::new("fallback", "id integer PRIMARY KEY");
    let empty = with_params(
        &base,
        "application_name=&fallback_application_name=pgws_fallback",
    );
    let stream = fixture.start(&empty).await;
    assert_eq!(
        fixture.walsender_application_name().as_deref(),
        Some("pgws_fallback")
    );
    fixture.finish(stream).await;
}

/// `options` pins GUCs for the session, so decoded text renders the way it would
/// on any connection with the same settings. `TimeZone` is the visible one: the
/// text of a `timestamptz` is rendered in the walsender's time zone.
///
/// Both connections pin their settings, so the result does not depend on the
/// server's, database's or role's defaults (`DateStyle` is pinned too: it decides
/// how a `timestamptz` is written).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn options_pin_the_time_zone_of_decoded_text() {
    let instant = "'2026-01-01 00:00:00+00'";
    let base = replication_conn_string();

    // First a control pinned to another zone, so the Tokyo one cannot match by accident.
    let mut fixture = Fixture::new("tz_default", "id integer PRIMARY KEY, ts timestamptz");
    let url = with_params(&base, "options=-c%20TimeZone%3DUTC%20-c%20DateStyle%3DISO");
    let mut stream = fixture.start(&url).await;
    let control = fixture
        .decoded(&mut stream, &format!("1, {instant}"), "ts")
        .await
        .expect("timestamptz text");
    fixture.finish(stream).await;
    assert_eq!(control, "2026-01-01 00:00:00+00");

    let mut fixture = Fixture::new("tz_pinned", "id integer PRIMARY KEY, ts timestamptz");
    let url = with_params(
        &base,
        "options=-c%20TimeZone%3DAsia/Tokyo%20-c%20DateStyle%3DISO",
    );
    let mut stream = fixture.start(&url).await;
    let pinned = fixture
        .decoded(&mut stream, &format!("1, {instant}"), "ts")
        .await
        .expect("timestamptz text");
    fixture.finish(stream).await;
    assert_eq!(pinned, "2026-01-01 09:00:00+09");
}

/// `DateStyle` and `IntervalStyle` pin the text of dates and intervals the same way,
/// several `-c` settings in one `options` string included. The control pins the
/// ISO/postgres styles, so the result does not depend on the server's defaults.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn options_pin_the_date_and_interval_styles_of_decoded_text() {
    let values = "1, '2026-02-01', '1 year 2 mons 3 days 04:05:06'";
    let columns = "id integer PRIMARY KEY, d date, iv interval";
    let base = replication_conn_string();

    // First a control pinned to the other styles, so the pinned one cannot match by accident.
    let mut fixture = Fixture::new("style_default", columns);
    let url = with_params(
        &base,
        "options=-c%20DateStyle%3DISO%20-c%20IntervalStyle%3Dpostgres",
    );
    let mut stream = fixture.start(&url).await;
    let [date, interval] = fixture
        .decoded_columns(&mut stream, values, ["d", "iv"])
        .await;
    fixture.finish(stream).await;
    assert_eq!(date.as_deref(), Some("2026-02-01"));
    assert_eq!(interval.as_deref(), Some("1 year 2 mons 3 days 04:05:06"));

    let mut fixture = Fixture::new("style_pinned", columns);
    let url = with_params(
        &base,
        "options=-c%20DateStyle%3DGerman%20-c%20IntervalStyle%3Diso_8601",
    );
    let mut stream = fixture.start(&url).await;
    let [date, interval] = fixture
        .decoded_columns(&mut stream, values, ["d", "iv"])
        .await;
    fixture.finish(stream).await;
    assert_eq!(date.as_deref(), Some("01.02.2026"));
    assert_eq!(interval.as_deref(), Some("P1Y2M3DT4H5M6S"));
}

/// The stream decodes UTF-8, so `options` must not be able to move the session to
/// another client encoding: `é` would then arrive as the single byte `0xE9`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn options_cannot_change_the_client_encoding() {
    let mut fixture = Fixture::new("encoding", "id integer PRIMARY KEY, note text");
    let url = with_params(
        &replication_conn_string(),
        "options=-c%20client_encoding%3DLATIN1",
    );
    let mut stream = fixture.start(&url).await;
    let note = fixture
        .decoded(&mut stream, "1, '\u{e9}t\u{e9}'", "note")
        .await;
    fixture.finish(stream).await;
    assert_eq!(note.as_deref(), Some("\u{e9}t\u{e9}"));
}

/// An `options` string naming a setting the server does not know fails the
/// connection (SQLSTATE 42704). No slot has been looked at yet, so it must not
/// surface as a `ReplicationSlot` error, which tells an operator to drop the slot
/// and re-sync.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live PostgreSQL with wal_level = logical"]
async fn an_unknown_setting_in_options_does_not_blame_the_slot() {
    let url = with_params(
        &replication_conn_string(),
        "options=-c%20not_a_real_setting%3D1",
    );
    let config = ReplicationStreamConfig::builder("it_params_badopt", "it_params_badopt")
        .with_retry_config(RetryConfig {
            max_attempts: 1,
            ..RetryConfig::default()
        });
    let err = LogicalReplicationStream::new(&url, config)
        .await
        .err()
        .expect("an unknown setting must fail the connection");
    assert!(
        !matches!(err, ReplicationError::ReplicationSlot(..)),
        "{err:?}"
    );
    assert!(err.to_string().contains("not_a_real_setting"), "{err}");
}
