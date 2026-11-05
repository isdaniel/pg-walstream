#![cfg(any(feature = "libpq", feature = "rustls-tls"))]

//! Startup metadata read on the stream's own connection.
//!
//! ```bash
//! export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
//! export DATABASE_URL_REGULAR="postgresql://postgres:postgres@localhost:5432/test_walstream"
//! cargo test --test startup_metadata -- --ignored --nocapture --test-threads=1
//! ```

use pg_walstream::{
    CancellationToken, ColumnValue, EventType, LogicalReplicationStream, Lsn,
    PgReplicationConnection, ReplicationStreamConfig, RetryConfig,
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// `url` rerouted through a local proxy that holds every server reply for 5 ms, so no round trip completes within a single poll.
async fn through_a_slow_proxy(url: &str) -> (String, tokio::task::JoinHandle<()>) {
    let (scheme, rest) = url.split_once("://").expect("URI connection string");
    let (userinfo, rest) = rest.rsplit_once('@').expect("user in the URI");
    let path_at = rest.find(['/', '?']).unwrap_or(rest.len());
    let (server, tail) = rest.split_at(path_at);
    let server = if server.contains(':') {
        server.to_string()
    } else {
        format!("{server}:5432")
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let proxy = tokio::spawn(async move {
        loop {
            let (client, _) = listener.accept().await.unwrap();
            let upstream = tokio::net::TcpStream::connect(&server).await.unwrap();
            let (mut client_read, mut client_write) = client.into_split();
            let (mut upstream_read, mut upstream_write) = upstream.into_split();
            tokio::spawn(async move {
                let _ = tokio::io::copy(&mut client_read, &mut upstream_write).await;
            });
            tokio::spawn(async move {
                let mut buf = vec![0u8; 64 * 1024];
                while let Ok(n) = upstream_read.read(&mut buf).await {
                    if n == 0 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    if client_write.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    (
        format!("{scheme}://{userinfo}@127.0.0.1:{port}{tail}"),
        proxy,
    )
}

fn replication_conn_string() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/postgres?replication=database".to_string()
    })
}

fn regular_conn_string() -> String {
    std::env::var("DATABASE_URL_REGULAR")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:5432/postgres".to_string())
}

fn regular() -> PgReplicationConnection {
    PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection")
}

fn scalar(conn: &mut PgReplicationConnection, sql: &str) -> Option<String> {
    conn.exec(sql).expect(sql).get_value(0, 0)
}

fn drop_slot(conn: &mut PgReplicationConnection, slot: &str) {
    let _ = conn.exec(&format!(
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
         WHERE slot_name = '{slot}'"
    ));
}

fn config(slot: &str, publication: &str) -> ReplicationStreamConfig {
    ReplicationStreamConfig::builder(slot, publication).with_retry_config(RetryConfig {
        max_attempts: 1,
        ..RetryConfig::default()
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn server_identity_describes_the_streaming_server() {
    let mut regular = regular();
    // `system_identifier` is a signed bigint holding the unsigned control-file value.
    let expected: u64 = scalar(
        &mut regular,
        "SELECT (system_identifier::numeric + CASE WHEN system_identifier < 0 \
         THEN 18446744073709551616 ELSE 0 END)::text FROM pg_control_system()",
    )
    .unwrap()
    .parse()
    .unwrap();
    let timeline: u32 = scalar(
        &mut regular,
        "SELECT timeline_id FROM pg_control_checkpoint()",
    )
    .unwrap()
    .parse()
    .unwrap();

    let mut stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        config("it_startup_identity", "it_startup_identity"),
    )
    .await
    .expect("stream");

    assert_eq!(stream.server_identity().system_id(), expected);
    assert_eq!(stream.server_identity().timeline(), timeline);
    let history = stream.timeline_history().await.expect("history");
    assert_eq!(history.is_empty(), timeline == 1, "{history:?}");
    assert!(
        history.iter().all(|switch| switch.timeline() < timeline),
        "{history:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn slot_position_is_read_before_start_and_refused_after() {
    let (slot, publication, table) = ("it_startup_slot", "it_startup_slot", "it_startup_slot");
    let mut regular = regular();
    drop_slot(&mut regular, slot);
    regular
        .exec(&format!(
            "CREATE TABLE IF NOT EXISTS {table} (id INT PRIMARY KEY)"
        ))
        .expect("table");
    regular
        .exec(&format!("DROP PUBLICATION IF EXISTS {publication}"))
        .expect("drop publication");
    regular
        .exec(&format!(
            "CREATE PUBLICATION {publication} FOR TABLE {table}"
        ))
        .expect("publication");

    let mut missing =
        LogicalReplicationStream::new(&replication_conn_string(), config(slot, publication))
            .await
            .expect("stream");
    assert_eq!(missing.slot_confirmed_flush_lsn().await.unwrap(), None);
    drop(missing);

    regular
        .exec(&format!(
            "SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput')"
        ))
        .expect("slot");
    let expected = scalar(
        &mut regular,
        &format!(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = '{slot}'"
        ),
    )
    .unwrap();

    let mut stream =
        LogicalReplicationStream::new(&replication_conn_string(), config(slot, publication))
            .await
            .expect("stream");
    let position = stream
        .slot_confirmed_flush_lsn()
        .await
        .unwrap()
        .expect("the slot exists");
    assert_eq!(
        position,
        Lsn::new(pg_walstream::parse_lsn(&expected).unwrap())
    );

    stream.start(None).await.expect("start");
    assert!(
        stream.slot_confirmed_flush_lsn().await.is_err(),
        "a streaming connection cannot run the slot query"
    );

    regular
        .exec(&format!("INSERT INTO {table} VALUES (1)"))
        .expect("insert");
    let cancel = CancellationToken::new();
    let first = tokio::time::timeout(Duration::from_secs(10), stream.next_event(&cancel))
        .await
        .expect("an event arrives")
        .expect("event");
    assert!(
        matches!(first.event_type, EventType::Begin { .. }),
        "{first:?}"
    );

    drop(stream);
    drop_slot(&mut regular, slot);
    let _ = regular.exec(&format!("DROP PUBLICATION IF EXISTS {publication}"));
    let _ = regular.exec(&format!("DROP TABLE IF EXISTS {table}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn slot_position_of_a_physical_slot_is_an_error() {
    let slot = "it_startup_physical";
    let mut regular = regular();
    drop_slot(&mut regular, slot);
    regular
        .exec(&format!(
            "SELECT pg_create_physical_replication_slot('{slot}')"
        ))
        .expect("slot");

    let mut stream =
        LogicalReplicationStream::new(&replication_conn_string(), config(slot, "unused"))
            .await
            .expect("stream");
    let err = stream.slot_confirmed_flush_lsn().await.unwrap_err();
    assert!(err.to_string().contains("not a logical slot"), "{err}");

    drop(stream);
    drop_slot(&mut regular, slot);
}

/// A binary stream sends each published column in binary exactly when the catalog read before `start` says its type sends binary.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn type_catalog_predicts_the_binary_columns() {
    let name = "it_type_catalog";
    let cleanup = [
        "DROP PUBLICATION IF EXISTS it_type_catalog",
        "DROP TABLE IF EXISTS it_type_catalog",
        "DROP TYPE IF EXISTS it_type_catalog_mood",
        "DROP DOMAIN IF EXISTS it_type_catalog_positive",
        "DROP TYPE IF EXISTS it_type_catalog_floats",
        "DROP TYPE IF EXISTS it_type_catalog_pair",
    ];
    let mut regular = regular();
    drop_slot(&mut regular, name);
    for sql in cleanup.into_iter().chain([
        "CREATE TYPE it_type_catalog_mood AS ENUM ('sad', 'happy')",
        "CREATE DOMAIN it_type_catalog_positive AS int4 CHECK (VALUE > 0)",
        "CREATE TYPE it_type_catalog_floats AS RANGE (subtype = float8)",
        "CREATE TYPE it_type_catalog_pair AS (gone int8, n int4, x float8)",
        "ALTER TYPE it_type_catalog_pair DROP ATTRIBUTE gone",
        "CREATE TABLE it_type_catalog (id int4 PRIMARY KEY, acl aclitem, \
         mood it_type_catalog_mood, amount it_type_catalog_positive, \
         pair it_type_catalog_pair, unlisted text)",
        "CREATE PUBLICATION it_type_catalog \
         FOR TABLE it_type_catalog (id, acl, mood, amount, pair)",
    ]) {
        regular.exec(sql).expect(sql);
    }

    let mut stream = LogicalReplicationStream::new(
        &replication_conn_string(),
        config(name, name).with_binary(true),
    )
    .await
    .expect("stream");
    let catalog = stream.type_catalog().await.expect("catalog");
    let relations = stream.published_relations().await.expect("relations");
    assert!(catalog.windows(2).all(|w| w[0].oid() < w[1].oid()));
    let named = |name: &str| {
        catalog
            .iter()
            .find(|t| t.name() == name)
            .unwrap_or_else(|| panic!("{name} is in the catalog"))
    };
    let positive = named("it_type_catalog_positive");
    assert_eq!((positive.kind(), positive.base_type()), ('d', Some(23)));
    assert_eq!(named("it_type_catalog_mood").kind(), 'e');
    assert_eq!(
        named("_it_type_catalog_mood").element(),
        Some(named("it_type_catalog_mood").oid())
    );
    assert_eq!(named("point").element(), None);
    for range in [
        "it_type_catalog_floats",
        "it_type_catalog_floats_multirange",
    ] {
        assert_eq!(named(range).range_subtype(), Some(701), "{range}");
    }
    assert!(!named("aclitem").sends_binary());
    assert_eq!(named("it_type_catalog_pair").fields(), [23, 701]);
    let [relation] = relations.as_slice() else {
        panic!("{relations:?}");
    };
    let columns: Vec<&str> = relation.columns.iter().map(|c| &*c.name).collect();
    assert_eq!(columns, ["id", "acl", "mood", "amount", "pair"]);

    stream.start(None).await.expect("start");
    assert!(
        stream.type_catalog().await.is_err(),
        "a streaming connection cannot read the catalog"
    );

    regular
        .exec(
            "INSERT INTO it_type_catalog \
             VALUES (1, (current_user || '=r/' || current_user)::aclitem, 'happy', 7, ROW(1, 2.5), 'x')",
        )
        .expect("insert");
    let cancel = CancellationToken::new();
    let row = loop {
        let event = tokio::time::timeout(Duration::from_secs(10), stream.next_event(&cancel))
            .await
            .expect("an event arrives")
            .expect("event");
        if let EventType::Insert { data, .. } = event.event_type {
            break data;
        }
    };
    for column in &relation.columns {
        let value = row.get(&column.name).expect("the column is in the row");
        let sends_binary = catalog
            .iter()
            .any(|t| t.oid() == column.type_id && t.sends_binary());
        assert_eq!(
            matches!(value, ColumnValue::Binary(_)),
            sends_binary,
            "{}: {value:?}",
            column.name
        );
    }

    drop(stream);
    drop_slot(&mut regular, name);
    for sql in cleanup {
        let _ = regular.exec(sql);
    }
}

/// `Duration::ZERO` is "no timeout", so a stream whose server answers in a few milliseconds still starts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn a_zero_connection_timeout_does_not_bound_startup() {
    let (url, proxy) = through_a_slow_proxy(&replication_conn_string()).await;
    let stream = LogicalReplicationStream::new(
        &url,
        config("it_startup_zero", "it_startup_zero").with_connection_timeout(Duration::ZERO),
    )
    .await
    .expect("Duration::ZERO means no timeout");
    assert_ne!(stream.server_identity().system_id(), 0);
    drop(stream);
    proxy.abort();
}

/// The blocking commands kept next to their `_async` siblings still reach the server: identify it, create a temporary slot and start streaming from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live PostgreSQL with wal_level=logical"]
async fn blocking_commands_identify_create_and_start() {
    let slot = "it_startup_blocking";
    let mut regular = regular();
    drop_slot(&mut regular, slot);
    let _ = regular.exec(&format!("DROP PUBLICATION IF EXISTS {slot}"));
    regular
        .exec(&format!("CREATE PUBLICATION {slot}"))
        .expect("publication");

    let stream = LogicalReplicationStream::new(&replication_conn_string(), config(slot, slot))
        .await
        .expect("stream");
    let expected = *stream.server_identity();

    let mut conn =
        PgReplicationConnection::connect(&replication_conn_string()).expect("blocking connect");
    let identified = conn.identify_system().expect("IDENTIFY_SYSTEM");
    assert_eq!(
        identified
            .get_value(0, 0)
            .and_then(|v| v.parse::<u64>().ok()),
        Some(expected.system_id())
    );
    assert_eq!(
        identified
            .get_value(0, 1)
            .and_then(|v| v.parse::<u32>().ok()),
        Some(expected.timeline())
    );

    let created = conn
        .create_replication_slot_with_options(
            slot,
            pg_walstream::SlotType::Logical,
            Some("pgoutput"),
            &pg_walstream::ReplicationSlotOptions {
                temporary: true,
                ..Default::default()
            },
        )
        .expect("CREATE_REPLICATION_SLOT");
    assert_eq!(created.get_value(0, 0).as_deref(), Some(slot));

    conn.start_replication(
        slot,
        0,
        &[
            ("proto_version", "1"),
            ("publication_names", &format!("\"{slot}\"")),
        ],
    )
    .expect("START_REPLICATION");
    assert!(
        conn.exec("SELECT 1").is_err(),
        "a streaming connection must refuse queries"
    );

    drop(conn);
    let _ = regular.exec(&format!("DROP PUBLICATION IF EXISTS {slot}"));
}
