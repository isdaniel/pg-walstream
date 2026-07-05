//! Derive Router Example (Live Database)
//!
//! Demonstrates how to connect to a real PostgreSQL database, stream WAL events,
//! and route them into typed handlers using `#[derive(WalTable)]` and the
//! `WalRouter::on_*_of` Level-A closures.
//!
//! ## Prerequisites
//!
//! 1. PostgreSQL with `wal_level = logical`
//! 2. Set `DATABASE_URL` environment variable
//!
//! ## Running
//!
//! ```bash
//! cd examples/derive-router
//! DATABASE_URL='postgresql://postgres:pass@host:5432/db?replication=database&sslmode=require' cargo run
//! ```
//!
//! Then in another terminal, run SQL against the same database:
//! ```sql
//! INSERT INTO users (username, email, score, active) VALUES ('alice', 'alice@example.com', 95.5, true);
//! UPDATE users SET score = 99.0 WHERE username = 'alice';
//! DELETE FROM users WHERE username = 'alice';
//! ```

use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use pg_walstream::{
    CancellationToken, LogicalReplicationStream, ReplicationSlotOptions, ReplicationStreamConfig,
    RetryConfig, StreamingMode, WalRouter, wal_table
};
use postgres_openssl::MakeTlsConnector;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

// ---------------------------------------------------------------------------
// 1. Define your model struct — derive Deserialize + WalTable
// ---------------------------------------------------------------------------

/// Matches the `typed_deser_users` table we create below:
/// ```sql
/// CREATE TABLE typed_deser_users (
///     id         BIGSERIAL PRIMARY KEY,
///     username   VARCHAR(50) NOT NULL,
///     email      TEXT,
///     score      DOUBLE PRECISION NOT NULL DEFAULT 0.0,
///     active     BOOLEAN NOT NULL DEFAULT true
/// );
/// ```
// #[derive(Debug, Deserialize, pg_walstream::WalTable)]
// #[wal(table = "typed_deser_users")]
#[wal_table("typed_deser_users")]
#[derive(Debug, Deserialize)]
struct User {
    id: i64,
    username: String,
    email: Option<String>, // nullable column → Option
    score: f64,
    active: bool,
}

fn fmt_user(u: &User) -> String {
    format!(
        "User {{ id: {}, username: {:?}, email: {:?}, score: {}, active: {} }}",
        u.id, u.username, u.email, u.score, u.active
    )
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const TABLE_NAME: &str = "typed_deser_users";
const PUBLICATION: &str = "typed_deser_pub";
const SLOT_NAME: &str = "typed_deser_slot";

// ---------------------------------------------------------------------------
// Database helpers
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct DbConfig {
    host: String,
    port: u16,
    db: String,
    user: String,
    pass: String,
    sslmode: String,
}

fn parse_db_config() -> DbConfig {
    let url = std::env::var("DATABASE_URL").expect(
        "DATABASE_URL env var required.\n\
         Example: DATABASE_URL='postgresql://postgres:pass@localhost:5432/postgres?replication=database'",
    );

    let url = url
        .trim_start_matches("postgresql://")
        .trim_start_matches("postgres://");

    // Split on the LAST '@' so passwords containing '@' parse correctly.
    let (creds, rest) = url.rsplit_once('@').unwrap_or(("postgres:postgres", url));
    let (user, pass) = creds.split_once(':').unwrap_or((creds, ""));

    let (host_port, db_params) = rest.split_once('/').unwrap_or((rest, "postgres"));
    let (host, port) = if host_port.contains(':') {
        let (h, p) = host_port.rsplit_once(':').unwrap();
        (h, p.parse::<u16>().unwrap_or(5432))
    } else {
        (host_port, 5432)
    };

    let (db, params) = db_params.split_once('?').unwrap_or((db_params, ""));

    let mut sslmode = "prefer".to_string();
    for param in params.split('&') {
        if let Some(val) = param.strip_prefix("sslmode=") {
            sslmode = val.to_string();
        }
    }

    DbConfig {
        host: host.to_string(),
        port,
        db: db.to_string(),
        user: user.to_string(),
        pass: pass.to_string(),
        sslmode,
    }
}

fn repl_conn_string(cfg: &DbConfig) -> String {
    format!(
        "postgresql://{}:{}@{}:{}/{}?replication=database&sslmode={}",
        cfg.user, cfg.pass, cfg.host, cfg.port, cfg.db, cfg.sslmode
    )
}

fn regular_conn_string(cfg: &DbConfig) -> String {
    format!(
        "host={} port={} dbname={} user={} password={} sslmode={}",
        cfg.host, cfg.port, cfg.db, cfg.user, cfg.pass, cfg.sslmode
    )
}

async fn pg_connect(conn_str: &str) -> tokio_postgres::Client {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_verify(SslVerifyMode::NONE);
    let tls = MakeTlsConnector::new(builder.build());
    let (client, conn) = tokio_postgres::connect(conn_str, tls)
        .await
        .expect("Failed to connect to PostgreSQL");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

// ---------------------------------------------------------------------------
// Setup & cleanup
// ---------------------------------------------------------------------------

async fn setup_database(cfg: &DbConfig) {
    println!("Setting up table and publication...");
    let client = pg_connect(&regular_conn_string(cfg)).await;

    let create_table = format!(
        "CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id BIGSERIAL PRIMARY KEY,
            username VARCHAR(50) NOT NULL,
            email TEXT,
            score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            active BOOLEAN NOT NULL DEFAULT true
        )"
    );
    client.execute(&create_table as &str, &[]).await.unwrap();

    let _ = client
        .execute(
            &format!("ALTER TABLE {TABLE_NAME} REPLICA IDENTITY FULL") as &str,
            &[],
        )
        .await;

    let pub_exists = client
        .query(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&PUBLICATION],
        )
        .await
        .map(|rows| !rows.is_empty())
        .unwrap_or(false);

    if !pub_exists {
        let sql = format!("CREATE PUBLICATION {PUBLICATION} FOR TABLE {TABLE_NAME}");
        match client.execute(&sql as &str, &[]).await {
            Ok(_) => println!("Created publication: {PUBLICATION}"),
            Err(e) => println!("Publication note: {e}"),
        }
    }

    // Drop old slot if exists
    let drop_sql = format!(
        "SELECT pg_drop_replication_slot('{SLOT_NAME}') \
         WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{SLOT_NAME}')"
    );
    let _ = client.execute(&drop_sql as &str, &[]).await;

    // Seed some initial data so there's something to see
    let count: i64 = client
        .query_one(&format!("SELECT count(*) FROM {TABLE_NAME}") as &str, &[])
        .await
        .unwrap()
        .get(0);

    if count == 0 {
        println!("Seeding initial data...");
        client
            .execute(
                &format!(
                    "INSERT INTO {TABLE_NAME} (username, email, score, active) VALUES
                     ('alice', 'alice@example.com', 95.5, true),
                     ('bob', NULL, 72.0, true),
                     ('charlie', 'charlie@co.org', 88.3, false)"
                ) as &str,
                &[],
            )
            .await
            .unwrap();
    }

    println!("Setup complete.\n");
}

async fn cleanup_database(cfg: &DbConfig) {
    println!("\nCleaning up...");
    let client = pg_connect(&regular_conn_string(cfg)).await;

    let drop_slot = format!(
        "SELECT pg_drop_replication_slot('{SLOT_NAME}') \
         WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{SLOT_NAME}')"
    );
    let _ = client.execute(&drop_slot as &str, &[]).await;

    let _ = client
        .execute(
            &format!("DROP PUBLICATION IF EXISTS {PUBLICATION}") as &str,
            &[],
        )
        .await;

    let _ = client
        .execute(&format!("DROP TABLE IF EXISTS {TABLE_NAME}") as &str, &[])
        .await;

    println!("Cleanup done.");
}

// ---------------------------------------------------------------------------
// Generate some DML to produce WAL events
// ---------------------------------------------------------------------------

async fn generate_changes(cfg: &DbConfig) {
    let client = pg_connect(&regular_conn_string(cfg)).await;

    println!("  Generating INSERT...");
    client
        .execute(
            &format!(
                "INSERT INTO {TABLE_NAME} (username, email, score, active) \
                 VALUES ('dave', 'dave@example.com', 45.0, true)"
            ) as &str,
            &[],
        )
        .await
        .unwrap();

    println!("  Generating UPDATE...");
    client
        .execute(
            &format!(
                "UPDATE {TABLE_NAME} SET score = 99.9, email = 'dave.new@example.com' \
                 WHERE username = 'dave'"
            ) as &str,
            &[],
        )
        .await
        .unwrap();

    println!("  Generating DELETE...");
    client
        .execute(
            &format!("DELETE FROM {TABLE_NAME} WHERE username = 'dave'") as &str,
            &[],
        )
        .await
        .unwrap();

    println!("  DML complete — WAL events should arrive shortly.\n");
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "rustls-tls")]
    {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }

    println!("=== pg-walstream Derive Router (Live DB) ===\n");

    let cfg = parse_db_config();
    println!(
        "Target: {}:{}/{} (ssl={})\n",
        cfg.host, cfg.port, cfg.db, cfg.sslmode
    );

    // 1. Setup table & publication
    setup_database(&cfg).await;

    // 2. Start WAL replication stream
    println!("Starting WAL replication stream...");
    let stream_config = ReplicationStreamConfig::new(
        SLOT_NAME.to_string(),
        PUBLICATION.to_string(),
        4,
        StreamingMode::Parallel,
        Duration::from_secs(5),
        Duration::from_secs(30),
        Duration::from_secs(60),
        RetryConfig::default(),
    )
    .with_slot_options(ReplicationSlotOptions {
        temporary: true,
        ..Default::default()
    });

    let mut stream = LogicalReplicationStream::new(&repl_conn_string(&cfg), stream_config).await?;
    stream.start(None).await?;

    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();
    let mut event_stream = stream.into_stream(cancel_token.clone());

    // 3. Setup Ctrl+C handler for graceful shutdown
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        println!("\nReceived Ctrl+C, shutting down...");
        cancel_token_clone.cancel();
    });

    // 4. Generate some DML in the background so we have events to consume
    let gen_cfg = cfg.clone();
    tokio::spawn(async move {
        generate_changes(&gen_cfg).await;
    });

    println!("Listening for WAL events... (Press Ctrl+C to stop)\n");
    println!("{:-<70}", "");

    // 5. Route events through a WalRouter using the derived table + Level-A closures.
    use std::sync::atomic::{AtomicU64, Ordering};
    let count = Arc::new(AtomicU64::new(0));
    let mut router = WalRouter::new();
    {
        let c = count.clone();
        router.on_insert_of::<User, _>(move |u| {
            let c = c.clone();
            async move { c.fetch_add(1, Ordering::SeqCst); println!("[INSERT] {}", fmt_user(&u)); Ok(()) }
        });
    }
    {
        let c = count.clone();
        router.on_update_of::<User, _>(move |old, new| {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                println!("[UPDATE] {} => {}", old.as_ref().map(fmt_user).unwrap_or_else(|| "<none>".into()), fmt_user(&new));
                Ok(())
            }
        });
    }
    {
        let c = count.clone();
        router.on_delete_of::<User, _>(move |u| {
            let c = c.clone();
            async move { c.fetch_add(1, Ordering::SeqCst); println!("[DELETE] {}", fmt_user(&u)); Ok(()) }
        });
    }
    router.run(&mut event_stream).await?;
    let _ = event_stream.shutdown().await;
    println!("\nTotal DML events handled: {}", count.load(Ordering::SeqCst));

    // 6. Cleanup
    cleanup_database(&cfg).await;

    println!("\n=== Done ===");
    Ok(())
}
