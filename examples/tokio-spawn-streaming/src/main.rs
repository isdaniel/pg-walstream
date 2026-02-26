//! Demonstrates using `LogicalReplicationStream` inside `tokio::spawn`.
//!
//! # Why this matters
//!
//! `tokio::spawn` requires the spawned future to be `Send`. Starting from pg_walstream v0.5.0
//! the internal `PgReplicationConnection` holds `&mut self` across `.await` points
//! (async flush via `AsyncFd::writable()`). For the resulting future to be `Send`,
//! the connection type only needs to be `Send` (not `Sync`), because `&mut T: Send`
//! requires `T: Send`. pg_walstream provides this, so this pattern works.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────┐
//! │         Main task                │
//! │  - Creates stream & config       │
//! │  - Spawns producer on new task   │
//! │  - Receives events via channel   │
//! └──────────┬───────────────────────┘
//!            │ tokio::spawn
//!            ▼
//! ┌──────────────────────────────────┐
//! │       Producer task (spawned)     │
//! │  - Owns LogicalReplicationStream │
//! │  - Calls next_event_with_retry() │
//! │  - Sends events over channel     │
//! └──────────────────────────────────┘
//! ```
//!
//! ## Prerequisites
//!
//! 1. PostgreSQL 14+ with `wal_level = logical`
//! 2. A publication: `CREATE PUBLICATION my_publication FOR ALL TABLES;`
//!
//! ## Usage
//!
//! ```bash
//! export DATABASE_URL="postgresql://postgres:password@localhost:5432/postgres?replication=database"
//! cargo run -p tokio-spawn-streaming
//! ```

use pg_walstream::{
    CancellationToken, ChangeEvent, LogicalReplicationStream, ReplicationError,
    ReplicationStreamConfig, RetryConfig, StreamingMode,
};
use std::env;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, warn, Level};

/// Run the replication producer inside a spawned task.
///
/// This function **owns** the `LogicalReplicationStream` and is designed to be
/// passed directly to `tokio::spawn`.  The key requirement is that the returned
/// future is `Send`, which is guaranteed because `LogicalReplicationStream` is `Send`
/// and all async methods take `&mut self` (so no `Sync` is needed).
async fn run_producer(
    mut stream: LogicalReplicationStream,
    cancel_token: CancellationToken,
    tx: mpsc::Sender<ChangeEvent>,
) {
    // Start replication from the current WAL position
    if let Err(e) = stream.start(None).await {
        error!("Failed to start replication: {e}");
        return;
    }

    info!("Producer started, streaming WAL events...");

    loop {
        match stream.next_event_with_retry(&cancel_token).await {
            Ok(event) => {
                // Update applied LSN so PostgreSQL can reclaim WAL
                stream
                    .shared_lsn_feedback
                    .update_applied_lsn(event.lsn.value());

                // Send event to consumer; if the receiver is dropped, stop
                if tx.send(event).await.is_err() {
                    warn!("Consumer dropped, stopping producer");
                    break;
                }
            }
            Err(e) if matches!(e, ReplicationError::Cancelled(_)) => {
                info!("Producer cancelled, shutting down gracefully");
                break;
            }
            Err(e) => {
                error!("Fatal replication error: {e}");
                break;
            }
        }
    }

    // Best-effort: send final feedback and close
    if let Err(e) = stream.stop().await {
        warn!("Error during stream shutdown: {e}");
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .init();

    let connection_string = env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:password@localhost:5432/postgres?replication=database".to_string()
    });

    // ─── Configure ───────────────────────────────────────────────────
    let config = ReplicationStreamConfig::new(
        "spawn_example_slot".to_string(),
        "my_publication".to_string(),
        2,
        StreamingMode::On,
        Duration::from_secs(10),
        Duration::from_secs(30),
        Duration::from_secs(60),
        RetryConfig::default(),
    );

    // ─── Create stream ──────────────────────────────────────────────
    let stream = LogicalReplicationStream::new(&connection_string, config).await?;

    // ─── Channel & cancellation ─────────────────────────────────────
    let (tx, mut rx) = mpsc::channel::<ChangeEvent>(256);
    let cancel_token = CancellationToken::new();
    let cancel_for_signal = cancel_token.clone();

    // Ctrl+C handler
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for ctrl-c");
        info!("Shutdown signal received");
        cancel_for_signal.cancel();
    });

    let producer_handle = tokio::spawn(run_producer(stream, cancel_token.clone(), tx));

    // ─── Consume events on the main task ────────────────────────────
    info!("Consumer ready, waiting for events...");
    let mut count = 0u64;

    while let Some(event) = rx.recv().await {
        count += 1;
        info!(
            "[{count}] {event_type:?} @ LSN {lsn}",
            event_type = event.event_type,
            lsn = event.lsn,
        );
    }

    // Wait for the producer task to finish
    if let Err(e) = producer_handle.await {
        error!("Producer task panicked: {e}");
    }

    info!("Processed {count} events total. Done.");
    Ok(())
}
