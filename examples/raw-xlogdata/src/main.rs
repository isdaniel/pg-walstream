//! Raw XLogData streaming with auto-ack.
//!
//! `next_raw_event` returns the **undecoded** pgoutput payload plus WAL positions
//! (`RawXLogData`) instead of a parsed `ChangeEvent` — for consumers that bring
//! their own decoder. Keepalives, feedback, and cancellation are still handled;
//! only pgoutput decoding is skipped, and there is no auto-ack (you own restart
//! semantics). "Auto-ack" here just means advancing the applied LSN after each
//! message is durably processed.
//!
//! ## Prerequisites
//!
//! PostgreSQL 14+ with `wal_level = logical` and a publication:
//! ```sql
//! CREATE PUBLICATION my_publication FOR ALL TABLES;
//! ```
//!
//! ## Usage
//!
//! ```bash
//! export DATABASE_URL="postgresql://postgres:password@localhost:5432/postgres?replication=database"
//! cargo run
//! ```

use pg_walstream::{
    CancellationToken, LogicalReplicationStream, ReplicationError, ReplicationStreamConfig,
};
use std::env;
use tracing::{error, info, Level};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .init();

    let connection_string = env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:password@localhost:5432/postgres?replication=database".to_string()
    });

    let config = ReplicationStreamConfig::builder("raw_slot", "my_publication");

    let mut stream = LogicalReplicationStream::new(&connection_string, config).await?;
    stream.start(None).await?;
    info!("Streaming raw XLogData... (Press Ctrl+C to stop)");

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Received shutdown signal");
        cancel_clone.cancel();
    });

    loop {
        match stream.next_raw_event(&cancel).await {
            Ok(raw) => {
                // Bring your own decoder for `raw.data` (undecoded pgoutput bytes).
                info!(
                    wal_start = %raw.wal_start,
                    wal_end = %raw.wal_end,
                    payload_len = raw.data.len(),
                    "raw XLogData",
                );

                // Auto-ack: advance the applied LSN AFTER the payload is durably processed. Ack with `wal_end` (next byte after this message).
                stream
                    .shared_lsn_feedback
                    .update_applied_lsn(raw.wal_end.value());
            }
            Err(ReplicationError::Cancelled(_) | ReplicationError::StreamStopped(_)) => {
                info!("Stream ended gracefully");
                break;
            }
            Err(e) => {
                error!("Stream error: {}", e);
                return Err(e.into());
            }
        }
    }

    Ok(())
}
