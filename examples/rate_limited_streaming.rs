//! Rate-limited PostgreSQL WAL streaming example with futures::Stream
//!
//! This example demonstrates how to:
//! - Wrap EventStream with futures::stream::unfold for Stream trait compatibility
//! - Apply rate limiting to control event processing speed
//! - Use stream combinators for flow control
//! - Prevent overwhelming downstream systems with too many events
//! - Implement backpressure and throttling
//!
//! ## Use Cases
//!
//! - Protecting downstream APIs from being overwhelmed
//! - Spreading load over time for cost optimization
//! - Complying with rate limits of external services
//! - Controlled batch processing
//!
//! ## Prerequisites
//!
//! 1. PostgreSQL server running (version 14+)
//! 2. Database with logical replication enabled in postgresql.conf:
//!    ```
//!    wal_level = logical
//!    max_replication_slots = 4
//!    max_wal_senders = 4
//!    ```
//! 3. A publication created on the source database:
//!    ```sql
//!    CREATE PUBLICATION my_publication FOR ALL TABLES;
//!    ```
//!
//! ## Environment Variables
//!
//! Set the following environment variable or modify the connection string:
//! ```bash
//! export DATABASE_URL="postgresql://postgres:password@localhost:5432/postgres?replication=database"
//! ```
//!
//! ## Usage
//!
//! ```bash
//! # Limit to 10 events per second
//! cargo run --example rate_limited_streaming
//! ```

use futures::stream;
use pg_walstream::{
    CancellationToken, EventType, LogicalReplicationStream, ReplicationStreamConfig, RetryConfig,
    StreamingMode,
};
use std::env;
use std::time::{Duration, Instant};
use tokio_stream::StreamExt as TokioStreamExt;
use tracing::{error, info, warn, Level};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .init();

    info!("Starting rate-limited PostgreSQL WAL streaming example");

    // Get connection string from environment or use default
    let connection_string = env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:password@localhost:5432/postgres?replication=database".to_string()
    });

    info!("Connection string: {}", mask_password(&connection_string));

    // Configure rate limiting
    let max_events_per_second = env::var("MAX_EVENTS_PER_SECOND")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10); // Default: 10 events/second

    info!(
        "Rate limit configured: {} events per second",
        max_events_per_second
    );

    // Configure the replication stream
    let config = ReplicationStreamConfig::new(
        "rate_limited_slot".to_string(), // Replication slot name
        "my_publication".to_string(),    // Publication name
        2,                               // Protocol version
        StreamingMode::On,               // Streaming mode
        Duration::from_secs(10),         // Feedback interval
        Duration::from_secs(30),         // Connection timeout
        Duration::from_secs(60),         // Health check interval
        RetryConfig::default(),
    );

    info!("Creating replication stream...");

    // Create and initialize the stream
    let mut stream = LogicalReplicationStream::new(&connection_string, config).await?;

    info!("Stream created successfully");

    // Start replication from the latest position
    stream.start(None).await?;

    info!("Replication started successfully");
    info!("Listening for changes with rate limiting... (Press Ctrl+C to stop)");

    // Create cancellation token for graceful shutdown
    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();

    // Setup Ctrl+C handler
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for ctrl-c");
        info!("Received shutdown signal, cleaning up...");
        cancel_token_clone.cancel();
    });

    // Convert to EventStream
    let event_stream = stream.into_stream(cancel_token);

    // Wrap with futures::stream::unfold to get a proper futures::Stream
    let pg_stream = stream::unfold(event_stream, |mut event_stream| async move {
        match event_stream.next().await {
            Ok(event) => {
                // Update applied LSN after successful event retrieval
                event_stream.update_applied_lsn(event.lsn.value());
                Some((Ok(event), event_stream))
            }
            Err(e) => {
                // Return error and stop the stream
                Some((Err(e), event_stream))
            }
        }
    });

    // Apply rate limiting using stream combinator!
    // Calculate delay between events based on desired rate
    let delay_between_events = Duration::from_secs(1) / max_events_per_second;

    info!(
        "Applying rate limit: {} events/sec ({}ms between events)",
        max_events_per_second,
        delay_between_events.as_millis()
    );

    // Use tokio_stream's throttle combinator to rate limit the stream
    let mut rate_limited_stream = Box::pin(pg_stream.throttle(delay_between_events));

    // Statistics
    let mut event_count = 0;
    let mut insert_count = 0;
    let mut update_count = 0;
    let mut delete_count = 0;
    let start_time = Instant::now();

    // Process events with rate limiting applied by stream combinator
    while let Some(result) = TokioStreamExt::next(&mut rate_limited_stream).await {
        match result {
            Ok(event) => {
                event_count += 1;

                // Process events by type
                match &event.event_type {
                    EventType::Insert { schema, table, .. } => {
                        insert_count += 1;
                        info!("INSERT into {}.{} at LSN {}", schema, table, event.lsn);
                    }
                    EventType::Update { schema, table, .. } => {
                        update_count += 1;
                        info!("UPDATE {}.{} at LSN {}", schema, table, event.lsn);
                    }
                    EventType::Delete { schema, table, .. } => {
                        delete_count += 1;
                        info!("DELETE from {}.{} at LSN {}", schema, table, event.lsn);
                    }
                    EventType::Begin {
                        transaction_id,
                        final_lsn,
                        ..
                    } => {
                        info!("BEGIN transaction {} at LSN {}", transaction_id, final_lsn);
                    }
                    EventType::Commit { commit_lsn, .. } => {
                        info!("COMMIT transaction at LSN {}", commit_lsn);
                    }
                    EventType::Truncate(tables) => {
                        warn!("TRUNCATE tables: {:?}", tables);
                    }
                    _ => {
                        info!("Event at LSN {}", event.lsn);
                    }
                }

                // Show statistics every 20 events
                if event_count % 20 == 0 {
                    let elapsed = start_time.elapsed();
                    let actual_rate = event_count as f64 / elapsed.as_secs_f64();

                    info!(
                        "Statistics: {} total events ({} inserts, {} updates, {} deletes) | \
                         Actual rate: {:.1} events/sec | Target: {} events/sec",
                        event_count,
                        insert_count,
                        update_count,
                        delete_count,
                        actual_rate,
                        max_events_per_second
                    );
                }
            }
            Err(e) => {
                error!("Error: {}", e);
                break;
            }
        }
    }

    let total_duration = start_time.elapsed();
    let actual_rate = event_count as f64 / total_duration.as_secs_f64();

    info!(
        "Stream ended. Final statistics:\n\
         Total events: {}\n\
         - Inserts: {}\n\
         - Updates: {}\n\
         - Deletes: {}\n\
         Duration: {:.2}s\n\
         Actual rate: {:.2} events/sec\n\
         Configured limit: {} events/sec",
        event_count,
        insert_count,
        update_count,
        delete_count,
        total_duration.as_secs_f64(),
        actual_rate,
        max_events_per_second
    );
    info!("Graceful shutdown complete");

    Ok(())
}

/// Mask password in connection string for logging
fn mask_password(conn_str: &str) -> String {
    if let Some(proto_end) = conn_str.find("://") {
        let proto = &conn_str[..proto_end + 3];
        let rest = &conn_str[proto_end + 3..];

        if let Some(at_pos) = rest.find('@') {
            let credentials = &rest[..at_pos];
            let after_at = &rest[at_pos..];

            if let Some(colon_pos) = credentials.find(':') {
                let user = &credentials[..colon_pos];
                return format!("{}{}:****{}", proto, user, after_at);
            }
        }
    }
    conn_str.to_string()
}
