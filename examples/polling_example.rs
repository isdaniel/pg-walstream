//! Polling-based PostgreSQL WAL streaming example
//!
//! This example demonstrates the lower-level polling API without using the Stream feature.
//! This is useful when you want more control over the polling loop or when integrating
//! with systems that don't work well with async streams.
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
//! ```bash
//! export DATABASE_URL="postgresql://postgres:password@localhost:5432/postgres?replication=database"
//! ```
//!
//! ## Usage
//!
//! ```bash
//! cargo run --example polling_example
//! ```

use pg_walstream::{
    CancellationToken, EventType, LogicalReplicationStream, ReplicationStreamConfig, RetryConfig,
};
use std::env;
use std::time::Duration;
use tracing::{error, info, Level};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .init();

    info!("Starting PostgreSQL WAL polling example");

    // Get connection string from environment or use default
    let connection_string = env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:password@localhost:5432/postgres?replication=database".to_string()
    });

    info!("Connection string: {}", mask_password(&connection_string));

    // Configure the replication stream
    let config = ReplicationStreamConfig::new(
        "polling_slot".to_string(),   // Replication slot name
        "my_publication".to_string(), // Publication name (must exist)
        2,                            // Protocol version
        true,                         // Enable streaming
        Duration::from_secs(10),      // Feedback interval
        Duration::from_secs(30),      // Connection timeout
        Duration::from_secs(60),      // Health check interval
        RetryConfig::default(),       // Retry configuration
    );

    info!("Creating replication stream...");

    // Create and initialize the stream
    let mut stream = LogicalReplicationStream::new(&connection_string, config).await?;

    info!("Stream created successfully");

    // Start replication from the latest position
    stream.start(None).await?;

    info!("Replication started successfully");
    info!("Polling for changes... (Press Ctrl+C to stop)");

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

    let mut event_count = 0;

    // Manual polling loop
    loop {
        // Check if we should stop
        if cancel_token.is_cancelled() {
            info!("Cancellation requested, stopping...");
            break;
        }

        // Poll for the next event
        match stream.next_event(&cancel_token).await {
            Ok(Some(event)) => {
                event_count += 1;

                // Display event information based on type
                match &event.event_type {
                    EventType::Insert {
                        schema,
                        table,
                        relation_oid,
                        data,
                    } => {
                        info!(
                            "INSERT on {}.{} (OID: {}, LSN: {})",
                            schema, table, relation_oid, event.lsn
                        );
                        info!("   Data: {} columns", data.len());
                    }
                    EventType::Update {
                        schema,
                        table,
                        relation_oid,
                        old_data,
                        new_data,
                        ..
                    } => {
                        info!(
                            "UPDATE on {}.{} (OID: {}, LSN: {})",
                            schema, table, relation_oid, event.lsn
                        );
                        if old_data.is_some() {
                            info!("   Has old data");
                        }
                        info!("   New data: {} columns", new_data.len());
                    }
                    EventType::Delete {
                        schema,
                        table,
                        relation_oid,
                        old_data,
                        ..
                    } => {
                        info!(
                            "DELETE on {}.{} (OID: {}, LSN: {})",
                            schema, table, relation_oid, event.lsn
                        );
                        info!("   Deleted data: {} columns", old_data.len());
                    }
                    EventType::Begin {
                        transaction_id,
                        commit_timestamp,
                    } => {
                        info!(
                            "BEGIN TRANSACTION (XID: {}, Time: {})",
                            transaction_id, commit_timestamp
                        );
                    }
                    EventType::Commit { commit_timestamp } => {
                        info!(
                            "COMMIT TRANSACTION (LSN: {}, Time: {})",
                            event.lsn, commit_timestamp
                        );
                    }
                    EventType::Relation => {
                        info!("RELATION (LSN: {})", event.lsn);
                    }
                    _ => {
                        info!("Event: {:?} (LSN: {})", event.event_type, event.lsn);
                    }
                }

                // Update LSN feedback after processing
                stream
                    .shared_lsn_feedback
                    .update_applied_lsn(event.lsn.value());

                if event_count % 10 == 0 {
                    info!("Processed {} events so far", event_count);
                }
            }
            Ok(None) => {
                // No event available within timeout, continue polling
            }
            Err(e) => {
                error!("Error polling for event: {}", e);
                break;
            }
        }

        // Optional: Add a small delay to prevent tight polling
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    info!("Polling stopped. Total events processed: {}", event_count);
    info!("Example completed successfully");
    Ok(())
}

/// Mask password in connection string for logging
fn mask_password(conn_str: &str) -> String {
    if let Some(start) = conn_str.find("://") {
        if let Some(at) = conn_str[start + 3..].find('@') {
            let before = &conn_str[..start + 3];
            let after = &conn_str[start + 3 + at..];
            if let Some(colon) = conn_str[start + 3..start + 3 + at].rfind(':') {
                let user = &conn_str[start + 3..start + 3 + colon];
                return format!("{}{}:****{}", before, user, after);
            }
        }
    }
    conn_str.to_string()
}
