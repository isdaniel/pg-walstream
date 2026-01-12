//! Basic PostgreSQL WAL streaming example with futures::Stream
//!
//! This example demonstrates how to:
//! - Connect to PostgreSQL with replication enabled
//! - Create a replication slot
//! - Wrap EventStream with futures::stream::unfold for Stream trait compatibility
//! - Use stream combinators (filter, take_while, etc.)
//! - Process insert, update, and delete events
//! - Handle LSN feedback for WAL management
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
//! cargo run --example basic_streaming
//! ```

use futures::stream::{self, StreamExt};
use pg_walstream::{
    CancellationToken, LogicalReplicationStream, ReplicationStreamConfig, RetryConfig,
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

    info!("Starting PostgreSQL WAL streaming example");

    // Get connection string from environment or use default
    let connection_string = env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:password@localhost:5432/postgres?replication=database".to_string()
    });

    info!("Connection string: {}", mask_password(&connection_string));

    // Configure the replication stream
    let config = ReplicationStreamConfig::new(
        "example_slot".to_string(),   // Replication slot name
        "my_publication".to_string(), // Publication name (must exist)
        2,                            // Protocol version (2 supports streaming)
        true,                         // Enable streaming of large transactions
        Duration::from_secs(10),      // Send feedback every 10 seconds
        Duration::from_secs(30),      // Connection timeout
        Duration::from_secs(60),      // Health check interval
        RetryConfig::default(),       // Use default retry strategy
    );

    info!("Creating replication stream...");

    // Create and initialize the stream
    let mut stream = LogicalReplicationStream::new(&connection_string, config).await?;

    info!("Stream created successfully");

    // Start replication from the latest position (None = latest)
    stream.start(None).await?;

    info!("Replication started successfully");
    info!("Listening for changes... (Press Ctrl+C to stop)");
    info!("You can now make changes to your database tables to see events");

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
    // This allows us to use stream combinators!
    // Use Box::pin to pin the stream on the heap so we can reuse it
    let mut pg_stream = Box::pin(stream::unfold(
        event_stream,
        |mut event_stream| async move {
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
        },
    ));

    // Now we can use stream combinators!
    info!("Using futures::Stream combinators for advanced processing");

    while let Some(result) = pg_stream.as_mut().next().await {
        match result {
            Ok(event) => {
                info!("Received event: {:?}", event);
            }
            Err(e) => {
                error!("Error: {}", e);
                break;
            }
        }
    }

    info!("Graceful shutdown complete");

    Ok(())
}

/// Mask password in connection string for logging
fn mask_password(conn_str: &str) -> String {
    if let Some(proto_end) = conn_str.find("://") {
        let proto = &conn_str[..proto_end + 3]; // e.g., "postgresql://"
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
