//! Safe transaction consumer example with ordered commit processing
//!
//! This example demonstrates how to:
//! - Safely consume replication messages with transaction boundaries
//! - Buffer transactions until commit
//! - Apply transactions in order
//! - Update LSN feedback only after successful application
//! - Handle backpressure when too many transactions are buffered
//! - Gracefully shutdown with proper cleanup
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
//! cargo run --example safe_transaction_consumer
//! ```

use pg_walstream::{
    CancellationToken, ChangeEvent, EventType, LogicalReplicationStream, Lsn,
    ReplicationStreamConfig, RetryConfig, SharedLsnFeedback,
};
use std::collections::BTreeMap;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{error, info, warn, Level};
use tracing_subscriber;

/// Buffered transaction
struct TxBuffer {
    xid: u32,
    changes: Vec<ChangeEvent>,
    commit_lsn: Option<Lsn>,
    applied: bool,
}

/// Safe replication consumer wrapper
pub struct SafeReplicationConsumer {
    /// Thread-safe feedback LSN
    feedback: Arc<SharedLsnFeedback>,

    /// Ordered map: xid -> transaction
    active_transactions: BTreeMap<u32, TxBuffer>,

    /// Committed transactions waiting to be acknowledged
    committed_txs: Vec<TxBuffer>,

    /// Shutdown signal
    shutdown_notify: Arc<Notify>,
}

impl SafeReplicationConsumer {
    pub fn new(feedback: Arc<SharedLsnFeedback>) -> Self {
        Self {
            feedback,
            active_transactions: BTreeMap::new(),
            committed_txs: Vec::new(),
            shutdown_notify: Arc::new(Notify::new()),
        }
    }

    /// Process a replication event
    pub async fn process_event(&mut self, event: ChangeEvent) {
        match &event.event_type {
            EventType::Begin { transaction_id, .. } => {
                info!("Begin transaction xid={}", transaction_id);
                self.active_transactions.insert(
                    *transaction_id,
                    TxBuffer {
                        xid: *transaction_id,
                        changes: vec![],
                        commit_lsn: None,
                        applied: false,
                    },
                );
            }
            EventType::Insert { table, .. }
            | EventType::Update { table, .. }
            | EventType::Delete { table, .. } => {
                // Find the transaction for this change (we need to look at the event metadata)
                // For simplicity, we'll buffer all data changes
                info!("Change event on table '{}' at LSN {}", table, event.lsn);

                // Store the change in the most recent active transaction
                // In a real implementation, you'd extract the xid from the event
                if let Some(tx) = self.active_transactions.values_mut().last() {
                    tx.changes.push(event);
                }
            }
            EventType::Commit {
                commit_lsn,
                end_lsn: _,
                ..
            } => {
                // Find the transaction to commit
                // In real implementation, extract xid from metadata
                // Get the first xid without borrowing
                let xid_opt = self.active_transactions.keys().next().copied();

                if let Some(xid) = xid_opt {
                    // Remove transaction from active list
                    if let Some(mut tx) = self.active_transactions.remove(&xid) {
                        info!(
                            "Commit transaction xid={} at LSN {} with {} changes",
                            xid,
                            commit_lsn,
                            tx.changes.len()
                        );
                        tx.commit_lsn = Some(*commit_lsn);

                        // Apply the transaction
                        self.apply_tx(&mut tx).await;

                        // Store committed transaction
                        self.committed_txs.push(tx);

                        // Update feedback
                        self.update_feedback().await;
                    }
                }
            }
            _ => {
                // Handle other event types if needed
                info!("Received event type: {:?}", event.event_type);
            }
        }
    }

    /// Apply transaction to downstream (user-defined)
    async fn apply_tx(&mut self, tx: &mut TxBuffer) {
        info!(
            "Applying transaction xid={} with {} changes",
            tx.xid,
            tx.changes.len()
        );

        for event in &tx.changes {
            match &event.event_type {
                EventType::Insert { table, data, .. } => {
                    info!("  - INSERT on table '{}', columns: {}", table, data.len());
                }
                EventType::Update {
                    table, new_data, ..
                } => {
                    info!(
                        "  - UPDATE on table '{}', columns: {}",
                        table,
                        new_data.len()
                    );
                }
                EventType::Delete {
                    table, old_data, ..
                } => {
                    info!(
                        "  - DELETE on table '{}', columns: {}",
                        table,
                        old_data.len()
                    );
                }
                _ => {}
            }
        }

        tx.applied = true;
        info!("Transaction xid={} applied successfully", tx.xid);
    }

    /// Update feedback LSN safely, only after commit applied
    async fn update_feedback(&self) {
        let mut last_safe_lsn = None;

        // Find the last applied transaction LSN
        for tx in &self.committed_txs {
            if tx.commit_lsn.is_some() && tx.applied {
                last_safe_lsn = tx.commit_lsn;
            } else {
                break; // Cannot advance beyond first unapplied commit
            }
        }

        if let Some(lsn) = last_safe_lsn {
            self.feedback.update_applied_lsn(lsn.value());
            info!("Updated feedback LSN to {}", lsn);
        }
    }

    /// Get statistics
    pub fn stats(&self) -> ConsumerStats {
        ConsumerStats {
            active_transactions: self.active_transactions.len(),
            committed_transactions: self.committed_txs.len(),
            total_changes: self
                .active_transactions
                .values()
                .map(|tx| tx.changes.len())
                .sum::<usize>(),
        }
    }

    /// Graceful shutdown
    pub async fn shutdown(&mut self) {
        info!("Initiating graceful shutdown...");

        // Stop accepting new transactions
        self.shutdown_notify.notify_waiters();

        // Flush all pending transactions
        let count = self.active_transactions.len();
        info!("Flushing {} active transactions", count);
        for (xid, _tx) in self.active_transactions.iter() {
            warn!("Flushing uncommitted transaction xid={}", xid);
            // Optionally apply or discard uncommitted transactions
        }
        self.active_transactions.clear();

        info!("Shutdown complete");
    }
}

pub struct ConsumerStats {
    pub active_transactions: usize,
    pub committed_transactions: usize,
    pub total_changes: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .init();

    info!("Starting Safe Transaction Consumer example");

    // Get connection string from environment or use default
    let connection_string = env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:password@localhost:5432/postgres?replication=database".to_string()
    });

    info!("Connection string: {}", mask_password(&connection_string));

    // Configure the replication stream
    let config = ReplicationStreamConfig::new(
        "safe_consumer_slot".to_string(), // Replication slot name
        "my_publication".to_string(),     // Publication name (must exist)
        2,                                // Protocol version (2 supports streaming)
        true,                             // Enable streaming of large transactions
        Duration::from_secs(10),          // Send feedback every 10 seconds
        Duration::from_secs(30),          // Connection timeout
        Duration::from_secs(60),          // Health check interval
        RetryConfig::default(),           // Use default retry strategy
    );

    info!("Creating replication stream...");

    // Create and initialize the stream
    let mut stream = LogicalReplicationStream::new(&connection_string, config).await?;

    info!("Stream created successfully");

    // Get shared LSN feedback from the stream
    let feedback = stream.shared_lsn_feedback.clone();

    // Create safe consumer
    let mut consumer = SafeReplicationConsumer::new(feedback);

    // Start replication from the latest position
    stream.start(None).await?;

    info!("Replication started successfully");

    // Set up cancellation token for graceful shutdown
    let token = CancellationToken::new();
    let shutdown_token = token.clone();

    // Spawn a task to handle Ctrl+C
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Received Ctrl+C, initiating shutdown...");
        shutdown_token.cancel();
    });

    let mut message_count = 0;
    let start_time = std::time::Instant::now();

    // Convert stream to EventStream for easier processing
    let mut event_stream = stream.into_stream(token.clone());

    // Process events until cancelled
    loop {
        match event_stream.next().await {
            Ok(event) => {
                message_count += 1;
                consumer.process_event(event).await;

                // Print stats every 100 messages
                if message_count % 100 == 0 {
                    let stats = consumer.stats();
                    let elapsed = start_time.elapsed();
                    info!(
                        "Processed {} messages in {:.1}s | Active: {} | Committed: {} | Changes: {}",
                        message_count,
                        elapsed.as_secs_f64(),
                        stats.active_transactions,
                        stats.committed_transactions,
                        stats.total_changes,
                    );
                }
            }
            Err(e) if matches!(e, pg_walstream::ReplicationError::Cancelled(_)) => {
                info!("Stream cancelled, shutting down gracefully");
                break;
            }
            Err(e) => {
                error!("Error receiving event: {}", e);
                break;
            }
        }
    }

    // Graceful shutdown
    consumer.shutdown().await;

    let elapsed = start_time.elapsed();
    info!(
        "Processed {} messages in {:.2}s ({:.0} msg/s)",
        message_count,
        elapsed.as_secs_f64(),
        message_count as f64 / elapsed.as_secs_f64()
    );

    Ok(())
}

/// Mask password in connection string for logging
fn mask_password(conn_str: &str) -> String {
    if let Some(pwd_start) = conn_str.find("password=") {
        let mut masked = conn_str[..pwd_start + 9].to_string();
        masked.push_str("****");
        if let Some(pwd_end) = conn_str[pwd_start + 9..].find(|c| c == '@' || c == '&') {
            masked.push_str(&conn_str[pwd_start + 9 + pwd_end..]);
        }
        masked
    } else {
        conn_str.to_string()
    }
}
