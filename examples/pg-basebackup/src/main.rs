//! PostgreSQL Base Backup Example
//!
//! This example demonstrates how to create a base backup (similar to pg_basebackup utility)
//! using the PostgreSQL replication protocol. A base backup is a full physical copy of the
//! database cluster that can be used for:
//! - Setting up physical replication standby servers
//! - Point-in-time recovery (PITR)
//! - Disaster recovery
//! - Database migrations
//!
//! ## Prerequisites
//!
//! 1. PostgreSQL 14+ with replication configured:
//!    ```sql
//!    -- In postgresql.conf:
//!    wal_level = replica
//!    max_wal_senders = 10
//!    max_replication_slots = 10
//!    
//!    -- In pg_hba.conf:
//!    host replication postgres 127.0.0.1/32 trust
//!    ```
//!
//! 2. PostgreSQL server running on localhost:5432
//!
//! ## Usage
//!
//! ```bash
//! # Basic backup to ./backup directory
//! cargo run --example pg_basebackup
//!
//! # Or to a custom backup directory:
//! BACKUP_DIR="/tmp/pg_basebackup" cargo run --example pg_basebackup
//!
//! # Or with custom connection string:
//! DATABASE_URL="postgresql://postgres:password@localhost:5432/postgres?replication=database" \
//!   cargo run --example pg_basebackup
//! ```
//!
//! ## What This Example Does
//!
//! 1. Connects to PostgreSQL in replication mode
//! 2. Initiates a BASE_BACKUP command with options:
//!    - PROGRESS: Shows backup progress
//!    - CHECKPOINT: Fast checkpoint for quicker start
//!    - WAL: Includes WAL files in backup
//!    - MANIFEST: Creates backup manifest for verification
//! 3. Receives backup data as a stream of tar archives
//! 4. Writes the backup data to disk
//! 5. Verifies the backup was successful
//!
//! ## Output Structure
//!
//! The backup creates a directory structure like:
//! ```
//! backup/
//! ├── base.tar           # Main database cluster data
//! ├── pg_wal.tar         # WAL files (if WAL option enabled)
//! ├── backup_manifest    # Backup verification manifest
//! └── tablespaces/       # Additional tablespace tar files (if any)
//! ```

mod backup_flow;
mod tar;

use backup_flow::process_backup_stream;
use pg_walstream::{BaseBackupOptions, PgReplicationConnection};
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .with_thread_ids(false)
        .init();

    info!("═══════════════════════════════════════════");
    info!("PostgreSQL Base Backup Example");
    info!("═══════════════════════════════════════════");

    // Get connection string from environment or use default
    let mut connection_string = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:password@localhost:5432/postgres?replication=database".to_string()
    });

    // Add TCP keepalive parameters if not already present
    if !connection_string.contains("keepalives") {
        let separator = if connection_string.contains('?') {
            "&"
        } else {
            "?"
        };
        connection_string.push_str(&format!(
            "{}keepalives=1&keepalives_idle=30&keepalives_interval=10&keepalives_count=3",
            separator
        ));
        info!("Added TCP keepalive parameters to connection");
    }

    info!(
        "Connection string: {}",
        connection_string.replace(
            |c: char| c.is_ascii_digit() && connection_string.contains("password"),
            "*"
        )
    );

    // Output directory for backup
    let output_dir =
        PathBuf::from(std::env::var("BACKUP_DIR").unwrap_or_else(|_| "./backup".to_string()));
    info!("Backup output directory: {}", output_dir.display());

    // Connect to PostgreSQL
    info!("Connecting to PostgreSQL...");
    let mut conn = PgReplicationConnection::connect(&connection_string)?;
    info!("Connected successfully");

    // Identify system
    info!("Identifying system...");
    let _ = conn.identify_system()?;

    // Configure base backup options
    let options = BaseBackupOptions {
        label: Some("pg_walstream_backup".to_string()),
        progress: true,                                 // Show progress information
        checkpoint: Some("fast".to_string()),           // Fast checkpoint
        wal: true,                                      // Include WAL files for restore
        wait: false,                                    // Wait for WAL archiving
        manifest: Some("yes".to_string()),              // Generate manifest for verification
        manifest_checksums: Some("SHA256".to_string()), // Use SHA256 checksums
        verify_checksums: true,                         // Verify data checksums
        tablespace_map: true,                           // Include tablespace map
        ..Default::default()
    };

    info!("Starting base backup with options:");
    info!("  Label: {:?}", options.label);
    info!("  Progress: {}", options.progress);
    info!("  Checkpoint: {:?}", options.checkpoint);
    info!("  Include WAL: {}", options.wal);
    info!("  Wait for WAL archiving: {}", options.wait);
    info!("  Manifest: {:?}", options.manifest);

    // Start base backup
    conn.base_backup(&options)?;
    info!("Base backup command sent");

    // Process backup stream
    info!("Receiving backup data...");
    info!("─────────────────────────────────────────");
    let cancel_token = CancellationToken::new();
    let expect_manifest = !matches!(options.manifest.as_deref(), Some("no"));
    process_backup_stream(&mut conn, &output_dir, &cancel_token, expect_manifest).await?;

    info!("\nBackup can be used for:");
    info!("  • Setting up a standby server");
    info!("  • Point-in-time recovery (PITR)");
    info!("  • Disaster recovery");
    info!("\nTo restore, copy the backup to PostgreSQL data directory");
    info!("and start the server in recovery mode.");

    Ok(())
}
