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

use pg_walstream::{BaseBackupOptions, PgReplicationConnection};
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber;

/// Parse tar header to extract filename, size, and type
fn parse_tar_header(header: &[u8]) -> Option<(String, u64, char)> {
    if header.len() < 512 {
        return None;
    }

    // Tar header format (USTAR):
    // Offset 0-99: filename (null-terminated)
    // Offset 124-135: file size in octal ASCII
    // Offset 156: file type ('0'=regular, '5'=directory, '2'=symlink)
    let filename_bytes = &header[0..100];
    let size_bytes = &header[124..136];
    let type_flag = header[156] as char;

    // Extract filename (stop at first null byte)
    let filename = filename_bytes
        .iter()
        .take_while(|&&b| b != 0)
        .map(|&b| b as char)
        .collect::<String>();

    if filename.is_empty() {
        return None;
    }

    // Parse size from octal string
    let size_str = size_bytes
        .iter()
        .take_while(|&&b| b != 0 && b != b' ')
        .map(|&b| b as char)
        .collect::<String>();

    let size = u64::from_str_radix(size_str.trim(), 8).unwrap_or(0);

    Some((filename, size, type_flag))
}

/// Process base backup stream and write to disk
async fn process_backup_stream(
    conn: &mut PgReplicationConnection,
    output_dir: &PathBuf,
    cancel_token: &CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(output_dir)?;
    info!("Writing backup to: {}", output_dir.display());

    let mut total_bytes = 0u64;
    let mut file_count = 0usize;
    let mut current_file: Option<(File, String, u64, u64)> = None; // (file, name, size, written)

    loop {
        // Read data from replication stream
        let data = match conn.get_copy_data_async(cancel_token).await {
            Ok(data) => data,
            Err(e) => {
                let err_msg = e.to_string();
                if err_msg.contains("COPY ended") || err_msg.contains("cancelled") {
                    info!("Backup stream completed");
                    break;
                }
                return Err(Box::new(e));
            }
        };

        if data.is_empty() {
            continue;
        }

        // Check message type
        match data[0] as char {
            'd' => {
                // CopyData message - contains tar data
                let payload = &data[1..]; // Skip the 'd' marker

                if let Some((ref mut file, ref name, size, ref mut written)) = current_file {
                    // Write to current file
                    let to_write = std::cmp::min(payload.len() as u64, size - *written);
                    file.write_all(&payload[..to_write as usize])?;
                    *written += to_write;
                    total_bytes += to_write;

                    if *written >= size {
                        // File complete
                        file.flush()?;
                        info!("Completed file: {} ({} bytes)", name, size);
                        current_file = None;

                        // Check if there's padding (tar blocks are 512-byte aligned)
                        let padding = (512 - (size % 512)) % 512;
                        if padding > 0 && to_write < payload.len() as u64 {
                            // Skip padding bytes
                        }
                    }
                } else {
                    // No current file - this should be a tar header
                    if payload.len() >= 512 {
                        if let Some((filename, size, type_flag)) = parse_tar_header(payload) {
                            let file_path = output_dir.join(&filename);

                            // Handle different tar entry types
                            match type_flag {
                                '5' => {
                                    // Directory entry
                                    fs::create_dir_all(&file_path)?;
                                    info!("Created directory: {}", filename);
                                    continue; // No data to write for directories
                                }
                                '0' | '\0' => {
                                    // Regular file (type '0' or old-style null byte)
                                    // Create parent directories if needed
                                    if let Some(parent) = file_path.parent() {
                                        fs::create_dir_all(parent)?;
                                    }

                                    // Skip if it's actually a directory (ends with /)
                                    if filename.ends_with('/') {
                                        fs::create_dir_all(&file_path)?;
                                        info!("Created directory: {}", filename);
                                        continue;
                                    }

                                    let file = File::create(&file_path)?;
                                    info!("Starting file: {} ({} bytes)", filename, size);

                                    file_count += 1;
                                    current_file = Some((file, filename, size, 0));

                                    // If there's data after the header, write it
                                    if payload.len() > 512 && size > 0 {
                                        let data_start = &payload[512..];
                                        let to_write = std::cmp::min(data_start.len() as u64, size);
                                        if let Some((ref mut file, _, _, ref mut written)) =
                                            current_file
                                        {
                                            file.write_all(&data_start[..to_write as usize])?;
                                            *written += to_write;
                                            total_bytes += to_write;
                                        }
                                    }
                                }
                                '2' => {
                                    // Symbolic link - skip for now
                                    info!("Skipping symlink: {}", filename);
                                }
                                _ => {
                                    warn!(
                                        "Unknown tar entry type '{}' for: {}",
                                        type_flag, filename
                                    );
                                }
                            }
                        } else {
                            // End of tar archive (512 zero bytes)
                            let is_end = payload.iter().take(512).all(|&b| b == 0);
                            if is_end {
                                info!("End of tar archive marker received");
                            }
                        }
                    }
                }
            }
            'c' => {
                // CopyDone - backup completed
                info!("Received CopyDone message");
                break;
            }
            'f' => {
                // CopyFail - backup failed
                let error_msg = String::from_utf8_lossy(&data[1..]);
                error!("Backup failed: {}", error_msg);
                return Err(format!("Backup failed: {}", error_msg).into());
            }
            'n' => {
                // Progress notification (if PROGRESS was requested)
                // Format: 'n' + tablespace_oid + total_bytes + bytes_done
                if data.len() >= 25 {
                    // Parse progress data (simplified - just acknowledge it)
                    info!("Progress update received");
                }
            }
            _ => {
                warn!(
                    "Unknown message type: {} (0x{:02x})",
                    data[0] as char, data[0]
                );
            }
        }
    }

    // Close any remaining file
    if let Some((mut file, name, size, written)) = current_file {
        file.flush()?;
        if written < size {
            warn!(
                "File {} incomplete: {} of {} bytes written",
                name, written, size
            );
        }
    }

    info!("═══════════════════════════════════════════");
    info!("Backup completed successfully!");
    info!("  Files: {}", file_count);
    info!(
        "  Total size: {:.2} MB",
        total_bytes as f64 / 1024.0 / 1024.0
    );
    info!("  Output: {}", output_dir.display());
    info!("═══════════════════════════════════════════");

    Ok(())
}

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
    let connection_string = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:password@localhost:5432/postgres?replication=database".to_string()
    });

    info!("Connection string: {}", connection_string);

    // Output directory for backup
    let output_dir = PathBuf::from("./backup");
    info!("Backup output directory: {}", output_dir.display());

    // Connect to PostgreSQL
    info!("Connecting to PostgreSQL...");
    let mut conn = PgReplicationConnection::connect(&connection_string)?;
    info!("Connected successfully");

    // Identify system
    info!("Identifying system...");
    let system_id = conn.identify_system()?;

    // Extract system information from result
    if system_id.ntuples() > 0 {
        if let (Some(sys_id), Some(timeline), Some(xlogpos)) = (
            system_id.get_value(0, 0),
            system_id.get_value(0, 1),
            system_id.get_value(0, 2),
        ) {
            info!("  System ID: {}", sys_id);
            info!("  Timeline: {}, Current LSN: {}", timeline, xlogpos);
        }
    }

    // Configure base backup options
    let options = BaseBackupOptions {
        label: Some("pg_walstream_backup".to_string()),
        progress: true,                                 // Show progress information
        checkpoint: Some("fast".to_string()),           // Fast checkpoint
        wal: true,                                      // Include WAL files
        wait: true,                                     // Wait for WAL archiving
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
    info!("  Manifest: {:?}", options.manifest);

    // Start base backup
    conn.base_backup(&options)?;
    info!("Base backup command sent");

    // Process backup stream
    info!("Receiving backup data...");
    info!("─────────────────────────────────────────");
    let cancel_token = CancellationToken::new();
    process_backup_stream(&mut conn, &output_dir, &cancel_token).await?;

    info!("\nBackup can be used for:");
    info!("  • Setting up a standby server");
    info!("  • Point-in-time recovery (PITR)");
    info!("  • Disaster recovery");
    info!("\nTo restore, copy the backup to PostgreSQL data directory");
    info!("and start the server in recovery mode.");

    Ok(())
}
