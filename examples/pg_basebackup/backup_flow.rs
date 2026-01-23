use crate::tar::{rewrite_tablespace_symlinks, TarArchiveStream};
use pg_walstream::PgReplicationConnection;
use std::fs::File;
use std::future::Future;
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

#[derive(Debug)]
enum StreamAction {
    Data(Vec<u8>),
    Idle,
    Cancel,
    Completed,
}

#[derive(Debug)]
enum BackupStreamMode {
    None,
    Archive {
        name: String,
        extractor: TarArchiveStream,
    },
    Manifest {
        file: File,
        bytes_written: u64,
    },
}

#[derive(Debug, Default)]
struct BackupTotals {
    bytes: u64,
    files: usize,
    had_incomplete_archive: bool,
}

impl BackupTotals {
    fn record_archive(
        &mut self,
        name: &str,
        extractor: &mut TarArchiveStream,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let completed = extractor.finish()?;
        self.bytes += extractor.total_bytes();
        self.files += extractor.file_count();

        if !completed || !extractor.is_complete() {
            warn!("Archive {} did not finish cleanly", name);
            self.had_incomplete_archive = true;
        }

        Ok(())
    }
}

fn finish_stream_mode(
    mode: BackupStreamMode,
    totals: &mut BackupTotals,
) -> Result<(), Box<dyn std::error::Error>> {
    match mode {
        BackupStreamMode::Archive {
            name,
            mut extractor,
        } => {
            totals.record_archive(&name, &mut extractor)?;
        }
        BackupStreamMode::Manifest { mut file, .. } => {
            file.flush()?;
        }
        BackupStreamMode::None => {}
    }

    Ok(())
}

fn log_idle_if_needed(last_data_at: Instant, last_idle_log_at: &mut Instant, interval: Duration) {
    if last_data_at.elapsed() >= interval && last_idle_log_at.elapsed() >= interval {
        info!(
            "No data received for {}s; backup still running (idle can occur while waiting for checkpoint or WAL)",
            last_data_at.elapsed().as_secs()
        );
        info!("Hint: check pg_stat_progress_basebackup on the source server for current phase");
        *last_idle_log_at = Instant::now();
    }
}

fn ensure_empty_output_dir(output_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    if output_dir.exists() {
        let mut entries = output_dir.read_dir()?;
        if entries.next().is_some() {
            return Err(format!(
                "Backup output directory {} is not empty; remove it or choose a new path",
                output_dir.display()
            )
            .into());
        }
    }

    std::fs::create_dir_all(output_dir)?;
    info!("Writing backup to: {}", output_dir.display());
    Ok(())
}

async fn read_stream_action(
    conn: &mut PgReplicationConnection,
    cancel_token: &CancellationToken,
    shutdown: &mut (impl Future<Output = Result<(), std::io::Error>> + Unpin),
    last_data_at: Instant,
    last_idle_log_at: &mut Instant,
    idle_log_interval: Duration,
) -> Result<StreamAction, Box<dyn std::error::Error>> {
    let read_fut = conn.get_copy_data_async(cancel_token);
    tokio::pin!(read_fut);

    loop {
        let idle_sleep = tokio::time::sleep(idle_log_interval);
        tokio::pin!(idle_sleep);

        let action = tokio::select! {
            _ = &mut *shutdown => {
                StreamAction::Cancel
            }
            _ = &mut idle_sleep => {
                StreamAction::Idle
            }
            result = &mut read_fut => {
                match result {
                    Ok(data) => StreamAction::Data(data),
                    Err(e) => {
                        let err_msg = e.to_string();
                        if err_msg.contains("COPY ended") {
                            info!("Backup stream completed");
                            StreamAction::Completed
                        } else if err_msg.contains("cancelled") && cancel_token.is_cancelled() {
                            StreamAction::Cancel
                        } else {
                            return Err(Box::new(e));
                        }
                    }
                }
            }
        };

        if matches!(action, StreamAction::Idle) {
            log_idle_if_needed(last_data_at, last_idle_log_at, idle_log_interval);
            continue;
        }

        return Ok(action);
    }
}

fn start_archive_mode(
    output_dir: &PathBuf,
    archive_name: String,
    spclocation: String,
    mode: &mut BackupStreamMode,
    totals: &mut BackupTotals,
) -> Result<(), Box<dyn std::error::Error>> {
    let previous_mode = std::mem::replace(mode, BackupStreamMode::None);
    finish_stream_mode(previous_mode, totals)?;

    let archive_dir = archive_output_dir(output_dir, &archive_name);
    std::fs::create_dir_all(&archive_dir)?;
    if spclocation.is_empty() {
        info!(
            "Starting archive: {} -> {}",
            archive_name,
            archive_dir.display()
        );
    } else {
        info!(
            "Starting tablespace archive: {} (source: {}) -> {}",
            archive_name,
            spclocation,
            archive_dir.display()
        );
    }

    let extractor = TarArchiveStream::new(archive_dir, &archive_name)?;
    *mode = BackupStreamMode::Archive {
        name: archive_name,
        extractor,
    };
    Ok(())
}

fn start_manifest_mode(
    output_dir: &PathBuf,
    mode: &mut BackupStreamMode,
    totals: &mut BackupTotals,
) -> Result<u64, Box<dyn std::error::Error>> {
    let previous_mode = std::mem::replace(mode, BackupStreamMode::None);
    finish_stream_mode(previous_mode, totals)?;

    let manifest_path = output_dir.join("backup_manifest");
    let file = File::create(&manifest_path)?;
    info!("Writing backup manifest: {}", manifest_path.display());
    *mode = BackupStreamMode::Manifest {
        file,
        bytes_written: 0,
    };
    Ok(0)
}

fn handle_data_chunk(
    payload: &[u8],
    output_dir: &PathBuf,
    mode: &mut BackupStreamMode,
    totals: &mut BackupTotals,
) -> Result<(), Box<dyn std::error::Error>> {
    match mode {
        BackupStreamMode::Archive { name, extractor } => {
            let completed = extractor.push(payload)?;
            if completed {
                totals.record_archive(name, extractor)?;
                *mode = BackupStreamMode::None;
            }
        }
        BackupStreamMode::Manifest {
            file,
            bytes_written,
        } => {
            file.write_all(payload)?;
            *bytes_written += payload.len() as u64;
        }
        BackupStreamMode::None => {
            warn!("Received data before archive start; defaulting to base directory");
            let mut extractor = TarArchiveStream::new(output_dir.clone(), "base.tar")?;
            let completed = extractor.push(payload)?;
            if completed {
                totals.record_archive("base.tar", &mut extractor)?;
            } else {
                *mode = BackupStreamMode::Archive {
                    name: "base.tar".to_string(),
                    extractor,
                };
            }
        }
    }

    Ok(())
}

fn handle_progress_message(data: &[u8]) {
    if data.len() >= 9 {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&data[1..9]);
        let bytes_done = i64::from_be_bytes(buf);
        debug!("Progress update: {} bytes", bytes_done);
    } else {
        debug!("Progress update received");
    }
}

fn handle_stream_data(
    data: Vec<u8>,
    output_dir: &PathBuf,
    mode: &mut BackupStreamMode,
    totals: &mut BackupTotals,
    saw_archive: &mut bool,
    saw_manifest: &mut bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if data.is_empty() {
        return Ok(());
    }

    match data[0] as char {
        'n' => {
            if let Some((archive_name, spclocation)) = parse_new_archive_message(&data[1..]) {
                start_archive_mode(output_dir, archive_name, spclocation, mode, totals)?;
                *saw_archive = true;
            } else {
                warn!("Unexpected new-archive message received");
            }
        }
        'm' => {
            start_manifest_mode(output_dir, mode, totals)?;
            *saw_manifest = true;
        }
        'd' => {
            let payload = &data[1..];
            handle_data_chunk(payload, output_dir, mode, totals)?;
            *saw_archive = true;
        }
        'p' => {
            handle_progress_message(&data);
        }
        _ => {
            warn!(
                "Unknown message type: {} (0x{:02x})",
                data[0] as char, data[0]
            );
        }
    }

    Ok(())
}

fn parse_cstring(input: &[u8]) -> Option<(String, usize)> {
    let end = input.iter().position(|&b| b == 0)?;
    let value = String::from_utf8_lossy(&input[..end]).to_string();
    Some((value, end + 1))
}

fn parse_new_archive_message(payload: &[u8]) -> Option<(String, String)> {
    let (archive_name, consumed) = parse_cstring(payload)?;
    let (spclocation, _) = parse_cstring(&payload[consumed..]).unwrap_or((String::new(), 0));
    Some((archive_name, spclocation))
}

fn strip_archive_extension(name: &str) -> &str {
    for ext in [".tar", ".tar.gz", ".tar.zst", ".tar.lz4"] {
        if let Some(stripped) = name.strip_suffix(ext) {
            return stripped;
        }
    }
    name
}

fn archive_output_dir(base_dir: &PathBuf, archive_name: &str) -> PathBuf {
    let base_name = strip_archive_extension(archive_name);
    if base_name == "base" {
        base_dir.clone()
    } else {
        base_dir.join("tablespaces").join(base_name)
    }
}

/// Process base backup stream and write to disk
pub async fn process_backup_stream(
    conn: &mut PgReplicationConnection,
    output_dir: &PathBuf,
    cancel_token: &CancellationToken,
    expect_manifest: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    ensure_empty_output_dir(output_dir)?;

    let mut mode = BackupStreamMode::None;
    let mut last_data_at = Instant::now();
    let idle_log_interval = Duration::from_secs(30);
    let mut last_idle_log_at = Instant::now();
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    let mut was_cancelled = false;
    let mut saw_archive = false;
    let mut saw_manifest = false;
    let mut totals = BackupTotals::default();

    loop {
        // Read data from replication stream, or emit idle heartbeat logs
        let action = read_stream_action(
            conn,
            cancel_token,
            &mut shutdown,
            last_data_at,
            &mut last_idle_log_at,
            idle_log_interval,
        )
        .await?;

        match action {
            StreamAction::Cancel => {
                warn!("Received Ctrl-C, requesting graceful shutdown...");
                was_cancelled = true;
                cancel_token.cancel();
                break;
            }
            StreamAction::Completed => {
                break;
            }
            StreamAction::Idle => {
                continue;
            }
            StreamAction::Data(data) => {
                last_data_at = Instant::now();
                last_idle_log_at = Instant::now();
                handle_stream_data(
                    data,
                    output_dir,
                    &mut mode,
                    &mut totals,
                    &mut saw_archive,
                    &mut saw_manifest,
                )?;
            }
        }
    }

    let final_mode = std::mem::replace(&mut mode, BackupStreamMode::None);
    if let BackupStreamMode::Manifest { bytes_written, .. } = &final_mode {
        info!("Backup manifest size: {} bytes", bytes_written);
    }
    finish_stream_mode(final_mode, &mut totals)?;

    if was_cancelled {
        error!("Backup cancelled by user");
        return Err("Backup cancelled by user; output is incomplete and UNUSABLE".into());
    }

    if !saw_archive {
        error!("No archive data received");
        return Err("Backup stream ended without any archive data".into());
    }

    if totals.had_incomplete_archive {
        error!("Incomplete archive detected");
        return Err("Backup FAILED: Archive data is incomplete and UNUSABLE for restore".into());
    }

    if expect_manifest && !saw_manifest {
        error!("Missing backup manifest");
        return Err("Backup manifest was expected but not received".into());
    }

    info!("Verifying backup integrity...");
    verify_backup(output_dir, expect_manifest)?;
    info!("Backup integrity verified");

    rewrite_tablespace_symlinks(output_dir)?;
    info!("Tablespace symlinks normalized for restore");

    // Create recovery.signal for PostgreSQL 16 restore
    create_recovery_signal(output_dir)?;

    info!("═══════════════════════════════════════════");
    info!("Backup completed successfully!");
    info!("  Files: {}", totals.files);
    info!(
        "  Total size: {:.2} MB",
        totals.bytes as f64 / 1024.0 / 1024.0
    );
    info!("  Output: {}", output_dir.display());
    info!("═══════════════════════════════════════════");

    Ok(())
}

fn verify_backup(
    output_dir: &PathBuf,
    expect_manifest: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let pg_control = output_dir.join("global/pg_control");
    let backup_label = output_dir.join("backup_label");
    let backup_manifest = output_dir.join("backup_manifest");

    if !pg_control.exists() {
        return Err(format!(
            "Backup FAILED: missing critical file {}",
            pg_control.display()
        )
        .into());
    }

    if !backup_label.exists() {
        return Err(format!(
            "Backup FAILED: missing critical file {}",
            backup_label.display()
        )
        .into());
    }

    if expect_manifest && !backup_manifest.exists() {
        return Err(format!("Backup FAILED: missing {}", backup_manifest.display()).into());
    }

    Ok(())
}

/// Create recovery.signal file for PostgreSQL 12+ restore
fn create_recovery_signal(output_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let recovery_signal = output_dir.join("recovery.signal");
    std::fs::write(&recovery_signal, "")?;
    info!("Created recovery.signal for PostgreSQL 12+ restore");
    Ok(())
}
