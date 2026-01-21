use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::os::unix::fs::symlink;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tracing::{debug, error, info};

#[derive(Debug)]
struct TarHeader {
    filename: String,
    size: u64,
    type_flag: char,
    link_name: Option<String>,
}

/// Parse tar header to extract filename, size, type, and link name
fn parse_tar_header(header: &[u8]) -> Option<TarHeader> {
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
    let linkname_bytes = &header[157..257];
    let magic_bytes = &header[257..263];
    let prefix_bytes = &header[345..500];

    // Extract filename (stop at first null byte)
    let filename = String::from_utf8_lossy(
        &filename_bytes
            .iter()
            .take_while(|&&b| b != 0)
            .copied()
            .collect::<Vec<u8>>(),
    )
    .to_string();

    if filename.is_empty() {
        return None;
    }

    // Parse size from octal string
    let size_str = String::from_utf8_lossy(
        &size_bytes
            .iter()
            .take_while(|&&b| b != 0 && b != b' ')
            .copied()
            .collect::<Vec<u8>>(),
    )
    .to_string();

    let size = u64::from_str_radix(size_str.trim(), 8).unwrap_or(0);

    let link_name = String::from_utf8_lossy(
        &linkname_bytes
            .iter()
            .take_while(|&&b| b != 0)
            .copied()
            .collect::<Vec<u8>>(),
    )
    .to_string();

    let mut full_name = filename;
    if magic_bytes == b"ustar\0" || magic_bytes == b"ustar " {
        let prefix = String::from_utf8_lossy(
            &prefix_bytes
                .iter()
                .take_while(|&&b| b != 0)
                .copied()
                .collect::<Vec<u8>>(),
        )
        .to_string();

        if !prefix.is_empty() {
            full_name = format!("{}/{}", prefix, full_name);
        }
    }

    Some(TarHeader {
        filename: full_name,
        size,
        type_flag,
        link_name: if link_name.is_empty() {
            None
        } else {
            Some(link_name)
        },
    })
}

#[derive(Debug)]
struct TarFileState {
    file: BufWriter<File>,
    name: String,
    remaining: u64,
    padding: usize,
}

#[derive(Debug)]
pub struct TarStreamExtractor {
    output_dir: PathBuf,
    buffer: Vec<u8>,
    cursor: usize,
    current: Option<TarFileState>,
    skip_remaining: u64,
    skip_padding: usize,
    zero_block_count: u8,
    archive_finished: bool,
    incomplete: bool,
    total_bytes: u64,
    file_count: usize,
    started_at: Instant,
    last_log_at: Instant,
    last_logged_bytes: u64,
    log_interval: Duration,
}

impl TarStreamExtractor {
    pub fn new(output_dir: PathBuf) -> Self {
        let now = Instant::now();
        Self {
            output_dir,
            buffer: Vec::with_capacity(64 * 1024),
            cursor: 0,
            current: None,
            skip_remaining: 0,
            skip_padding: 0,
            zero_block_count: 0,
            archive_finished: false,
            incomplete: false,
            total_bytes: 0,
            file_count: 0,
            started_at: now,
            last_log_at: now,
            last_logged_bytes: 0,
            log_interval: Duration::from_secs(5),
        }
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub fn file_count(&self) -> usize {
        self.file_count
    }

    pub fn push(&mut self, data: &[u8]) -> Result<bool, Box<dyn std::error::Error>> {
        let was_finished = self.archive_finished;
        if !data.is_empty() {
            self.buffer.extend_from_slice(data);
        }
        self.process()?;
        self.log_progress_if_needed();
        self.compact_buffer();
        Ok(!was_finished && self.archive_finished)
    }

    pub fn finish(&mut self) -> Result<bool, Box<dyn std::error::Error>> {
        if let Some(mut current) = self.current.take() {
            current.file.flush()?;
            if current.remaining > 0 {
                error!(
                    "CRITICAL: File {} incomplete: {} bytes remaining",
                    current.name, current.remaining
                );
                self.incomplete = true;
            }
        }

        if self.skip_remaining > 0 {
            error!(
                "CRITICAL: Skipping data incomplete: {} bytes remaining",
                self.skip_remaining
            );
            self.incomplete = true;
        }

        if self.available() > 0 {
            error!(
                "CRITICAL: Unconsumed tar data remaining: {} bytes",
                self.available()
            );
            self.incomplete = true;
        }

        if !self.archive_finished {
            error!("CRITICAL: Archive ended without end-of-archive markers");
            self.incomplete = true;
        }

        self.log_progress(true);

        if self.incomplete {
            Err("Archive extraction incomplete - backup is corrupted".into())
        } else {
            Ok(true)
        }
    }

    pub fn is_complete(&self) -> bool {
        self.archive_finished
            && !self.incomplete
            && self.current.is_none()
            && self.skip_remaining == 0
    }

    fn process(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            if self.archive_finished {
                break;
            }

            if let Some(mut current) = self.current.take() {
                if self.available() == 0 {
                    self.current = Some(current);
                    break;
                }

                let to_write = std::cmp::min(self.available() as u64, current.remaining);
                if to_write > 0 {
                    let slice = self.take_bytes(to_write as usize);
                    current.file.write_all(slice)?;
                    current.remaining -= to_write;
                    self.total_bytes += to_write;
                }

                if current.remaining == 0 {
                    current.file.flush()?;
                    debug!("Completed file: {}", current.name);

                    if current.padding > 0 {
                        if self.available() < current.padding {
                            self.current = Some(current);
                            break;
                        }
                        self.take_bytes(current.padding);
                    }
                } else {
                    self.current = Some(current);
                    break;
                }

                continue;
            }

            if self.skip_remaining > 0 {
                let to_skip = std::cmp::min(self.available() as u64, self.skip_remaining);
                if to_skip > 0 {
                    self.take_bytes(to_skip as usize);
                    self.skip_remaining -= to_skip;
                }

                if self.skip_remaining == 0 {
                    if self.skip_padding > 0 {
                        if self.available() < self.skip_padding {
                            break;
                        }
                        self.take_bytes(self.skip_padding);
                    }
                    self.skip_padding = 0;
                } else {
                    break;
                }

                continue;
            }

            if self.available() < 512 {
                break;
            }

            let header_block = self.peek_bytes(512);
            if header_block.iter().all(|&b| b == 0) {
                self.take_bytes(512);
                self.zero_block_count = self.zero_block_count.saturating_add(1);
                if self.zero_block_count >= 2 {
                    self.archive_finished = true;
                }
                continue;
            }

            self.zero_block_count = 0;
            let header = self.take_bytes(512).to_vec();
            if let Some(tar_header) = parse_tar_header(&header) {
                let TarHeader {
                    filename,
                    size,
                    type_flag,
                    link_name,
                } = tar_header;
                let file_path = self.output_dir.join(&filename);
                let padding = (512 - (size % 512) as usize) % 512;

                match type_flag {
                    '5' => {
                        fs::create_dir_all(&file_path)?;
                        debug!("Created directory: {}", filename);
                    }
                    '0' | '\0' => {
                        if let Some(parent) = file_path.parent() {
                            fs::create_dir_all(parent)?;
                        }

                        if filename.ends_with('/') {
                            fs::create_dir_all(&file_path)?;
                            debug!("Created directory: {}", filename);
                            continue;
                        }

                        let file = BufWriter::with_capacity(1024 * 1024, File::create(&file_path)?);
                        debug!("Starting file: {} ({} bytes)", filename, size);
                        self.file_count += 1;

                        if size == 0 {
                            // Empty file, nothing to read
                            continue;
                        }

                        self.current = Some(TarFileState {
                            file,
                            name: filename,
                            remaining: size,
                            padding,
                        });
                    }
                    '2' => {
                        if let Some(parent) = file_path.parent() {
                            fs::create_dir_all(parent)?;
                        }

                        if let Some(link_target) = link_name {
                            if file_path.exists() {
                                fs::remove_file(&file_path)?;
                            }
                            symlink(&link_target, &file_path)?;
                            debug!("Created symlink: {} -> {}", filename, link_target);
                        } else {
                            debug!("Skipping symlink without target: {}", filename);
                        }

                        if size > 0 {
                            self.skip_remaining = size;
                            self.skip_padding = padding;
                        }
                    }
                    _ => {
                        debug!("Unknown tar entry type '{}' for: {}", type_flag, filename);
                        if size > 0 {
                            self.skip_remaining = size;
                            self.skip_padding = padding;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn available(&self) -> usize {
        self.buffer.len().saturating_sub(self.cursor)
    }

    fn peek_bytes(&self, len: usize) -> &[u8] {
        let end = self.cursor + len;
        &self.buffer[self.cursor..end]
    }

    fn take_bytes(&mut self, len: usize) -> &[u8] {
        let start = self.cursor;
        let end = self.cursor + len;
        self.cursor = end;
        &self.buffer[start..end]
    }

    fn compact_buffer(&mut self) {
        if self.cursor == 0 {
            return;
        }

        if self.cursor > 64 * 1024 || self.cursor >= self.buffer.len() {
            self.buffer.drain(0..self.cursor);
            self.cursor = 0;
        }
    }

    fn log_progress_if_needed(&mut self) {
        if self.last_log_at.elapsed() >= self.log_interval {
            self.log_progress(false);
        }
    }

    fn log_progress(&mut self, final_log: bool) {
        let elapsed = self.started_at.elapsed();
        let elapsed_secs = elapsed.as_secs_f64().max(0.001);
        let total_mb = self.total_bytes as f64 / 1024.0 / 1024.0;
        let delta_bytes = self.total_bytes.saturating_sub(self.last_logged_bytes);
        let delta_mb = delta_bytes as f64 / 1024.0 / 1024.0;
        let delta_secs = self.last_log_at.elapsed().as_secs_f64().max(0.001);
        let inst_rate = delta_mb / delta_secs;
        let avg_rate = total_mb / elapsed_secs;

        if final_log {
            info!(
                "Backup progress (final): {:.2} MB, files: {}, avg {:.2} MB/s, elapsed {:.1}s",
                total_mb, self.file_count, avg_rate, elapsed_secs
            );
        } else {
            info!(
                "Backup progress: {:.2} MB (+{:.2} MB), files: {}, {:.2} MB/s avg {:.2} MB/s",
                total_mb, delta_mb, self.file_count, inst_rate, avg_rate
            );
        }

        self.last_log_at = Instant::now();
        self.last_logged_bytes = self.total_bytes;
    }
}

pub fn rewrite_tablespace_symlinks(output_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let tablespace_map = output_dir.join("tablespace_map");
    if !tablespace_map.exists() {
        return Ok(());
    }

    let contents = std::fs::read_to_string(&tablespace_map)?;
    let tblspc_dir = output_dir.join("pg_tblspc");
    fs::create_dir_all(&tblspc_dir)?;

    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let mut parts = line.split_whitespace();
        let oid = parts.next().unwrap_or_default();
        if oid.is_empty() {
            continue;
        }

        let local_tablespace = output_dir.join("tablespaces").join(oid);
        fs::create_dir_all(&local_tablespace)?;

        let link_path = tblspc_dir.join(oid);
        if link_path.exists() {
            fs::remove_file(&link_path)?;
        }

        let relative_target = PathBuf::from("..").join("tablespaces").join(oid);
        symlink(&relative_target, &link_path)?;
        debug!(
            "Rewrote tablespace symlink: {} -> {}",
            link_path.display(),
            relative_target.display()
        );
    }

    Ok(())
}
