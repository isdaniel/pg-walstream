//! NativeConnection struct and implementation.
//!
//! Pure-Rust PostgreSQL connection for replication, providing the same
//! public API as the libpq `PgReplicationConnection`.

use bytes::{Bytes, BytesMut};
use std::collections::VecDeque;
use std::time::SystemTime;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use super::conninfo::ConnInfo;
use super::startup::{self, Transport};
use super::{copy, query, wire};
use super::{NativePgResult, NativeResultStatus};

use crate::buffer::BufferWriter;
use crate::error::{ReplicationError, Result};
use crate::protocol::build_hot_standby_feedback_message;
use crate::types::{
    format_lsn, system_time_to_postgres_timestamp, BaseBackupOptions, ReplicationSlotOptions,
    SlotType, XLogRecPtr, INVALID_XLOG_REC_PTR,
};

/// Initial capacity for the read buffer (64 KiB).
#[allow(dead_code)]
const READ_BUF_INITIAL_CAPACITY: usize = 64 * 1024;

/// Run an async future synchronously, handling both "inside a runtime" and
/// "outside a runtime" contexts. When called from within a tokio runtime,
/// uses `block_in_place` to release the worker thread and drives the future
/// on the current runtime's handle. When called outside a runtime, creates
/// a temporary one.
fn run_sync<F: std::future::Future>(fut: F) -> F::Output {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(fut))
    } else {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to create runtime for sync bridge");
        rt.block_on(fut)
    }
}

/// Sanitize a string value for use in PostgreSQL replication protocol commands
/// by escaping single quotes (replacing ' with '')
#[inline]
fn sanitize_sql_string_value(value: &str) -> String {
    value.replace('\'', "''")
}

/// Sanitize a string value and wrap it in single quotes for SQL
#[inline]
fn quote_sql_string_value(value: &str) -> String {
    format!("'{}'", sanitize_sql_string_value(value))
}

/// Quote a SQL identifier by escaping internal double quotes.
#[inline]
fn quote_sql_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// Pure-Rust PostgreSQL connection for replication.
///
/// Provides the same public API as the libpq `PgReplicationConnection`
/// so that `stream.rs` works unchanged regardless of backend.
pub struct NativeConnection {
    transport: Transport,
    /// Read buffer — `read_buf()` fills this directly from TLS, then
    /// `drain_read_buffer()` parses complete messages out of it.
    read_buf: BytesMut,
    /// Pre-drained messages waiting to be consumed (drain-loop optimization).
    pending_messages: VecDeque<Bytes>,
    /// Server version number (e.g. 160001 for PG 16.1).
    server_ver: i32,
    /// Whether we are in COPY (replication) mode.
    in_copy_mode: bool,
    /// Track whether the connection is alive. Set to false on detected errors.
    alive: bool,
}

// NativeConnection is just data + owned transport — no raw pointers.
unsafe impl Send for NativeConnection {}

impl NativeConnection {
    // ── Connection establishment ─────────────────────────────────────────

    /// Create a new PostgreSQL connection for logical replication.
    ///
    /// Establishes a TCP connection (optionally upgraded to TLS via rustls),
    /// performs the v3.0 startup handshake and authentication.
    pub fn connect(conninfo: &str) -> Result<Self> {
        run_sync(Self::connect_async(conninfo))
    }

    async fn connect_async(conninfo: &str) -> Result<Self> {
        let info = ConnInfo::parse(conninfo)?;
        debug!("connect_async: parsed conninfo, host={}", info.host);
        let (transport, server_version, buf) = startup::connect(&info).await?;
        debug!(
            "connect_async: startup complete, version={}",
            server_version
        );

        debug!(
            "Connected to PostgreSQL {} via native rustls",
            server_version
        );

        Ok(Self {
            transport,
            read_buf: buf,
            pending_messages: VecDeque::with_capacity(256),
            server_ver: server_version,
            in_copy_mode: false,
            alive: true,
        })
    }

    // ── Query execution ─────────────────────────────────────────────────

    /// Execute a replication command (like IDENTIFY_SYSTEM).
    pub fn exec(&mut self, sql: &str) -> Result<NativePgResult> {
        run_sync(self.exec_async(sql))
    }

    async fn exec_async(&mut self, sql: &str) -> Result<NativePgResult> {
        let result = query::simple_query(&mut self.transport, &mut self.read_buf, sql).await?;

        let status_str = format!("{:?}", result.status());
        info!("query : {} pg_result.status() : {}", sql, status_str);

        if !result.is_ok() {
            let error_msg = result
                .error_message()
                .unwrap_or_else(|| "Unknown error".to_string());
            return Err(ReplicationError::protocol(format!(
                "Query execution failed: {error_msg}"
            )));
        }

        Ok(result)
    }

    /// Send IDENTIFY_SYSTEM command.
    pub fn identify_system(&mut self) -> Result<NativePgResult> {
        debug!("Sending IDENTIFY_SYSTEM command");
        let result = self.exec("IDENTIFY_SYSTEM")?;

        if result.ntuples() > 0 {
            if let (Some(systemid), Some(timeline), Some(xlogpos)) = (
                result.get_value(0, 0),
                result.get_value(0, 1),
                result.get_value(0, 2),
            ) {
                debug!(
                    "System identification: systemid={}, timeline={}, xlogpos={}",
                    systemid, timeline, xlogpos
                );
            }
        }

        Ok(result)
    }

    // ── Replication ─────────────────────────────────────────────────────

    /// Start logical replication.
    pub fn start_replication(
        &mut self,
        slot_name: &str,
        start_lsn: XLogRecPtr,
        options: &[(&str, &str)],
    ) -> Result<()> {
        let quoted_slot = quote_sql_identifier(slot_name);
        let mut options_str = String::new();
        for (i, (key, value)) in options.iter().enumerate() {
            if i > 0 {
                options_str.push_str(", ");
            }
            let quoted_key = quote_sql_identifier(key);
            let sanitized_value = sanitize_sql_string_value(value);
            options_str.push_str(&format!("{quoted_key} '{sanitized_value}'"));
        }

        let sql = if start_lsn == INVALID_XLOG_REC_PTR {
            format!("START_REPLICATION SLOT {quoted_slot} LOGICAL 0/0 ({options_str})")
        } else {
            format!(
                "START_REPLICATION SLOT {} LOGICAL {} ({})",
                quoted_slot,
                format_lsn(start_lsn),
                options_str
            )
        };

        debug!("Starting replication: {}", sql);

        run_sync(async {
            let result = query::simple_query(&mut self.transport, &mut self.read_buf, &sql).await?;

            if result.status() != &NativeResultStatus::CopyBoth {
                let error_msg = result
                    .error_message()
                    .unwrap_or_else(|| "Unknown error".to_string());
                return Err(ReplicationError::protocol(format!(
                    "START_REPLICATION did not enter COPY mode: {error_msg}"
                )));
            }

            Ok(())
        })?;

        self.in_copy_mode = true;
        debug!("Replication started successfully");
        Ok(())
    }

    /// Get copy data from replication stream (truly async, non-blocking).
    ///
    /// Implements drain-loop batch queue optimization with zero-copy extraction.
    pub async fn get_copy_data_async(
        &mut self,
        cancellation_token: &CancellationToken,
    ) -> Result<Bytes> {
        self.ensure_replication_mode()?;

        let result = copy::get_copy_data(
            &mut self.transport,
            &mut self.read_buf,
            &mut self.pending_messages,
            cancellation_token,
        )
        .await;

        if let Err(ReplicationError::TransientConnection(_)) = &result {
            self.alive = false;
        }

        result
    }

    /// Send feedback to the server (standby status update).
    pub async fn send_standby_status_update(
        &mut self,
        received_lsn: XLogRecPtr,
        flushed_lsn: XLogRecPtr,
        applied_lsn: XLogRecPtr,
        reply_requested: bool,
    ) -> Result<()> {
        self.ensure_replication_mode()?;

        let timestamp = system_time_to_postgres_timestamp(SystemTime::now());

        let mut buffer = BufferWriter::with_capacity(34);
        buffer.write_u8(b'r')?;
        buffer.write_u64(received_lsn)?;
        buffer.write_u64(flushed_lsn)?;
        buffer.write_u64(applied_lsn)?;
        buffer.write_i64(timestamp)?;
        buffer.write_u8(if reply_requested { 1 } else { 0 })?;

        let reply_data = buffer.freeze();
        copy::put_copy_data(&mut self.transport, &reply_data).await?;

        info!(
            "Sent standby status update: received={}, flushed={}, applied={}, reply_requested={}",
            format_lsn(received_lsn),
            format_lsn(flushed_lsn),
            format_lsn(applied_lsn),
            reply_requested
        );

        Ok(())
    }

    /// Send hot standby feedback message to the server.
    pub async fn send_hot_standby_feedback(
        &mut self,
        xmin: u32,
        xmin_epoch: u32,
        catalog_xmin: u32,
        catalog_xmin_epoch: u32,
    ) -> Result<()> {
        self.ensure_replication_mode()?;

        let feedback_data =
            build_hot_standby_feedback_message(xmin, xmin_epoch, catalog_xmin, catalog_xmin_epoch)?;

        copy::put_copy_data(&mut self.transport, &feedback_data).await?;

        debug!(
            "Sent hot standby feedback: xmin={}, catalog_xmin={}",
            xmin, catalog_xmin
        );
        Ok(())
    }

    // ── Connection info ─────────────────────────────────────────────────

    /// Check if the connection is still alive.
    pub fn is_alive(&self) -> bool {
        self.alive
    }

    /// Get the server version.
    pub fn server_version(&self) -> i32 {
        self.server_ver
    }

    // ── Replication slot management ─────────────────────────────────────

    /// Create a replication slot with advanced options.
    pub fn create_replication_slot_with_options(
        &mut self,
        slot_name: &str,
        slot_type: SlotType,
        output_plugin: Option<&str>,
        options: &ReplicationSlotOptions,
    ) -> Result<NativePgResult> {
        let sql = Self::build_create_slot_sql(slot_name, slot_type, output_plugin, options)?;
        debug!("Creating replication slot: {}", sql);
        self.exec(&sql)
    }

    fn build_create_slot_sql(
        slot_name: &str,
        slot_type: SlotType,
        output_plugin: Option<&str>,
        options: &ReplicationSlotOptions,
    ) -> Result<String> {
        let mut parts: Vec<&str> = Vec::new();
        let quoted_slot = quote_sql_identifier(slot_name);

        parts.push("CREATE_REPLICATION_SLOT");
        parts.push(&quoted_slot);

        if options.temporary {
            parts.push("TEMPORARY");
        }

        parts.push(slot_type.as_str());

        let quoted_plugin: String;

        match slot_type {
            SlotType::Physical => {
                if options.reserve_wal {
                    parts.push("RESERVE_WAL");
                }
            }
            SlotType::Logical => {
                let plugin = output_plugin.ok_or_else(|| {
                    ReplicationError::protocol(
                        "Output plugin required for LOGICAL slots".to_string(),
                    )
                })?;
                quoted_plugin = quote_sql_identifier(plugin);
                parts.push(&quoted_plugin);

                if options.two_phase {
                    parts.push("TWO_PHASE");
                } else if let Some(ref snapshot) = options.snapshot {
                    match snapshot.as_str() {
                        "export" => parts.push("EXPORT_SNAPSHOT"),
                        "nothing" => parts.push("NOEXPORT_SNAPSHOT"),
                        "use" => parts.push("USE_SNAPSHOT"),
                        other => {
                            return Err(ReplicationError::config(format!(
                                "Invalid snapshot option '{}': expected 'export', 'nothing', or 'use'",
                                other
                            )));
                        }
                    }
                }

                if options.failover {
                    parts.push("FAILOVER");
                }
            }
        }

        Ok(format!("{};", parts.join(" ")))
    }

    /// Alter a replication slot (logical slots only).
    pub fn alter_replication_slot(
        &mut self,
        slot_name: &str,
        two_phase: Option<bool>,
        failover: Option<bool>,
    ) -> Result<NativePgResult> {
        let mut opts = Vec::new();

        if let Some(tp) = two_phase {
            opts.push(format!("TWO_PHASE {}", tp));
        }

        if let Some(failover_value) = failover {
            opts.push(format!("FAILOVER {}", failover_value));
        }

        if opts.is_empty() {
            return Err(ReplicationError::protocol(
                "At least one option must be specified for ALTER_REPLICATION_SLOT".to_string(),
            ));
        }

        let options_str = Self::build_sql_options(&opts);
        let quoted_slot = quote_sql_identifier(slot_name);
        let sql = format!("ALTER_REPLICATION_SLOT {}{};", quoted_slot, options_str);

        debug!("Altering replication slot: {}", sql);
        let result = self.exec(&sql)?;
        debug!("Replication slot {} altered", slot_name);
        Ok(result)
    }

    fn build_sql_options(options: &[String]) -> String {
        if options.is_empty() {
            String::new()
        } else {
            format!(" ({})", options.join(", "))
        }
    }

    fn build_drop_slot_sql(slot_name: &str, wait: bool) -> String {
        let quoted_slot = quote_sql_identifier(slot_name);
        if wait {
            format!("DROP_REPLICATION_SLOT {} WAIT;", quoted_slot)
        } else {
            format!("DROP_REPLICATION_SLOT {};", quoted_slot)
        }
    }

    /// Drop a replication slot.
    pub fn drop_replication_slot(&mut self, slot_name: &str, wait: bool) -> Result<()> {
        let sql = Self::build_drop_slot_sql(slot_name, wait);
        debug!("Dropping replication slot: {}", sql);
        let result = self.exec(&sql)?;
        if !result.is_ok() {
            return Err(ReplicationError::replication_slot(format!(
                "Failed to drop replication slot '{}': {}",
                slot_name,
                result
                    .error_message()
                    .unwrap_or_else(|| "unknown error".to_string())
            )));
        }
        debug!("Replication slot {} dropped", slot_name);
        Ok(())
    }

    fn build_read_slot_sql(slot_name: &str) -> String {
        let quoted_slot = quote_sql_identifier(slot_name);
        format!("READ_REPLICATION_SLOT {};", quoted_slot)
    }

    /// Read information about a replication slot.
    pub fn read_replication_slot(
        &mut self,
        slot_name: &str,
    ) -> Result<crate::types::ReplicationSlotInfo> {
        let sql = Self::build_read_slot_sql(slot_name);
        debug!("Reading replication slot: {}", sql);
        let result = self.exec(&sql)?;
        if !result.is_ok() {
            return Err(ReplicationError::replication_slot(format!(
                "Failed to read replication slot '{}': {}",
                slot_name,
                result
                    .error_message()
                    .unwrap_or_else(|| "unknown error".to_string())
            )));
        }

        let slot_type = result.get_value(0, 0);
        let restart_lsn = result
            .get_value(0, 1)
            .and_then(|s| crate::types::parse_lsn(&s).ok())
            .map(crate::types::Lsn::new);
        let restart_tli = result.get_value(0, 2).and_then(|s| s.parse::<i32>().ok());

        Ok(crate::types::ReplicationSlotInfo {
            slot_type,
            restart_lsn,
            restart_tli,
        })
    }

    /// Start physical replication.
    pub fn start_physical_replication(
        &mut self,
        slot_name: Option<&str>,
        start_lsn: XLogRecPtr,
        timeline_id: Option<u32>,
    ) -> Result<()> {
        let mut sql = String::from("START_REPLICATION ");

        if let Some(slot) = slot_name {
            let quoted_slot = quote_sql_identifier(slot);
            sql.push_str(&format!("SLOT {} ", quoted_slot));
        }

        sql.push_str("PHYSICAL ");

        let lsn_str = if start_lsn == INVALID_XLOG_REC_PTR {
            "0/0".to_string()
        } else {
            format_lsn(start_lsn)
        };
        sql.push_str(&lsn_str);

        if let Some(tli) = timeline_id {
            sql.push_str(&format!(" TIMELINE {}", tli));
        }

        debug!("Starting physical replication: {}", sql);

        run_sync(async {
            let result = query::simple_query(&mut self.transport, &mut self.read_buf, &sql).await?;

            match result.status() {
                NativeResultStatus::CopyBoth | NativeResultStatus::CopyOut => Ok(()),
                _ => {
                    let error_msg = result
                        .error_message()
                        .unwrap_or_else(|| "Unknown error".to_string());
                    Err(ReplicationError::protocol(format!(
                        "START_REPLICATION did not enter COPY mode: {error_msg}"
                    )))
                }
            }
        })?;

        self.in_copy_mode = true;
        debug!("Physical replication started successfully");
        Ok(())
    }

    /// Start a base backup with options.
    pub fn base_backup(&mut self, options: &BaseBackupOptions) -> Result<NativePgResult> {
        let mut opts = Vec::new();

        if let Some(ref label) = options.label {
            opts.push(format!("LABEL {}", quote_sql_string_value(label)));
        }
        if let Some(ref target) = options.target {
            opts.push(format!("TARGET {}", quote_sql_string_value(target)));
        }
        if let Some(ref target_detail) = options.target_detail {
            opts.push(format!(
                "TARGET_DETAIL {}",
                quote_sql_string_value(target_detail)
            ));
        }
        if options.progress {
            opts.push("PROGRESS true".to_string());
        }
        if let Some(ref checkpoint) = options.checkpoint {
            opts.push(format!("CHECKPOINT {}", quote_sql_string_value(checkpoint)));
        }
        if options.wal {
            opts.push("WAL true".to_string());
        }
        if options.wait {
            opts.push("WAIT true".to_string());
        }
        if let Some(ref compression) = options.compression {
            opts.push(format!(
                "COMPRESSION {}",
                quote_sql_string_value(compression)
            ));
        }
        if let Some(ref compression_detail) = options.compression_detail {
            opts.push(format!(
                "COMPRESSION_DETAIL {}",
                quote_sql_string_value(compression_detail)
            ));
        }
        if let Some(max_rate) = options.max_rate {
            opts.push(format!("MAX_RATE {}", max_rate));
        }
        if options.tablespace_map {
            opts.push("TABLESPACE_MAP true".to_string());
        }
        if options.verify_checksums {
            opts.push("VERIFY_CHECKSUMS true".to_string());
        }
        if let Some(ref manifest) = options.manifest {
            opts.push(format!("MANIFEST {}", quote_sql_string_value(manifest)));
        }
        if let Some(ref manifest_checksums) = options.manifest_checksums {
            opts.push(format!(
                "MANIFEST_CHECKSUMS {}",
                quote_sql_string_value(manifest_checksums)
            ));
        }
        if options.incremental {
            opts.push("INCREMENTAL".to_string());
        }

        let sql = if opts.is_empty() {
            "BASE_BACKUP".to_string()
        } else {
            format!("BASE_BACKUP ({})", opts.join(", "))
        };

        debug!("Starting base backup: {}", sql);
        let result = self.exec(&sql)?;

        self.in_copy_mode = true;
        debug!("Base backup started successfully");
        Ok(result)
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    #[inline]
    fn ensure_replication_mode(&self) -> Result<()> {
        if !self.in_copy_mode {
            return Err(ReplicationError::protocol(
                "Connection is not in replication mode".to_string(),
            ));
        }
        Ok(())
    }

    /// Gracefully close the replication connection.
    fn close_connection(&mut self) {
        // Best-effort graceful shutdown.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_sync(async {
                if self.in_copy_mode {
                    let _ = copy::send_copy_done(&mut self.transport).await;
                }
                let terminate = wire::build_terminate();
                let _ = wire::write_all(&mut self.transport, &terminate).await;
                let _ = wire::flush(&mut self.transport).await;
            });
        }));

        self.pending_messages.clear();
        self.in_copy_mode = false;
        self.alive = false;
    }
}

impl Drop for NativeConnection {
    fn drop(&mut self) {
        self.close_connection();
    }
}

#[cfg(test)]
impl NativeConnection {
    /// Create a null connection for testing (DO NOT call any methods that touch the DB)
    pub(crate) fn null_for_testing() -> Self {
        // Create a pair of connected TCP sockets for testing.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let std_tcp = std::net::TcpStream::connect(addr).unwrap();
        std_tcp.set_nonblocking(true).unwrap();
        // Accept the other end so the socket stays connected
        let _peer = listener.accept().unwrap();

        // TcpStream::from_std requires a running reactor. If there's
        // already one (e.g. #[tokio::test]), use it. Otherwise create a
        // temporary runtime just for the conversion.
        let tcp = match tokio::runtime::Handle::try_current() {
            Ok(_) => tokio::net::TcpStream::from_std(std_tcp).unwrap(),
            Err(_) => {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .build()
                    .unwrap();
                let _guard = rt.enter();
                tokio::net::TcpStream::from_std(std_tcp).unwrap()
                // Note: `rt` is dropped here, but the TcpStream remains
                // valid as a raw fd wrapper. The tests using null_for_testing
                // never actually do I/O on it.
            }
        };

        Self {
            transport: Transport::Plain(tcp),
            read_buf: BytesMut::new(),
            pending_messages: VecDeque::new(),
            server_ver: 160000,
            in_copy_mode: false,
            alive: false, // null test connection is not alive
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ReplicationSlotOptions, SlotType};

    // === sanitize_sql_string_value ===

    #[test]
    fn test_sanitize_sql_string_value_no_quotes() {
        assert_eq!(sanitize_sql_string_value("test_value"), "test_value");
    }

    #[test]
    fn test_sanitize_sql_string_value_single_quote() {
        assert_eq!(sanitize_sql_string_value("test'value"), "test''value");
    }

    #[test]
    fn test_sanitize_sql_string_value_multiple_quotes() {
        assert_eq!(
            sanitize_sql_string_value("test'value'with'quotes"),
            "test''value''with''quotes"
        );
    }

    #[test]
    fn test_sanitize_sql_string_value_sql_injection_attempt() {
        assert_eq!(
            sanitize_sql_string_value("'; DROP TABLE users; --"),
            "''; DROP TABLE users; --"
        );
    }

    #[test]
    fn test_sanitize_sql_string_value_empty() {
        assert_eq!(sanitize_sql_string_value(""), "");
    }

    #[test]
    fn test_sanitize_sql_string_value_only_quote() {
        assert_eq!(sanitize_sql_string_value("'"), "''");
    }

    #[test]
    fn test_sanitize_sql_string_value_consecutive_quotes() {
        assert_eq!(sanitize_sql_string_value("''"), "''''");
    }

    // === quote_sql_string_value ===

    #[test]
    fn test_quote_sql_string_value_basic() {
        assert_eq!(quote_sql_string_value("test_value"), "'test_value'");
    }

    #[test]
    fn test_quote_sql_string_value_with_quotes() {
        assert_eq!(quote_sql_string_value("test'value"), "'test''value'");
    }

    #[test]
    fn test_quote_sql_string_value_sql_injection() {
        assert_eq!(
            quote_sql_string_value("'; DROP TABLE users; --"),
            "'''; DROP TABLE users; --'"
        );
    }

    #[test]
    fn test_quote_sql_string_value_empty() {
        assert_eq!(quote_sql_string_value(""), "''");
    }

    // === quote_sql_identifier ===

    #[test]
    fn test_quote_sql_identifier_simple() {
        assert_eq!(quote_sql_identifier("my_slot"), r#""my_slot""#);
    }

    #[test]
    fn test_quote_sql_identifier_with_double_quote() {
        assert_eq!(quote_sql_identifier(r#"a"b"#), r#""a""b""#);
    }

    #[test]
    fn test_quote_sql_identifier_multiple_quotes() {
        assert_eq!(quote_sql_identifier(r#"a""b"#), r#""a""""b""#);
    }

    #[test]
    fn test_quote_sql_identifier_empty() {
        assert_eq!(quote_sql_identifier(""), r#""""#);
    }

    #[test]
    fn test_quote_sql_identifier_special_chars() {
        assert_eq!(
            quote_sql_identifier("slot; DROP TABLE users; --"),
            r#""slot; DROP TABLE users; --""#
        );
    }

    // === Additional sanitization edge cases ===

    #[test]
    fn test_sanitize_complex_injection_attempt() {
        let input = "value' OR '1'='1";
        assert_eq!(sanitize_sql_string_value(input), "value'' OR ''1''=''1");
        assert_eq!(quote_sql_string_value(input), "'value'' OR ''1''=''1'");
    }

    #[test]
    fn test_sanitize_unicode_with_quotes() {
        assert_eq!(sanitize_sql_string_value("test'值'测试"), "test''值''测试");
    }

    #[test]
    fn test_sanitize_special_chars_without_quotes() {
        assert_eq!(
            sanitize_sql_string_value("test;value--comment/**/"),
            "test;value--comment/**/"
        );
    }

    #[test]
    fn test_sanitize_backslash_and_quote() {
        assert_eq!(sanitize_sql_string_value("test\\'value"), "test\\''value");
    }

    #[test]
    fn test_sanitize_newlines_and_quotes() {
        assert_eq!(
            sanitize_sql_string_value("line1'quote\nline2'quote"),
            "line1''quote\nline2''quote"
        );
    }

    // === build_sql_options ===

    #[test]
    fn test_build_sql_options_empty() {
        let options: Vec<String> = vec![];
        assert_eq!(NativeConnection::build_sql_options(&options), "");
    }

    #[test]
    fn test_build_sql_options_single() {
        let options = vec!["proto_version '2'".to_string()];
        assert_eq!(
            NativeConnection::build_sql_options(&options),
            " (proto_version '2')"
        );
    }

    #[test]
    fn test_build_sql_options_multiple() {
        let options = vec![
            "proto_version '2'".to_string(),
            "publication_names '\"my_pub\"'".to_string(),
            "streaming 'on'".to_string(),
        ];
        assert_eq!(
            NativeConnection::build_sql_options(&options),
            " (proto_version '2', publication_names '\"my_pub\"', streaming 'on')"
        );
    }

    // === build_create_slot_sql ===

    #[test]
    fn test_slot_sql_logical_default_options() {
        let opts = ReplicationSlotOptions::default();
        let sql = NativeConnection::build_create_slot_sql(
            "my_slot",
            SlotType::Logical,
            Some("pgoutput"),
            &opts,
        )
        .unwrap();
        assert_eq!(
            sql,
            "CREATE_REPLICATION_SLOT \"my_slot\" LOGICAL \"pgoutput\";"
        );
    }

    #[test]
    fn test_slot_sql_logical_temporary_export_snapshot() {
        let opts = ReplicationSlotOptions {
            temporary: true,
            snapshot: Some("export".to_string()),
            ..Default::default()
        };
        let sql = NativeConnection::build_create_slot_sql(
            "tmp_slot",
            SlotType::Logical,
            Some("pgoutput"),
            &opts,
        )
        .unwrap();
        assert_eq!(
            sql,
            "CREATE_REPLICATION_SLOT \"tmp_slot\" TEMPORARY LOGICAL \"pgoutput\" EXPORT_SNAPSHOT;"
        );
    }

    #[test]
    fn test_slot_sql_logical_noexport_snapshot() {
        let opts = ReplicationSlotOptions {
            snapshot: Some("nothing".to_string()),
            ..Default::default()
        };
        let sql = NativeConnection::build_create_slot_sql(
            "slot",
            SlotType::Logical,
            Some("pgoutput"),
            &opts,
        )
        .unwrap();
        assert_eq!(
            sql,
            "CREATE_REPLICATION_SLOT \"slot\" LOGICAL \"pgoutput\" NOEXPORT_SNAPSHOT;"
        );
    }

    #[test]
    fn test_slot_sql_logical_use_snapshot() {
        let opts = ReplicationSlotOptions {
            snapshot: Some("use".to_string()),
            ..Default::default()
        };
        let sql = NativeConnection::build_create_slot_sql(
            "slot",
            SlotType::Logical,
            Some("pgoutput"),
            &opts,
        )
        .unwrap();
        assert_eq!(
            sql,
            "CREATE_REPLICATION_SLOT \"slot\" LOGICAL \"pgoutput\" USE_SNAPSHOT;"
        );
    }

    #[test]
    fn test_slot_sql_logical_two_phase() {
        let opts = ReplicationSlotOptions {
            two_phase: true,
            ..Default::default()
        };
        let sql = NativeConnection::build_create_slot_sql(
            "slot",
            SlotType::Logical,
            Some("pgoutput"),
            &opts,
        )
        .unwrap();
        assert_eq!(
            sql,
            "CREATE_REPLICATION_SLOT \"slot\" LOGICAL \"pgoutput\" TWO_PHASE;"
        );
    }

    #[test]
    fn test_slot_sql_logical_two_phase_overrides_snapshot() {
        let opts = ReplicationSlotOptions {
            two_phase: true,
            snapshot: Some("export".to_string()),
            ..Default::default()
        };
        let sql = NativeConnection::build_create_slot_sql(
            "slot",
            SlotType::Logical,
            Some("pgoutput"),
            &opts,
        )
        .unwrap();
        assert_eq!(
            sql,
            "CREATE_REPLICATION_SLOT \"slot\" LOGICAL \"pgoutput\" TWO_PHASE;"
        );
    }

    #[test]
    fn test_slot_sql_logical_failover() {
        let opts = ReplicationSlotOptions {
            failover: true,
            ..Default::default()
        };
        let sql = NativeConnection::build_create_slot_sql(
            "slot",
            SlotType::Logical,
            Some("pgoutput"),
            &opts,
        )
        .unwrap();
        assert_eq!(
            sql,
            "CREATE_REPLICATION_SLOT \"slot\" LOGICAL \"pgoutput\" FAILOVER;"
        );
    }

    #[test]
    fn test_slot_sql_logical_export_snapshot_with_failover() {
        let opts = ReplicationSlotOptions {
            snapshot: Some("export".to_string()),
            failover: true,
            ..Default::default()
        };
        let sql = NativeConnection::build_create_slot_sql(
            "slot",
            SlotType::Logical,
            Some("pgoutput"),
            &opts,
        )
        .unwrap();
        assert_eq!(
            sql,
            "CREATE_REPLICATION_SLOT \"slot\" LOGICAL \"pgoutput\" EXPORT_SNAPSHOT FAILOVER;"
        );
    }

    #[test]
    fn test_slot_sql_physical_reserve_wal() {
        let opts = ReplicationSlotOptions {
            reserve_wal: true,
            ..Default::default()
        };
        let sql = NativeConnection::build_create_slot_sql("phys", SlotType::Physical, None, &opts)
            .unwrap();
        assert_eq!(
            sql,
            "CREATE_REPLICATION_SLOT \"phys\" PHYSICAL RESERVE_WAL;"
        );
    }

    #[test]
    fn test_slot_sql_physical_default() {
        let opts = ReplicationSlotOptions::default();
        let sql = NativeConnection::build_create_slot_sql("phys", SlotType::Physical, None, &opts)
            .unwrap();
        assert_eq!(sql, "CREATE_REPLICATION_SLOT \"phys\" PHYSICAL;");
    }

    #[test]
    fn test_slot_sql_physical_temporary() {
        let opts = ReplicationSlotOptions {
            temporary: true,
            ..Default::default()
        };
        let sql = NativeConnection::build_create_slot_sql("phys", SlotType::Physical, None, &opts)
            .unwrap();
        assert_eq!(sql, "CREATE_REPLICATION_SLOT \"phys\" TEMPORARY PHYSICAL;");
    }

    #[test]
    fn test_slot_sql_invalid_snapshot_value() {
        let opts = ReplicationSlotOptions {
            snapshot: Some("invalid".to_string()),
            ..Default::default()
        };
        let err = NativeConnection::build_create_slot_sql(
            "slot",
            SlotType::Logical,
            Some("pgoutput"),
            &opts,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("Invalid snapshot option"),
            "Expected invalid snapshot error, got: {err}"
        );
    }

    #[test]
    fn test_slot_sql_logical_missing_plugin() {
        let opts = ReplicationSlotOptions::default();
        let err = NativeConnection::build_create_slot_sql("slot", SlotType::Logical, None, &opts)
            .unwrap_err();
        assert!(
            err.to_string().contains("Output plugin required"),
            "Expected plugin error, got: {err}"
        );
    }

    #[test]
    fn test_slot_sql_slot_name_injection() {
        let opts = ReplicationSlotOptions::default();
        let sql = NativeConnection::build_create_slot_sql(
            r#"evil"PHYSICAL"#,
            SlotType::Logical,
            Some("test_decoding"),
            &opts,
        )
        .unwrap();
        assert_eq!(
            sql,
            r#"CREATE_REPLICATION_SLOT "evil""PHYSICAL" LOGICAL "test_decoding";"#
        );
    }

    #[test]
    fn test_slot_sql_plugin_name_injection() {
        let opts = ReplicationSlotOptions::default();
        let sql = NativeConnection::build_create_slot_sql(
            "safe_slot",
            SlotType::Logical,
            Some(r#"bad"plugin"#),
            &opts,
        )
        .unwrap();
        assert_eq!(
            sql,
            r#"CREATE_REPLICATION_SLOT "safe_slot" LOGICAL "bad""plugin";"#
        );
    }

    // === build_drop_slot_sql ===

    #[test]
    fn test_build_drop_slot_sql_without_wait() {
        assert_eq!(
            NativeConnection::build_drop_slot_sql("my_slot", false),
            r#"DROP_REPLICATION_SLOT "my_slot";"#
        );
    }

    #[test]
    fn test_build_drop_slot_sql_with_wait() {
        assert_eq!(
            NativeConnection::build_drop_slot_sql("my_slot", true),
            r#"DROP_REPLICATION_SLOT "my_slot" WAIT;"#
        );
    }

    #[test]
    fn test_build_drop_slot_sql_injection() {
        assert_eq!(
            NativeConnection::build_drop_slot_sql(r#"evil"slot"#, false),
            r#"DROP_REPLICATION_SLOT "evil""slot";"#
        );
    }

    #[test]
    fn test_build_drop_slot_sql_injection_with_wait() {
        assert_eq!(
            NativeConnection::build_drop_slot_sql(r#"evil"slot"#, true),
            r#"DROP_REPLICATION_SLOT "evil""slot" WAIT;"#
        );
    }

    // === build_read_slot_sql ===

    #[test]
    fn test_build_read_slot_sql_basic() {
        assert_eq!(
            NativeConnection::build_read_slot_sql("my_slot"),
            r#"READ_REPLICATION_SLOT "my_slot";"#
        );
    }

    #[test]
    fn test_build_read_slot_sql_injection() {
        assert_eq!(
            NativeConnection::build_read_slot_sql(r#"evil"slot"#),
            r#"READ_REPLICATION_SLOT "evil""slot";"#
        );
    }

    // === ensure_replication_mode, is_alive, server_version, close_connection, Drop ===

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_ensure_replication_mode_fails_when_not_replication() {
        let conn = NativeConnection::null_for_testing();
        let err = conn.ensure_replication_mode().unwrap_err();
        assert!(
            err.to_string().contains("not in replication mode"),
            "Expected replication mode error, got: {err}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_is_alive_returns_false_for_null_conn() {
        let conn = NativeConnection::null_for_testing();
        assert!(!conn.is_alive());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_server_version_returns_configured_value() {
        let conn = NativeConnection::null_for_testing();
        assert_eq!(conn.server_version(), 160000);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_close_connection_null_conn() {
        let mut conn = NativeConnection::null_for_testing();
        conn.close_connection(); // should not panic
        assert!(!conn.is_alive());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_drop_null_conn_does_not_panic() {
        let conn = NativeConnection::null_for_testing();
        drop(conn); // should not panic
    }
}
