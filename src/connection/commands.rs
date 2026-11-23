//! Replication commands shared by both connection backends.
//!
//! The libpq and native backends expose the same command methods, and each one is the same
//! "build the SQL, run it, check the result" sequence over a few backend primitives.
//! `replication_commands!` holds that sequence once and expands to the public inherent
//! methods, and the replication-mode guard they call, inside each backend's `impl` block. They
//! stay inherent (not a trait) so callers of `PgReplicationConnection` need no extra `use`.
//!
//! Commands whose only difference is how the backend enters COPY mode (`start_replication`,
//! `start_physical_replication`, `base_backup`, `upload_manifest`) stay in the backends.

use crate::error::{ReplicationError, Result, ServerError};
use crate::types::{parse_lsn, Lsn, ReplicationSlotInfo, ReplicationSlotOptions, SlotType};
use tracing::debug;

/// The part of a backend's result type that the shared commands read.
pub(super) trait QueryResult {
    fn is_ok(&self) -> bool;
    fn error_message(&self) -> Option<String>;
    fn get_value(&self, row: i32, col: i32) -> Option<String>;
}

/// The error for a result the server rejected: `context`, then the server's message.
pub(super) fn server_failure(
    server: Option<ServerError>,
    message: Option<String>,
    context: &str,
) -> ReplicationError {
    let message = message.unwrap_or_else(|| "Unknown error".to_string());
    ReplicationError::from_server(server, format!("{context}: {message}"))
}

pub(super) fn log_identity(result: &impl QueryResult) {
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

pub(super) fn create_slot_sql(
    server_version: i32,
    slot_name: &str,
    slot_type: SlotType,
    output_plugin: Option<&str>,
    options: &ReplicationSlotOptions,
) -> Result<String> {
    let sql = crate::sql_builder::prepare_create_slot(
        server_version,
        slot_name,
        slot_type,
        output_plugin,
        options,
    )?;
    debug!("Creating replication slot: {}", sql);
    Ok(sql)
}

pub(super) fn drop_slot_sql(slot_name: &str, wait: bool) -> Result<String> {
    let sql = crate::sql_builder::build_drop_slot_sql(slot_name, wait)?;
    debug!("Dropping replication slot: {}", sql);
    Ok(sql)
}

pub(super) fn slot_dropped(slot_name: &str, result: &impl QueryResult) -> Result<()> {
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

pub(super) fn slot_info(slot_name: &str, result: &impl QueryResult) -> Result<ReplicationSlotInfo> {
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
        .and_then(|s| parse_lsn(&s).ok())
        .map(Lsn::new);
    let restart_tli = result.get_value(0, 2).and_then(|s| s.parse::<i32>().ok());

    Ok(ReplicationSlotInfo {
        slot_type,
        restart_lsn,
        restart_tli,
    })
}

/// Expands to the public replication commands shared by both backends, returning `$result`.
///
/// `$result` must implement [`QueryResult`], and the expansion must sit in an inherent `impl`
/// inside `crate::connection`. That `impl` must provide `exec`, `exec_async` and
/// `server_version`, a `copy` field whose `is_replication()` says whether the connection is
/// streaming, plus `put_copy_data(&mut self, Bytes)`, which sends one CopyData message and
/// flushes it. `put_copy_data` returns a future (an `async fn` or
/// `-> impl Future`); the commands await it right away.
macro_rules! replication_commands {
    ($result:ty) => {
        #[inline]
        fn ensure_replication_mode(&self) -> $crate::error::Result<()> {
            if !self.copy.is_replication() {
                return Err($crate::error::ReplicationError::protocol(
                    "Connection is not in replication mode".to_string(),
                ));
            }
            Ok(())
        }

        /// Send IDENTIFY_SYSTEM command
        pub fn identify_system(&mut self) -> $crate::error::Result<$result> {
            tracing::debug!("Sending IDENTIFY_SYSTEM command");
            let result = self.exec("IDENTIFY_SYSTEM")?;
            $crate::connection::commands::log_identity(&result);
            Ok(result)
        }

        /// Non-blocking [`identify_system`](Self::identify_system).
        pub async fn identify_system_async(&mut self) -> $crate::error::Result<$result> {
            tracing::debug!("Sending IDENTIFY_SYSTEM command");
            let result = self.exec_async("IDENTIFY_SYSTEM").await?;
            $crate::connection::commands::log_identity(&result);
            Ok(result)
        }

        /// Send feedback to the server (standby status update)
        pub async fn send_standby_status_update(
            &mut self,
            received_lsn: $crate::types::XLogRecPtr,
            flushed_lsn: $crate::types::XLogRecPtr,
            applied_lsn: $crate::types::XLogRecPtr,
            reply_requested: bool,
        ) -> $crate::error::Result<()> {
            self.ensure_replication_mode()?;

            let reply_data = $crate::protocol::build_standby_status_update_message(
                received_lsn,
                flushed_lsn,
                applied_lsn,
                reply_requested,
            );
            self.put_copy_data(reply_data).await?;

            tracing::info!(
                "Sent standby status update: received={}, flushed={}, applied={}, reply_requested={}",
                $crate::types::format_lsn(received_lsn),
                $crate::types::format_lsn(flushed_lsn),
                $crate::types::format_lsn(applied_lsn),
                reply_requested
            );

            Ok(())
        }

        /// Send hot standby feedback message to the server
        pub async fn send_hot_standby_feedback(
            &mut self,
            xmin: u32,
            xmin_epoch: u32,
            catalog_xmin: u32,
            catalog_xmin_epoch: u32,
        ) -> $crate::error::Result<()> {
            self.ensure_replication_mode()?;

            let feedback_data = $crate::protocol::build_hot_standby_feedback_message(
                xmin,
                xmin_epoch,
                catalog_xmin,
                catalog_xmin_epoch,
            )?;

            self.put_copy_data(feedback_data).await?;

            tracing::debug!(
                "Sent hot standby feedback: xmin={}, catalog_xmin={}",
                xmin,
                catalog_xmin
            );
            Ok(())
        }

        /// Create a replication slot with advanced options
        ///
        /// The options are checked against the server version first (`TWO_PHASE` needs
        /// PostgreSQL 15+, `FAILOVER` 17+), then the command is built by
        /// [`build_create_slot_sql`](crate::sql_builder::build_create_slot_sql). Without
        /// `failover` it uses the legacy positional form, which PostgreSQL 14 also accepts:
        ///
        /// ```text
        /// CREATE_REPLICATION_SLOT name [TEMPORARY] PHYSICAL [RESERVE_WAL]
        /// CREATE_REPLICATION_SLOT name [TEMPORARY] LOGICAL plugin
        ///   [TWO_PHASE] [EXPORT_SNAPSHOT | NOEXPORT_SNAPSHOT | USE_SNAPSHOT]
        /// ```
        ///
        /// With `failover` it uses the parenthesized option list instead, the only grammar that
        /// has `FAILOVER` (PostgreSQL 17+):
        ///
        /// ```text
        /// CREATE_REPLICATION_SLOT name LOGICAL plugin
        ///   ([TWO_PHASE,] [SNAPSHOT 'export' | 'nothing' | 'use',] FAILOVER)
        /// ```
        ///
        /// The snapshot option values are mapped to positional keywords:
        /// - `"export"` → `EXPORT_SNAPSHOT`
        /// - `"nothing"` → `NOEXPORT_SNAPSHOT`
        /// - `"use"` → `USE_SNAPSHOT`
        ///
        /// **Note:** When both `two_phase` and `snapshot` are set, both options are emitted for LOGICAL slots — PostgreSQL accepts them together.
        ///
        /// See: <https://www.postgresql.org/docs/current/protocol-replication.html>
        pub fn create_replication_slot_with_options(
            &mut self,
            slot_name: &str,
            slot_type: $crate::types::SlotType,
            output_plugin: Option<&str>,
            options: &$crate::types::ReplicationSlotOptions,
        ) -> $crate::error::Result<$result> {
            let sql = $crate::connection::commands::create_slot_sql(
                self.server_version(),
                slot_name,
                slot_type,
                output_plugin,
                options,
            )?;
            self.exec(&sql)
        }

        /// Non-blocking [`create_replication_slot_with_options`](Self::create_replication_slot_with_options).
        pub async fn create_replication_slot_with_options_async(
            &mut self,
            slot_name: &str,
            slot_type: $crate::types::SlotType,
            output_plugin: Option<&str>,
            options: &$crate::types::ReplicationSlotOptions,
        ) -> $crate::error::Result<$result> {
            let sql = $crate::connection::commands::create_slot_sql(
                self.server_version(),
                slot_name,
                slot_type,
                output_plugin,
                options,
            )?;
            self.exec_async(&sql).await
        }

        /// Alter a replication slot (logical slots only)
        pub fn alter_replication_slot(
            &mut self,
            slot_name: &str,
            two_phase: Option<bool>,
            failover: Option<bool>,
        ) -> $crate::error::Result<$result> {
            let alter_slot_sql = $crate::sql_builder::prepare_alter_slot(
                self.server_version(),
                slot_name,
                two_phase,
                failover,
            )?;

            tracing::debug!("Altering replication slot: {}", alter_slot_sql);
            let result = self.exec(&alter_slot_sql)?;
            tracing::debug!("Replication slot {} altered", slot_name);
            Ok(result)
        }

        /// Drop a replication slot
        ///
        /// Generates: `DROP_REPLICATION_SLOT "slot_name" [WAIT]`
        ///
        /// # Arguments
        ///
        /// * `slot_name` - Name of the replication slot to drop
        /// * `wait` - If true, the command waits until the slot becomes inactive instead of returning an error when the slot is in use
        pub fn drop_replication_slot(
            &mut self,
            slot_name: &str,
            wait: bool,
        ) -> $crate::error::Result<()> {
            let sql = $crate::connection::commands::drop_slot_sql(slot_name, wait)?;
            let result = self.exec(&sql)?;
            $crate::connection::commands::slot_dropped(slot_name, &result)
        }

        /// Non-blocking [`drop_replication_slot`](Self::drop_replication_slot).
        pub async fn drop_replication_slot_async(
            &mut self,
            slot_name: &str,
            wait: bool,
        ) -> $crate::error::Result<()> {
            let sql = $crate::connection::commands::drop_slot_sql(slot_name, wait)?;
            let result = self.exec_async(&sql).await?;
            $crate::connection::commands::slot_dropped(slot_name, &result)
        }

        /// Read information about a replication slot
        ///
        /// Generates: `READ_REPLICATION_SLOT "slot_name"`
        ///
        /// Returns slot type, restart LSN, and restart timeline.
        /// Requires PostgreSQL 15+.
        pub fn read_replication_slot(
            &mut self,
            slot_name: &str,
        ) -> $crate::error::Result<$crate::types::ReplicationSlotInfo> {
            let sql =
                $crate::sql_builder::prepare_read_slot(self.server_version(), slot_name)?;

            tracing::debug!("Reading replication slot: {}", sql);
            let result = self.exec(&sql)?;
            $crate::connection::commands::slot_info(slot_name, &result)
        }
    };
}

pub(super) use replication_commands;

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug)]
    struct Fake {
        ok: bool,
        error: Option<&'static str>,
        row: [Option<&'static str>; 3],
    }

    impl QueryResult for Fake {
        fn is_ok(&self) -> bool {
            self.ok
        }
        fn error_message(&self) -> Option<String> {
            self.error.map(str::to_string)
        }
        fn get_value(&self, _row: i32, col: i32) -> Option<String> {
            self.row
                .get(col as usize)
                .copied()
                .flatten()
                .map(str::to_string)
        }
    }

    struct FakeCopy {
        replication: bool,
    }

    impl FakeCopy {
        fn is_replication(&self) -> bool {
            self.replication
        }
    }

    /// A backend stand-in that records what the shared commands ask of it, so the macro
    /// itself is exercised without a socket.
    struct FakeConn {
        version: i32,
        copy: FakeCopy,
        reply: Fake,
        exec_fails: bool,
        sql: Vec<String>,
        sent: Vec<Bytes>,
    }

    impl FakeConn {
        fn new(version: i32) -> Self {
            Self {
                version,
                copy: FakeCopy { replication: true },
                reply: rows([None; 3]),
                exec_fails: false,
                sql: Vec::new(),
                sent: Vec::new(),
            }
        }

        fn exec(&mut self, sql: &str) -> Result<Fake> {
            self.sql.push(sql.to_string());
            if self.exec_fails {
                return Err(ReplicationError::protocol("exec failed".to_string()));
            }
            Ok(self.reply.clone())
        }

        async fn exec_async(&mut self, sql: &str) -> Result<Fake> {
            self.exec(sql)
        }

        fn server_version(&self) -> i32 {
            self.version
        }

        async fn put_copy_data(&mut self, data: Bytes) -> Result<()> {
            self.sent.push(data);
            Ok(())
        }

        replication_commands!(Fake);
    }

    /// Everything the subscriber installed by [`capture_logs`] formatted, at DEBUG and above.
    #[derive(Clone, Default)]
    struct Logs(Arc<Mutex<Vec<u8>>>);

    impl Logs {
        fn text(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    impl std::io::Write for Logs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Logs {
        type Writer = Logs;
        fn make_writer(&'a self) -> Logs {
            self.clone()
        }
    }

    /// Log arguments are only evaluated when a subscriber enables the level, so the lines that
    /// format them run only under a capturing subscriber. Keep the guard alive for the test.
    fn capture_logs() -> (Logs, tracing::subscriber::DefaultGuard) {
        let logs = Logs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_ansi(false)
            .with_writer(logs.clone())
            .finish();
        (logs, tracing::subscriber::set_default(subscriber))
    }

    fn assert_logged(logs: &Logs, needle: &str) {
        let text = logs.text();
        assert!(text.contains(needle), "{needle:?} not in {text:?}");
    }

    fn only(sent: &[Bytes]) -> &Bytes {
        assert_eq!(sent.len(), 1, "expected exactly one CopyData message");
        &sent[0]
    }

    fn rows(row: [Option<&'static str>; 3]) -> Fake {
        Fake {
            ok: true,
            error: None,
            row,
        }
    }

    fn failed(error: Option<&'static str>) -> Fake {
        Fake {
            ok: false,
            error,
            row: [None; 3],
        }
    }

    #[test]
    fn slot_dropped_passes_an_ok_result() {
        assert!(slot_dropped("s", &rows([None; 3])).is_ok());
    }

    #[test]
    fn slot_dropped_names_the_slot_and_the_server_message() {
        let err = slot_dropped("my_slot", &failed(Some("boom"))).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Replication slot error: Failed to drop replication slot 'my_slot': boom"
        );
    }

    #[test]
    fn slot_dropped_without_a_server_message_says_unknown() {
        let err = slot_dropped("s", &failed(None)).unwrap_err();
        assert!(err.to_string().ends_with("'s': unknown error"), "{err}");
    }

    #[test]
    fn slot_info_parses_all_three_columns() {
        let info = slot_info("s", &rows([Some("physical"), Some("0/16B3748"), Some("3")])).unwrap();
        assert_eq!(info.slot_type.as_deref(), Some("physical"));
        assert_eq!(info.restart_lsn.map(u64::from), Some(0x16B_3748));
        assert_eq!(info.restart_tli, Some(3));
    }

    #[test]
    fn slot_info_maps_null_columns_to_none() {
        let info = slot_info("s", &rows([None; 3])).unwrap();
        assert!(
            info.slot_type.is_none() && info.restart_lsn.is_none() && info.restart_tli.is_none()
        );
    }

    #[test]
    fn slot_info_drops_unparsable_lsn_and_timeline() {
        let info = slot_info("s", &rows([Some("physical"), Some("nonsense"), Some("x")])).unwrap();
        assert!(info.restart_lsn.is_none() && info.restart_tli.is_none());
    }

    #[test]
    fn slot_info_names_the_slot_and_the_server_message() {
        let err = slot_info("my_slot", &failed(Some("boom"))).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Replication slot error: Failed to read replication slot 'my_slot': boom"
        );
    }

    #[test]
    fn slot_info_without_a_server_message_says_unknown() {
        let err = slot_info("s", &failed(None)).unwrap_err();
        assert!(err.to_string().ends_with("'s': unknown error"), "{err}");
    }

    #[test]
    fn server_failure_puts_the_context_before_the_server_message() {
        let err = server_failure(None, Some("boom".to_string()), "ctx");
        assert!(err.to_string().ends_with("ctx: boom"), "{err}");
        let err = server_failure(None, None, "ctx");
        assert!(err.to_string().ends_with("ctx: Unknown error"), "{err}");
    }

    #[test]
    fn drop_slot_sql_quotes_the_name_and_honours_wait() {
        assert_eq!(
            drop_slot_sql("s", true).unwrap(),
            r#"DROP_REPLICATION_SLOT "s" WAIT;"#
        );
        assert_eq!(
            drop_slot_sql("s", false).unwrap(),
            r#"DROP_REPLICATION_SLOT "s";"#
        );
        assert!(drop_slot_sql("bad\0name", false).is_err());
    }

    #[test]
    fn create_slot_sql_builds_after_the_version_preflight() {
        let opts = ReplicationSlotOptions::default();
        let sql = create_slot_sql(170000, "s", SlotType::Logical, Some("pgoutput"), &opts).unwrap();
        assert!(sql.starts_with(r#"CREATE_REPLICATION_SLOT "s""#), "{sql}");
        assert!(create_slot_sql(
            170000,
            "bad\0name",
            SlotType::Logical,
            Some("pgoutput"),
            &opts
        )
        .is_err());
    }

    #[test]
    fn log_identity_reports_all_three_columns_and_skips_a_short_row() {
        let (logs, _guard) = capture_logs();
        log_identity(&rows([Some("1"), None, None]));
        assert_eq!(logs.text(), "");
        log_identity(&rows([Some("7"), Some("2"), Some("0/16B3748")]));
        assert_logged(
            &logs,
            "System identification: systemid=7, timeline=2, xlogpos=0/16B3748",
        );
    }

    #[test]
    fn identify_system_runs_the_command_on_both_paths() {
        let (logs, _guard) = capture_logs();
        let mut conn = FakeConn::new(170000);
        conn.reply = rows([Some("7"), Some("2"), Some("0/16B3748")]);

        let result = conn.identify_system().unwrap();
        assert_eq!(result.get_value(0, 0).as_deref(), Some("7"));
        let result = futures::executor::block_on(conn.identify_system_async()).unwrap();
        assert_eq!(result.get_value(0, 2).as_deref(), Some("0/16B3748"));

        assert_eq!(conn.sql, ["IDENTIFY_SYSTEM", "IDENTIFY_SYSTEM"]);
        assert_eq!(logs.text().matches("System identification").count(), 2);

        conn.exec_fails = true;
        assert!(conn.identify_system().is_err());
    }

    #[test]
    fn standby_status_update_sends_one_r_message() {
        let (logs, _guard) = capture_logs();
        let mut conn = FakeConn::new(170000);

        futures::executor::block_on(conn.send_standby_status_update(0x10, 0x20, 0x30, true))
            .unwrap();

        let msg = only(&conn.sent);
        assert_eq!(msg.len(), 34);
        assert_eq!(msg[0], b'r');
        assert_eq!(msg[1..9], 0x10u64.to_be_bytes());
        assert_eq!(msg[9..17], 0x20u64.to_be_bytes());
        assert_eq!(msg[17..25], 0x30u64.to_be_bytes());
        assert_eq!(msg[33], 1, "reply_requested");
        assert_logged(
            &logs,
            "Sent standby status update: received=0/10, flushed=0/20, applied=0/30, \
             reply_requested=true",
        );
    }

    #[test]
    fn standby_status_update_outside_replication_mode_sends_nothing() {
        let mut conn = FakeConn::new(170000);
        conn.copy.replication = false;

        let err = futures::executor::block_on(conn.send_standby_status_update(1, 2, 3, false))
            .unwrap_err();

        assert!(err.to_string().contains("not in replication mode"), "{err}");
        assert!(conn.sent.is_empty());
    }

    #[test]
    fn hot_standby_feedback_sends_one_h_message() {
        let (logs, _guard) = capture_logs();
        let mut conn = FakeConn::new(170000);

        futures::executor::block_on(conn.send_hot_standby_feedback(11, 12, 13, 14)).unwrap();

        let msg = only(&conn.sent);
        assert_eq!(msg.len(), 25);
        assert_eq!(msg[0], b'h');
        assert_eq!(msg[9..13], 11u32.to_be_bytes());
        assert_eq!(msg[13..17], 12u32.to_be_bytes());
        assert_eq!(msg[17..21], 13u32.to_be_bytes());
        assert_eq!(msg[21..25], 14u32.to_be_bytes());
        assert_logged(&logs, "Sent hot standby feedback: xmin=11, catalog_xmin=13");
    }

    #[test]
    fn hot_standby_feedback_outside_replication_mode_sends_nothing() {
        let mut conn = FakeConn::new(170000);
        conn.copy.replication = false;

        let err =
            futures::executor::block_on(conn.send_hot_standby_feedback(1, 2, 3, 4)).unwrap_err();

        assert!(err.to_string().contains("not in replication mode"), "{err}");
        assert!(conn.sent.is_empty());
    }

    #[test]
    fn create_slot_builds_the_sql_for_the_server_version_on_both_paths() {
        let opts = ReplicationSlotOptions::default();
        let mut conn = FakeConn::new(170000);

        conn.create_replication_slot_with_options("s", SlotType::Logical, Some("pgoutput"), &opts)
            .unwrap();
        futures::executor::block_on(conn.create_replication_slot_with_options_async(
            "s",
            SlotType::Logical,
            Some("pgoutput"),
            &opts,
        ))
        .unwrap();

        assert_eq!(conn.sql.len(), 2);
        assert!(conn
            .sql
            .iter()
            .all(|q| q.starts_with(r#"CREATE_REPLICATION_SLOT "s""#)));
    }

    #[test]
    fn create_slot_preflight_rejects_before_anything_is_sent() {
        let opts = ReplicationSlotOptions {
            two_phase: true,
            ..ReplicationSlotOptions::default()
        };
        let mut conn = FakeConn::new(140000);

        let sync = conn.create_replication_slot_with_options(
            "s",
            SlotType::Logical,
            Some("pgoutput"),
            &opts,
        );
        let asynchronous =
            futures::executor::block_on(conn.create_replication_slot_with_options_async(
                "s",
                SlotType::Logical,
                Some("pgoutput"),
                &opts,
            ));

        for err in [sync.unwrap_err(), asynchronous.unwrap_err()] {
            assert!(err.to_string().contains("TWO_PHASE"), "{err}");
        }
        assert!(conn.sql.is_empty());
    }

    #[test]
    fn alter_slot_sends_the_alter_command() {
        let (logs, _guard) = capture_logs();
        let mut conn = FakeConn::new(180000);

        conn.alter_replication_slot("s", Some(true), None).unwrap();

        assert_eq!(
            conn.sql,
            [r#"ALTER_REPLICATION_SLOT "s" (TWO_PHASE true);"#]
        );
        assert_logged(&logs, "Replication slot s altered");
    }

    #[test]
    fn alter_slot_preflight_rejects_before_anything_is_sent() {
        let mut conn = FakeConn::new(160000);
        let err = conn
            .alter_replication_slot("s", None, Some(true))
            .unwrap_err();
        assert!(err.to_string().contains("requires PostgreSQL 17+"), "{err}");
        assert!(conn.sql.is_empty());
    }

    #[test]
    fn drop_slot_sends_wait_only_when_asked_on_both_paths() {
        let mut conn = FakeConn::new(170000);

        for wait in [true, false] {
            conn.drop_replication_slot("s", wait).unwrap();
            futures::executor::block_on(conn.drop_replication_slot_async("s", wait)).unwrap();
        }

        let with_wait = r#"DROP_REPLICATION_SLOT "s" WAIT;"#;
        let without = r#"DROP_REPLICATION_SLOT "s";"#;
        assert_eq!(conn.sql, [with_wait, with_wait, without, without]);
    }

    #[test]
    fn drop_slot_reports_a_failed_result_and_an_exec_error() {
        let mut conn = FakeConn::new(170000);
        conn.reply = failed(Some("slot is active"));
        let err = conn.drop_replication_slot("s", false).unwrap_err();
        assert!(err.to_string().ends_with("'s': slot is active"), "{err}");
        let err =
            futures::executor::block_on(conn.drop_replication_slot_async("s", false)).unwrap_err();
        assert!(err.to_string().ends_with("'s': slot is active"), "{err}");

        conn.exec_fails = true;
        let err = conn.drop_replication_slot("s", false).unwrap_err();
        assert!(err.to_string().contains("exec failed"), "{err}");
    }

    #[test]
    fn read_slot_parses_the_reply() {
        let mut conn = FakeConn::new(170000);
        conn.reply = rows([Some("physical"), Some("0/16B3748"), Some("3")]);

        let info = conn.read_replication_slot("s").unwrap();

        assert_eq!(conn.sql, [r#"READ_REPLICATION_SLOT "s";"#]);
        assert_eq!(info.slot_type.as_deref(), Some("physical"));
        assert_eq!(info.restart_lsn.map(u64::from), Some(0x16B_3748));
        assert_eq!(info.restart_tli, Some(3));
    }

    #[test]
    fn read_slot_preflight_rejects_before_anything_is_sent() {
        let mut conn = FakeConn::new(140000);
        let err = conn.read_replication_slot("s").unwrap_err();
        assert!(err.to_string().contains("requires PostgreSQL 15+"), "{err}");
        assert!(conn.sql.is_empty());
    }
}
