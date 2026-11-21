//! Replication commands shared by both connection backends.
//!
//! The libpq and native backends expose the same command methods, and each one is the same
//! "build the SQL, run it, check the result" sequence over a few backend primitives.
//! `replication_commands!` holds that sequence once and expands to the public inherent
//! methods inside each backend's `impl` block. They stay inherent (not a trait) so callers of
//! `PgReplicationConnection` need no extra `use`.
//!
//! Commands whose only difference is how the backend enters COPY mode (`start_replication`,
//! `start_physical_replication`, `base_backup`, `upload_manifest`) stay in the backends.

use crate::error::{ReplicationError, Result};
use crate::types::{parse_lsn, Lsn, ReplicationSlotInfo, ReplicationSlotOptions, SlotType};
use tracing::debug;

/// The part of a backend's result type that the shared commands read.
pub(super) trait QueryResult {
    fn is_ok(&self) -> bool;
    fn error_message(&self) -> Option<String>;
    fn get_value(&self, row: i32, col: i32) -> Option<String>;
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
/// The invoking `impl` must provide `exec`, `exec_async`, `server_version`,
/// `ensure_replication_mode`, and an async `put_copy_data(&mut self, Bytes)` that sends one
/// CopyData message and flushes it.
macro_rules! replication_commands {
    ($result:ty) => {
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
        /// Uses the positional keyword syntax supported across PostgreSQL 14+:
        ///
        /// ```text
        /// CREATE_REPLICATION_SLOT name [TEMPORARY] PHYSICAL [RESERVE_WAL]
        /// CREATE_REPLICATION_SLOT name [TEMPORARY] LOGICAL plugin
        ///   [EXPORT_SNAPSHOT | NOEXPORT_SNAPSHOT | USE_SNAPSHOT | TWO_PHASE]
        ///   [FAILOVER]
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
        fn get_value(&self, row: i32, col: i32) -> Option<String> {
            if row != 0 {
                return None;
            }
            self.row
                .get(col as usize)
                .copied()
                .flatten()
                .map(str::to_string)
        }
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
    fn log_identity_tolerates_a_short_row() {
        log_identity(&rows([Some("1"), None, None]));
        log_identity(&rows([Some("1"), Some("1"), Some("0/0")]));
    }
}
