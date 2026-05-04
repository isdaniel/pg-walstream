//! SQL builder utilities for PostgreSQL replication management.
//!
//! Provides safe quoting primitives and SQL statement builders for replication
//! slot management, subscription management, and base backup commands.
//! All quoting functions use pre-allocated buffers and char-by-char iteration
//! to avoid intermediate allocations.

use crate::error::{ReplicationError, Result};
use crate::types::{format_lsn, BaseBackupOptions, ReplicationSlotOptions, SlotType, XLogRecPtr};

/// Invalid/zero LSN pointer (re-imported for internal use).
const INVALID_XLOG_REC_PTR: u64 = 0;

/// Quote a PostgreSQL identifier by wrapping in double quotes and escaping
/// internal double quotes (doubling them).
///
/// # Errors
///
/// Returns an error if `name` contains a null byte (`\0`), which is invalid in
/// PostgreSQL identifiers and could cause truncation-based injection via
/// the C-string wire protocol.
///
/// # Example
///
/// ```
/// use pg_walstream::sql_builder::quote_ident;
///
/// assert_eq!(quote_ident("my_slot").unwrap(), r#""my_slot""#);
/// assert_eq!(quote_ident(r#"a"b"#).unwrap(), r#""a""b""#);
/// assert!(quote_ident("bad\0name").is_err());
/// ```
#[inline]
pub fn quote_ident(name: &str) -> Result<String> {
    if name.contains('\0') {
        return Err(ReplicationError::config(
            "SQL identifier must not contain null bytes".to_string(),
        ));
    }
    let mut out = String::with_capacity(name.len() + 2);
    out.push('"');
    for ch in name.chars() {
        if ch == '"' {
            out.push('"');
        }
        out.push(ch);
    }
    out.push('"');
    Ok(out)
}

/// Quote a PostgreSQL string literal by wrapping in single quotes and escaping
/// internal single quotes (doubling them).
///
/// # Errors
///
/// Returns an error if `value` contains a null byte (`\0`), which is invalid in
/// PostgreSQL string literals and could cause truncation-based injection via
/// the C-string wire protocol.
///
/// # Example
///
/// ```
/// use pg_walstream::sql_builder::quote_literal;
///
/// assert_eq!(quote_literal("hello").unwrap(), "'hello'");
/// assert_eq!(quote_literal("it's").unwrap(), "'it''s'");
/// assert!(quote_literal("bad\0value").is_err());
/// ```
#[inline]
pub fn quote_literal(value: &str) -> Result<String> {
    if value.contains('\0') {
        return Err(ReplicationError::config(
            "SQL literal must not contain null bytes".to_string(),
        ));
    }
    let mut out = String::with_capacity(value.len() + 2);
    out.push('\'');
    for ch in value.chars() {
        if ch == '\'' {
            out.push('\'');
        }
        out.push(ch);
    }
    out.push('\'');
    Ok(out)
}

// ═══════════════════════════════════════════════════════════════════════════
// Replication slot SQL builders
// ═══════════════════════════════════════════════════════════════════════════

/// Build the SQL for `CREATE_REPLICATION_SLOT`.
///
/// # Example
///
/// ```
/// use pg_walstream::sql_builder::build_create_slot_sql;
/// use pg_walstream::types::{ReplicationSlotOptions, SlotType};
///
/// let opts = ReplicationSlotOptions::default();
/// let sql = build_create_slot_sql("my_slot", SlotType::Logical, Some("pgoutput"), &opts).unwrap();
/// assert_eq!(sql, r#"CREATE_REPLICATION_SLOT "my_slot" LOGICAL "pgoutput";"#);
/// ```
pub fn build_create_slot_sql(
    slot_name: &str,
    slot_type: SlotType,
    output_plugin: Option<&str>,
    options: &ReplicationSlotOptions,
) -> Result<String> {
    let mut parts: Vec<&str> = Vec::with_capacity(6);

    let quoted_slot = quote_ident(slot_name)?;

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
                ReplicationError::protocol("Output plugin required for LOGICAL slots".to_string())
            })?;
            quoted_plugin = quote_ident(plugin)?;
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
                            "Invalid snapshot option '{}': \
                             expected 'export', 'nothing', or 'use'",
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

/// Build the SQL for `ALTER_REPLICATION_SLOT`.
///
/// # Example
///
/// ```
/// use pg_walstream::sql_builder::build_alter_slot_sql;
///
/// let sql = build_alter_slot_sql("my_slot", Some(true), None).unwrap();
/// assert_eq!(sql, r#"ALTER_REPLICATION_SLOT "my_slot" (TWO_PHASE true);"#);
/// ```
pub fn build_alter_slot_sql(
    slot_name: &str,
    two_phase: Option<bool>,
    failover: Option<bool>,
) -> Result<String> {
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

    let options_str = build_sql_options(&opts);
    let quoted_slot = quote_ident(slot_name)?;
    Ok(format!(
        "ALTER_REPLICATION_SLOT {}{};",
        quoted_slot, options_str
    ))
}

/// Build the SQL for `DROP_REPLICATION_SLOT`.
///
/// # Example
///
/// ```
/// use pg_walstream::sql_builder::build_drop_slot_sql;
///
/// assert_eq!(build_drop_slot_sql("my_slot", false).unwrap(), r#"DROP_REPLICATION_SLOT "my_slot";"#);
/// assert_eq!(build_drop_slot_sql("my_slot", true).unwrap(), r#"DROP_REPLICATION_SLOT "my_slot" WAIT;"#);
/// ```
#[inline]
pub fn build_drop_slot_sql(slot_name: &str, wait: bool) -> Result<String> {
    let quoted_slot = quote_ident(slot_name)?;
    if wait {
        Ok(format!("DROP_REPLICATION_SLOT {} WAIT;", quoted_slot))
    } else {
        Ok(format!("DROP_REPLICATION_SLOT {};", quoted_slot))
    }
}

/// Build the SQL for `READ_REPLICATION_SLOT`.
///
/// # Example
///
/// ```
/// use pg_walstream::sql_builder::build_read_slot_sql;
///
/// assert_eq!(build_read_slot_sql("my_slot").unwrap(), r#"READ_REPLICATION_SLOT "my_slot";"#);
/// ```
#[inline]
pub fn build_read_slot_sql(slot_name: &str) -> Result<String> {
    let quoted_slot = quote_ident(slot_name)?;
    Ok(format!("READ_REPLICATION_SLOT {};", quoted_slot))
}

/// Build the SQL for `START_REPLICATION SLOT ... LOGICAL`.
///
/// # Example
///
/// ```
/// use pg_walstream::sql_builder::build_start_replication_sql;
///
/// let sql = build_start_replication_sql("my_slot", 0, &[("proto_version", "1")]).unwrap();
/// assert_eq!(sql, r#"START_REPLICATION SLOT "my_slot" LOGICAL 0/0 ("proto_version" '1')"#);
/// ```
pub fn build_start_replication_sql(
    slot_name: &str,
    start_lsn: XLogRecPtr,
    options: &[(&str, &str)],
) -> Result<String> {
    let quoted_slot = quote_ident(slot_name)?;
    let lsn_str = if start_lsn == INVALID_XLOG_REC_PTR {
        "0/0".to_string()
    } else {
        format_lsn(start_lsn)
    };

    if options.is_empty() {
        return Ok(format!(
            "START_REPLICATION SLOT {quoted_slot} LOGICAL {lsn_str}"
        ));
    }

    let mut options_parts = Vec::with_capacity(options.len());
    for (k, v) in options {
        options_parts.push(format!("{} {}", quote_ident(k)?, quote_literal(v)?));
    }
    let options_str = options_parts.join(", ");

    Ok(format!(
        "START_REPLICATION SLOT {quoted_slot} LOGICAL {lsn_str} ({options_str})"
    ))
}

/// Build the SQL for `START_REPLICATION ... PHYSICAL`.
///
/// # Example
///
/// ```
/// use pg_walstream::sql_builder::build_start_physical_replication_sql;
///
/// let sql = build_start_physical_replication_sql(Some("my_slot"), 0, None).unwrap();
/// assert_eq!(sql, r#"START_REPLICATION SLOT "my_slot" PHYSICAL 0/0"#);
/// ```
pub fn build_start_physical_replication_sql(
    slot_name: Option<&str>,
    start_lsn: XLogRecPtr,
    timeline_id: Option<u32>,
) -> Result<String> {
    let mut sql = String::with_capacity(64);
    sql.push_str("START_REPLICATION ");

    if let Some(slot) = slot_name {
        let quoted_slot = quote_ident(slot)?;
        sql.push_str("SLOT ");
        sql.push_str(&quoted_slot);
        sql.push(' ');
    }

    sql.push_str("PHYSICAL ");

    if start_lsn == INVALID_XLOG_REC_PTR {
        sql.push_str("0/0");
    } else {
        sql.push_str(&format_lsn(start_lsn));
    }

    if let Some(tli) = timeline_id {
        sql.push_str(" TIMELINE ");
        sql.push_str(&tli.to_string());
    }

    Ok(sql)
}

/// Build the SQL for `BASE_BACKUP`.
///
/// # Example
///
/// ```
/// use pg_walstream::sql_builder::build_base_backup_sql;
/// use pg_walstream::types::BaseBackupOptions;
///
/// let opts = BaseBackupOptions::default();
/// assert_eq!(build_base_backup_sql(&opts).unwrap(), "BASE_BACKUP");
/// ```
pub fn build_base_backup_sql(options: &BaseBackupOptions) -> Result<String> {
    let mut opts = Vec::new();

    if let Some(ref label) = options.label {
        opts.push(format!("LABEL {}", quote_literal(label)?));
    }

    if let Some(ref target) = options.target {
        opts.push(format!("TARGET {}", quote_literal(target)?));
    }

    if let Some(ref target_detail) = options.target_detail {
        opts.push(format!("TARGET_DETAIL {}", quote_literal(target_detail)?));
    }

    if options.progress {
        opts.push("PROGRESS true".to_string());
    }

    if let Some(ref checkpoint) = options.checkpoint {
        opts.push(format!("CHECKPOINT {}", quote_literal(checkpoint)?));
    }

    if options.wal {
        opts.push("WAL true".to_string());
    }

    if options.wait {
        opts.push("WAIT true".to_string());
    }

    if let Some(ref compression) = options.compression {
        opts.push(format!("COMPRESSION {}", quote_literal(compression)?));
    }

    if let Some(ref compression_detail) = options.compression_detail {
        opts.push(format!(
            "COMPRESSION_DETAIL {}",
            quote_literal(compression_detail)?
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
        opts.push(format!("MANIFEST {}", quote_literal(manifest)?));
    }

    if let Some(ref manifest_checksums) = options.manifest_checksums {
        opts.push(format!(
            "MANIFEST_CHECKSUMS {}",
            quote_literal(manifest_checksums)?
        ));
    }

    if options.incremental {
        opts.push("INCREMENTAL".to_string());
    }

    if opts.is_empty() {
        Ok("BASE_BACKUP".to_string())
    } else {
        Ok(format!("BASE_BACKUP ({})", opts.join(", ")))
    }
}

/// Options for building a `CREATE SUBSCRIPTION` SQL statement.
///
/// All fields borrow from the caller — no allocation or cloning required.
/// Use [`Default`] for the WITH-clause flags to get the typical migration
/// defaults (`create_slot = false`, `enabled = true`, `copy_data = false`).
///
/// # Example
///
/// ```
/// use pg_walstream::sql_builder::CreateSubscriptionOptions;
///
/// let opts = CreateSubscriptionOptions {
///     subscription_name: "my_sub",
///     connection_string: "host=localhost dbname=source",
///     publication: "my_pub",
///     slot_name: "my_slot",
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone, Copy)]
pub struct CreateSubscriptionOptions<'a> {
    pub subscription_name: &'a str,
    pub connection_string: &'a str,
    pub publication: &'a str,
    pub slot_name: &'a str,
    pub create_slot: bool,
    pub enabled: bool,
    pub copy_data: bool,
}

impl<'a> Default for CreateSubscriptionOptions<'a> {
    #[inline]
    fn default() -> Self {
        Self {
            subscription_name: "",
            connection_string: "",
            publication: "",
            slot_name: "",
            create_slot: false,
            enabled: true,
            copy_data: false,
        }
    }
}

/// Build a `CREATE SUBSCRIPTION` statement.
///
/// # Example
///
/// ```
/// use pg_walstream::sql_builder::{build_create_subscription_sql, CreateSubscriptionOptions};
///
/// let opts = CreateSubscriptionOptions {
///     subscription_name: "my_sub",
///     connection_string: "host=localhost dbname=source",
///     publication: "my_pub",
///     slot_name: "my_slot",
///     ..Default::default()
/// };
/// let sql = build_create_subscription_sql(&opts).unwrap();
/// assert!(sql.starts_with("CREATE SUBSCRIPTION"));
/// ```
pub fn build_create_subscription_sql(opts: &CreateSubscriptionOptions<'_>) -> Result<String> {
    let sub = quote_ident(opts.subscription_name)?;
    let conn = quote_literal(opts.connection_string)?;
    let pubname = quote_ident(opts.publication)?;
    let slot = quote_literal(opts.slot_name)?;

    let create_slot_str = if opts.create_slot { "true" } else { "false" };
    let enabled_str = if opts.enabled { "true" } else { "false" };
    let copy_data_str = if opts.copy_data { "true" } else { "false" };

    Ok(format!(
        "CREATE SUBSCRIPTION {sub} CONNECTION {conn} PUBLICATION {pubname} \
         WITH (create_slot = {create_slot_str}, slot_name = {slot}, \
         enabled = {enabled_str}, copy_data = {copy_data_str})"
    ))
}

/// Build an `ALTER SUBSCRIPTION ... DISABLE` statement.
///
/// # Example
///
/// ```
/// use pg_walstream::sql_builder::build_disable_subscription_sql;
///
/// let sql = build_disable_subscription_sql("my_sub").unwrap();
/// assert_eq!(sql, r#"ALTER SUBSCRIPTION "my_sub" DISABLE"#);
/// ```
#[inline]
pub fn build_disable_subscription_sql(name: &str) -> Result<String> {
    Ok(format!("ALTER SUBSCRIPTION {} DISABLE", quote_ident(name)?))
}

/// Build an `ALTER SUBSCRIPTION ... SET (slot_name = NONE)` statement to detach a slot.
///
/// # Example
///
/// ```
/// use pg_walstream::sql_builder::build_detach_slot_sql;
///
/// let sql = build_detach_slot_sql("my_sub").unwrap();
/// assert_eq!(sql, r#"ALTER SUBSCRIPTION "my_sub" SET (slot_name = NONE)"#);
/// ```
#[inline]
pub fn build_detach_slot_sql(name: &str) -> Result<String> {
    Ok(format!(
        "ALTER SUBSCRIPTION {} SET (slot_name = NONE)",
        quote_ident(name)?
    ))
}

/// Build a `DROP SUBSCRIPTION` statement.
///
/// # Example
///
/// ```
/// use pg_walstream::sql_builder::build_drop_subscription_sql;
///
/// let sql = build_drop_subscription_sql("my_sub").unwrap();
/// assert_eq!(sql, r#"DROP SUBSCRIPTION "my_sub""#);
/// ```
#[inline]
pub fn build_drop_subscription_sql(name: &str) -> Result<String> {
    Ok(format!("DROP SUBSCRIPTION {}", quote_ident(name)?))
}

// ═══════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════

/// Format a list of options as ` (opt1, opt2, ...)`.
/// Returns an empty string if the list is empty.
#[inline]
pub fn build_sql_options(options: &[String]) -> String {
    if options.is_empty() {
        String::new()
    } else {
        format!(" ({})", options.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── quote_ident ──────────────────────────────────────────────────────

    #[test]
    fn quote_ident_simple() {
        assert_eq!(quote_ident("my_slot").unwrap(), r#""my_slot""#);
    }

    #[test]
    fn quote_ident_with_internal_double_quote() {
        assert_eq!(quote_ident(r#"a"b"#).unwrap(), r#""a""b""#);
    }

    #[test]
    fn quote_ident_multiple_quotes() {
        assert_eq!(quote_ident(r#"a""b"#).unwrap(), r#""a""""b""#);
    }

    #[test]
    fn quote_ident_empty() {
        assert_eq!(quote_ident("").unwrap(), r#""""#);
    }

    #[test]
    fn quote_ident_special_chars() {
        assert_eq!(
            quote_ident("slot; DROP TABLE users; --").unwrap(),
            r#""slot; DROP TABLE users; --""#
        );
    }

    #[test]
    fn quote_ident_unicode() {
        assert_eq!(quote_ident("テスト").unwrap(), r#""テスト""#);
    }

    #[test]
    fn quote_ident_mixed_unicode_and_quotes() {
        assert_eq!(quote_ident(r#"名前"テスト"#).unwrap(), r#""名前""テスト""#);
    }

    #[test]
    fn quote_ident_rejects_null_byte() {
        assert!(quote_ident("evil\0injection").is_err());
    }

    // ── quote_literal ────────────────────────────────────────────────────

    #[test]
    fn quote_literal_simple() {
        assert_eq!(quote_literal("hello").unwrap(), "'hello'");
    }

    #[test]
    fn quote_literal_with_internal_single_quote() {
        assert_eq!(quote_literal("it's").unwrap(), "'it''s'");
    }

    #[test]
    fn quote_literal_multiple_quotes() {
        assert_eq!(quote_literal("a''b").unwrap(), "'a''''b'");
    }

    #[test]
    fn quote_literal_empty() {
        assert_eq!(quote_literal("").unwrap(), "''");
    }

    #[test]
    fn quote_literal_sql_injection_attempt() {
        assert_eq!(
            quote_literal("'; DROP TABLE users; --").unwrap(),
            "'''; DROP TABLE users; --'"
        );
    }

    #[test]
    fn quote_literal_unicode() {
        assert_eq!(quote_literal("日本語").unwrap(), "'日本語'");
    }

    #[test]
    fn quote_literal_newlines() {
        assert_eq!(quote_literal("line1\nline2").unwrap(), "'line1\nline2'");
    }

    #[test]
    fn quote_literal_complex_injection() {
        assert_eq!(
            quote_literal("value' OR '1'='1").unwrap(),
            "'value'' OR ''1''=''1'"
        );
    }

    #[test]
    fn quote_literal_backslash_and_quote() {
        assert_eq!(quote_literal("test\\'value").unwrap(), "'test\\''value'");
    }

    #[test]
    fn quote_literal_rejects_null_byte() {
        assert!(quote_literal("evil\0injection").is_err());
    }

    // ── build_create_slot_sql ────────────────────────────────────────────

    #[test]
    fn create_slot_logical_default() {
        let opts = ReplicationSlotOptions::default();
        let sql =
            build_create_slot_sql("my_slot", SlotType::Logical, Some("pgoutput"), &opts).unwrap();
        assert_eq!(
            sql,
            r#"CREATE_REPLICATION_SLOT "my_slot" LOGICAL "pgoutput";"#
        );
    }

    #[test]
    fn create_slot_logical_temporary_export_snapshot() {
        let opts = ReplicationSlotOptions {
            temporary: true,
            snapshot: Some("export".to_string()),
            ..Default::default()
        };
        let sql =
            build_create_slot_sql("tmp_slot", SlotType::Logical, Some("pgoutput"), &opts).unwrap();
        assert_eq!(
            sql,
            r#"CREATE_REPLICATION_SLOT "tmp_slot" TEMPORARY LOGICAL "pgoutput" EXPORT_SNAPSHOT;"#
        );
    }

    #[test]
    fn create_slot_logical_noexport_snapshot() {
        let opts = ReplicationSlotOptions {
            snapshot: Some("nothing".to_string()),
            ..Default::default()
        };
        let sql =
            build_create_slot_sql("slot", SlotType::Logical, Some("pgoutput"), &opts).unwrap();
        assert_eq!(
            sql,
            r#"CREATE_REPLICATION_SLOT "slot" LOGICAL "pgoutput" NOEXPORT_SNAPSHOT;"#
        );
    }

    #[test]
    fn create_slot_logical_use_snapshot() {
        let opts = ReplicationSlotOptions {
            snapshot: Some("use".to_string()),
            ..Default::default()
        };
        let sql =
            build_create_slot_sql("slot", SlotType::Logical, Some("pgoutput"), &opts).unwrap();
        assert_eq!(
            sql,
            r#"CREATE_REPLICATION_SLOT "slot" LOGICAL "pgoutput" USE_SNAPSHOT;"#
        );
    }

    #[test]
    fn create_slot_logical_two_phase() {
        let opts = ReplicationSlotOptions {
            two_phase: true,
            ..Default::default()
        };
        let sql =
            build_create_slot_sql("slot", SlotType::Logical, Some("pgoutput"), &opts).unwrap();
        assert_eq!(
            sql,
            r#"CREATE_REPLICATION_SLOT "slot" LOGICAL "pgoutput" TWO_PHASE;"#
        );
    }

    #[test]
    fn create_slot_logical_two_phase_overrides_snapshot() {
        let opts = ReplicationSlotOptions {
            two_phase: true,
            snapshot: Some("export".to_string()),
            ..Default::default()
        };
        let sql =
            build_create_slot_sql("slot", SlotType::Logical, Some("pgoutput"), &opts).unwrap();
        assert_eq!(
            sql,
            r#"CREATE_REPLICATION_SLOT "slot" LOGICAL "pgoutput" TWO_PHASE;"#
        );
    }

    #[test]
    fn create_slot_logical_failover() {
        let opts = ReplicationSlotOptions {
            failover: true,
            ..Default::default()
        };
        let sql =
            build_create_slot_sql("slot", SlotType::Logical, Some("pgoutput"), &opts).unwrap();
        assert_eq!(
            sql,
            r#"CREATE_REPLICATION_SLOT "slot" LOGICAL "pgoutput" FAILOVER;"#
        );
    }

    #[test]
    fn create_slot_logical_export_with_failover() {
        let opts = ReplicationSlotOptions {
            snapshot: Some("export".to_string()),
            failover: true,
            ..Default::default()
        };
        let sql =
            build_create_slot_sql("slot", SlotType::Logical, Some("pgoutput"), &opts).unwrap();
        assert_eq!(
            sql,
            r#"CREATE_REPLICATION_SLOT "slot" LOGICAL "pgoutput" EXPORT_SNAPSHOT FAILOVER;"#
        );
    }

    #[test]
    fn create_slot_physical_reserve_wal() {
        let opts = ReplicationSlotOptions {
            reserve_wal: true,
            ..Default::default()
        };
        let sql = build_create_slot_sql("phys", SlotType::Physical, None, &opts).unwrap();
        assert_eq!(
            sql,
            r#"CREATE_REPLICATION_SLOT "phys" PHYSICAL RESERVE_WAL;"#
        );
    }

    #[test]
    fn create_slot_physical_default() {
        let opts = ReplicationSlotOptions::default();
        let sql = build_create_slot_sql("phys", SlotType::Physical, None, &opts).unwrap();
        assert_eq!(sql, r#"CREATE_REPLICATION_SLOT "phys" PHYSICAL;"#);
    }

    #[test]
    fn create_slot_physical_temporary() {
        let opts = ReplicationSlotOptions {
            temporary: true,
            ..Default::default()
        };
        let sql = build_create_slot_sql("phys", SlotType::Physical, None, &opts).unwrap();
        assert_eq!(sql, r#"CREATE_REPLICATION_SLOT "phys" TEMPORARY PHYSICAL;"#);
    }

    #[test]
    fn create_slot_invalid_snapshot_value() {
        let opts = ReplicationSlotOptions {
            snapshot: Some("invalid".to_string()),
            ..Default::default()
        };
        let err =
            build_create_slot_sql("slot", SlotType::Logical, Some("pgoutput"), &opts).unwrap_err();
        assert!(err.to_string().contains("Invalid snapshot option"));
    }

    #[test]
    fn create_slot_logical_missing_plugin() {
        let opts = ReplicationSlotOptions::default();
        let err = build_create_slot_sql("slot", SlotType::Logical, None, &opts).unwrap_err();
        assert!(err.to_string().contains("Output plugin required"));
    }

    #[test]
    fn create_slot_name_injection() {
        let opts = ReplicationSlotOptions::default();
        let sql = build_create_slot_sql(
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
    fn create_slot_plugin_name_injection() {
        let opts = ReplicationSlotOptions::default();
        let sql =
            build_create_slot_sql("safe_slot", SlotType::Logical, Some(r#"bad"plugin"#), &opts)
                .unwrap();
        assert_eq!(
            sql,
            r#"CREATE_REPLICATION_SLOT "safe_slot" LOGICAL "bad""plugin";"#
        );
    }

    // ── build_alter_slot_sql ─────────────────────────────────────────────

    #[test]
    fn alter_slot_two_phase() {
        let sql = build_alter_slot_sql("my_slot", Some(true), None).unwrap();
        assert_eq!(sql, r#"ALTER_REPLICATION_SLOT "my_slot" (TWO_PHASE true);"#);
    }

    #[test]
    fn alter_slot_failover() {
        let sql = build_alter_slot_sql("my_slot", None, Some(true)).unwrap();
        assert_eq!(sql, r#"ALTER_REPLICATION_SLOT "my_slot" (FAILOVER true);"#);
    }

    #[test]
    fn alter_slot_both() {
        let sql = build_alter_slot_sql("my_slot", Some(false), Some(true)).unwrap();
        assert_eq!(
            sql,
            r#"ALTER_REPLICATION_SLOT "my_slot" (TWO_PHASE false, FAILOVER true);"#
        );
    }

    #[test]
    fn alter_slot_no_options_error() {
        let err = build_alter_slot_sql("my_slot", None, None).unwrap_err();
        assert!(err.to_string().contains("At least one option"));
    }

    #[test]
    fn alter_slot_injection() {
        let sql = build_alter_slot_sql(r#"evil"slot"#, Some(true), None).unwrap();
        assert!(sql.contains(r#""evil""slot""#));
    }

    // ── build_drop_slot_sql ──────────────────────────────────────────────

    #[test]
    fn drop_slot_without_wait() {
        assert_eq!(
            build_drop_slot_sql("my_slot", false).unwrap(),
            r#"DROP_REPLICATION_SLOT "my_slot";"#
        );
    }

    #[test]
    fn drop_slot_with_wait() {
        assert_eq!(
            build_drop_slot_sql("my_slot", true).unwrap(),
            r#"DROP_REPLICATION_SLOT "my_slot" WAIT;"#
        );
    }

    #[test]
    fn drop_slot_injection() {
        let sql = build_drop_slot_sql(r#"evil"slot"#, false).unwrap();
        assert_eq!(sql, r#"DROP_REPLICATION_SLOT "evil""slot";"#);
    }

    // ── build_read_slot_sql ──────────────────────────────────────────────

    #[test]
    fn read_slot_basic() {
        assert_eq!(
            build_read_slot_sql("my_slot").unwrap(),
            r#"READ_REPLICATION_SLOT "my_slot";"#
        );
    }

    #[test]
    fn read_slot_injection() {
        assert_eq!(
            build_read_slot_sql(r#"evil"slot"#).unwrap(),
            r#"READ_REPLICATION_SLOT "evil""slot";"#
        );
    }

    // ── build_start_replication_sql ──────────────────────────────────────

    #[test]
    fn start_replication_zero_lsn() {
        let sql = build_start_replication_sql(
            "my_slot",
            0,
            &[("proto_version", "1"), ("publication_names", "my_pub")],
        )
        .unwrap();
        assert_eq!(
            sql,
            r#"START_REPLICATION SLOT "my_slot" LOGICAL 0/0 ("proto_version" '1', "publication_names" 'my_pub')"#
        );
    }

    #[test]
    fn start_replication_valid_lsn() {
        let lsn: XLogRecPtr = 0x0000_0001_0000_0000;
        let sql = build_start_replication_sql("test_slot", lsn, &[("proto_version", "2")]).unwrap();
        assert!(sql.contains("START_REPLICATION SLOT \"test_slot\" LOGICAL"));
        assert!(sql.contains("(\"proto_version\" '2')"));
        assert!(!sql.contains("0/0"));
    }

    #[test]
    fn start_replication_multiple_options() {
        let sql = build_start_replication_sql(
            "slot1",
            0,
            &[
                ("proto_version", "1"),
                ("publication_names", "pub1"),
                ("messages", "true"),
            ],
        )
        .unwrap();
        assert!(
            sql.contains(r#""proto_version" '1', "publication_names" 'pub1', "messages" 'true'"#)
        );
    }

    #[test]
    fn start_replication_empty_options() {
        let sql = build_start_replication_sql("slot1", 0, &[]).unwrap();
        assert_eq!(sql, r#"START_REPLICATION SLOT "slot1" LOGICAL 0/0"#);
    }

    #[test]
    fn start_replication_option_injection() {
        let sql = build_start_replication_sql(r#"evil"slot"#, 0, &[("key", "it's")]).unwrap();
        assert!(sql.contains(r#""evil""slot""#));
        assert!(sql.contains("'it''s'"));
    }

    #[test]
    fn start_replication_single_option() {
        let sql = build_start_replication_sql("my_slot", 0, &[("proto_version", "1")]).unwrap();
        assert_eq!(
            sql,
            r#"START_REPLICATION SLOT "my_slot" LOGICAL 0/0 ("proto_version" '1')"#
        );
    }

    // ── build_start_physical_replication_sql ──────────────────────────────

    #[test]
    fn start_physical_with_slot_zero_lsn() {
        let sql = build_start_physical_replication_sql(Some("my_slot"), 0, None).unwrap();
        assert_eq!(sql, r#"START_REPLICATION SLOT "my_slot" PHYSICAL 0/0"#);
    }

    #[test]
    fn start_physical_no_slot() {
        let sql = build_start_physical_replication_sql(None, 0, None).unwrap();
        assert_eq!(sql, "START_REPLICATION PHYSICAL 0/0");
    }

    #[test]
    fn start_physical_with_lsn() {
        let lsn: XLogRecPtr = 0x0000_0001_0000_0000;
        let sql = build_start_physical_replication_sql(Some("slot"), lsn, None).unwrap();
        assert!(sql.contains("PHYSICAL 1/0"));
        assert!(!sql.contains("0/0"));
    }

    #[test]
    fn start_physical_with_timeline() {
        let sql = build_start_physical_replication_sql(Some("slot"), 0, Some(3)).unwrap();
        assert_eq!(
            sql,
            r#"START_REPLICATION SLOT "slot" PHYSICAL 0/0 TIMELINE 3"#
        );
    }

    #[test]
    fn start_physical_no_slot_with_timeline() {
        let sql = build_start_physical_replication_sql(None, 0, Some(5)).unwrap();
        assert_eq!(sql, "START_REPLICATION PHYSICAL 0/0 TIMELINE 5");
    }

    #[test]
    fn start_physical_slot_injection() {
        let sql = build_start_physical_replication_sql(Some(r#"evil"slot"#), 0, None).unwrap();
        assert!(sql.contains(r#"SLOT "evil""slot""#));
    }

    // ── build_base_backup_sql ────────────────────────────────────────────

    #[test]
    fn base_backup_default() {
        let opts = BaseBackupOptions::default();
        assert_eq!(build_base_backup_sql(&opts).unwrap(), "BASE_BACKUP");
    }

    #[test]
    fn base_backup_with_label() {
        let opts = BaseBackupOptions {
            label: Some("my_backup".to_string()),
            ..Default::default()
        };
        assert_eq!(
            build_base_backup_sql(&opts).unwrap(),
            "BASE_BACKUP (LABEL 'my_backup')"
        );
    }

    #[test]
    fn base_backup_with_target() {
        let opts = BaseBackupOptions {
            target: Some("client".to_string()),
            ..Default::default()
        };
        assert_eq!(
            build_base_backup_sql(&opts).unwrap(),
            "BASE_BACKUP (TARGET 'client')"
        );
    }

    #[test]
    fn base_backup_with_target_detail() {
        let opts = BaseBackupOptions {
            target: Some("server".to_string()),
            target_detail: Some("/var/backups".to_string()),
            ..Default::default()
        };
        assert_eq!(
            build_base_backup_sql(&opts).unwrap(),
            "BASE_BACKUP (TARGET 'server', TARGET_DETAIL '/var/backups')"
        );
    }

    #[test]
    fn base_backup_with_progress() {
        let opts = BaseBackupOptions {
            progress: true,
            ..Default::default()
        };
        assert_eq!(
            build_base_backup_sql(&opts).unwrap(),
            "BASE_BACKUP (PROGRESS true)"
        );
    }

    #[test]
    fn base_backup_with_checkpoint() {
        let opts = BaseBackupOptions {
            checkpoint: Some("fast".to_string()),
            ..Default::default()
        };
        assert_eq!(
            build_base_backup_sql(&opts).unwrap(),
            "BASE_BACKUP (CHECKPOINT 'fast')"
        );
    }

    #[test]
    fn base_backup_with_wal() {
        let opts = BaseBackupOptions {
            wal: true,
            ..Default::default()
        };
        assert_eq!(
            build_base_backup_sql(&opts).unwrap(),
            "BASE_BACKUP (WAL true)"
        );
    }

    #[test]
    fn base_backup_with_wait() {
        let opts = BaseBackupOptions {
            wait: true,
            ..Default::default()
        };
        assert_eq!(
            build_base_backup_sql(&opts).unwrap(),
            "BASE_BACKUP (WAIT true)"
        );
    }

    #[test]
    fn base_backup_with_compression() {
        let opts = BaseBackupOptions {
            compression: Some("gzip".to_string()),
            ..Default::default()
        };
        assert_eq!(
            build_base_backup_sql(&opts).unwrap(),
            "BASE_BACKUP (COMPRESSION 'gzip')"
        );
    }

    #[test]
    fn base_backup_with_compression_detail() {
        let opts = BaseBackupOptions {
            compression: Some("zstd".to_string()),
            compression_detail: Some("level=3".to_string()),
            ..Default::default()
        };
        assert_eq!(
            build_base_backup_sql(&opts).unwrap(),
            "BASE_BACKUP (COMPRESSION 'zstd', COMPRESSION_DETAIL 'level=3')"
        );
    }

    #[test]
    fn base_backup_with_max_rate() {
        let opts = BaseBackupOptions {
            max_rate: Some(1024),
            ..Default::default()
        };
        assert_eq!(
            build_base_backup_sql(&opts).unwrap(),
            "BASE_BACKUP (MAX_RATE 1024)"
        );
    }

    #[test]
    fn base_backup_with_tablespace_map() {
        let opts = BaseBackupOptions {
            tablespace_map: true,
            ..Default::default()
        };
        assert_eq!(
            build_base_backup_sql(&opts).unwrap(),
            "BASE_BACKUP (TABLESPACE_MAP true)"
        );
    }

    #[test]
    fn base_backup_with_verify_checksums() {
        let opts = BaseBackupOptions {
            verify_checksums: true,
            ..Default::default()
        };
        assert_eq!(
            build_base_backup_sql(&opts).unwrap(),
            "BASE_BACKUP (VERIFY_CHECKSUMS true)"
        );
    }

    #[test]
    fn base_backup_with_manifest() {
        let opts = BaseBackupOptions {
            manifest: Some("yes".to_string()),
            ..Default::default()
        };
        assert_eq!(
            build_base_backup_sql(&opts).unwrap(),
            "BASE_BACKUP (MANIFEST 'yes')"
        );
    }

    #[test]
    fn base_backup_with_manifest_checksums() {
        let opts = BaseBackupOptions {
            manifest: Some("yes".to_string()),
            manifest_checksums: Some("SHA256".to_string()),
            ..Default::default()
        };
        assert_eq!(
            build_base_backup_sql(&opts).unwrap(),
            "BASE_BACKUP (MANIFEST 'yes', MANIFEST_CHECKSUMS 'SHA256')"
        );
    }

    #[test]
    fn base_backup_incremental() {
        let opts = BaseBackupOptions {
            incremental: true,
            ..Default::default()
        };
        assert_eq!(
            build_base_backup_sql(&opts).unwrap(),
            "BASE_BACKUP (INCREMENTAL)"
        );
    }

    #[test]
    fn base_backup_multiple_options() {
        let opts = BaseBackupOptions {
            label: Some("backup".to_string()),
            progress: true,
            wal: true,
            verify_checksums: true,
            ..Default::default()
        };
        assert_eq!(
            build_base_backup_sql(&opts).unwrap(),
            "BASE_BACKUP (LABEL 'backup', PROGRESS true, WAL true, VERIFY_CHECKSUMS true)"
        );
    }

    #[test]
    fn base_backup_label_injection() {
        let opts = BaseBackupOptions {
            label: Some("evil'; DROP TABLE users; --".to_string()),
            ..Default::default()
        };
        assert_eq!(
            build_base_backup_sql(&opts).unwrap(),
            "BASE_BACKUP (LABEL 'evil''; DROP TABLE users; --')"
        );
    }

    // ── build_sql_options ────────────────────────────────────────────────

    #[test]
    fn sql_options_empty() {
        assert_eq!(build_sql_options(&[]), "");
    }

    #[test]
    fn sql_options_single() {
        let opts = vec!["proto_version '2'".to_string()];
        assert_eq!(build_sql_options(&opts), " (proto_version '2')");
    }

    #[test]
    fn sql_options_multiple() {
        let opts = vec![
            "proto_version '2'".to_string(),
            "publication_names '\"my_pub\"'".to_string(),
            "streaming 'on'".to_string(),
        ];
        assert_eq!(
            build_sql_options(&opts),
            " (proto_version '2', publication_names '\"my_pub\"', streaming 'on')"
        );
    }

    // ── build_create_subscription_sql ────────────────────────────────────

    #[test]
    fn create_subscription_basic() {
        let opts = CreateSubscriptionOptions {
            subscription_name: "my_sub",
            connection_string: "host=localhost dbname=source",
            publication: "my_pub",
            slot_name: "my_slot",
            ..Default::default()
        };
        let sql = build_create_subscription_sql(&opts).unwrap();
        assert_eq!(
            sql,
            "CREATE SUBSCRIPTION \"my_sub\" \
             CONNECTION 'host=localhost dbname=source' \
             PUBLICATION \"my_pub\" \
             WITH (create_slot = false, slot_name = 'my_slot', enabled = true, copy_data = false)"
        );
    }

    #[test]
    fn create_subscription_with_special_chars() {
        let opts = CreateSubscriptionOptions {
            subscription_name: r#"sub"name"#,
            connection_string: "host=db password='secret'",
            publication: "pub",
            slot_name: "slot'name",
            ..Default::default()
        };
        let sql = build_create_subscription_sql(&opts).unwrap();
        assert_eq!(
            sql,
            "CREATE SUBSCRIPTION \"sub\"\"name\" \
             CONNECTION 'host=db password=''secret''' \
             PUBLICATION \"pub\" \
             WITH (create_slot = false, slot_name = 'slot''name', enabled = true, copy_data = false)"
        );
    }

    #[test]
    fn create_subscription_empty_names() {
        let opts = CreateSubscriptionOptions {
            subscription_name: "",
            connection_string: "",
            publication: "",
            slot_name: "",
            ..Default::default()
        };
        let sql = build_create_subscription_sql(&opts).unwrap();
        assert_eq!(
            sql,
            "CREATE SUBSCRIPTION \"\" \
             CONNECTION '' \
             PUBLICATION \"\" \
             WITH (create_slot = false, slot_name = '', enabled = true, copy_data = false)"
        );
    }

    #[test]
    fn create_subscription_create_slot_true() {
        let opts = CreateSubscriptionOptions {
            subscription_name: "sub",
            connection_string: "host=localhost",
            publication: "pub",
            slot_name: "slot",
            create_slot: true,
            ..Default::default()
        };
        let sql = build_create_subscription_sql(&opts).unwrap();
        assert!(sql.contains("create_slot = true"));
        assert!(sql.contains("enabled = true"));
        assert!(sql.contains("copy_data = false"));
    }

    #[test]
    fn create_subscription_copy_data_true() {
        let opts = CreateSubscriptionOptions {
            subscription_name: "sub",
            connection_string: "host=localhost",
            publication: "pub",
            slot_name: "slot",
            copy_data: true,
            ..Default::default()
        };
        let sql = build_create_subscription_sql(&opts).unwrap();
        assert!(sql.contains("copy_data = true"));
    }

    #[test]
    fn create_subscription_disabled() {
        let opts = CreateSubscriptionOptions {
            subscription_name: "sub",
            connection_string: "host=localhost",
            publication: "pub",
            slot_name: "slot",
            enabled: false,
            ..Default::default()
        };
        let sql = build_create_subscription_sql(&opts).unwrap();
        assert!(sql.contains("enabled = false"));
    }

    #[test]
    fn create_subscription_all_true() {
        let opts = CreateSubscriptionOptions {
            subscription_name: "sub",
            connection_string: "host=localhost",
            publication: "pub",
            slot_name: "slot",
            create_slot: true,
            enabled: true,
            copy_data: true,
        };
        let sql = build_create_subscription_sql(&opts).unwrap();
        assert_eq!(
            sql,
            "CREATE SUBSCRIPTION \"sub\" \
             CONNECTION 'host=localhost' \
             PUBLICATION \"pub\" \
             WITH (create_slot = true, slot_name = 'slot', enabled = true, copy_data = true)"
        );
    }

    #[test]
    fn create_subscription_all_false() {
        let opts = CreateSubscriptionOptions {
            subscription_name: "sub",
            connection_string: "host=localhost",
            publication: "pub",
            slot_name: "slot",
            create_slot: false,
            enabled: false,
            copy_data: false,
        };
        let sql = build_create_subscription_sql(&opts).unwrap();
        assert_eq!(
            sql,
            "CREATE SUBSCRIPTION \"sub\" \
             CONNECTION 'host=localhost' \
             PUBLICATION \"pub\" \
             WITH (create_slot = false, slot_name = 'slot', enabled = false, copy_data = false)"
        );
    }

    // ── build_disable_subscription_sql ───────────────────────────────────

    #[test]
    fn disable_subscription_basic() {
        assert_eq!(
            build_disable_subscription_sql("my_sub").unwrap(),
            r#"ALTER SUBSCRIPTION "my_sub" DISABLE"#
        );
    }

    #[test]
    fn disable_subscription_with_quotes() {
        assert_eq!(
            build_disable_subscription_sql(r#"sub"name"#).unwrap(),
            r#"ALTER SUBSCRIPTION "sub""name" DISABLE"#
        );
    }

    // ── build_detach_slot_sql ────────────────────────────────────────────

    #[test]
    fn detach_slot_basic() {
        assert_eq!(
            build_detach_slot_sql("my_sub").unwrap(),
            r#"ALTER SUBSCRIPTION "my_sub" SET (slot_name = NONE)"#
        );
    }

    #[test]
    fn detach_slot_with_quotes() {
        assert_eq!(
            build_detach_slot_sql(r#"sub"x"#).unwrap(),
            r#"ALTER SUBSCRIPTION "sub""x" SET (slot_name = NONE)"#
        );
    }

    // ── build_drop_subscription_sql ──────────────────────────────────────

    #[test]
    fn drop_subscription_basic() {
        assert_eq!(
            build_drop_subscription_sql("my_sub").unwrap(),
            r#"DROP SUBSCRIPTION "my_sub""#
        );
    }

    #[test]
    fn drop_subscription_with_quotes() {
        assert_eq!(
            build_drop_subscription_sql(r#"sub"name"#).unwrap(),
            r#"DROP SUBSCRIPTION "sub""name""#
        );
    }

    #[test]
    fn drop_subscription_injection_attempt() {
        assert_eq!(
            build_drop_subscription_sql("evil\"; DROP TABLE users; --").unwrap(),
            "DROP SUBSCRIPTION \"evil\"\"; DROP TABLE users; --\""
        );
    }

    // ── Null byte error propagation through builders ───────────────────

    #[test]
    fn create_slot_rejects_null_in_slot_name() {
        let opts = ReplicationSlotOptions::default();
        let err =
            build_create_slot_sql("slot\0name", SlotType::Logical, Some("pgoutput"), &opts)
                .unwrap_err();
        assert!(err.to_string().contains("null bytes"));
    }

    #[test]
    fn create_slot_rejects_null_in_plugin() {
        let opts = ReplicationSlotOptions::default();
        let err =
            build_create_slot_sql("slot", SlotType::Logical, Some("pg\0output"), &opts)
                .unwrap_err();
        assert!(err.to_string().contains("null bytes"));
    }

    #[test]
    fn alter_slot_rejects_null_in_slot_name() {
        let err = build_alter_slot_sql("slot\0x", Some(true), None).unwrap_err();
        assert!(err.to_string().contains("null bytes"));
    }

    #[test]
    fn drop_slot_rejects_null_in_slot_name() {
        let err = build_drop_slot_sql("slot\0x", false).unwrap_err();
        assert!(err.to_string().contains("null bytes"));
    }

    #[test]
    fn read_slot_rejects_null_in_slot_name() {
        let err = build_read_slot_sql("slot\0x").unwrap_err();
        assert!(err.to_string().contains("null bytes"));
    }

    #[test]
    fn start_replication_rejects_null_in_slot_name() {
        let err =
            build_start_replication_sql("slot\0x", 0, &[("proto_version", "1")]).unwrap_err();
        assert!(err.to_string().contains("null bytes"));
    }

    #[test]
    fn start_replication_rejects_null_in_option_key() {
        let err =
            build_start_replication_sql("slot", 0, &[("key\0x", "value")]).unwrap_err();
        assert!(err.to_string().contains("null bytes"));
    }

    #[test]
    fn start_replication_rejects_null_in_option_value() {
        let err =
            build_start_replication_sql("slot", 0, &[("key", "val\0ue")]).unwrap_err();
        assert!(err.to_string().contains("null bytes"));
    }

    #[test]
    fn start_physical_rejects_null_in_slot_name() {
        let err = build_start_physical_replication_sql(Some("slot\0x"), 0, None).unwrap_err();
        assert!(err.to_string().contains("null bytes"));
    }

    #[test]
    fn base_backup_rejects_null_in_label() {
        let opts = BaseBackupOptions {
            label: Some("label\0x".to_string()),
            ..Default::default()
        };
        let err = build_base_backup_sql(&opts).unwrap_err();
        assert!(err.to_string().contains("null bytes"));
    }

    #[test]
    fn base_backup_rejects_null_in_target() {
        let opts = BaseBackupOptions {
            target: Some("target\0x".to_string()),
            ..Default::default()
        };
        let err = build_base_backup_sql(&opts).unwrap_err();
        assert!(err.to_string().contains("null bytes"));
    }

    #[test]
    fn base_backup_rejects_null_in_compression() {
        let opts = BaseBackupOptions {
            compression: Some("gzip\0x".to_string()),
            ..Default::default()
        };
        let err = build_base_backup_sql(&opts).unwrap_err();
        assert!(err.to_string().contains("null bytes"));
    }

    #[test]
    fn create_subscription_rejects_null_in_name() {
        let opts = CreateSubscriptionOptions {
            subscription_name: "sub\0x",
            connection_string: "host=localhost",
            publication: "pub",
            slot_name: "slot",
            ..Default::default()
        };
        let err = build_create_subscription_sql(&opts).unwrap_err();
        assert!(err.to_string().contains("null bytes"));
    }

    #[test]
    fn create_subscription_rejects_null_in_connection() {
        let opts = CreateSubscriptionOptions {
            subscription_name: "sub",
            connection_string: "host=\0localhost",
            publication: "pub",
            slot_name: "slot",
            ..Default::default()
        };
        let err = build_create_subscription_sql(&opts).unwrap_err();
        assert!(err.to_string().contains("null bytes"));
    }

    #[test]
    fn create_subscription_rejects_null_in_publication() {
        let opts = CreateSubscriptionOptions {
            subscription_name: "sub",
            connection_string: "host=localhost",
            publication: "pub\0x",
            slot_name: "slot",
            ..Default::default()
        };
        let err = build_create_subscription_sql(&opts).unwrap_err();
        assert!(err.to_string().contains("null bytes"));
    }

    #[test]
    fn create_subscription_rejects_null_in_slot_name() {
        let opts = CreateSubscriptionOptions {
            subscription_name: "sub",
            connection_string: "host=localhost",
            publication: "pub",
            slot_name: "slot\0x",
            ..Default::default()
        };
        let err = build_create_subscription_sql(&opts).unwrap_err();
        assert!(err.to_string().contains("null bytes"));
    }

    #[test]
    fn disable_subscription_rejects_null() {
        let err = build_disable_subscription_sql("sub\0x").unwrap_err();
        assert!(err.to_string().contains("null bytes"));
    }

    #[test]
    fn detach_slot_rejects_null() {
        let err = build_detach_slot_sql("sub\0x").unwrap_err();
        assert!(err.to_string().contains("null bytes"));
    }

    #[test]
    fn drop_subscription_rejects_null() {
        let err = build_drop_subscription_sql("sub\0x").unwrap_err();
        assert!(err.to_string().contains("null bytes"));
    }

    // ── Compatibility with existing behavior ─────────────────────────────

    #[test]
    fn quote_ident_matches_legacy_behavior() {
        let cases = [
            "my_slot",
            r#"a"b"#,
            r#"a""b"#,
            "",
            "slot; DROP TABLE users; --",
            r#"evil"PHYSICAL"#,
        ];
        for input in cases {
            let legacy = format!("\"{}\"", input.replace('"', "\"\""));
            assert_eq!(
                quote_ident(input).unwrap(),
                legacy,
                "mismatch for input: {input:?}"
            );
        }
    }

    #[test]
    fn quote_literal_matches_legacy_behavior() {
        let cases = [
            "test_value",
            "test'value",
            "test'value'with'quotes",
            "'; DROP TABLE users; --",
            "",
            "'",
            "''",
        ];
        for input in cases {
            let legacy = format!("'{}'", input.replace('\'', "''"));
            assert_eq!(
                quote_literal(input).unwrap(),
                legacy,
                "mismatch for input: {input:?}"
            );
        }
    }
}
