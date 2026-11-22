#![allow(dead_code)]

//! Helpers shared by the live-PostgreSQL integration suites.
//!
//! Lives in a subdirectory on purpose: CI enumerates `integration-tests/*.rs`
//! as suites, so a flat `common.rs` would be run (and fail) as one.

use pg_walstream::PgReplicationConnection;
use std::time::Duration;

pub fn replication_conn_string() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/test_walstream?replication=database"
            .to_string()
    })
}

/// Remove the `replication=database` parameter from `url`, keeping every other
/// parameter and a well-formed query string whichever position it occupied.
///
/// The query starts at the first `?` after the userinfo: like libpq, userinfo
/// ends at the first `@` or `/`, so a raw `?` in a password is not a query.
pub fn strip_replication_param(url: &str) -> String {
    let authority = url.find("://").map_or(0, |i| i + 3);
    let from = match url[authority..].find(['@', '/']) {
        Some(i) if url.as_bytes()[authority + i] == b'@' => authority + i + 1,
        _ => authority,
    };
    let Some(q) = url[from..].find('?').map(|i| from + i) else {
        return url.to_string();
    };
    let (base, query) = (&url[..q], &url[q + 1..]);
    let kept: Vec<&str> = query
        .split('&')
        .filter(|p| *p != "replication=database")
        .collect();
    if kept.is_empty() {
        base.to_string()
    } else {
        format!("{base}?{}", kept.join("&"))
    }
}

pub fn regular_conn_string() -> String {
    std::env::var("DATABASE_URL_REGULAR")
        .unwrap_or_else(|_| strip_replication_param(&replication_conn_string()))
}

pub fn regular_conn() -> PgReplicationConnection {
    PgReplicationConnection::connect(&regular_conn_string()).expect("regular connection")
}

pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();
}

pub fn ca_cert_path() -> String {
    std::env::var("SSL_CA_CERT_PATH").expect("SSL_CA_CERT_PATH must be set for SSL tests")
}

/// First cell of the first row; panics if the query fails.
pub fn value(conn: &mut PgReplicationConnection, sql: &str) -> Option<String> {
    conn.exec(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .get_value(0, 0)
}

/// Swallows every error: a slot that cannot be dropped is not a test failure.
pub fn drop_slot_best_effort(slot_name: &str) {
    if let Ok(mut conn) = PgReplicationConnection::connect(&replication_conn_string()) {
        let _ = conn.exec(&format!(
            "SELECT pg_drop_replication_slot('{slot_name}') \
             WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot_name}')"
        ));
    }
}

/// Poll until the slot's `active` flag clears. The walsender exits
/// asynchronously after the stream is dropped, and dropping a still-active slot
/// raises 55006 — which `drop_slot_best_effort` swallows, leaking the slot.
/// Bounded at 5s.
fn wait_for_slot_inactive(slot_name: &str) {
    let Ok(mut conn) = PgReplicationConnection::connect(&replication_conn_string()) else {
        return;
    };
    for _ in 0..50 {
        let active = conn
            .exec(&format!(
                "SELECT active FROM pg_replication_slots WHERE slot_name = '{slot_name}'"
            ))
            .ok()
            .and_then(|r| r.get_value(0, 0))
            .is_some_and(|v| v == "t");
        if !active {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Drops its slot on `Drop` so a panicking assertion never leaks it (an orphaned
/// slot pins WAL and can exhaust server disk). Does not wait for the walsender
/// to release the slot, so the stream must already be gone.
pub struct SlotGuard<S: AsRef<str>>(pub S);

impl<S: AsRef<str>> Drop for SlotGuard<S> {
    fn drop(&mut self) {
        drop_slot_best_effort(self.0.as_ref());
    }
}

/// Like [`SlotGuard`], but first waits for the walsender to release the slot.
/// `Drop` is sync, hence the blocking wait.
pub struct WaitingSlotGuard(pub &'static str);

impl Drop for WaitingSlotGuard {
    fn drop(&mut self) {
        wait_for_slot_inactive(self.0);
        drop_slot_best_effort(self.0);
    }
}

/// Drops its slot with a suite-supplied `drop_slot` however the test leaves.
///
/// Without it a failed assertion leaks the slot (the trailing `drop_slot` is
/// unreachable once a panic unwinds), and enough leaks exhaust
/// `max_replication_slots` into a run of `53400 all replication slots are in
/// use` failures that bury the assertion that actually broke.
///
/// Declare it *before* the stream it must outlive, so the stream is dropped (and
/// the slot inactive) by the time `drop_slot` runs.
///
/// The suites' `drop_slot` retries and panics when the slot survives; a panic
/// inside `Drop` during unwind aborts the process, so it is swallowed when we
/// are already unwinding.
pub struct UnwindSafeSlotGuard {
    slot: &'static str,
    drop_slot: fn(&str),
}

impl UnwindSafeSlotGuard {
    pub fn new(slot: &'static str, drop_slot: fn(&str)) -> Self {
        Self { slot, drop_slot }
    }
}

impl Drop for UnwindSafeSlotGuard {
    fn drop(&mut self) {
        let (slot, drop_slot) = (self.slot, self.drop_slot);
        if std::thread::panicking() {
            let _ = std::panic::catch_unwind(|| drop_slot(slot));
        } else {
            drop_slot(slot);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::strip_replication_param;

    #[test]
    fn strip_replication_param_cases() {
        for (url, want) in [
            ("postgresql://u:p@h/db", "postgresql://u:p@h/db"),
            (
                "postgresql://u:p@h/db?replication=database",
                "postgresql://u:p@h/db",
            ),
            (
                "postgresql://u:p@h/db?replication=database&sslmode=require",
                "postgresql://u:p@h/db?sslmode=require",
            ),
            (
                "postgresql://u:p@h/db?a=1&replication=database&b=2",
                "postgresql://u:p@h/db?a=1&b=2",
            ),
            (
                "postgresql://u:p@h/db?sslmode=require&replication=database",
                "postgresql://u:p@h/db?sslmode=require",
            ),
            // A raw `?` in userinfo is part of the password, not a query start.
            ("postgresql://user:p?ss@h/db", "postgresql://user:p?ss@h/db"),
            (
                "postgresql://user:p?ss@h/db?replication=database",
                "postgresql://user:p?ss@h/db",
            ),
            (
                "postgresql://user:p?ss@h/db?replication=database&sslmode=require",
                "postgresql://user:p?ss@h/db?sslmode=require",
            ),
            // A literal `@` in the query is not a credentials designator.
            (
                "postgresql://h/db?application_name=svc@prod&replication=database",
                "postgresql://h/db?application_name=svc@prod",
            ),
        ] {
            assert_eq!(strip_replication_param(url), want, "{url}");
        }
    }
}
