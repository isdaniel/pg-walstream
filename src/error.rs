//! Error types for PostgreSQL logical replication operations
//!
//! This module provides error types specifically for replication protocol
//! operations, connection handling, and message parsing.

use crate::prelude::*;
use crate::types::Lsn;

/// Comprehensive error types for replication operations
#[derive(Debug)]
pub enum ReplicationError {
    /// Protocol parsing errors
    Protocol(String),

    /// Buffer operation errors
    Buffer(String),

    /// Connection errors that can be retried (transient)
    TransientConnection(String),

    /// Connection errors that should not be retried (permanent)
    PermanentConnection(String),

    /// Replication connection errors
    ReplicationConnection(String),

    /// Authentication errors
    Authentication(String),

    /// Replication slot errors
    ReplicationSlot(String),

    /// Timeout errors
    Timeout(String),

    /// Operation cancelled errors
    Cancelled(String),

    /// Configuration errors
    Config(String),

    #[cfg(feature = "std")]
    /// IO errors
    Io(std::io::Error),

    #[cfg(feature = "std")]
    /// String conversion errors (from CString operations)
    StringConversion(std::ffi::NulError),

    /// Deserialization errors (when converting RowData to user types)
    Deserialize(String),

    /// Native (rustls-tls) backend worker thread failure: the thread could not
    /// be spawned, exited early, or dropped a reply. Transient so the stream
    /// retry logic can reconnect.
    Backend(String),

    /// Bounded replay reached its configured `stop_at_lsn`. This is a clean, expected terminal signal (treated like a graceful end of stream), never retried.
    ///
    /// Carries the LSN at which streaming stopped. That is the crossing transaction's commit `end_lsn` in the usual case; when the target lies *beyond* the last commit it is instead the server's send position as reported by a keepalive, which is still a valid resume point (nothing at or below it is undelivered). On the raw path ([`LogicalReplicationStream::next_raw_event`](crate::LogicalReplicationStream::next_raw_event)) it is the transport-level `wal_end` and may fall mid-transaction.
    StreamStopped(Lsn),
}

impl core::fmt::Display for ReplicationError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Protocol(msg) => write!(f, "Protocol parsing error: {msg}"),
            Self::Buffer(msg) => write!(f, "Buffer error: {msg}"),
            Self::TransientConnection(msg) => write!(f, "Transient connection error: {msg}"),
            Self::PermanentConnection(msg) => write!(f, "Permanent connection error: {msg}"),
            Self::ReplicationConnection(msg) => write!(f, "Replication connection error: {msg}"),
            Self::Authentication(msg) => write!(f, "Authentication failed: {msg}"),
            Self::ReplicationSlot(msg) => write!(f, "Replication slot error: {msg}"),
            Self::Timeout(msg) => write!(f, "Operation timed out: {msg}"),
            Self::Cancelled(msg) => write!(f, "Operation was cancelled: {msg}"),
            Self::Config(msg) => write!(f, "Configuration error: {msg}"),
            #[cfg(feature = "std")]
            Self::Io(err) => write!(f, "IO error: {err}"),
            #[cfg(feature = "std")]
            Self::StringConversion(err) => write!(f, "String conversion error: {err}"),
            Self::Deserialize(msg) => write!(f, "Deserialization error: {msg}"),
            Self::Backend(msg) => write!(f, "Backend worker error: {msg}"),
            Self::StreamStopped(lsn) => {
                write!(f, "Replication stopped at stop_at_lsn (reached {lsn})")
            }
        }
    }
}

impl core::error::Error for ReplicationError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            #[cfg(feature = "std")]
            Self::Io(err) => Some(err),
            #[cfg(feature = "std")]
            Self::StringConversion(err) => Some(err),
            _ => None,
        }
    }
}

#[cfg(feature = "std")]
impl From<std::io::Error> for ReplicationError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

#[cfg(feature = "std")]
impl From<std::ffi::NulError> for ReplicationError {
    fn from(err: std::ffi::NulError) -> Self {
        Self::StringConversion(err)
    }
}

impl serde::de::Error for ReplicationError {
    fn custom<T: core::fmt::Display>(msg: T) -> Self {
        ReplicationError::Deserialize(msg.to_string())
    }
}

impl ReplicationError {
    /// Create a new protocol error
    pub fn protocol<S: Into<String>>(msg: S) -> Self {
        ReplicationError::Protocol(msg.into())
    }

    /// Create a new buffer error
    pub fn buffer<S: Into<String>>(msg: S) -> Self {
        ReplicationError::Buffer(msg.into())
    }

    /// Create a new transient connection error (can be retried)
    pub fn transient_connection<S: Into<String>>(msg: S) -> Self {
        ReplicationError::TransientConnection(msg.into())
    }

    /// Create a new permanent connection error (should not be retried)
    pub fn permanent_connection<S: Into<String>>(msg: S) -> Self {
        ReplicationError::PermanentConnection(msg.into())
    }

    /// Create a new replication connection error
    pub fn connection<S: Into<String>>(msg: S) -> Self {
        ReplicationError::ReplicationConnection(msg.into())
    }

    /// Create a new authentication error
    pub fn authentication<S: Into<String>>(msg: S) -> Self {
        ReplicationError::Authentication(msg.into())
    }

    /// Create a new replication slot error
    pub fn replication_slot<S: Into<String>>(msg: S) -> Self {
        ReplicationError::ReplicationSlot(msg.into())
    }

    /// Classify a server error by its SQLSTATE.
    ///
    /// Codes that retrying can never fix become permanent variants (which
    /// [`is_permanent`](Self::is_permanent) reports as `true`, stopping the retry
    /// loop); everything else stays `Protocol` so the streaming layer keeps
    /// retrying.
    ///
    /// Terminal for the slot — [`ReplicationSlot`](Self::ReplicationSlot):
    /// - `55000` *object_not_in_prerequisite_state* — the slot was invalidated
    ///   (`wal_removed`, `rows_removed`, `wal_level_insufficient`, or PG18's
    ///   `idle_timeout`), or `wal_level` is below `logical`. Recovery requires
    ///   dropping the slot and re-syncing; retrying cannot help.
    /// - `42704` *undefined_object* — the slot does not exist.
    ///
    /// Terminal for the credentials — [`Authentication`](Self::Authentication):
    /// - `28000` *invalid_authorization_specification*, `28P01` *invalid_password*.
    ///
    /// Terminal for the request — [`PermanentConnection`](Self::PermanentConnection):
    /// - `42501` *insufficient_privilege* (e.g. "must be superuser or replication
    ///   role", "permission denied for publication"),
    /// - `3D000` *invalid_catalog_name* — the database does not exist,
    /// - `42601` *syntax_error* — we generated SQL the server cannot parse,
    /// - `22023` *invalid_parameter_value* — pgoutput rejected an output-plugin
    ///   option in `parse_output_parameters`; a retry re-sends byte-identical
    ///   options.
    /// - `F0000` *config_file_error* — e.g. "client certificates can only be
    ///   checked if a root certificate store is available" (`auth.c`); a server
    ///   misconfiguration that reconnecting cannot change.
    ///
    /// Deliberately **not** terminal, despite looking it:
    /// - `0A000` *feature_not_supported* — on PG ≤ 15 this is "logical decoding
    ///   cannot be used while in recovery" (`CheckLogicalDecodingRequirements`),
    ///   which resolves the moment a standby is promoted; marking it permanent
    ///   would break failover. PG 16 replaced that branch with `55000` when logical
    ///   decoding on standby landed, so on PG 16+ the `0A000`s a logical client can
    ///   still reach (a proto_version mismatch in `pgoutput.c`, `READ_REPLICATION_SLOT`
    ///   on a logical slot, conflicting column lists) are genuinely terminal and this
    ///   carve-out merely costs a bounded retry loop before they surface.
    ///
    /// Explicitly transient — [`TransientConnection`](Self::TransientConnection):
    /// - `57P01`/`57P02`/`57P03` (admin shutdown, crash shutdown, cannot connect
    ///   now), `53300` (too many connections), `55006` *object_in_use* ("replication
    ///   slot is already active for PID"), `40001` (serialization failure), and the
    ///   `08xxx` codes a client can actually receive: `08000`, `08003`, `08006`,
    ///   `08P01`. `08P01` *protocol_violation* is included not on its own merits —
    ///   a retry re-sends the same bytes — but because PgBouncer uses it as its
    ///   default SQLSTATE for every pooler error it forwards, including a routine
    ///   "server shutting down"; classifying it permanent would hard-kill consumers
    ///   on a pooler bounce.
    ///
    /// These must NOT fall through to `Protocol`. A walsender killed by
    /// `pg_terminate_backend()` reports 57P01 as an ErrorResponse *inside the open
    /// COPY stream* (`ProcessInterrupts` in postgres.c), on a socket that is still
    /// live — so `next_event_with_retry`'s "unrecoverable decode error on a live
    /// connection" guard would classify a routine restart as terminal and end the
    /// consumer instead of reconnecting. (Arming `recover_connection` is a separate
    /// mechanism: both backends drop their liveness flag on any non-cancelled error
    /// out of the COPY read path, regardless of variant. This classification is what
    /// keeps the error away from that guard and what `is_transient` reports to
    /// consumers.)
    ///
    /// Match on the code rather than the message: PostgreSQL 18 reworded slot
    /// invalidation from "can no longer get changes from" to "can no longer
    /// access", but the SQLSTATE is stable across major versions.
    ///
    /// An empty `sqlstate` (no diagnostics available) yields a plain `Protocol`
    /// error with no prefix.
    ///
    /// Crate-internal, and gated on a connection backend for the same reason as
    /// [`Self::stream_stopped`]: only the backends parse server diagnostics.
    #[cfg(any(feature = "libpq", feature = "rustls-tls"))]
    pub(crate) fn from_sqlstate<S: Into<String>>(sqlstate: &str, msg: S) -> Self {
        let msg = msg.into();
        match sqlstate {
            "" => ReplicationError::Protocol(msg),
            "55000" | "42704" => ReplicationError::ReplicationSlot(format!("[{sqlstate}] {msg}")),
            "28000" | "28P01" => ReplicationError::Authentication(format!("[{sqlstate}] {msg}")),
            "42501" | "3D000" | "42601" | "22023" | "F0000" => {
                ReplicationError::PermanentConnection(format!("[{sqlstate}] {msg}"))
            }
            "57P01" | "57P02" | "57P03" | "53300" | "55006" | "40001" | "08000" | "08003"
            | "08006" | "08P01" => {
                ReplicationError::TransientConnection(format!("[{sqlstate}] {msg}"))
            }
            _ => ReplicationError::Protocol(format!("[{sqlstate}] {msg}")),
        }
    }

    /// [`from_sqlstate`](Self::from_sqlstate) for an ErrorResponse received during
    /// connection startup, before any replication command has been issued.
    ///
    /// Identical except that `55000` is **not** reported as
    /// [`ReplicationSlot`](Self::ReplicationSlot). At startup the server has not
    /// looked at a slot: `CheckMyDatabase` (postinit.c) raises `55000` for
    /// `database "%s" is not currently accepting connections` and
    /// `cannot connect to invalid database "%s"`. Routing those to `ReplicationSlot`
    /// — documented as "invalidated / does not exist, drop it and re-sync" — points
    /// an operator at a perfectly healthy slot. Both variants are permanent, so this
    /// changes the diagnosis, not the control flow.
    ///
    /// `rustls-tls` only: the libpq backend never sees a raw startup ErrorResponse
    /// — `PQconnectdb` handles the startup exchange itself.
    #[cfg(feature = "rustls-tls")]
    pub(crate) fn from_sqlstate_startup<S: Into<String>>(sqlstate: &str, msg: S) -> Self {
        let msg = msg.into();
        if sqlstate == "55000" {
            return ReplicationError::PermanentConnection(format!("[{sqlstate}] {msg}"));
        }
        Self::from_sqlstate(sqlstate, msg)
    }

    /// Create a new timeout error
    pub fn timeout<S: Into<String>>(msg: S) -> Self {
        ReplicationError::Timeout(msg.into())
    }

    /// Create a new cancellation error
    pub fn cancelled<S: Into<String>>(msg: S) -> Self {
        ReplicationError::Cancelled(msg.into())
    }

    /// Create a new configuration error
    pub fn config<S: Into<String>>(msg: S) -> Self {
        ReplicationError::Config(msg.into())
    }

    /// Create a new deserialization error
    pub fn deserialize<S: Into<String>>(msg: S) -> Self {
        ReplicationError::Deserialize(msg.into())
    }

    /// Create a new backend worker error
    pub fn backend<S: Into<String>>(msg: S) -> Self {
        ReplicationError::Backend(msg.into())
    }

    /// Create a bounded-replay terminal stop signal at the given LSN.
    ///
    /// Crate-internal: the library emits this; external consumers match the [`ReplicationError::StreamStopped`] variant rather than constructing it.
    ///
    /// Gated on a connection backend: the streaming layer that emits it  (`crate::stream`) is compiled only with `libpq`/`rustls-tls`, so this helper is dead code in any build without one — including `--features std` on its own.
    #[cfg(any(feature = "libpq", feature = "rustls-tls"))]
    pub(crate) fn stream_stopped(lsn: Lsn) -> Self {
        ReplicationError::StreamStopped(lsn)
    }

    /// Check if the error is transient (can be retried).
    ///
    /// Note: this is an advisory classification for consumers (logging, metrics, custom retry policies). It is NOT the predicate the library itself uses to drive retries — the streaming layer retries anything that is not [`is_permanent`](Self::is_permanent), [`is_cancelled`](Self::is_cancelled), the internal stream-stopped terminal, or a decode-stage failure on a still-live connection. That last exclusion covers exactly the "grey zone" variants (`Protocol`, `Buffer`, `Deserialize`): they return `false` here, and the stream retries them only once the connection is already dead — on a live one the offending frame has been consumed and retrying would silently skip it.
    pub fn is_transient(&self) -> bool {
        #[cfg(feature = "std")]
        if matches!(self, ReplicationError::Io(_)) {
            return true;
        }
        matches!(
            self,
            ReplicationError::TransientConnection(_)
                | ReplicationError::Timeout(_)
                | ReplicationError::ReplicationConnection(_)
                | ReplicationError::Backend(_)
        )
    }

    /// Check if the error is permanent (should not be retried)
    pub fn is_permanent(&self) -> bool {
        matches!(
            self,
            ReplicationError::PermanentConnection(_)
                | ReplicationError::Authentication(_)
                | ReplicationError::ReplicationSlot(_)
                | ReplicationError::Config(_)
        )
    }

    /// Check if the error is due to cancellation
    pub fn is_cancelled(&self) -> bool {
        matches!(self, ReplicationError::Cancelled(_))
    }

    /// Check if the error is the terminal bounded-replay stop signal.
    ///
    /// Gated on a connection backend for the same reason as [`Self::stream_stopped`]: its only caller lives in the backend-gated `crate::stream`.
    #[cfg(any(feature = "libpq", feature = "rustls-tls"))]
    pub(crate) fn is_stream_stopped(&self) -> bool {
        matches!(self, ReplicationError::StreamStopped(_))
    }
}

/// Result type for replication operations
pub type Result<T> = core::result::Result<T, ReplicationError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_error() {
        let err = ReplicationError::protocol("test error");
        assert_eq!(err.to_string(), "Protocol parsing error: test error");
        match err {
            ReplicationError::Protocol(msg) => assert_eq!(msg, "test error"),
            _ => panic!("Expected Protocol error"),
        }
    }

    #[test]
    fn test_buffer_error() {
        let err = ReplicationError::buffer("buffer overflow");
        match err {
            ReplicationError::Buffer(msg) => assert_eq!(msg, "buffer overflow"),
            _ => panic!("Expected Buffer error"),
        }
    }

    #[test]
    fn test_transient_connection_error() {
        let err = ReplicationError::transient_connection("connection lost");
        assert!(err.is_transient());
        assert!(!err.is_permanent());
        assert!(!err.is_cancelled());
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_io_error_is_transient() {
        let err = ReplicationError::Io(std::io::Error::other("disk hiccup"));
        assert!(err.is_transient());
        assert!(!err.is_permanent());
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_io_error_display() {
        let err = ReplicationError::Io(std::io::Error::other("boom"));
        assert_eq!(err.to_string(), "IO error: boom");
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_string_conversion_error_display() {
        let nul = std::ffi::CString::new("a\0b").unwrap_err();
        let err = ReplicationError::StringConversion(nul);
        assert!(err.to_string().starts_with("String conversion error:"));
    }

    #[test]
    fn test_permanent_connection_error() {
        let err = ReplicationError::permanent_connection("invalid host");
        assert!(!err.is_transient());
        assert!(err.is_permanent());
    }

    #[test]
    fn test_authentication_error() {
        let err = ReplicationError::authentication("invalid password");
        assert!(err.is_permanent());
        assert_eq!(err.to_string(), "Authentication failed: invalid password");
    }

    #[test]
    fn test_replication_slot_error() {
        let err = ReplicationError::replication_slot("slot not found");
        assert!(err.is_permanent());
    }

    #[test]
    fn test_timeout_error() {
        let err = ReplicationError::timeout("operation timed out");
        assert!(err.is_transient());
    }

    #[test]
    fn test_cancelled_error() {
        let err = ReplicationError::cancelled("user cancelled");
        assert!(err.is_cancelled());
        assert!(!err.is_transient());
        assert!(!err.is_permanent());
    }

    /// A rejected configuration fails identically on every attempt without ever
    /// opening a socket, so it is permanent — retrying it just burns the backoff
    /// budget. (It is still not `is_transient`, which is a narrower predicate.)
    #[test]
    fn test_config_error() {
        let err = ReplicationError::config("invalid config");
        assert!(!err.is_transient());
        assert!(err.is_permanent());
    }

    #[test]
    fn test_connection_alias() {
        let err = ReplicationError::connection("test");
        match err {
            ReplicationError::ReplicationConnection(_) => {}
            _ => panic!("Expected ReplicationConnection error"),
        }
    }

    #[test]
    fn test_connection_alias_display() {
        let err = ReplicationError::connection("connection lost");
        assert_eq!(
            err.to_string(),
            "Replication connection error: connection lost"
        );
    }

    #[test]
    fn test_replication_slot_display() {
        let err = ReplicationError::replication_slot("slot not found");
        assert_eq!(err.to_string(), "Replication slot error: slot not found");
    }

    #[test]
    fn test_cancelled_display() {
        let err = ReplicationError::cancelled("user cancelled");
        assert_eq!(err.to_string(), "Operation was cancelled: user cancelled");
    }

    #[test]
    fn test_config_error_display() {
        let err = ReplicationError::config("missing field");
        assert_eq!(err.to_string(), "Configuration error: missing field");
        assert!(!err.is_transient());
        assert!(err.is_permanent());
        assert!(!err.is_cancelled());
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: ReplicationError = io_err.into();
        assert!(err.is_transient());
        match err {
            ReplicationError::Io(_) => {}
            _ => panic!("Expected Io error"),
        }
    }

    #[test]
    fn test_nul_error_conversion() {
        let nul_err = std::ffi::CString::new("hello\0world").unwrap_err();
        let err: ReplicationError = nul_err.into();
        match err {
            ReplicationError::StringConversion(_) => {}
            _ => panic!("Expected StringConversion error"),
        }
    }

    #[test]
    fn test_error_display() {
        let err = ReplicationError::Protocol("test".to_string());
        assert!(format!("{err}").contains("Protocol parsing error"));

        let err = ReplicationError::Buffer("test".to_string());
        assert!(format!("{err}").contains("Buffer error"));

        let err = ReplicationError::Timeout("test".to_string());
        assert!(format!("{err}").contains("Operation timed out"));
    }

    #[test]
    fn test_result_type_alias() {
        let ok_result: Result<i32> = Ok(42);
        if let Ok(val) = ok_result {
            assert_eq!(val, 42);
        }

        let err_result: Result<i32> = Err(ReplicationError::protocol("test error"));
        assert!(err_result.is_err());
    }

    #[test]
    fn test_error_source() {
        use std::error::Error;

        // Io error should have a source
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: ReplicationError = io_err.into();
        assert!(err.source().is_some());

        // NulError should have a source
        let nul_err = std::ffi::CString::new("hello\0world").unwrap_err();
        let err: ReplicationError = nul_err.into();
        assert!(err.source().is_some());

        // String variants should not have a source
        let err = ReplicationError::protocol("test");
        assert!(err.source().is_none());
    }

    #[test]
    fn test_deserialize_error() {
        let err = ReplicationError::deserialize("field type mismatch");
        match err {
            ReplicationError::Deserialize(msg) => assert_eq!(msg, "field type mismatch"),
            _ => panic!("Expected Deserialize error"),
        }
    }

    #[test]
    fn test_deserialize_error_display() {
        let err = ReplicationError::deserialize("cannot parse 'abc' as u32");
        assert_eq!(
            err.to_string(),
            "Deserialization error: cannot parse 'abc' as u32"
        );
    }

    #[test]
    fn test_deserialize_error_classification() {
        let err = ReplicationError::deserialize("test");
        assert!(!err.is_transient());
        assert!(!err.is_permanent());
        assert!(!err.is_cancelled());
    }

    #[test]
    fn test_deserialize_error_source() {
        use std::error::Error;
        let err = ReplicationError::deserialize("test");
        assert!(err.source().is_none());
    }

    #[test]
    fn test_serde_de_error_custom() {
        use serde::de::Error;
        let err = ReplicationError::custom("serde custom error");
        match err {
            ReplicationError::Deserialize(msg) => assert_eq!(msg, "serde custom error"),
            _ => panic!("Expected Deserialize error from serde::de::Error::custom"),
        }
    }

    #[test]
    fn test_backend_error() {
        let err = ReplicationError::backend("worker thread is gone");
        match err {
            ReplicationError::Backend(ref msg) => assert_eq!(msg, "worker thread is gone"),
            _ => panic!("Expected Backend error"),
        }
        assert_eq!(
            err.to_string(),
            "Backend worker error: worker thread is gone"
        );
    }

    #[test]
    fn test_backend_error_is_transient() {
        let err = ReplicationError::backend("reply dropped");
        assert!(err.is_transient());
        assert!(!err.is_permanent());
        assert!(!err.is_cancelled());
    }
}

#[cfg(all(test, any(feature = "libpq", feature = "rustls-tls")))]
mod stop_signal_tests {
    use super::*;
    use crate::types::Lsn;

    /// The two slot-fatal SQLSTATEs must become permanent errors so the retry
    /// loop gives up instead of reconnecting into the same dead slot.
    #[test]
    fn test_from_sqlstate_slot_fatal_is_permanent() {
        for code in ["55000", "42704"] {
            let err = ReplicationError::from_sqlstate(code, "can no longer access slot");
            assert!(
                matches!(err, ReplicationError::ReplicationSlot(_)),
                "{code} should map to ReplicationSlot, got {err:?}"
            );
            assert!(err.is_permanent(), "{code} should be permanent");
            assert!(err.to_string().contains(code), "{err}");
        }
    }

    /// A busy slot (55006) or a transient server hiccup must stay retryable — and
    /// must be `TransientConnection`, not `Protocol`. A walsender killed by
    /// `pg_terminate_backend()` reports 57P01 as an ErrorResponse inside the open
    /// COPY stream, on a still-live socket; leaving it in `Protocol` made
    /// `next_event_with_retry`'s "unrecoverable decode error on a live connection"
    /// guard treat a routine restart as terminal, and left the native backend's
    /// `alive` flag set so recovery never armed.
    #[test]
    fn test_from_sqlstate_other_codes_stay_retryable() {
        for code in [
            "55006", "57P01", "57P02", "57P03", "53300", "40001", "08006",
        ] {
            let err = ReplicationError::from_sqlstate(code, "boom");
            assert!(
                matches!(err, ReplicationError::TransientConnection(_)),
                "{code} should be TransientConnection, got {err:?}"
            );
            assert!(!err.is_permanent(), "{code} should not be permanent");
            assert!(err.is_transient(), "{code} should report as transient");
            assert!(err.to_string().contains(code), "{err}");
        }
    }

    /// An unclassified code keeps the old `Protocol` fallback.
    #[test]
    fn from_sqlstate_unknown_code_falls_back_to_protocol() {
        let err = ReplicationError::from_sqlstate("XX999", "weird");
        assert!(matches!(err, ReplicationError::Protocol(_)), "{err:?}");
        assert!(!err.is_permanent());
    }

    /// Credential and request failures fail identically on every attempt, so they
    /// must stop the retry loop instead of hammering the server (and writing one
    /// failed-login line per attempt to its log).
    #[test]
    fn from_sqlstate_permanent_auth_and_request_codes() {
        for code in ["28000", "28P01"] {
            let err = ReplicationError::from_sqlstate(code, "password authentication failed");
            assert!(
                matches!(err, ReplicationError::Authentication(_)),
                "{code} should be Authentication, got {err:?}"
            );
            assert!(err.is_permanent(), "{code} must be permanent");
        }
        for code in ["42501", "3D000", "42601", "22023", "F0000"] {
            let err = ReplicationError::from_sqlstate(code, "nope");
            assert!(
                matches!(err, ReplicationError::PermanentConnection(_)),
                "{code} should be PermanentConnection, got {err:?}"
            );
            assert!(err.is_permanent(), "{code} must be permanent");
        }
    }

    /// At startup the server has not looked at a slot: `CheckMyDatabase` raises
    /// 55000 for `database "..." is not currently accepting connections`.
    /// Reporting that as `ReplicationSlot` — documented as "invalidated, drop it
    /// and re-sync" — sends an operator to destroy a healthy slot. Still
    /// permanent either way, so only the diagnosis changes.
    #[cfg(feature = "rustls-tls")]
    #[test]
    fn from_sqlstate_startup_does_not_blame_the_slot_for_55000() {
        let err = ReplicationError::from_sqlstate_startup(
            "55000",
            "database \"app\" is not currently accepting connections",
        );
        assert!(
            matches!(err, ReplicationError::PermanentConnection(_)),
            "{err:?}"
        );
        assert!(err.is_permanent());

        // Post-connect, 55000 still means the slot.
        assert!(matches!(
            ReplicationError::from_sqlstate("55000", "can no longer access slot"),
            ReplicationError::ReplicationSlot(_)
        ));

        // Every other code classifies identically on both paths.
        for code in ["28P01", "3D000", "57P03", "53300", "42704", "XX999"] {
            assert_eq!(
                core::mem::discriminant(&ReplicationError::from_sqlstate_startup(code, "m")),
                core::mem::discriminant(&ReplicationError::from_sqlstate(code, "m")),
                "{code} must classify the same at startup"
            );
        }
    }

    /// `0A000` looks permanent but PostgreSQL raises it from
    /// `CheckLogicalDecodingRequirements` for "logical decoding cannot be used
    /// while in recovery" — which clears when a standby is promoted. Classifying
    /// it permanent would break failover, so it must stay retryable.
    #[test]
    fn from_sqlstate_feature_not_supported_stays_retryable() {
        let err = ReplicationError::from_sqlstate(
            "0A000",
            "logical decoding cannot be used while in recovery",
        );
        assert!(matches!(err, ReplicationError::Protocol(_)), "{err:?}");
        assert!(
            !err.is_permanent(),
            "0A000 must stay retryable for failover"
        );
    }

    /// No diagnostics available: no empty `[]` prefix in the message.
    #[test]
    fn test_from_sqlstate_empty_code() {
        let err = ReplicationError::from_sqlstate("", "connection reset");
        assert!(matches!(err, ReplicationError::Protocol(_)));
        assert!(!err.to_string().contains('['), "{err}");
    }

    #[test]
    fn stream_stopped_is_terminal_not_retryable() {
        let e = ReplicationError::stream_stopped(Lsn::new(0x1A2B));
        assert!(e.is_stream_stopped(), "should report as stream-stopped");
        assert!(!e.is_transient(), "stop signal must never be retried");
        assert!(
            !e.is_permanent(),
            "stop signal is a graceful terminal, not a permanent error"
        );
        assert!(
            !e.is_cancelled(),
            "stop signal is distinct from cancellation"
        );
    }

    #[test]
    fn stream_stopped_displays_reached_lsn() {
        let e = ReplicationError::StreamStopped(Lsn::new(0x100));
        let s = format!("{e}");
        assert!(
            s.contains("stop"),
            "message should mention stopping, got: {s}"
        );
    }
}
