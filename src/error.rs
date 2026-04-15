//! Error types for PostgreSQL logical replication operations
//!
//! This module provides error types specifically for replication protocol
//! operations, connection handling, and message parsing.

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

    /// IO errors
    Io(std::io::Error),

    /// String conversion errors (from CString operations)
    StringConversion(std::ffi::NulError),

    /// Generic replication errors
    Generic(String),
}

impl std::fmt::Display for ReplicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
            Self::Io(err) => write!(f, "IO error: {err}"),
            Self::StringConversion(err) => write!(f, "String conversion error: {err}"),
            Self::Generic(msg) => write!(f, "Replication error: {msg}"),
        }
    }
}

impl std::error::Error for ReplicationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::StringConversion(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ReplicationError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<std::ffi::NulError> for ReplicationError {
    fn from(err: std::ffi::NulError) -> Self {
        Self::StringConversion(err)
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
    pub fn replication_connection<S: Into<String>>(msg: S) -> Self {
        ReplicationError::ReplicationConnection(msg.into())
    }

    /// Create a new connection error (alias for replication_connection)
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

    /// Create a new generic error
    pub fn generic<S: Into<String>>(msg: S) -> Self {
        ReplicationError::Generic(msg.into())
    }

    /// Check if the error is transient (can be retried)
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            ReplicationError::TransientConnection(_)
                | ReplicationError::Timeout(_)
                | ReplicationError::Io(_)
                | ReplicationError::ReplicationConnection(_)
        )
    }

    /// Check if the error is permanent (should not be retried)
    pub fn is_permanent(&self) -> bool {
        matches!(
            self,
            ReplicationError::PermanentConnection(_)
                | ReplicationError::Authentication(_)
                | ReplicationError::ReplicationSlot(_)
        )
    }

    /// Check if the error is due to cancellation
    pub fn is_cancelled(&self) -> bool {
        matches!(self, ReplicationError::Cancelled(_))
    }
}

/// Result type for replication operations
pub type Result<T> = std::result::Result<T, ReplicationError>;

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

    #[test]
    fn test_config_error() {
        let err = ReplicationError::config("invalid config");
        assert!(!err.is_transient());
        assert!(!err.is_permanent());
    }

    #[test]
    fn test_generic_error() {
        let err = ReplicationError::generic("something went wrong");
        match err {
            ReplicationError::Generic(msg) => assert_eq!(msg, "something went wrong"),
            _ => panic!("Expected Generic error"),
        }
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
    fn test_replication_connection_display() {
        let err = ReplicationError::replication_connection("slot error");
        assert_eq!(err.to_string(), "Replication connection error: slot error");
        assert!(err.is_transient());
        assert!(!err.is_permanent());
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
        assert!(!err.is_permanent());
        assert!(!err.is_cancelled());
    }

    #[test]
    fn test_generic_error_display() {
        let err = ReplicationError::generic("unknown issue");
        assert_eq!(err.to_string(), "Replication error: unknown issue");
        assert!(!err.is_transient());
        assert!(!err.is_permanent());
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
}
