//! Error types for PostgreSQL logical replication operations
//!
//! This module provides error types specifically for replication protocol
//! operations, connection handling, and message parsing.

use thiserror::Error;

/// Comprehensive error types for replication operations
#[derive(Error, Debug)]
pub enum ReplicationError {
    /// Protocol parsing errors
    #[error("Protocol parsing error: {0}")]
    Protocol(String),

    /// Buffer operation errors
    #[error("Buffer error: {0}")]
    Buffer(String),

    /// Connection errors that can be retried (transient)
    #[error("Transient connection error: {0}")]
    TransientConnection(String),

    /// Connection errors that should not be retried (permanent)
    #[error("Permanent connection error: {0}")]
    PermanentConnection(String),

    /// Replication connection errors
    #[error("Replication connection error: {0}")]
    ReplicationConnection(String),

    /// Authentication errors
    #[error("Authentication failed: {0}")]
    Authentication(String),

    /// Replication slot errors
    #[error("Replication slot error: {0}")]
    ReplicationSlot(String),

    /// Timeout errors
    #[error("Operation timed out: {0}")]
    Timeout(String),

    /// Operation cancelled errors
    #[error("Operation was cancelled: {0}")]
    Cancelled(String),

    /// Configuration errors
    #[error("Configuration error: {0}")]
    Config(String),

    /// IO errors
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// String conversion errors (from CString operations)
    #[error("String conversion error: {0}")]
    StringConversion(#[from] std::ffi::NulError),

    /// Generic replication errors
    #[error("Replication error: {0}")]
    Generic(String),
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
    fn test_result_type() {
        let ok_result: Result<i32> = Ok(42);
        assert_eq!(ok_result.expect("should be ok"), 42);

        let err_result: Result<i32> = Err(ReplicationError::protocol("test"));
        assert!(err_result.is_err());
    }
}
