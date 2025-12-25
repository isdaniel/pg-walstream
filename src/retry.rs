//! Connection retry logic with exponential backoff
//!
//! This module provides retry mechanisms for PostgreSQL replication connections,
//! with configurable backoff strategies and error categorization.

use crate::connection::PgReplicationConnection;
use crate::error::{ReplicationError, Result};
use std::time::{Duration, Instant};
use tracing::{debug, error, info};

/// Configuration for retry logic
///
/// Defines the parameters for exponential backoff retry behavior when
/// connecting to PostgreSQL or recovering from transient failures.
///
/// # Example
///
/// ```
/// use pg_walstream::RetryConfig;
/// use std::time::Duration;
///
/// // Use default configuration (5 attempts, 1s to 60s delays)
/// let config = RetryConfig::default();
///
/// // Or create custom configuration
/// let custom_config = RetryConfig {
///     max_attempts: 10,
///     initial_delay: Duration::from_millis(500),
///     max_delay: Duration::from_secs(30),
///     multiplier: 2.0,
///     max_duration: Duration::from_secs(600),
///     jitter: true,
/// };
/// ```
#[derive(Debug, Copy, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts before giving up
    pub max_attempts: u32,
    /// Initial delay before the first retry
    pub initial_delay: Duration,
    /// Maximum delay between retries (caps exponential growth)
    pub max_delay: Duration,
    /// Multiplier for exponential backoff (typically 2.0)
    pub multiplier: f64,
    /// Maximum total duration for all retry attempts
    pub max_duration: Duration,
    /// Whether to add random jitter to delays (reduces thundering herd)
    pub jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
            max_duration: Duration::from_secs(300),
            jitter: true,
        }
    }
}

/// Custom exponential backoff implementation
#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    initial_delay: Duration,
    max_delay: Duration,
    multiplier: f64,
    jitter: bool,
    current_delay: Duration,
    attempt: u32,
}

impl ExponentialBackoff {
    /// Create a new exponential backoff instance
    ///
    /// Initializes the backoff state machine with parameters from the provided
    /// RetryConfig. The backoff starts at `initial_delay` and grows by `multiplier`
    /// on each call to `next_delay()`, up to `max_delay`.
    ///
    /// # Arguments
    ///
    /// * `config` - Retry configuration specifying backoff parameters
    ///
    /// # Example
    ///
    /// ```
    /// use pg_walstream::{ExponentialBackoff, RetryConfig};
    ///
    /// let config = RetryConfig::default();
    /// let mut backoff = ExponentialBackoff::new(&config);
    ///
    /// let delay1 = backoff.next_delay();
    /// let delay2 = backoff.next_delay();
    /// assert!(delay2 > delay1); // Delay increases exponentially
    /// ```
    pub fn new(config: &RetryConfig) -> Self {
        Self {
            initial_delay: config.initial_delay,
            max_delay: config.max_delay,
            multiplier: config.multiplier,
            jitter: config.jitter,
            current_delay: config.initial_delay,
            attempt: 0,
        }
    }

    /// Get the next delay duration
    ///
    /// Returns the current delay and advances the internal state for the next call.
    /// Each call increases the delay by the configured multiplier until it reaches
    /// the maximum delay. Optional jitter is applied to prevent thundering herd problems.
    ///
    /// # Returns
    ///
    /// Duration to wait before the next retry attempt.
    ///
    /// # Example
    ///
    /// ```
    /// use pg_walstream::{ExponentialBackoff, RetryConfig};
    /// use std::time::Duration;
    ///
    /// let config = RetryConfig {
    ///     max_attempts: 5,
    ///     initial_delay: Duration::from_millis(100),
    ///     max_delay: Duration::from_secs(10),
    ///     multiplier: 2.0,
    ///     max_duration: Duration::from_secs(60),
    ///     jitter: false,
    /// };
    ///
    /// let mut backoff = ExponentialBackoff::new(&config);
    /// let d1 = backoff.next_delay(); // ~100ms
    /// let d2 = backoff.next_delay(); // ~200ms
    /// let d3 = backoff.next_delay(); // ~400ms
    /// ```
    pub fn next_delay(&mut self) -> Duration {
        let delay = self.current_delay;

        // Calculate next delay with exponential backoff
        let next_delay_ms = (self.current_delay.as_millis() as f64 * self.multiplier) as u64;
        self.current_delay = Duration::from_millis(next_delay_ms).min(self.max_delay);
        self.attempt += 1;

        // Add jitter if enabled
        if self.jitter {
            self.add_jitter(delay)
        } else {
            delay
        }
    }

    /// Add jitter to the delay (±30% randomization)
    fn add_jitter(&self, delay: Duration) -> Duration {
        // Simple jitter implementation without external dependencies
        // Use current time as a simple source of randomness
        let now = Instant::now();
        let nanos = now.elapsed().subsec_nanos();
        let jitter_factor = 0.3;

        // Calculate jitter as ±30% of the delay
        let base_millis = delay.as_millis() as f64;
        let jitter_range = base_millis * jitter_factor;
        let jitter = (nanos % 1000) as f64 / 1000.0; // 0.0 to 1.0
        let jitter_adjustment = (jitter - 0.5) * 2.0 * jitter_range; // -jitter_range to +jitter_range

        let final_millis = (base_millis + jitter_adjustment).max(0.0) as u64;
        Duration::from_millis(final_millis)
    }

    /// Reset the backoff to initial state
    pub fn reset(&mut self) {
        self.current_delay = self.initial_delay;
        self.attempt = 0;
    }

    /// Get current attempt number
    pub fn attempt(&self) -> u32 {
        self.attempt
    }
}

impl RetryConfig {
    /// Create an exponential backoff policy from retry configuration
    pub fn to_backoff(&self) -> ExponentialBackoff {
        ExponentialBackoff::new(self)
    }
}

/// Retry wrapper for PostgreSQL replication connection operations
pub struct ReplicationConnectionRetry {
    config: RetryConfig,
    connection_string: String,
}

impl ReplicationConnectionRetry {
    /// Create a new retry wrapper
    pub fn new(config: RetryConfig, connection_string: String) -> Self {
        Self {
            config,
            connection_string,
        }
    }

    /// Retry connection establishment with exponential backoff
    ///
    /// Attempts to establish a PostgreSQL replication connection with automatic
    /// retry logic. Uses exponential backoff with optional jitter to handle
    /// transient connection failures gracefully.
    ///
    /// # Returns
    ///
    /// Returns a connected `PgReplicationConnection` on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - All retry attempts are exhausted
    /// - Maximum duration is exceeded
    /// - A permanent error occurs (authentication, unsupported version, etc.)
    ///
    /// # Example
    ///
    /// ```no_run
    /// use pg_walstream::{ReplicationConnectionRetry, RetryConfig};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let retry_handler = ReplicationConnectionRetry::new(
    ///     RetryConfig::default(),
    ///     "postgresql://postgres:password@localhost/mydb?replication=database".to_string(),
    /// );
    ///
    /// let connection = retry_handler.connect_with_retry().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect_with_retry(&self) -> Result<PgReplicationConnection> {
        let start_time = Instant::now();
        let mut backoff = self.config.to_backoff();

        info!("Attempting to connect to PostgreSQL with retry logic");

        for attempt in 1..=self.config.max_attempts {
            debug!(
                "Attempting PostgreSQL connection (attempt {}/{})",
                attempt, self.config.max_attempts
            );

            // Check if we've exceeded the maximum duration
            if start_time.elapsed() >= self.config.max_duration {
                error!(
                    "Connection attempts exceeded maximum duration ({:?})",
                    self.config.max_duration
                );
                return Err(ReplicationError::connection(format!(
                    "Connection attempts exceeded maximum duration of {:?}",
                    self.config.max_duration
                )));
            }

            match PgReplicationConnection::connect(&self.connection_string) {
                Ok(conn) => {
                    let elapsed = start_time.elapsed();
                    info!(
                        "Successfully connected to PostgreSQL on attempt {} after {:?}",
                        attempt, elapsed
                    );
                    return Ok(conn);
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    error!("Connection attempt {} failed: {}", attempt, error_msg);

                    // If this is the last attempt, return the error
                    if attempt >= self.config.max_attempts {
                        let elapsed = start_time.elapsed();
                        error!("All connection attempts failed after {:?}", elapsed);
                        return Err(e);
                    }

                    // Wait before the next attempt
                    let delay = backoff.next_delay();
                    debug!("Waiting {:?} before next attempt", delay);
                    tokio::time::sleep(delay).await;
                }
            }
        }

        // This should never be reached due to the loop logic above
        let elapsed = start_time.elapsed();
        error!("Connection failed after all attempts and {:?}", elapsed);
        Err(ReplicationError::connection(
            "Connection failed after all retry attempts".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exponential_backoff_basic() {
        let config = RetryConfig {
            max_attempts: 5,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
            max_duration: Duration::from_secs(300),
            jitter: false,
        };

        let mut backoff = ExponentialBackoff::new(&config);

        // First delay should be the initial delay
        let first_delay = backoff.next_delay();
        assert_eq!(first_delay, Duration::from_millis(100));
        assert_eq!(backoff.attempt(), 1);

        // Second delay should be doubled
        let second_delay = backoff.next_delay();
        assert_eq!(second_delay, Duration::from_millis(200));
        assert_eq!(backoff.attempt(), 2);

        // Third delay should be doubled again
        let third_delay = backoff.next_delay();
        assert_eq!(third_delay, Duration::from_millis(400));
        assert_eq!(backoff.attempt(), 3);
    }

    #[test]
    fn test_exponential_backoff_max_delay() {
        let config = RetryConfig {
            max_attempts: 10,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(500), // Low max delay for testing
            multiplier: 2.0,
            max_duration: Duration::from_secs(300),
            jitter: false,
        };

        let mut backoff = ExponentialBackoff::new(&config);

        // Skip to a point where we should hit the max delay
        backoff.next_delay(); // 100ms
        backoff.next_delay(); // 200ms
        backoff.next_delay(); // 400ms
        let delay = backoff.next_delay(); // Should be capped at 500ms, not 800ms

        // The current delay should be capped at max_delay
        assert!(delay <= Duration::from_millis(500));
    }

    #[test]
    fn test_exponential_backoff_reset() {
        let config = RetryConfig {
            max_attempts: 5,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
            max_duration: Duration::from_secs(300),
            jitter: false,
        };

        let mut backoff = ExponentialBackoff::new(&config);

        // Make a few attempts
        backoff.next_delay();
        backoff.next_delay();
        assert_eq!(backoff.attempt(), 2);

        // Reset should go back to initial state
        backoff.reset();
        assert_eq!(backoff.attempt(), 0);

        let delay = backoff.next_delay();
        assert_eq!(delay, Duration::from_millis(100));
        assert_eq!(backoff.attempt(), 1);
    }

    #[test]
    fn test_exponential_backoff_jitter() {
        let config = RetryConfig {
            max_attempts: 5,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
            max_duration: Duration::from_secs(300),
            jitter: true,
        };

        let mut backoff = ExponentialBackoff::new(&config);

        let delay = backoff.next_delay();
        // With jitter, the delay should be around 100ms but not exactly 100ms
        // Allow for ±30% jitter range
        assert!(delay >= Duration::from_millis(70));
        assert!(delay <= Duration::from_millis(130));
    }

    #[test]
    fn test_max_attempts() {
        let config = RetryConfig {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
            max_duration: Duration::from_secs(300),
            jitter: false,
        };

        let mut backoff = ExponentialBackoff::new(&config);

        backoff.next_delay();
        assert_eq!(backoff.attempt(), 1);
        backoff.next_delay();
        assert_eq!(backoff.attempt(), 2);
        backoff.next_delay();
        assert_eq!(backoff.attempt(), 3);

        // After 3 attempts, we've reached max_attempts
        assert_eq!(backoff.attempt(), config.max_attempts);
    }

    #[test]
    fn test_multiplier_effect() {
        let config = RetryConfig {
            max_attempts: 5,
            initial_delay: Duration::from_millis(10),
            max_delay: Duration::from_secs(60),
            multiplier: 3.0, // 3x multiplier
            max_duration: Duration::from_secs(300),
            jitter: false,
        };

        let mut backoff = ExponentialBackoff::new(&config);

        assert_eq!(backoff.next_delay(), Duration::from_millis(10));
        assert_eq!(backoff.next_delay(), Duration::from_millis(30));
        assert_eq!(backoff.next_delay(), Duration::from_millis(90));
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_attempts, 5);
        assert_eq!(config.initial_delay, Duration::from_secs(1));
        assert_eq!(config.max_delay, Duration::from_secs(60));
        assert_eq!(config.multiplier, 2.0);
    }

    #[test]
    fn test_backoff_current_delay_field() {
        let config = RetryConfig {
            max_attempts: 5,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
            max_duration: Duration::from_secs(300),
            jitter: false,
        };

        let mut backoff = ExponentialBackoff::new(&config);

        assert_eq!(backoff.current_delay, Duration::from_millis(100));
        backoff.next_delay();
        assert_eq!(backoff.current_delay, Duration::from_millis(200));
    }

    #[test]
    fn test_reset_clears_state() {
        let config = RetryConfig {
            max_attempts: 5,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
            max_duration: Duration::from_secs(300),
            jitter: false,
        };

        let mut backoff = ExponentialBackoff::new(&config);

        backoff.next_delay();
        backoff.next_delay();

        backoff.reset();

        assert_eq!(backoff.attempt(), 0);
        assert_eq!(backoff.current_delay, Duration::from_millis(100));
    }

    #[test]
    fn test_to_backoff() {
        let config = RetryConfig::default();
        let backoff = config.to_backoff();
        assert_eq!(backoff.attempt(), 0);
        assert_eq!(backoff.current_delay, config.initial_delay);
    }
}
