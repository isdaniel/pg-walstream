//! Thread-safe LSN tracking for CDC replication feedback
//!
//! This module provides thread-safe tracking of LSN positions for replication feedback
//! to PostgreSQL (write_lsn, flush_lsn, replay_lsn).
//!
//! The PostgreSQL replication protocol expects three different LSN values:
//! - `write_lsn`: Data received from the stream
//! - `flush_lsn`: Data written to destination (before commit)  
//! - `replay_lsn`: Data committed to destination
//!
//! Since the producer reads from PostgreSQL and the consumer writes to the destination,
//! we need a thread-safe way to share the committed LSN from consumer back to producer
//! for accurate feedback to PostgreSQL.

use crate::types::{format_lsn, CachePadded, XLogRecPtr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, info};

/// Thread-safe tracker for LSN positions used in replication feedback
///
/// This tracker is designed to be shared between the producer (which sends feedback
/// to PostgreSQL) and the consumer (which commits transactions to the destination).
///
/// # Example
///
/// ```
/// use pg_walstream::lsn::SharedLsnFeedback;
/// use std::sync::Arc;
///
/// let feedback = SharedLsnFeedback::new_shared();
///
/// // Consumer updates LSN after flushing data
/// feedback.update_flushed_lsn(1000);
///
/// // Consumer updates LSN after committing transaction  
/// feedback.update_applied_lsn(1000);
///
/// // Producer reads LSN values for feedback to PostgreSQL
/// let (flushed, applied) = feedback.get_feedback_lsn();
/// ```
#[derive(Debug)]
pub struct SharedLsnFeedback {
    /// Last flushed LSN - data written to destination before commit
    flushed_lsn: CachePadded<AtomicU64>,
    /// Last applied/replayed LSN - data committed to destination
    applied_lsn: CachePadded<AtomicU64>,
}

impl SharedLsnFeedback {
    /// Create a new shared LSN feedback tracker
    ///
    /// Initializes both flushed_lsn and applied_lsn to 0.
    /// Use `new_shared()` if you need an Arc-wrapped instance for sharing between threads.
    ///
    /// # Example
    ///
    /// ```
    /// use pg_walstream::lsn::SharedLsnFeedback;
    ///
    /// let feedback = SharedLsnFeedback::new();
    /// assert_eq!(feedback.get_flushed_lsn(), 0);
    /// assert_eq!(feedback.get_applied_lsn(), 0);
    /// ```
    pub fn new() -> Self {
        Self {
            flushed_lsn: CachePadded::new(AtomicU64::new(0)),
            applied_lsn: CachePadded::new(AtomicU64::new(0)),
        }
    }

    /// Create a new shared LSN feedback tracker wrapped in Arc for sharing
    ///
    /// This is the preferred way to create a feedback tracker that will be shared
    /// between the producer (reading from PostgreSQL) and consumer (writing to destination).
    ///
    /// # Returns
    ///
    /// An `Arc<SharedLsnFeedback>` that can be cloned and shared across threads safely.
    ///
    /// # Example
    ///
    /// ```
    /// use pg_walstream::lsn::SharedLsnFeedback;
    /// use std::sync::Arc;
    ///
    /// let feedback = SharedLsnFeedback::new_shared();
    /// let consumer_feedback = Arc::clone(&feedback);
    ///
    /// // Consumer thread updates LSN after commit
    /// consumer_feedback.update_applied_lsn(1000);
    ///
    /// // Producer thread reads LSN for PostgreSQL feedback
    /// let (flushed, applied) = feedback.get_feedback_lsn();
    /// ```
    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    /// Update the flushed LSN if the new value is greater
    ///
    /// This should be called when data has been written/flushed to the destination
    /// database, but not yet committed (e.g., during batch writes).
    #[inline(always)]
    pub fn update_flushed_lsn(&self, lsn: XLogRecPtr) {
        if lsn == 0 {
            return;
        }

        loop {
            let current = self.flushed_lsn.load(Ordering::Acquire);
            if lsn <= current {
                return;
            }
            match self.flushed_lsn.compare_exchange_weak(
                current,
                lsn,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    debug!(
                        "SharedLsnFeedback: Updated flushed LSN from {} to {}",
                        current, lsn
                    );
                    return;
                }
                Err(_) => continue,
            }
        }
    }

    /// Update the applied/replayed LSN if the new value is greater
    ///
    /// This should be called when a transaction has been successfully committed
    /// to the destination database. This is the most important LSN as PostgreSQL
    /// uses it to determine which WAL can be recycled.
    #[inline(always)]
    pub fn update_applied_lsn(&self, lsn: XLogRecPtr) {
        if lsn == 0 {
            return;
        }
        loop {
            let current = self.applied_lsn.load(Ordering::Acquire);
            if lsn <= current {
                break;
            }
            match self.applied_lsn.compare_exchange_weak(
                current,
                lsn,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    debug!(
                        "SharedLsnFeedback: Updated applied LSN from {} to {}",
                        current, lsn
                    );
                    break;
                }
                Err(_) => continue,
            }
        }

        // Applied data is implicitly flushed, update flushed as well
        self.update_flushed_lsn(lsn);
    }

    /// Get the current flushed LSN
    ///
    /// Returns the last LSN value that was flushed to the destination database.
    /// This represents data that has been written but may not yet be committed.
    ///
    /// # Returns
    ///
    /// The current flushed LSN as a u64 value (XLogRecPtr).
    #[inline(always)]
    pub fn get_flushed_lsn(&self) -> XLogRecPtr {
        self.flushed_lsn.load(Ordering::Acquire)
    }

    /// Get the current applied LSN
    ///
    /// Returns the last LSN value that was successfully committed to the destination database.
    /// This is the most important LSN as PostgreSQL uses it to determine which WAL segments
    /// can be safely recycled.
    ///
    /// # Returns
    ///
    /// The current applied LSN as a u64 value (XLogRecPtr).
    #[inline(always)]
    pub fn get_applied_lsn(&self) -> XLogRecPtr {
        self.applied_lsn.load(Ordering::Acquire)
    }

    /// Get both LSN values atomically for feedback
    ///
    /// Retrieves both flushed and applied LSN values. Note that these are read
    /// sequentially but both use atomic operations, so they represent a consistent
    /// state at the time of reading.
    ///
    /// # Returns
    ///
    /// A tuple of (flushed_lsn, applied_lsn) as u64 values.
    ///
    /// # Example
    ///
    /// ```
    /// use pg_walstream::lsn::SharedLsnFeedback;
    ///
    /// let feedback = SharedLsnFeedback::new();
    /// feedback.update_applied_lsn(1000);
    ///
    /// let (flushed, applied) = feedback.get_feedback_lsn();
    /// assert_eq!(flushed, 1000);
    /// assert_eq!(applied, 1000);
    /// ```
    #[inline(always)]
    pub fn get_feedback_lsn(&self) -> (XLogRecPtr, XLogRecPtr) {
        let flushed = self.flushed_lsn.load(Ordering::Acquire);
        let applied = self.applied_lsn.load(Ordering::Acquire);
        (flushed, applied)
    }

    /// Log current LSN state (for debugging)
    pub fn log_state(&self, prefix: &str) {
        let flushed = self.get_flushed_lsn();
        let applied = self.get_applied_lsn();
        info!(
            "{}: flushed_lsn={}, applied_lsn={}",
            prefix,
            format_lsn(flushed),
            format_lsn(applied)
        );
    }
}

impl Default for SharedLsnFeedback {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for SharedLsnFeedback {
    fn clone(&self) -> Self {
        Self {
            flushed_lsn: CachePadded::new(AtomicU64::new(self.flushed_lsn.load(Ordering::Acquire))),
            applied_lsn: CachePadded::new(AtomicU64::new(self.applied_lsn.load(Ordering::Acquire))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_lsn_feedback_new() {
        let feedback = SharedLsnFeedback::new();
        assert_eq!(feedback.get_flushed_lsn(), 0);
        assert_eq!(feedback.get_applied_lsn(), 0);
    }

    #[test]
    fn test_update_flushed_lsn() {
        let feedback = SharedLsnFeedback::new();

        feedback.update_flushed_lsn(100);
        assert_eq!(feedback.get_flushed_lsn(), 100);

        // Smaller value should not update
        feedback.update_flushed_lsn(50);
        assert_eq!(feedback.get_flushed_lsn(), 100);

        // Larger value should update
        feedback.update_flushed_lsn(200);
        assert_eq!(feedback.get_flushed_lsn(), 200);

        // Zero should not update
        feedback.update_flushed_lsn(0);
        assert_eq!(feedback.get_flushed_lsn(), 200);
    }

    #[test]
    fn test_update_applied_lsn() {
        let feedback = SharedLsnFeedback::new();

        feedback.update_applied_lsn(100);
        assert_eq!(feedback.get_applied_lsn(), 100);
        // Applied should also update flushed
        assert_eq!(feedback.get_flushed_lsn(), 100);

        // Smaller value should not update
        feedback.update_applied_lsn(50);
        assert_eq!(feedback.get_applied_lsn(), 100);

        // Larger value should update both
        feedback.update_applied_lsn(200);
        assert_eq!(feedback.get_applied_lsn(), 200);
        assert_eq!(feedback.get_flushed_lsn(), 200);
    }

    #[test]
    fn test_get_feedback_lsn() {
        let feedback = SharedLsnFeedback::new();

        feedback.update_flushed_lsn(100);
        feedback.update_applied_lsn(50);

        let (flushed, applied) = feedback.get_feedback_lsn();
        assert_eq!(flushed, 100);
        assert_eq!(applied, 50);
    }

    #[test]
    fn test_clone() {
        let feedback = SharedLsnFeedback::new();
        feedback.update_flushed_lsn(100);
        feedback.update_applied_lsn(50);

        let cloned = feedback.clone();
        assert_eq!(cloned.get_flushed_lsn(), 100);
        assert_eq!(cloned.get_applied_lsn(), 50);

        // Modifying the clone should not affect the original
        cloned.update_applied_lsn(200);
        assert_eq!(feedback.get_applied_lsn(), 50);
        assert_eq!(cloned.get_applied_lsn(), 200);
    }

    #[test]
    fn test_new_shared() {
        let feedback = SharedLsnFeedback::new_shared();
        assert_eq!(feedback.get_flushed_lsn(), 0);
        assert_eq!(feedback.get_applied_lsn(), 0);

        // Test that Arc works correctly
        let feedback_clone = Arc::clone(&feedback);
        feedback.update_applied_lsn(100);
        assert_eq!(feedback_clone.get_applied_lsn(), 100);
    }

    #[test]
    fn test_concurrent_updates() {
        use std::sync::Arc;
        use std::thread;

        let feedback = SharedLsnFeedback::new_shared();
        let mut handles = vec![];

        // Spawn multiple threads updating LSNs concurrently
        for i in 0..10 {
            let feedback_clone = Arc::clone(&feedback);
            let handle = thread::spawn(move || {
                for j in 0..100 {
                    let lsn = (i * 100 + j) as u64;
                    feedback_clone.update_flushed_lsn(lsn);
                    feedback_clone.update_applied_lsn(lsn);
                }
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // The final values should be the maximum from all updates
        let (flushed, applied) = feedback.get_feedback_lsn();
        assert!(flushed <= 999);
        assert!(applied <= 999);
        assert!(flushed >= 900); // Should be close to max
        assert!(applied >= 900);
    }

    #[test]
    fn test_monotonic_increase() {
        let feedback = SharedLsnFeedback::new();

        // Update with increasing values
        for i in 1..=100 {
            feedback.update_flushed_lsn(i);
            assert_eq!(feedback.get_flushed_lsn(), i);
        }

        // Try to update with smaller values - should not change
        for i in (1..=50).rev() {
            feedback.update_flushed_lsn(i);
            assert_eq!(feedback.get_flushed_lsn(), 100);
        }
    }

    #[test]
    fn test_applied_updates_flushed() {
        let feedback = SharedLsnFeedback::new();

        feedback.update_flushed_lsn(50);
        feedback.update_applied_lsn(100);

        // Applied LSN update should also update flushed if it's higher
        assert_eq!(feedback.get_applied_lsn(), 100);
        assert_eq!(feedback.get_flushed_lsn(), 100);
    }

    #[test]
    fn test_zero_lsn_ignored() {
        let feedback = SharedLsnFeedback::new();

        feedback.update_flushed_lsn(100);
        feedback.update_flushed_lsn(0); // Should be ignored
        assert_eq!(feedback.get_flushed_lsn(), 100);

        feedback.update_applied_lsn(0); // Should be ignored
        assert_eq!(feedback.get_applied_lsn(), 0);
    }
}
