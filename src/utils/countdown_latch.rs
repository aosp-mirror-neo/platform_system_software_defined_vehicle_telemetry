// Copyright 2025 Google LLC

//! Countdown Latch is a synchronization primitive that allows one or more tasks to wait until
//! a set of operations being performed in other tasks completes.
//!
//! This implementation supports multiple "levels", where each level can independently track
//! and wait on its own count. This is useful for multi-phase workflows, where later phases
//! depend on earlier ones completing.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

/// Levels of the `MultiLevelCountdownLatch`.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Hash)]
pub struct Level(pub u32);

/// A multi-level countdown latch that allows tasks to coordinate across
/// multiple independent phases or levels.
///
/// `MultiLevelCountdownLatch` instances are created using the corresponding builder to initialize
/// the levels with counts. Tasks can then wait on the level using `wait(level).await`, and reduce
/// the count using `count_down(level).await`. Once the count reaches zero, all waiters on that
/// level are notified and future waiters return immediately.
#[derive(Debug, Clone)]
pub struct MultiLevelCountdownLatch {
    notifiers: Arc<Mutex<HashMap<Level, (u32, Arc<Notify>)>>>,
}

impl MultiLevelCountdownLatch {
    /// Create a `MultiLevelCountdownLatchBuilder` to initialize the countdown latch.
    pub fn builder() -> MultiLevelCountdownLatchBuilder {
        MultiLevelCountdownLatchBuilder { notifiers: HashMap::new() }
    }

    /// Decrements the count for a given level.
    /// When the count reaches zero, all waiting tasks are notified.
    /// If the level hasn't been initialized or has already reached zero, this is a no-op.
    pub async fn count_down(&self, level: Level) {
        if let Some(notifier) =
            self.notifiers.lock().await.get_mut(&level).and_then(|(count, notifier)| {
                let do_notify = *count == 1;
                *count = count.saturating_sub(1);
                if do_notify {
                    Some(notifier)
                } else {
                    None
                }
            })
        {
            notifier.notify_waiters();
        }
    }

    /// Waits until the count for the given level reaches zero. If the level hasn't been
    /// initialized or the count is already zero, returns immediately.
    pub async fn wait(&self, level: Level) {
        if let Some(notifier) = {
            let notifiers_guard = self.notifiers.lock().await;
            match notifiers_guard.get(&level) {
                None | Some((0, _)) => None,
                Some((_, notifier)) => Some(Arc::clone(notifier)),
            }
        } {
            // Release the `notifiers` guard, call `notifier.notified()` to register for
            // `notify_waiters()`.
            let notification = notifier.notified();
            let do_await = {
                // Lock `notifiers` again to make sure the `count` is still non-zero before
                // `await`ing.
                let notifiers_guard = self.notifiers.lock().await;
                !matches!(notifiers_guard.get(&level), None | Some((0, _)))
            };
            if do_await {
                // Release `notifiers` guard again to unblock `count_down` calls and `await`.
                notification.await;
            }
        }
    }
}

/// MultiLevelCountdownLatch builder.
pub struct MultiLevelCountdownLatchBuilder {
    notifiers: HashMap<Level, (u32, Arc<Notify>)>,
}

impl MultiLevelCountdownLatchBuilder {
    /// Initialize a `MultiLevelCountdownLatch` level with count. Subsequent calls with the same
    /// `level` override the count.
    pub fn with_level(mut self, level: Level, count: u32) -> Self {
        self.notifiers.insert(level, (count, Arc::new(Notify::new())));
        self
    }

    /// Consume the builder into a `MultiLevelCountdownLatch`.
    pub fn build(self) -> MultiLevelCountdownLatch {
        MultiLevelCountdownLatch { notifiers: Arc::new(Mutex::new(self.notifiers)) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{timeout, Duration};

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn test_init_and_wait_zero() {
        let latch = MultiLevelCountdownLatch::builder().with_level(Level(1), 0).build();
        // Should return immediately since count is zero
        latch.wait(Level(1)).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn test_wait_uninitialized_ok() {
        let latch = MultiLevelCountdownLatch::builder().build();
        // Should return immediately
        latch.wait(Level(123)).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn test_countdown_uninitialized_ok() {
        let latch = MultiLevelCountdownLatch::builder().build();
        // Should be a safe no-op
        latch.count_down(Level(999)).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn test_countdown_to_zero() {
        let latch = MultiLevelCountdownLatch::builder().with_level(Level(1), 2).build();

        let latch_clone = latch.clone();
        let waiter = tokio::spawn(async move {
            latch_clone.wait(Level(1)).await;
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        latch.count_down(Level(1)).await;
        latch.count_down(Level(1)).await;

        // Waiter should complete after countdown reaches 0
        assert!(waiter.await.is_ok());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn test_multiple_waiters_notified() {
        let latch = MultiLevelCountdownLatch::builder().with_level(Level(42), 1).build();

        let waiter1 = {
            let latch = latch.clone();
            tokio::spawn(async move {
                latch.wait(Level(42)).await;
            })
        };

        let waiter2 = {
            let latch = latch.clone();
            tokio::spawn(async move {
                latch.wait(Level(42)).await;
            })
        };

        tokio::time::sleep(Duration::from_millis(10)).await;
        latch.count_down(Level(42)).await;

        let _ = waiter1.await;
        let _ = waiter2.await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn test_countdown_before_wait() {
        let latch = MultiLevelCountdownLatch::builder().with_level(Level(5), 1).build();

        latch.count_down(Level(5)).await;
        // Should return immediately since count is already zero
        latch.wait(Level(5)).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn test_double_with_level_overrides_previous_count() {
        let latch = MultiLevelCountdownLatch::builder()
            .with_level(Level(1), 100)
            .with_level(Level(1), 1)
            .build();

        latch.count_down(Level(1)).await;
        latch.wait(Level(1)).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn test_multi_level_independence() {
        // Tests that different levels are completely independent.
        // A waiter on one level should not be affected by countdowns on another.
        let latch = MultiLevelCountdownLatch::builder()
            .with_level(Level(1), 1)
            .with_level(Level(2), 1)
            .build();

        // This waiter should be released
        let waiter1 = {
            let latch = latch.clone();
            tokio::spawn(async move {
                latch.wait(Level(1)).await;
            })
        };

        // This waiter should NOT be released
        let waiter2 = {
            let latch = latch.clone();
            tokio::spawn(async move {
                latch.wait(Level(2)).await;
            })
        };

        tokio::time::sleep(Duration::from_millis(10)).await;

        // Count down only level 1
        latch.count_down(Level(1)).await;

        // Assert waiter1 completes
        assert!(waiter1.await.is_ok());
        // Assert waiter2 does NOT complete
        assert!(timeout(Duration::from_millis(50), waiter2).await.is_err());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn test_countdown_does_not_underflow() {
        // Tests that calling `count_down` after the count has reached zero is a no-op
        // and does not cause an integer underflow or other panic.
        let latch = MultiLevelCountdownLatch::builder().with_level(Level(1), 1).build();

        latch.count_down(Level(1)).await;
        latch.count_down(Level(1)).await;
        latch.count_down(Level(1)).await;
        latch.wait(Level(1)).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn test_waiter_arriving_after_countdown() {
        // Tests that a task that begins waiting *after* the count has already
        // reached zero returns immediately.
        let latch = MultiLevelCountdownLatch::builder().with_level(Level(1), 1).build();

        // Count down to zero before any waiter exists
        latch.count_down(Level(1)).await;

        // Now, spawn a waiter. It should not block.
        let late_waiter = tokio::spawn(async move {
            latch.wait(Level(1)).await;
        });

        assert!(timeout(Duration::from_secs(1), late_waiter).await.is_ok());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn test_concurrent_countdowns() {
        // Tests that concurrent countdowns from multiple tasks are handled correctly.
        const NUM_TASKS: u32 = 20;
        let latch = MultiLevelCountdownLatch::builder().with_level(Level(1), NUM_TASKS).build();
        let completion_count = Arc::new(AtomicUsize::new(0));

        // Spawn a waiter that will be unblocked when the count reaches zero
        let waiter = tokio::spawn({
            let latch = latch.clone();
            let completion_count = Arc::clone(&completion_count);
            async move {
                latch.wait(Level(1)).await;
                completion_count.fetch_add(1, Ordering::AcqRel);
            }
        });

        // Spawn multiple tasks to count down concurrently
        for _ in 0..NUM_TASKS {
            tokio::spawn({
                let latch = latch.clone();
                async move {
                    latch.count_down(Level(1)).await;
                }
            });
        }

        // The waiter should complete within a reasonable time
        assert!(waiter.await.is_ok());
        // Check that the waiter task actually ran to completion
        assert_eq!(completion_count.load(Ordering::Acquire), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn test_chained_level_waits() {
        // Tests a chained or sequential workflow where the completion of one level's
        // countdown triggers a task that will perform the next level's countdown.
        // This simulates a multi-stage process: Phase 1 (triggered by level 0)
        // must complete before Phase 2 (triggered by level 1) can begin.
        let latch = MultiLevelCountdownLatch::builder()
            .with_level(Level(0), 1)
            .with_level(Level(1), 1)
            .with_level(Level(2), 1)
            .build();

        // Task for Phase 2: Waits for level 1, then counts down level 2.
        let phase_2_handle = tokio::spawn({
            let latch = latch.clone();
            async move {
                // 1. Wait for the signal that Phase 1 is complete.
                latch.wait(Level(1)).await;
                // 2. Do "work" and then signal that Phase 2 is complete.
                latch.count_down(Level(2)).await;
            }
        });

        // Task for Phase 1: Waits for level 0, then counts down level 1.
        tokio::spawn({
            let latch = latch.clone();
            async move {
                // 1. Wait for the initial signal (Phase 0).
                latch.wait(Level(0)).await;
                // 2. Do "work" and then signal that Phase 1 is complete.
                latch.count_down(Level(1)).await;
            }
        });

        // Kick off the entire chain by counting down the first level.
        latch.count_down(Level(0)).await;

        assert!(phase_2_handle.await.is_ok());
    }
}
