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

/// A multi-level countdown latch that allows tasks to coordinate across
/// multiple independent phases or levels.
///
/// Each level must be explicitly initialized with a count using `init_level`.
/// Tasks can then wait on the level using `wait(level)`, and reduce the count
/// using `count_down(level)`. Once the count reaches zero, all waiters on that
/// level are notified and future waiters return immediately.
#[derive(Debug, Clone, Default)]
pub struct MultiLevelCountdownLatch {
    state: Arc<State>,
}

/// Shared internal state for the latch.
#[derive(Debug, Default)]
struct State {
    inner: Mutex<StateInner>,
}

/// Internal mutable fields: count and notifier map.
#[derive(Debug, Default)]
struct StateInner {
    counts: HashMap<u32, u32>,
    notifiers: HashMap<u32, Arc<Notify>>,
}

impl MultiLevelCountdownLatch {
    /// Creates a new, empty countdown latch.
    pub fn new() -> Self {
        Self {
            state: Arc::new(State {
                inner: Mutex::new(StateInner { counts: HashMap::new(), notifiers: HashMap::new() }),
            }),
        }
    }

    /// Creates a new coundown latch with a given level being initialized with a countdown value
    pub fn new_with_level(level: u32, count: u32) -> Self {
        Self {
            state: Arc::new(State {
                inner: Mutex::new(StateInner {
                    counts: HashMap::from([(level, count)]),
                    notifiers: HashMap::from([(level, Arc::new(Notify::new()))]),
                }),
            }),
        }
    }

    /// Initializes a given level with a countdown value.
    pub async fn init_level(&self, level: u32, count: u32) {
        let mut inner = self.state.inner.lock().await;

        assert!(!inner.counts.contains_key(&level), "Level {} already initialized", level);

        inner.counts.insert(level, count);
        inner.notifiers.insert(level, Arc::new(Notify::new()));
    }

    /// Decrements the count for a given level.
    /// When the count reaches zero, all waiting tasks are notified.
    /// If the level hasn't been initialized or has already reached zero, this is a no-op.
    pub async fn count_down(&self, level: u32) {
        let notify = {
            let mut inner = self.state.inner.lock().await;

            let count = match inner.counts.get_mut(&level) {
                Some(c) => c,
                None => return,
            };

            if *count > 0 {
                *count -= 1;
            }

            if *count == 0 {
                inner.notifiers.remove(&level)
            } else {
                None
            }
        };

        if let Some(notify) = notify {
            notify.notify_waiters();
        }
    }

    /// Waits until the count for the given level reaches zero.
    /// If the level hasn't been initialized, this returns immediately.
    pub async fn wait(&self, level: u32) {
        loop {
            let maybe_notify = {
                let inner = self.state.inner.lock().await;

                match inner.counts.get(&level) {
                    Some(&0) => return,
                    Some(_) => inner.notifiers.get(&level).cloned(),
                    None => return,
                }
            };

            if let Some(notify) = maybe_notify {
                notify.notified().await;
            } else {
                return;
            }
        }
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
        let latch = MultiLevelCountdownLatch::new();
        latch.init_level(1, 0).await;
        // Should return immediately since count is zero
        latch.wait(1).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn test_wait_uninitialized_ok() {
        let latch = MultiLevelCountdownLatch::new();
        // Should return immediately
        latch.wait(123).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn test_countdown_uninitialized_ok() {
        let latch = MultiLevelCountdownLatch::new();
        // Should be a safe no-op
        latch.count_down(999).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn test_countdown_to_zero() {
        let latch = MultiLevelCountdownLatch::new_with_level(1, 2);

        let latch_clone = latch.clone();
        let waiter = tokio::spawn(async move {
            latch_clone.wait(1).await;
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        latch.count_down(1).await;
        latch.count_down(1).await;

        // Waiter should complete after countdown reaches 0
        assert!(waiter.await.is_ok());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn test_multiple_waiters_notified() {
        let latch = MultiLevelCountdownLatch::new();
        latch.init_level(42, 1).await;

        let waiter1 = {
            let latch = latch.clone();
            tokio::spawn(async move {
                latch.wait(42).await;
            })
        };

        let waiter2 = {
            let latch = latch.clone();
            tokio::spawn(async move {
                latch.wait(42).await;
            })
        };

        tokio::time::sleep(Duration::from_millis(10)).await;
        latch.count_down(42).await;

        assert!(timeout(Duration::from_secs(1), waiter1).await.is_ok());
        assert!(timeout(Duration::from_secs(1), waiter2).await.is_ok());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn test_countdown_before_wait() {
        let latch = MultiLevelCountdownLatch::new();
        latch.init_level(5, 1).await;

        latch.count_down(5).await;
        // Should return immediately since count is already zero
        latch.wait(5).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic(expected = "Level 1 already initialized")]
    async fn test_double_init_panics() {
        let latch = MultiLevelCountdownLatch::new();
        latch.init_level(1, 1).await;
        latch.init_level(1, 2).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn test_multi_level_independence() {
        // Tests that different levels are completely independent.
        // A waiter on one level should not be affected by countdowns on another.
        let latch = MultiLevelCountdownLatch::new();
        latch.init_level(1, 1).await;
        latch.init_level(2, 1).await;

        // This waiter should be released
        let waiter1 = {
            let latch = latch.clone();
            tokio::spawn(async move {
                latch.wait(1).await;
            })
        };

        // This waiter should NOT be released
        let waiter2 = {
            let latch = latch.clone();
            tokio::spawn(async move {
                latch.wait(2).await;
            })
        };

        tokio::time::sleep(Duration::from_millis(10)).await;

        // Count down only level 1
        latch.count_down(1).await;

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
        let latch = MultiLevelCountdownLatch::new_with_level(1, 1);

        latch.count_down(1).await;
        latch.count_down(1).await;
        latch.count_down(1).await;

        let inner = latch.state.inner.lock().await;
        assert_eq!(*inner.counts.get(&1).unwrap(), 0);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn test_waiter_arriving_after_countdown() {
        // Tests that a task that begins waiting *after* the count has already
        // reached zero returns immediately.
        let latch = MultiLevelCountdownLatch::new_with_level(1, 1);

        // Count down to zero before any waiter exists
        latch.count_down(1).await;

        // Now, spawn a waiter. It should not block.
        let late_waiter = tokio::spawn(async move {
            latch.wait(1).await;
        });

        assert!(timeout(Duration::from_secs(1), late_waiter).await.is_ok());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn test_concurrent_countdowns() {
        // Tests that concurrent countdowns from multiple tasks are handled correctly.
        const NUM_TASKS: u32 = 20;
        let latch = MultiLevelCountdownLatch::new_with_level(1, NUM_TASKS);
        let completion_count = Arc::new(AtomicUsize::new(0));

        // Spawn a waiter that will be unblocked when the count reaches zero
        let waiter = tokio::spawn({
            let latch = latch.clone();
            let completion_count = Arc::clone(&completion_count);
            async move {
                latch.wait(1).await;
                completion_count.fetch_add(1, Ordering::AcqRel);
            }
        });

        // Spawn multiple tasks to count down concurrently
        for _ in 0..NUM_TASKS {
            tokio::spawn({
                let latch = latch.clone();
                async move {
                    latch.count_down(1).await;
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
        let latch = MultiLevelCountdownLatch::new();
        latch.init_level(0, 1).await;
        latch.init_level(1, 1).await;
        latch.init_level(2, 1).await;

        // Task for Phase 2: Waits for level 1, then counts down level 2.
        let phase_2_handle = tokio::spawn({
            let latch = latch.clone();
            async move {
                // 1. Wait for the signal that Phase 1 is complete.
                latch.wait(1).await;
                // 2. Do "work" and then signal that Phase 2 is complete.
                latch.count_down(2).await;
            }
        });

        // Task for Phase 1: Waits for level 0, then counts down level 1.
        tokio::spawn({
            let latch = latch.clone();
            async move {
                // 1. Wait for the initial signal (Phase 0).
                latch.wait(0).await;
                // 2. Do "work" and then signal that Phase 1 is complete.
                latch.count_down(1).await;
            }
        });

        // Kick off the entire chain by counting down the first level.
        latch.count_down(0).await;

        assert!(phase_2_handle.await.is_ok());
    }
}
