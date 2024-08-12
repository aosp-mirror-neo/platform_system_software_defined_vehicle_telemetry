// Copyright 2023 Google LLC

//! Utility functions for unit tests.

use log::trace;
use protobuf::MessageDyn;
use rand::{distributions::Alphanumeric, thread_rng, Rng};

/// Creates a Box with a callback that accepts a message and does nothing.
pub fn create_empty_fn_box() -> Box<dyn FnMut(Box<dyn MessageDyn>) + Send + Sync> {
    Box::new(|_| {})
}

/// Generate a unique Data Tunnel topic name for testing.
///
/// TODO(b/343140446): We should clean up data tunnel topics.
pub fn unique_dt_topic_name() -> String {
    const PREFIX: &str = "TelemetryTest.";
    const SUFFIX_LEN: usize = 30;
    const _: () = assert!(PREFIX.len() + SUFFIX_LEN <= 48, "Generated data tunnel topic name must be less than or equal to Data Tunnel's maximum topic name length.");

    let suffix = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(SUFFIX_LEN)
        .map(char::from)
        .collect::<String>();
    let name = format!("{PREFIX}{suffix}");

    // Print the generated topic name for easier debugging - we print it to both logs and stderr, so
    // it shows up in atest output as well.
    let msg = format!("Generated data tunnel topic name: {name}");
    trace!("{msg}");
    eprintln!("{msg}");

    name
}

#[cfg(test)]
mod tests {
    use std::thread;
    use tokio::time::{sleep, Duration};

    // These tests cannot be inside of the `sdv_telemetry_test_utils_macros`, because that crate is
    // a proc macro crate.

    #[tokio::test]
    #[sdv_telemetry_test_utils_macros::timeout]
    async fn timeout_does_not_trigger() {
        sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    #[sdv_telemetry_test_utils_macros::timeout]
    #[should_panic(expected = "Test timeout of 1000ms reached (call stalled for ")]
    async fn timeout_triggers() {
        sleep(Duration::from_millis(1100)).await;
    }

    #[tokio::test]
    #[sdv_telemetry_test_utils_macros::timeout(100)]
    #[should_panic(expected = "Test timeout of 100ms reached (call stalled for ")]
    async fn timeout_triggers_with_explicit_duration() {
        sleep(Duration::from_millis(200)).await;
    }

    #[tokio::test]
    #[sdv_telemetry_test_utils_macros::timeout(100)]
    #[should_panic(expected = "Test timeout of 100ms reached!")]
    async fn timeout_triggers_after_sync_sleep() {
        thread::sleep(Duration::from_millis(200));
        sleep(Duration::MAX).await;
    }
}
