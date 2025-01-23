// Copyright 2023 Google LLC

//! Utility functions for unit tests.

use log::trace;
use parking_lot::Mutex;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use std::fmt::Debug;
use std::future::Future;
use tokio::sync::oneshot;

pub use sdv_telemetry_test_utils_macros::parameterized_test;
pub use sdv_telemetry_test_utils_macros::timeout;

/// Generate a unique Data Tunnel topic name for testing.
///
/// TODO(b/343140446): We should clean up data tunnel topics.
pub fn unique_dt_topic_name() -> String {
    const PREFIX: &str = "TelemetryTest";
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

/// A thin wrapper around `tokio::sync::oneshot::channel`, which wraps the sending side into a
/// callback, and handles all errors by `panic!`ing with appropriate messages. The returned callback
/// is deliberately typed as `Fn`, not `FnOnce`, but will `panic!` when called more than once.
pub fn oneshot_callback<T: Debug>() -> (impl Fn(T), impl Future<Output = T>) {
    let (tx, rx) = oneshot::channel();

    let tx = Mutex::new(Some(tx));
    let tx = move |value: T| {
        let Some(tx) = tx.lock().take() else {
            panic!("Callback should only be called once. Value: {value:?}")
        };
        tx.send(value).unwrap_or_else(|value: T| {
            panic!("Callback should not be called after receiver was dropped. Value: {value:?}")
        });
    };

    let rx = async { rx.await.expect("Callback should not be dropped before being called.") };

    (tx, rx)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::thread;
    use tokio::time::{sleep, Duration};

    // These tests cannot be inside of the `sdv_telemetry_test_utils_macros`, because that crate is
    // a proc macro crate.

    #[tokio::test]
    #[timeout]
    async fn timeout_does_not_trigger() {
        sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    #[timeout]
    #[should_panic(expected = "Test timeout of 1000ms reached (call stalled for ")]
    async fn timeout_triggers() {
        sleep(Duration::from_millis(1100)).await;
    }

    #[tokio::test]
    #[timeout(100)]
    #[should_panic(expected = "Test timeout of 100ms reached (call stalled for ")]
    async fn timeout_triggers_with_explicit_duration() {
        sleep(Duration::from_millis(200)).await;
    }

    #[tokio::test]
    #[timeout(100)]
    #[should_panic(expected = "Test timeout of 100ms reached!")]
    async fn timeout_triggers_after_sync_sleep() {
        thread::sleep(Duration::from_millis(200));
        sleep(Duration::MAX).await;
    }

    parameterized_test!(
        #[test]
        parameterized_sync_no_args,
        || {},
        test_case: (),
    );

    parameterized_test!(
        #[test]
        #[should_panic(expected = "panic message")]
        parameterized_sync_should_panic_no_args,
        || {
            panic!("panic message");
        },
        test_case: (),
    );

    parameterized_test!(
        #[test]
        parameterized_sync_args,
        |n| {
            assert_eq!(n % 2, 1);
        },
        one: (1,),
        three: (3,),
    );

    parameterized_test!(
        #[timeout]
        #[tokio::test]
        parameterized_async_no_args,
        || async {},
        test_case: (),
    );

    parameterized_test!(
        #[timeout]
        #[tokio::test]
        parameterized_async_args,
        |n| async move {
            assert_eq!(n % 2, 1);
        },
        one: (1,),
        three: (3,),
    );

    parameterized_test!(
        #[timeout]
        #[tokio::test]
        parameterized_async_args_references,
        |arg: &[&std::time::Duration]| async move {
            let _ = format!("{arg:?}");
            sleep(Duration::from_millis(1)).await;
            let _ = format!("{arg:?}");
            sleep(Duration::from_millis(1)).await;
            let _ = format!("{arg:?}");
        },
        zero: (&[&std::time::Duration::ZERO],),
    );
}
