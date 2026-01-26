// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Utility functions for unit tests.

mod downcast_matcher;
mod msg_dyn_matcher;

use lazy_static::lazy_static;
use log::trace;
use parking_lot::Mutex;
use rand::{distr::Alphanumeric, rng, Rng};
use sdv_telemetry_test_proto::test_message::{TestEnum, TestMessage, TestNestedMessage};
use std::fmt::Debug;
use std::future::Future;
use tokio::sync::oneshot;

pub use downcast_matcher::downcast_ref;
pub use msg_dyn_matcher::msg_dyn_eq;
pub use sdv_telemetry_test_utils_macros::parameterized_test;
pub use sdv_telemetry_test_utils_macros::timeout;

lazy_static! {
    /// An instance of TestMessage filled with test data.
    pub static ref TEST_MESSAGE: TestMessage = {
        let mut test_message = TestMessage::new();
        // initialize scalar (optional) fields
        test_message.bool_field = true;
        test_message.int_field = 80;
        test_message.float_field = 100.0;
        test_message.string_field = "Hello world".to_string();
        test_message.enum_field = TestEnum::SOME.into();
        test_message.bytes_field = "Hello world".as_bytes().to_vec();

        // initialize nested field
        let mut test_nested_message = TestNestedMessage::new();
        test_nested_message.bool_field = true;
        test_nested_message.repeated_i32s = vec![1, 2];
        test_message.message_field = protobuf::MessageField::some(test_nested_message.clone());

        // initialize repeated fields
        test_message.repeated_bools = vec![true, true, false, true];
        test_message.repeated_i64s = vec![3, 4, 5];
        test_message.repeated_doubles = vec![
            std::f64::consts::PI, std::f64::consts::TAU, std::f64::consts::E];
        test_message.repeated_enums = vec![
            TestEnum::SOME.into(), TestEnum::SOME.into(), TestEnum::NONE.into()];
        test_message.repeated_messages = vec![
            test_nested_message.clone(), test_nested_message];

        test_message
    };
}

/// Generate a unique Data Tunnel topic name for testing.
///
/// TODO(b/343140446): We should clean up data tunnel topics.
pub fn unique_dt_topic_name() -> String {
    const PREFIX: &str = "TelemetryTest";
    const SUFFIX_LEN: usize = 30;
    const _: () = assert!(PREFIX.len() + SUFFIX_LEN <= 48, "Generated data tunnel topic name must be less than or equal to Data Tunnel's maximum topic name length.");

    let suffix =
        rng().sample_iter(&Alphanumeric).take(SUFFIX_LEN).map(char::from).collect::<String>();
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
