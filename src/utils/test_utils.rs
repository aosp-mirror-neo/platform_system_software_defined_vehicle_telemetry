// Copyright 2023 Google LLC. All Rights Reserved.

//! Utility functions for unit tests.
use protobuf::MessageDyn;
use std::future::Future;
use tokio::{time, time::Duration};

/// Creates a Box with a callback that accepts a message and does nothing.
pub fn create_empty_fn_box() -> Box<dyn Fn(Box<dyn MessageDyn>) + Send + Sync> {
    Box::new(|_| {})
}

/// Awaits specified future with a timeout of 150 seconds.
/// The function is used in tests to halt the test instead of timing out the whole test suite.
/// TODO(b/243397516): remove this workaround once Rust tooling is fixed.
pub async fn with_timeout<T>(future: impl Future<Output = T>) -> T {
    tokio::select! {
        result = future => result,
        _ = time::sleep(Duration::from_secs(150)) => panic!("Test timed out"),
    }
}
