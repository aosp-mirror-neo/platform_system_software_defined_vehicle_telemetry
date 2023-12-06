// Copyright 2023 Google LLC. All Rights Reserved.

//! Utility functions for unit tests.

use std::{future::Future, sync::Arc};
use tokio::{time, time::Duration};

/// Compares Arcs by extracting and comparing data pointers only
/// This is different from the behavior observed by Arc::ptr_eq
/// (https://rust-lang.github.io/rust-clippy/master/index.html#/vtable_address_comparisons)
pub(crate) fn ptr_eq<T: ?Sized>(left: &Arc<T>, right: &Arc<T>) -> bool {
    Arc::as_ptr(left) == Arc::as_ptr(right)
}

/// Awaits specified future with a timeout of 150 seconds.
/// The function is used in tests to halt the test instead of timing out the whole test suite.
/// TODO(b/243397516): remove this workaround once Rust tooling is fixed.
pub(crate) async fn with_timeout<T>(future: impl Future<Output = T>) -> T {
    tokio::select! {
        result = future => result,
        _ = time::sleep(Duration::from_secs(150)) => panic!("Test timed out"),
    }
}
