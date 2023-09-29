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

pub(crate) async fn with_timeout<T>(future: impl Future<Output = T>, timeout_millis: u64) -> T {
    tokio::select! {
        result = future => result,
        _ = time::sleep(Duration::from_millis(timeout_millis)) => panic!("Test timed out"),
    }
}
