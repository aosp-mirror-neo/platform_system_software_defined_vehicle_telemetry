// Copyright 2023 Google LLC. All Rights Reserved.

//! Utility functions for unit tests.

use std::sync::Arc;

/// Compares Arcs by extracting and comparing data pointers only
/// This is different from the behavior observed by Arc::ptr_eq
/// (https://rust-lang.github.io/rust-clippy/master/index.html#/vtable_address_comparisons)
pub(crate) fn ptr_eq<T: ?Sized>(left: &Arc<T>, right: &Arc<T>) -> bool {
    Arc::as_ptr(left) == Arc::as_ptr(right)
}
