// Copyright 2023 Google LLC. All Rights Reserved.

//! This module implements code extensions for Option

/// Additional Option functions that are either not available in the standard library, or hidden
/// behind experiment flags.
pub trait OptionExtension<T> {
    /// Returns true if the option is a None or it is Some and the value inside of it
    /// matches a predicate.
    fn is_none_or(&self, f: impl FnOnce(&T) -> bool) -> bool;
}

impl<T> OptionExtension<T> for Option<T> {
    fn is_none_or(&self, f: impl FnOnce(&T) -> bool) -> bool {
        if let Some(v) = self {
            f(v)
        } else {
            true
        }
    }
}
