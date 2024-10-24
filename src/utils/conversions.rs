// Copyright 2024 Google LLC

//! This module implements safe number conversions.

/// Lossless and safe conversion of an `u32` into an `usize`. This makes our code incompatible with
/// devices with a pointer size of less than 32 bits.
#[cfg(any(target_pointer_width = "32", target_pointer_width = "64"))]
pub fn u32_into_usize(value: u32) -> usize {
    value.try_into().expect("Can never fail due to the `target_pointer_width` check.")
}
