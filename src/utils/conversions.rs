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

//! This module implements safe number conversions.

/// Lossless and safe conversion of an `u32` into an `usize`. This makes our code incompatible with
/// devices with a pointer size of less than 32 bits.
#[cfg(any(target_pointer_width = "32", target_pointer_width = "64"))]
pub fn u32_into_usize(value: u32) -> usize {
    value.try_into().expect("Can never fail due to the `target_pointer_width` check.")
}
