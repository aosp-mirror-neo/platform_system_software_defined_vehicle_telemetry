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

//! Defines the `PlatformAbstractionLayer` trait. If the "test_utils" feature is enabled, then it
//! also defines a mock implementation and a stub factory for testing.

mod stub;

use sdv_telemetry_time_wrapper::Microseconds;
use std::fmt::Debug;

#[cfg(feature = "test_utils")]
pub use stub::new_stub_pal;
#[cfg(feature = "test_utils")]
pub use MockPlatformAbstractionLayer as MockPal;

/// Objects implementing `PlatformAbstractionLayer` provide platform-specific functionality to
/// abstract the difference between different platforms (i.e., Core vs. IVI).
#[cfg_attr(feature = "test_utils", mockall::automock)]
pub trait PlatformAbstractionLayer: Send + Sync + Debug {
    /// Returns the timestamp in `Microseconds` at which the OS was last resumed from sleep. Uses
    /// monotonic time since boot to represent the timestamp. If the OS hasn't slept since booting,
    /// returns `None`.
    fn get_resumed_at(&self) -> Option<Microseconds>;
}
