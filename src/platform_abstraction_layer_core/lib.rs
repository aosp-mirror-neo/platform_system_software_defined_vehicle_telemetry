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

//! Implements `CorePal`, which is an implementation of the `PlatformAbstractionLayer` trait for SDV
//! Core.

mod resumed_at;

use sdv_telemetry_platform_abstraction_layer::PlatformAbstractionLayer;

use crate::resumed_at::{ResumedAt, ResumedAtTracker};

/// A `PlatformAbstractionLayer` implementation for SDV Core.
#[derive(Debug)]
pub struct CorePal {
    resumed_at_tracker: ResumedAtTracker,
}

impl PlatformAbstractionLayer for CorePal {
    fn get_resumed_at(&self) -> ResumedAt {
        self.resumed_at_tracker.get_resumed_at()
    }
}

impl Default for CorePal {
    fn default() -> Self {
        Self::new()
    }
}

impl CorePal {
    /// Create a new instance of a `CorePal`.
    pub fn new() -> Self {
        Self { resumed_at_tracker: ResumedAtTracker::default() }
    }
}
