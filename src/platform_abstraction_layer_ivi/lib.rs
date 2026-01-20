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

//! Implements `IviPal`, which is an implementation of the `PlatformAbstractionLayer` trait for IVI.

use sdv_telemetry_platform_abstraction_layer::PlatformAbstractionLayer;
use sdv_telemetry_time_wrapper::Microseconds;

/// A `PlatformAbstractionLayer` implementation for IVI.
#[derive(Debug)]
pub struct IviPal {}

// TODO: b/397902280 - Implement for IVI.
impl PlatformAbstractionLayer for IviPal {
    fn get_resumed_at(&self) -> Option<Microseconds> {
        log::warn!(
            "IviPal::get_resumed_at called, which is not yet implemented (b/397902280). Returning `None`."
        );

        None
    }
}

impl Default for IviPal {
    fn default() -> Self {
        Self::new()
    }
}

impl IviPal {
    /// Create a new instance of a `IviPal`.
    pub fn new() -> Self {
        IviPal {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_resumed_at_returns_none() {
        let pal = IviPal::new();
        assert_eq!(pal.get_resumed_at(), None);
    }
}
