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

pub(crate) use sdv_telemetry_platform_abstraction_layer_core_sysprops::palcore::SysPropError;
use sdv_telemetry_platform_abstraction_layer_core_sysprops::palcore::{
    resumed_at_timestamp, set_resumed_at_timestamp,
};
use sdv_telemetry_time_wrapper::Microseconds;

#[cfg_attr(test, mockall::automock)]
pub(crate) trait ResumedAtStorage: Send {
    /// Read the stored time the VM resumed at. May return `Ok(None)` if it hasn't been stored yet.
    fn read(&mut self) -> Result<Option<Microseconds>, SysPropError>;
    /// Store the time the VM resumed at.
    fn write(&mut self, resumed_at: Microseconds) -> Result<(), SysPropError>;
}

/// Writes and reads the time the VM resumed at to/from a system property.
pub(crate) struct ResumedAtStorageImpl {}

impl ResumedAtStorage for ResumedAtStorageImpl {
    fn read(&mut self) -> Result<Option<Microseconds>, SysPropError> {
        Ok(resumed_at_timestamp()?.map(Microseconds))
    }

    fn write(&mut self, resumed_at: Microseconds) -> Result<(), SysPropError> {
        set_resumed_at_timestamp(resumed_at.0)
    }
}

impl Default for ResumedAtStorageImpl {
    fn default() -> Self {
        Self::new()
    }
}

impl ResumedAtStorageImpl {
    pub(crate) fn new() -> Self {
        Self {}
    }
}
