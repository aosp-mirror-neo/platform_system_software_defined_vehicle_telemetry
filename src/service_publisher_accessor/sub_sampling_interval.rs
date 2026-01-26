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

//! Sub-sampling interval.

use anyhow::anyhow;
use std::time::Duration;

/// Wrapper around an integer representing a sub-sampling interval.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct SubSamplingInterval {
    interval_ms: u32,
}

impl SubSamplingInterval {
    /// A sub-sampling interval of zero (= no sub-sampling).
    pub const ZERO: Self = Self { interval_ms: 0 };
    /// A sub-sampling interval with the maximum possible value.
    pub const MAX: Self = Self { interval_ms: u32::MAX };

    /// Returns the sub-sampling interval in milliseconds.
    pub fn as_millis(&self) -> u32 {
        self.interval_ms
    }

    /// Returns the sub-sampling interval as a `Duration`.
    pub fn as_duration(&self) -> Duration {
        Duration::from_millis(self.interval_ms.into())
    }
}

impl TryFrom<Duration> for SubSamplingInterval {
    type Error = anyhow::Error;

    fn try_from(interval: Duration) -> Result<Self, Self::Error> {
        let interval_ms = interval.as_millis().try_into().map_err(|_| anyhow!(
            "Duration {interval:?} is too big to convert it to a sub-sampling interval (must be 2^32 milliseconds or smaller)."
        ))?;
        Ok(SubSamplingInterval { interval_ms })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sub_sampling_interval_zero_is_zero() {
        assert_eq!(SubSamplingInterval::ZERO.as_duration(), Duration::ZERO);
    }

    #[test]
    fn sub_sampling_interval_from_too_big_duration_fails() {
        let duration = Duration::MAX;

        let sub_sampling_interval = SubSamplingInterval::try_from(duration);

        assert!(sub_sampling_interval.is_err());
    }

    #[test]
    fn sub_sampling_interval_as_millis_success() {
        let duration = Duration::from_millis(3333);

        let sub_sampling_interval = SubSamplingInterval::try_from(duration).unwrap();

        assert_eq!(sub_sampling_interval.as_millis(), 3333);
    }

    #[test]
    fn sub_sampling_interval_as_duration_success() {
        let duration = Duration::from_millis(3333);

        let sub_sampling_interval = SubSamplingInterval::try_from(duration).unwrap();

        assert_eq!(sub_sampling_interval.as_duration(), duration);
    }
}
