// Copyright 2023 Google LLC

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

    /// Returns the greatest common denominator of `self` and `other`, or `ZERO` if any of them is
    /// `ZERO`.
    pub fn gcd_or_zero(&self, other: &Self) -> Self {
        if self == &Self::ZERO || other == &Self::ZERO {
            return Self::ZERO;
        }
        use num_integer::Integer;
        Self { interval_ms: self.interval_ms.gcd(&other.interval_ms) }
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

    use sdv_telemetry_test_utils::parameterized_test;

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

    parameterized_test!(
        sub_sampling_interval_gcd_or_zero,

        |expected, a, b| {
            let expected = SubSamplingInterval::try_from(Duration::from_millis(expected)).unwrap();
            let a = SubSamplingInterval::try_from(Duration::from_millis(a)).unwrap();
            let b = SubSamplingInterval::try_from(Duration::from_millis(b)).unwrap();

            let result = a.gcd_or_zero(&b);

            assert_eq!(result, expected);
        },

        zero_and_zero: (0, 0, 0,),
        zero_and_five: (0, 0, 5,),
        five_and_zero: (0, 5, 0,),
        five_and_six: (1, 5, 6,),
        ten_and_fifty: (10, 10, 50,),
        thirty_and_forty_five: (15, 30, 45,),
    );
}
