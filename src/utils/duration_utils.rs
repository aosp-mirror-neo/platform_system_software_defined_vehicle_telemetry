// Copyright 2024 Google LLC. All Rights Reserved.

//! This module contains conversion functions from Rust's `Duration` type to protobuf's duration
//! type.

use protobuf::well_known_types::duration::Duration as DurationProto;
use std::time::Duration;

/// Tries to convert a protobuf `DurationProto` to a Rust `Duration`. Only positive durations are
/// supported.
pub fn try_into_duration(
    duration_proto: &DurationProto,
) -> Result<Duration, std::num::TryFromIntError> {
    Ok(Duration::new(duration_proto.seconds.try_into()?, duration_proto.nanos.try_into()?))
}

/// Tries to convert a Rust `Duration` to a protobuf `DurationProto`. May fail if the duration is
/// too large.
pub fn try_into_proto_duration(
    duration: &Duration,
) -> Result<DurationProto, std::num::TryFromIntError> {
    Ok(DurationProto {
        seconds: duration.as_secs().try_into()?,
        nanos: duration.subsec_nanos().try_into()?,
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_into_duration_success() {
        let duration_proto = DurationProto { seconds: 10, nanos: 20, ..Default::default() };

        let duration = try_into_duration(&duration_proto).unwrap();

        assert_eq!(duration.as_secs(), 10);
        assert_eq!(duration.subsec_nanos(), 20);
    }

    #[test]
    fn try_into_duration_maximum_value_success() {
        let duration_proto =
            DurationProto { seconds: i64::MAX, nanos: i32::MAX, ..Default::default() };

        let duration = try_into_duration(&duration_proto).unwrap();

        assert_eq!(
            duration,
            Duration::from_secs(i64::MAX.try_into().unwrap())
                + Duration::from_nanos(i32::MAX.try_into().unwrap())
        );
    }

    #[test]
    fn try_into_duration_negative_value_failure() {
        let duration_proto = DurationProto { seconds: -10, nanos: -123, ..Default::default() };

        let result = try_into_duration(&duration_proto);

        assert!(result.is_err());
    }

    #[test]
    fn try_into_proto_duration_success() {
        let duration = Duration::new(10, 20);

        let duration_proto = try_into_proto_duration(&duration).unwrap();

        assert_eq!(duration_proto.seconds, 10);
        assert_eq!(duration_proto.nanos, 20);
    }

    #[test]
    fn try_into_proto_duration_overflow_failure() {
        let duration = Duration::MAX;

        let result = try_into_proto_duration(&duration);

        assert!(result.is_err());
    }
}
