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

    use sdv_telemetry_test_utils::parameterized_test;

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

    parameterized_test!(
        #[test]
        try_into_duration_negative,

        |duration_proto| {
            let result = try_into_duration(&duration_proto);

            assert!(
                result.is_err(),
                "try_into_duration({duration_proto:?}) = {result:?}, but wanted error"
            );
        },

        seconds_failure: (DurationProto { seconds: -10, nanos: 123, ..Default::default() }, ),
        nanos_failure: (DurationProto { seconds: 10, nanos: -123, ..Default::default() }, ),
    );

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
