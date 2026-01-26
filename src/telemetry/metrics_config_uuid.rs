/*
 * Copyright (C) 2026 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::fmt;
use std::{ffi::OsString, str::FromStr};
use uuid::{fmt::Hyphenated as HyphenatedUuid, Uuid};

#[derive(thiserror::Error, Debug)]
pub(crate) enum MetricsConfigUuidParseError {
    #[error("Metrics Config ID is formatted incorrectly ({err}); must be a valid UUID in lowercase hyphenated format (e.g., 'a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8'), but got: '{input}'")]
    InvalidFormat { input: String, err: String },

    #[error("Metrics Config ID must not be the Nil UUID (https://www.ietf.org/rfc/rfc9562.html#name-nil-uuid), but got: '{input}'")]
    IsNil { input: String },
    #[error("Metrics Config ID must not be the Max UUID (https://www.ietf.org/rfc/rfc9562.html#name-max-uuid), but got: '{input}'")]
    IsMax { input: String },
}

#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Debug)]
pub(crate) struct MetricsConfigUuid {
    uuid: HyphenatedUuid,
}

impl FromStr for MetricsConfigUuid {
    type Err = MetricsConfigUuidParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let uuid =
            Uuid::try_parse(input).map_err(|err| MetricsConfigUuidParseError::InvalidFormat {
                input: input.to_owned(),
                err: err.to_string(),
            })?;
        let uuid = HyphenatedUuid::from(uuid);

        // `Uuid::try_parse` supports multiple non-standard UUID formats. This check here ensures
        // that the input format is the hyphenated, standardized format (e.g.,
        // 'a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8'). The standard technically allows both upper- and
        // lowercase UUIDs, but we deliberately only support lowercase UUIDs.
        if uuid.to_string() != input {
            return Err(MetricsConfigUuidParseError::InvalidFormat {
                input: input.to_owned(),
                err: "Invalid format".to_owned(),
            });
        }

        if uuid.as_uuid().is_nil() {
            return Err(MetricsConfigUuidParseError::IsNil { input: input.to_owned() });
        }
        if uuid.as_uuid().is_max() {
            return Err(MetricsConfigUuidParseError::IsMax { input: input.to_owned() });
        }

        Ok(Self { uuid })
    }
}

impl fmt::Display for MetricsConfigUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // IMPORTANT: Whatever is written here, is used _as is_ as a file name. Therefore, we need
        // to ensure that we never add a "/" or "../" or similar here.
        write!(f, "{}", self.uuid)
    }
}

impl MetricsConfigUuid {
    /// Converts `self` into an `OsString` for use as a file name.
    pub(crate) fn to_os_string(self) -> OsString {
        self.to_string().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::assert_that;
    use googletest::matchers::{anything, err, ok, pat};
    use sdv_telemetry_test_utils::parameterized_test;

    parameterized_test!(
        #[test]
        try_parse,

        |input, matcher| {
            let result = MetricsConfigUuid::from_str(input);
            assert_that!(result, matcher);
        },

        lower_case: ("a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8", ok(anything())),

        // We deliberately do not allow non-lowercase UUIDs, even though RFC 9562 technically allows
        // them. This is because we internally store the UUID in numeric format, which loses
        // information about the UUID's casing. However, our Binder and Client APIs operate on
        // strings, and callers may not be aware that they have to compare UUIDs case-insensitively.
        // By enforcing lowercase UUIDs, we remove this potential pitfall for our clients.
        mixed_case: ("A1a2a3a4-b1B2-c1c2-d1d2-d3d4d5d6d7d8", err(pat!(MetricsConfigUuidParseError::InvalidFormat { .. }))),
        upper_case: ("A1A2A3A4-B1B2-C1C2-D1D2-D3D4D5D6D7D8", err(pat!(MetricsConfigUuidParseError::InvalidFormat { .. }))),

        // We disallow all other formats that are sometimes used for UUIDs:
        no_hyphens: ("a1a2a3a4b1b2c1c2d1d2d3d4d5d6d7d8", err(pat!(MetricsConfigUuidParseError::InvalidFormat { .. }))),
        urn: ("urn:uuid:A1A2A3A4-B1B2-C1C2-D1D2-D3D4D5D6D7D8", err(pat!(MetricsConfigUuidParseError::InvalidFormat { .. }))),
        braced: ("{a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8}", err(pat!(MetricsConfigUuidParseError::InvalidFormat { .. }))),

        // We disallow the special "Nil UUID": https://www.ietf.org/rfc/rfc9562.html#name-nil-uuid
        nil: ("00000000-0000-0000-0000-000000000000", err(pat!(MetricsConfigUuidParseError::IsNil { .. }))),
        // We disallow the special "Max UUID": https://www.ietf.org/rfc/rfc9562.html#name-max-uuid
        max: ("ffffffff-ffff-ffff-ffff-ffffffffffff", err(pat!(MetricsConfigUuidParseError::IsMax { .. }))),
    );

    #[test]
    fn display_and_to_string_success() {
        let uuid = MetricsConfigUuid::from_str("a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8").unwrap();

        assert_eq!(format!("{uuid}"), "a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8");
        assert_eq!(uuid.to_string(), "a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8");
    }
}
