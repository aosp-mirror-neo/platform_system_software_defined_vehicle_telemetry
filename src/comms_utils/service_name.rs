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

//! Abstraction of Comms Stack service names.

use anyhow::{anyhow, Context};
use sdv_comms::id::ServiceFqin;
use sdv_comms::sd::UnitType;
use std::fmt;
use std::str::FromStr;

/// A `ServiceName` is the identifier of an SDV Comms service.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum ServiceName {
    /// The service is addressed via its unit type, and optionally unit name.
    UnitTypeBased {
        /// The service's unit type.
        unit_type: UnitType,
        /// Optionally, the service's unit name.
        unit_name: Option<String>,
    },
    /// The service is addressed via its FQIN and unit name.
    FqinBased {
        /// The service's FQIN.
        fqin: ServiceFqin,
        /// The service's unit name.
        unit_name: String,
    },
}

impl ServiceName {
    /// Tries to parse a string into a `ServiceName`.
    pub fn parse(input: &str) -> anyhow::Result<Self> {
        if input.contains(':') {
            let (fqin, unit_name) = input.rsplit_once("|").ok_or(anyhow!(
                "Missing `|` in input '{input}' to divide the FQIN from the Service Unit Name."
            ))?;
            ServiceFqin::from_str(fqin)
                .with_context(|| "Failed to parse FQIN.")
                .map(|fqin| Self::FqinBased { fqin, unit_name: unit_name.to_owned() })
        } else {
            let (unit_type, unit_name) = input
                .rsplit_once("|")
                .map(|(unit_type, unit_name)| (unit_type, Some(unit_name.to_owned())))
                .unwrap_or((input, None));
            UnitType::from_str(unit_type)
                .with_context(|| "Failed to parse Unit Type.")
                .map(|unit_type| Self::UnitTypeBased { unit_type, unit_name })
        }
    }
}

impl fmt::Display for ServiceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnitTypeBased { unit_type, unit_name } => {
                write!(
                    f,
                    "UnitTypeBased(unit_type: {unit_type}, unit_name: {})",
                    unit_name.as_deref().unwrap_or("N/A")
                )
            }
            Self::FqinBased { fqin, unit_name } => {
                write!(f, "FqinBased(fqin: {fqin}, unit_name: {unit_name})")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::assert_that;
    use googletest::matchers::anything;
    use googletest::matchers::eq;
    use googletest::matchers::err;
    use googletest::matchers::ok;
    use sdv_comms::id::ServiceFqin;
    use sdv_comms::sd::UnitType;
    use sdv_telemetry_test_utils::parameterized_test;
    use std::str::FromStr;

    parameterized_test!(
        #[test]
        service_name_parser,

        |input, expected| {
            assert_that!(ServiceName::parse(input), expected);
        },

        unsupported_service_name_fails: (
            "hello",
            err(anything()),
        ),
        multiple_pipes_fails: (
            "vm:com.google.sdv.telemetry.tests.Blah/instance|the-unit-name|another-thing",
            err(anything()),
        ),
        fqin_fails: (
            "vm:com.google.sdv.telemetry.tests.Blah/instance",
            err(anything()),
        ),
        fqin_with_unit_name_success: (
            "vm:com.google.sdv.telemetry.tests.Blah/instance|the-unit-name",
            ok(
                eq(
                    &ServiceName::FqinBased{
                        fqin: ServiceFqin::from_str("vm:com.google.sdv.telemetry.tests.Blah/instance").unwrap(),
                        unit_name: "the-unit-name".to_owned()
                    }
                )
            ),
        ),
        unit_type_without_unit_name_success: (
            "com.google.sdv.telemetry.tests.Blah#Blubb",
            ok(
                eq(
                    &ServiceName::UnitTypeBased{
                        unit_type: UnitType::from_str("com.google.sdv.telemetry.tests.Blah#Blubb").unwrap(),
                        unit_name: None,
                    }
                )
            ),
        ),
        unit_type_with_unit_name_success: (
            "com.google.sdv.telemetry.tests.Blah#Blubb|the-unit-name",
            ok(
                eq(
                    &ServiceName::UnitTypeBased{
                        unit_type: UnitType::from_str("com.google.sdv.telemetry.tests.Blah#Blubb").unwrap(),
                        unit_name: Some("the-unit-name".to_owned())
                    }
                )
            ),
        ),
    );
}
