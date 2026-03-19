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

use std::str::FromStr;

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
/// Errors related to creating a `PublisherName`.
pub enum PublisherNameError {
    /// The publisher name must not be empty.
    #[error("The publisher name must not be empty")]
    #[non_exhaustive]
    Empty,
}

/// A unique identifier for a publisher.
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
#[non_exhaustive]
pub struct PublisherName(pub(crate) String);

impl TryFrom<String> for PublisherName {
    type Error = PublisherNameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(PublisherNameError::Empty);
        }
        Ok(Self(value))
    }
}

impl FromStr for PublisherName {
    type Err = PublisherNameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.to_owned().try_into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::expect_that;
    use googletest::matchers::eq;
    use googletest::matchers::err;
    use googletest::matchers::matches_pattern;
    use googletest::matchers::ok;
    use googletest::matchers::pat;

    #[googletest::test]
    fn success() {
        let publisher_name = "my_publisher".parse::<PublisherName>();
        expect_that!(publisher_name, ok(pat!(PublisherName(eq("my_publisher")))));
    }

    #[googletest::test]
    fn empty_string_fails() {
        let publisher_name = "".parse::<PublisherName>();
        expect_that!(publisher_name, err(matches_pattern!(PublisherNameError::Empty)));
    }
}
