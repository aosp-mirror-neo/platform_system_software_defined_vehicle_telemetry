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

//! This module contains utility functions for `Option`

/// Provides utility methods for `Option`.
pub trait OptionExtension<T> {
    /// Replaces the contained value by the provided `value`, and returns the old value as well as a
    /// reference to the new value. This is similar to `Option::replace`, but not only returns the
    /// old value, but also a reference to the new value.
    fn take_and_insert(&mut self, value: T) -> (Option<T>, &mut T);

    /// Calls a function if `None`. This is the counterpart to `Option::inspect`.
    ///
    /// Returns the original option.
    fn inspect_none<F>(self, f: F) -> Self
    where
        F: FnOnce();
}

impl<T> OptionExtension<T> for Option<T> {
    fn take_and_insert(&mut self, value: T) -> (Self, &mut T) {
        (self.take(), self.insert(value))
    }

    fn inspect_none<F>(self, f: F) -> Self
    where
        F: FnOnce(),
    {
        if self.is_none() {
            f();
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use googletest::matchers::{eq, none, some};
    use googletest::{assert_that, expect_that};
    use sdv_telemetry_test_utils::parameterized_test;

    use super::OptionExtension;

    parameterized_test!(
        #[googletest::test]
        option_extension_take_and_insert,

        |mut maybe_value: Option<u64>, value, result_matcher, option_matcher| {
            expect_that!(maybe_value.take_and_insert(value), result_matcher);
            expect_that!(maybe_value, option_matcher);
        },

        none: (None,      123, (none(),         eq(&&123)), some(eq(123))),
        some: (Some(123), 456, (some(eq(&123)), eq(&&456)), some(eq(456))),
    );

    parameterized_test!(
        #[test]
        option_extension_inspect_none,

        |value: Option<u64>, should_inspect: bool| {
            let mut inspected = false;
            value.inspect_none(|| inspected = true);
            assert_that!(inspected, eq(should_inspect));
        },

        none: (None,      true),
        some: (Some(123), false),
    );
}
