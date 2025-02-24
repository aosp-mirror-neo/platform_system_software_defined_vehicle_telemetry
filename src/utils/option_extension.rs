// Copyright 2025 Google LLC

//! This module contains utility functions for `Option`

/// Provides utility methods for `Option`.
pub trait OptionExtension<T> {
    /// Replaces the contained value by the provided `value`, and returns the old value as well as a
    /// reference to the new value. This is similar to `Option::replace`, but not only returns the
    /// old value, but also a reference to the new value.
    fn take_and_insert(&mut self, value: T) -> (Option<T>, &mut T);
}

impl<T> OptionExtension<T> for Option<T> {
    fn take_and_insert(&mut self, value: T) -> (Self, &mut T) {
        (self.take(), self.insert(value))
    }
}

#[cfg(test)]
mod tests {
    use googletest::expect_that;
    use googletest::matchers::{eq, none, some};
    use sdv_telemetry_test_utils::parameterized_test;

    use super::OptionExtension;

    parameterized_test!(
        #[googletest::test]
        option_extension,

        |mut maybe_value: Option<u64>, value, result_matcher, option_matcher| {
            expect_that!(maybe_value.take_and_insert(value), result_matcher);
            expect_that!(maybe_value, option_matcher);
        },

        take_and_insert_none: (None,      123, (none(),         eq(&&123)), some(eq(123))),
        take_and_insert_some: (Some(123), 456, (some(eq(&123)), eq(&&456)), some(eq(456))),
    );
}
