// Copyright 2025 Google LLC

use super::ConditionChecker;
use crate::expr::node_value::NodeValue;

/// Checks that the value is equal to `expected` (numbers are treated as `true` iff they are not 0).
pub(crate) struct ConstantConditionChecker {
    expected: bool,
}

impl ConditionChecker for ConstantConditionChecker {
    fn check(&mut self, value: NodeValue) -> anyhow::Result<bool> {
        Ok(match value {
            // TODO: b/307547570 - properly handle absent values
            NodeValue::None => false,

            NodeValue::Bool(boolean) => boolean == self.expected,
            // If the value is not a bool or None, it's numeric. Otherwise validation would fail.
            numeric_value => {
                let is_not_zero = !numeric_value.is_zero()?;

                is_not_zero == self.expected
            }
        })
    }
}

impl ConstantConditionChecker {
    pub(crate) fn new_is_true_or_non_zero() -> Self {
        Self { expected: true }
    }

    pub(crate) fn new_is_false_or_zero() -> Self {
        Self { expected: false }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::assert_that;
    use googletest::matchers::container_eq;
    use lazy_static::lazy_static;
    use sdv_telemetry_test_utils::parameterized_test;

    const NONE: NodeValue = NodeValue::None;
    const FALSE: NodeValue = NodeValue::Bool(false);
    const TRUE: NodeValue = NodeValue::Bool(true);
    const INT_0: NodeValue = NodeValue::Int32(0);
    const INT_1: NodeValue = NodeValue::Int32(1);

    lazy_static! {
        static ref STR_0: NodeValue = NodeValue::String("0".to_owned());
    }

    // Helper constants to make specifying the expected result more succinct.
    const T: Result<bool, ()> = Ok(true);
    const F: Result<bool, ()> = Ok(false);
    const E: Result<bool, ()> = Err(());

    fn true_or_non_zero() -> ConstantConditionChecker {
        ConstantConditionChecker::new_is_true_or_non_zero()
    }

    fn false_or_zero() -> ConstantConditionChecker {
        ConstantConditionChecker::new_is_false_or_zero()
    }

    parameterized_test!(
        #[test]
        constant_condition_checker,

        |mut condition_checker: ConstantConditionChecker, values: &[&NodeValue], want: &[Result<bool, ()>]| {
            let got = values
                .iter()
                .map(|value| condition_checker.check(value.to_owned().clone()).map_err(|_| ()))
                .collect::<Vec<_>>();

            assert_that!(got, container_eq(want.to_vec()));
        },

        is_true_or_non_zero_bool_1:    (true_or_non_zero(), &[&TRUE], &[T]),
        is_true_or_non_zero_bool_2:    (true_or_non_zero(), &[&FALSE, &TRUE, &FALSE, &TRUE], &[F, T, F, T]),
        is_true_or_non_zero_numeric_1: (true_or_non_zero(), &[&INT_1], &[T]),
        is_true_or_non_zero_numeric_2: (true_or_non_zero(), &[&INT_0, &INT_1, &INT_0, &INT_1], &[F, T, F, T]),
        is_true_or_non_zero_string:    (true_or_non_zero(), &[&STR_0], &[E]),
        is_true_or_non_zero_none:      (true_or_non_zero(), &[&NONE], &[F]),

        is_false_or_zero_bool_1:    (false_or_zero(), &[&FALSE], &[T]),
        is_false_or_zero_bool_2:    (false_or_zero(), &[&TRUE, &FALSE, &TRUE, &FALSE], &[F, T, F, T]),
        is_false_or_zero_numeric_1: (false_or_zero(), &[&INT_0], &[T]),
        is_false_or_zero_numeric_2: (false_or_zero(), &[&INT_1, &INT_0, &INT_1, &INT_0], &[F, T, F, T]),
        is_false_or_zero_string:    (false_or_zero(), &[&STR_0], &[E]),
        is_false_or_zero_none:      (false_or_zero(), &[&NONE], &[F]),
    );
}
