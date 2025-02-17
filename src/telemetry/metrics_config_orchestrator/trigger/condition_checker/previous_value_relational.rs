// Copyright 2025 Google LLC

use sdv_telemetry_proto_rust::expression::combination_node::RelationalOperator;

use super::{apply_relational_operator, ConditionChecker};
use crate::expr::node_value::NodeValue;

/// Compares current value with the previous value using provided operator.
pub(crate) struct PreviousValueRelationalConditionChecker {
    operator: RelationalOperator,
    previous_value: Option<NodeValue>,
}

impl ConditionChecker for PreviousValueRelationalConditionChecker {
    fn check(&mut self, current_value: NodeValue) -> anyhow::Result<bool> {
        if current_value.is_none() {
            // TODO: b/307547570 - properly handle absent values
            return Ok(false);
        }

        // Extract previous value and save current value. Return false if there's no previous value.
        let Some(previous_value) = self.previous_value.replace(current_value.clone()) else {
            return Ok(false);
        };

        apply_relational_operator(self.operator, previous_value, current_value)
    }
}

impl PreviousValueRelationalConditionChecker {
    pub(crate) fn new_all_changes() -> Self {
        Self { operator: RelationalOperator::NOT_EQ, previous_value: None }
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
    const TRUE: NodeValue = NodeValue::Bool(true);
    const INT_0: NodeValue = NodeValue::Int32(0);
    const INT_1: NodeValue = NodeValue::Int32(1);
    const INT_2: NodeValue = NodeValue::Int32(2);

    lazy_static! {
        static ref STR_0: NodeValue = NodeValue::String("0".to_owned());
        static ref STR_1: NodeValue = NodeValue::String("1".to_owned());
    }

    // Helper constants to make specifying the expected result more succinct.
    const T: Result<bool, ()> = Ok(true);
    const F: Result<bool, ()> = Ok(false);

    fn all_changes() -> PreviousValueRelationalConditionChecker {
        PreviousValueRelationalConditionChecker::new_all_changes()
    }

    parameterized_test!(
        #[test]
        previous_value_relational_condition_checker,

        |mut condition_checker: PreviousValueRelationalConditionChecker, values: &[&NodeValue], want: &[Result<bool, ()>]| {
            let got = values
                .iter()
                .map(|value| condition_checker.check(value.to_owned().clone()).map_err(|_| ()))
                .collect::<Vec<_>>();

            assert_that!(got, container_eq(want.to_vec()));
        },

        all_changes:        (all_changes(), &[&INT_1, /* change */ &INT_2, /* no change */ &INT_2], &[F, T, F]),
        all_changes_string: (all_changes(), &[&STR_0, /* change */ &STR_1, /* no change */ &STR_1], &[F, T, F]),
        all_changes_mixed:  (all_changes(), &[&STR_0, &STR_1, &TRUE, &INT_1, &INT_0], &[F, T, T, T, T]),
        all_changes_none:   (all_changes(), &[&NONE, &NONE], &[F, F]),
    );
}
