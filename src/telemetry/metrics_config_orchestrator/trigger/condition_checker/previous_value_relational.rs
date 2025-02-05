// Copyright 2025 Google LLC

use anyhow::{bail, Context};
use sdv_telemetry_proto_rust::expression::combination_node::{Operator, RelationalOperator};

use super::ConditionChecker;
use crate::expr::node_value::NodeValue;

/// Compares current value with the previous value using provided operator.
pub(crate) struct PreviousValueRelationalConditionChecker {
    operator: Operator,
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

        let result = current_value
            .operate_sync(&self.operator, || Ok(previous_value))
            .with_context(|| format!("Failed to apply operator {:?}", self.operator))?;
        let NodeValue::Bool(result) = result else {
            bail!("Operator {:?} evaluation returned non-bool result: {result:?}", self.operator);
        };
        Ok(result)
    }
}

impl PreviousValueRelationalConditionChecker {
    pub(crate) fn new_rising_edge() -> Self {
        Self {
            operator: Operator::RelationalOperator(RelationalOperator::GT.into()),
            previous_value: None,
        }
    }

    pub(crate) fn new_falling_edge() -> Self {
        Self {
            operator: Operator::RelationalOperator(RelationalOperator::LT.into()),
            previous_value: None,
        }
    }

    pub(crate) fn new_all_changes() -> Self {
        Self {
            operator: Operator::RelationalOperator(RelationalOperator::NOT_EQ.into()),
            previous_value: None,
        }
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
    const E: Result<bool, ()> = Err(());

    fn rising_edge() -> PreviousValueRelationalConditionChecker {
        PreviousValueRelationalConditionChecker::new_rising_edge()
    }

    fn falling_edge() -> PreviousValueRelationalConditionChecker {
        PreviousValueRelationalConditionChecker::new_falling_edge()
    }

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

        rising_edge:        (rising_edge(), &[&INT_1, /* rising */ &INT_2, /* falling */ &INT_1], &[F, T, F]),
        rising_edge_string: (rising_edge(), &[&STR_0, &STR_0], &[F, E]),
        rising_edge_none:   (rising_edge(), &[&NONE, &NONE], &[F, F]),

        falling_edge:        (falling_edge(), &[&INT_2, /* falling */ &INT_1, /* rising */ &INT_2], &[F, T, F]),
        falling_edge_string: (falling_edge(), &[&STR_0, &STR_0], &[F, E]),
        falling_edge_none:   (falling_edge(), &[&NONE, &NONE], &[F, F]),
    );
}
