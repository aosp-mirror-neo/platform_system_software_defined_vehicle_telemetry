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

use sdv_telemetry_expr::node_value::NodeValue;
use sdv_telemetry_proto_rust::expression::combination_node::RelationalOperator;
use sdv_telemetry_utils::option_extension::OptionExtension;

use super::{apply_relational_operator, ConditionChecker};

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
        let (previous_value, current_value) = self.previous_value.take_and_insert(current_value);
        let Some(previous_value) = previous_value else {
            return Ok(false);
        };

        apply_relational_operator(self.operator, &previous_value, current_value)
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
