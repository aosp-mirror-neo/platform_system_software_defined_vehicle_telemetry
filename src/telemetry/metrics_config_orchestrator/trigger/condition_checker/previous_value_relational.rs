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

use super::{apply_relational_operator, ConditionChecker, EvaluatedFirstValuePolicy};

/// Compares current value with the previous value using provided operator.
pub(crate) struct PreviousValueRelationalConditionChecker {
    operator: RelationalOperator,
    previous_value: Option<NodeValue>,

    // Handling policy of the first evaluation, as specified in the metrics configuration.
    first_value_policy: EvaluatedFirstValuePolicy,
}

impl ConditionChecker for PreviousValueRelationalConditionChecker {
    fn check(&mut self, current_value: NodeValue) -> anyhow::Result<bool> {
        if current_value.is_none() {
            // TODO: b/307547570 - properly handle absent values
            return Ok(false);
        }

        // Extract previous value and save current value.
        let (previous_value, current_value) = self.previous_value.take_and_insert(current_value);
        let previous_value = match previous_value {
            Some(previous_value) => previous_value,
            // Handle the very first expression evaluation
            None => match &self.first_value_policy {
                EvaluatedFirstValuePolicy::Skip => return Ok(false),
                EvaluatedFirstValuePolicy::FireTrigger => return Ok(true),
                EvaluatedFirstValuePolicy::CompareTo { value } => value.clone(),
            },
        };

        apply_relational_operator(self.operator, &previous_value, current_value)
    }
}

impl PreviousValueRelationalConditionChecker {
    pub(crate) fn new_all_changes(first_value_policy: EvaluatedFirstValuePolicy) -> Self {
        Self { operator: RelationalOperator::NOT_EQ, previous_value: None, first_value_policy }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::assert_that;
    use googletest::matchers::container_eq;
    use lazy_static::lazy_static;
    use protobuf::well_known_types::empty::Empty;
    use sdv_telemetry_test_utils::parameterized_test;
    use std::sync::Arc;

    use crate::metrics_config_orchestrator::protobuf_dynamic_message::ProtobufDynamicMessage;

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

    fn all_changes(
        first_value_policy: EvaluatedFirstValuePolicy,
    ) -> PreviousValueRelationalConditionChecker {
        PreviousValueRelationalConditionChecker::new_all_changes(first_value_policy)
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

        all_changes:        (all_changes(EvaluatedFirstValuePolicy::Skip), &[&INT_1, /* change */ &INT_2, /* no change */ &INT_2], &[F, T, F]),
        all_changes_string: (all_changes(EvaluatedFirstValuePolicy::Skip), &[&STR_0, /* change */ &STR_1, /* no change */ &STR_1], &[F, T, F]),
        all_changes_mixed:  (all_changes(EvaluatedFirstValuePolicy::Skip), &[&STR_0, &STR_1, &TRUE, &INT_1, &INT_0], &[F, T, T, T, T]),
        all_changes_nones:   (all_changes(EvaluatedFirstValuePolicy::Skip), &[&NONE, &NONE], &[F, F]),
        all_changes_none_first:   (all_changes(EvaluatedFirstValuePolicy::Skip), &[&NONE, &STR_1], &[F, F]),
        all_changes_none_between_change:   (all_changes(EvaluatedFirstValuePolicy::Skip), &[&INT_1, &NONE, &INT_2], &[F, F, T]),
        all_changes_none_between_no_change:   (all_changes(EvaluatedFirstValuePolicy::Skip), &[&INT_1, &NONE, &INT_1], &[F, F, F]),

        all_changes_fire_initial:        (all_changes(EvaluatedFirstValuePolicy::FireTrigger), &[&INT_1, /* change */ &INT_2, /* no change */ &INT_2], &[T, T, F]),
        all_changes_string_fire_initial: (all_changes(EvaluatedFirstValuePolicy::FireTrigger), &[&STR_0, /* change */ &STR_1, /* no change */ &STR_1], &[T, T, F]),
        all_changes_mixed_fire_initial:  (all_changes(EvaluatedFirstValuePolicy::FireTrigger), &[&STR_0, &STR_1, &TRUE, &INT_1, &INT_0], &[T, T, T, T, T]),
        all_changes_nones_fire_initial:   (all_changes(EvaluatedFirstValuePolicy::FireTrigger), &[&NONE, &NONE], &[F, F]),
        all_changes_none_first_fire_initial:   (all_changes(EvaluatedFirstValuePolicy::FireTrigger), &[&NONE, &STR_1], &[F, T]),
        all_changes_none_between_change_fire_initial:   (all_changes(EvaluatedFirstValuePolicy::FireTrigger), &[&INT_1, &NONE, &INT_2], &[T, F, T]),
        all_changes_none_between_no_change_fire_initial:   (all_changes(EvaluatedFirstValuePolicy::FireTrigger), &[&INT_1, &NONE, &INT_1], &[T, F, F]),

        all_changes_initial_value_change: (all_changes(EvaluatedFirstValuePolicy::CompareTo { value: INT_1.clone() }), &[&INT_2, &INT_2, &INT_1], &[T, F, T]),
        all_changes_initial_value_no_change: (all_changes(EvaluatedFirstValuePolicy::CompareTo { value: INT_1.clone() }), &[&INT_1, &INT_2, &INT_2], &[F, T, F]),
        all_changes_initial_value_string_change: (all_changes(EvaluatedFirstValuePolicy::CompareTo { value: STR_0.clone() }), &[&STR_1, &STR_1, &STR_0], &[T, F, T]),
        all_changes_initial_value_string_no_change: (all_changes(EvaluatedFirstValuePolicy::CompareTo { value: STR_0.clone() }), &[&STR_0, &STR_1, &STR_1], &[F, T, F]),
        all_changes_initial_value_first_none_change: (all_changes(EvaluatedFirstValuePolicy::CompareTo { value: INT_1.clone() }), &[&NONE, &INT_2, &INT_2], &[F, T, F]),
        all_changes_initial_value_first_none_no_change: (all_changes(EvaluatedFirstValuePolicy::CompareTo { value: INT_1.clone() }), &[&NONE, &INT_1, &INT_2], &[F, F, T]),
        all_changes_initial_value_different_node_types: (all_changes(EvaluatedFirstValuePolicy::CompareTo { value: NodeValue::Message(Arc::new(ProtobufDynamicMessage::new(Arc::new(Empty::default())))) }), &[&INT_1, &INT_2], &[T, T]),
    );
}
