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
use sdv_telemetry_proto_rust::expression::combination_node::{ArithmeticOperator, Operator};
use std::mem;

use crate::metrics_config_orchestrator::message_builder::{AggregationError, ValueAggregator};

/// Field value type where numeric delta is calculated.
#[cfg_attr(test, derive(std::fmt::Debug))]
pub(crate) struct DeltaAggregator {
    // Stores the last seen node to be used when calculating the delta for the next node.
    previous_value: NodeValue,
}

impl DeltaAggregator {
    pub(crate) fn new() -> Self {
        Self { previous_value: NodeValue::None }
    }
}

impl ValueAggregator for DeltaAggregator {
    fn compute(&mut self, node_value: Option<NodeValue>) -> Result<NodeValue, AggregationError> {
        let node_value = node_value.expect("Aggregator should be provided with a value");
        if node_value.is_none() {
            return Ok(NodeValue::None);
        }

        if !node_value.is_numeric() {
            panic!(
                "Validation should have ensured that delta aggregation type \
                    has numeric expression evaluation results"
            );
        }

        // Previous value is `None` for the first received node.
        if self.previous_value.is_none() {
            self.previous_value = node_value;
            Ok(NodeValue::None)
        } else {
            let previous_value = mem::replace(&mut self.previous_value, node_value.clone());
            let current_delta = node_value.operate_sync(
                &Operator::ArithmeticOperator(ArithmeticOperator::SUBTRACT.into()),
                || &previous_value,
            )?;
            Ok(current_delta)
        }
    }

    fn reset(&mut self) {
        self.previous_value = NodeValue::None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_computed_values_from_aggregator<T>(node_outputs: &[T]) -> Vec<NodeValue>
    where
        NodeValue: std::convert::From<T>,
        T: Copy,
    {
        let mut aggregator = DeltaAggregator::new();
        // Index 0 is ignored as delta for that is None which cannot be converted into a number.
        node_outputs
            .iter()
            .map(|value| aggregator.compute(Some((*value).into())).unwrap())
            .skip(1)
            .collect()
    }

    #[test]
    fn compute_with_int_ok() {
        let node_outputs: &[i64] = &[-2, 0, 2, 4, 6, 8, -32, 0];
        let expected_delta: &[i64] = &[2, 2, 2, 2, 2, -40, 32];

        let res = get_computed_values_from_aggregator(node_outputs);

        assert_eq!(res, expected_delta.iter().copied().map(NodeValue::from).collect::<Vec<_>>());
    }

    #[test]
    fn compute_with_float_ok() {
        let node_outputs: &[f64] = &[-2.50, 0.0, 2.20, 4.0, 6.0, 8.70, -32.0, 0.0];
        let expected_delta: &[f64] = &[2.5, 2.2, 1.8, 2.0, 2.7, -40.7, 32.0];

        let res = get_computed_values_from_aggregator(node_outputs);

        assert_eq!(res, expected_delta.iter().copied().map(NodeValue::from).collect::<Vec<_>>());
    }

    #[test]
    fn compute_with_none_value_ok() {
        let mut aggregator = DeltaAggregator::new();
        let res = aggregator.compute(Some(NodeValue::None)).unwrap();

        assert_eq!(res, NodeValue::None);
    }

    #[test]
    #[should_panic]
    fn compute_with_non_numeric_value_panics() {
        let mut aggregator = DeltaAggregator::new();
        aggregator.compute(Some(NodeValue::Bool(true))).unwrap();
    }

    #[test]
    #[should_panic]
    fn compute_without_node_value_panics() {
        let mut aggregator = DeltaAggregator::new();
        aggregator.compute(None).unwrap();
    }

    #[test]
    fn reset_ok() {
        let mut aggregator = DeltaAggregator::new();
        aggregator.compute(Some(2.into())).unwrap();

        aggregator.reset();

        assert_eq!(aggregator.previous_value, NodeValue::None);
    }
}
