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

use crate::metrics_config_orchestrator::message_builder::{AggregationError, ValueAggregator};

const DEFAULT_SUM_VALUE: NodeValue = NodeValue::Int32(0);

/// Field value type where numeric sum is calculated.
#[cfg_attr(test, derive(std::fmt::Debug))]
pub(crate) struct SumAggregator {
    // Stores the sum of values so far.
    current_sum: NodeValue,
}

impl SumAggregator {
    pub(crate) fn new() -> Self {
        Self { current_sum: DEFAULT_SUM_VALUE }
    }
}

impl ValueAggregator for SumAggregator {
    fn compute(&mut self, node_value: Option<NodeValue>) -> Result<NodeValue, AggregationError> {
        let node_value = node_value.expect("Aggregator should be provided with a value");
        if node_value.is_none() {
            return Ok(self.current_sum.clone());
        }

        self.current_sum =
            self.current_sum.clone().operate_sync(
                &Operator::ArithmeticOperator(ArithmeticOperator::ADD.into()),
                || &node_value,
            )?;
        Ok(self.current_sum.clone())
    }

    fn reset(&mut self) {
        self.current_sum = DEFAULT_SUM_VALUE;
    }

    fn default_value(&self) -> NodeValue {
        DEFAULT_SUM_VALUE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_computed_sums_from_aggregator<T>(node_outputs: &[T]) -> Vec<NodeValue>
    where
        NodeValue: std::convert::From<T>,
        T: Copy,
    {
        let mut aggregator = SumAggregator::new();
        node_outputs
            .iter()
            .map(|value| aggregator.compute(Some((*value).into())).unwrap())
            .collect()
    }

    #[test]
    fn compute_with_int_ok() {
        let node_outputs: &[i64] = &[-2, 0, 2, 4, 6, 8, -32];
        let expected_sum: &[i64] = &[-2, -2, 0, 4, 10, 18, -14];
        let res = get_computed_sums_from_aggregator(node_outputs);
        assert_eq!(res, expected_sum.iter().copied().map(NodeValue::from).collect::<Vec<_>>());
    }

    #[test]
    fn compute_with_float_ok() {
        let node_outputs: &[f64] = &[-2.0, 0.0, 2.0, 4.0, 6.0, 8.0, -32.0];
        let expected_sum: &[f64] = &[-2.0, -2.0, 0.0, 4.0, 10.0, 18.0, -14.0];
        let res = get_computed_sums_from_aggregator(node_outputs);
        assert_eq!(res, expected_sum.iter().copied().map(NodeValue::from).collect::<Vec<_>>());
    }

    #[test]
    fn compute_with_none_value_ok() {
        let mut aggregator = SumAggregator::new();
        let res = aggregator.compute(Some(NodeValue::None)).unwrap();
        assert_eq!(res, NodeValue::Int32(0));

        aggregator.compute(Some(2.into())).unwrap();
        aggregator.compute(Some(3.into())).unwrap();
        let res = aggregator.compute(Some(NodeValue::None)).unwrap();
        assert_eq!(res, NodeValue::Int64(5));
    }

    #[test]
    #[should_panic]
    fn compute_with_non_numeric_value_panics() {
        let mut bad_sum_aggregator = SumAggregator::new();
        bad_sum_aggregator.compute(Some(NodeValue::Bool(true))).unwrap();
    }

    #[test]
    #[should_panic]
    fn compute_without_node_value_panics() {
        let mut aggregator = SumAggregator::new();
        aggregator.compute(None).unwrap();
    }

    #[test]
    fn reset_ok() {
        let mut aggregator = SumAggregator::new();
        aggregator.compute(Some(2.into())).unwrap();

        aggregator.reset();

        assert_eq!(aggregator.current_sum, DEFAULT_SUM_VALUE);
    }

    #[test]
    fn default_value_zero() {
        let default_value = SumAggregator::new().default_value();
        assert_eq!(default_value, NodeValue::Int32(0));
    }
}
