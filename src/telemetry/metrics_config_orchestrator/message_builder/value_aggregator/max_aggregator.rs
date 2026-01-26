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

use super::node_value_extension::NodeValueExtension;
use crate::metrics_config_orchestrator::message_builder::{AggregationError, ValueAggregator};

/// Field value type where aggregation for numeric maximum is performed.
#[cfg_attr(test, derive(std::fmt::Debug))]
pub(crate) struct MaxAggregator {
    // Stores the maximum so far.
    current_max: Option<NodeValue>,
}

impl MaxAggregator {
    pub(crate) fn new() -> Self {
        Self { current_max: None }
    }
}

impl ValueAggregator for MaxAggregator {
    fn compute(&mut self, node_value: Option<NodeValue>) -> Result<NodeValue, AggregationError> {
        let node_value = node_value.expect("Aggregator should be provided with a value");
        if node_value.is_none() {
            return Ok(NodeValue::None);
        }

        if !node_value.is_numeric() {
            return Err(AggregationError::NumericAggregationTypeMismatch {
                value: format!("{node_value:?}"),
            });
        }

        if let Some(max) = &self.current_max {
            if max.clone().less_than(&node_value)? {
                self.current_max = Some(node_value.clone());
                Ok(node_value)
            } else {
                Ok(max.clone())
            }
        } else {
            self.current_max = Some(node_value.clone());
            Ok(node_value)
        }
    }

    fn reset(&mut self) {
        self.current_max = None;
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
        let mut aggregator = MaxAggregator::new();
        node_outputs
            .iter()
            .map(|value| aggregator.compute(Some((*value).into())).unwrap())
            .collect()
    }

    #[test]
    fn compute_with_int_ok() {
        let node_outputs: &[i32] = &[0, 1, 2, 0, -1, 3, 0, -1];
        let expected_max: &[i32] = &[0, 1, 2, 2, 2, 3, 3, 3];

        let res = get_computed_values_from_aggregator(node_outputs);

        assert_eq!(res, expected_max.iter().copied().map(NodeValue::from).collect::<Vec<_>>());
    }

    #[test]
    fn compute_with_float_ok() {
        let node_outputs: &[f32] = &[-1.0, -0.5, 1.0, 2.5, 0.0, -1.0];
        let expected_max: &[f32] = &[-1.0, -0.5, 1.0, 2.5, 2.5, 2.5];

        let res = get_computed_values_from_aggregator(node_outputs);

        assert_eq!(res, expected_max.iter().copied().map(NodeValue::from).collect::<Vec<_>>());
    }

    #[test]
    fn compute_with_none_value_ok() {
        let mut aggregator = MaxAggregator::new();
        let res = aggregator.compute(Some(NodeValue::None)).unwrap();

        assert_eq!(res, NodeValue::None);
    }

    #[test]
    fn compute_with_non_numeric_value_fails() {
        let mut aggregator = MaxAggregator::new();
        let err = aggregator.compute(Some(NodeValue::Bool(true))).unwrap_err();

        assert!(
            matches!(&err, AggregationError::NumericAggregationTypeMismatch { .. }),
            "Unexpected error: {err:?}"
        );
    }

    #[test]
    #[should_panic]
    fn compute_without_node_value_panics() {
        let mut aggregator = MaxAggregator::new();
        aggregator.compute(None).unwrap();
    }

    #[test]
    fn reset_ok() {
        let mut aggregator = MaxAggregator::new();
        aggregator.compute(Some(2.into())).unwrap();

        aggregator.reset();

        assert_eq!(aggregator.current_max, None);
    }
}
