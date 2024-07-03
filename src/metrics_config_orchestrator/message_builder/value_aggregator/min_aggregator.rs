// Copyright 2024 Google LLC

use super::node_value_extension::NodeValueExtension;
use crate::expr::node_value::NodeValue;
use crate::metrics_config_orchestrator::message_builder::{AggregationError, ValueAggregator};

/// Field value type where aggregation for numeric minimum is performed.
pub struct MinAggregator {
    // Stores the minimum so far.
    current_min: Option<NodeValue>,
}

impl MinAggregator {
    pub fn new() -> Self {
        Self { current_min: None }
    }
}

impl ValueAggregator for MinAggregator {
    fn compute(&mut self, node_value: Option<NodeValue>) -> Result<NodeValue, AggregationError> {
        let node_value = node_value.expect("Aggregator should be provided with a value");
        if node_value.is_none() {
            return Ok(NodeValue::None);
        }

        if !node_value.is_numeric() {
            return Err(AggregationError::NumericAggregationTypeMismatch {
                value: format!("{:?}", node_value),
            });
        }

        if let Some(min) = &self.current_min {
            if node_value.clone().less_than(min)? {
                self.current_min = Some(node_value.clone());
                Ok(node_value)
            } else {
                Ok(min.clone())
            }
        } else {
            self.current_min = Some(node_value.clone());
            Ok(node_value)
        }
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
        let mut aggregator = MinAggregator::new();
        node_outputs
            .iter()
            .map(|value| aggregator.compute(Some((*value).into())).unwrap())
            .collect()
    }

    #[test]
    fn min_aggregator_compute_with_int_ok() {
        let node_outputs: &[i32] = &[0, 1, 2, 0, -1, -2, -1];
        let expected_min: &[i32] = &[0, 0, 0, 0, -1, -2, -2];

        let res = get_computed_values_from_aggregator(node_outputs);

        assert_eq!(res, expected_min.iter().copied().map(NodeValue::from).collect::<Vec<_>>());
    }

    #[test]
    fn min_aggregator_compute_with_float_ok() {
        let node_outputs: &[f32] = &[5.0, 2.5, 1.25, 0.625, 0.625, 1.25, 2.5];
        let expected_min: &[f32] = &[5.0, 2.5, 1.25, 0.625, 0.625, 0.625, 0.625];

        let res = get_computed_values_from_aggregator(node_outputs);

        assert_eq!(res, expected_min.iter().copied().map(NodeValue::from).collect::<Vec<_>>());
    }

    #[test]
    fn min_aggregator_compute_with_none_value_ok() {
        let mut aggregator = MinAggregator::new();
        let res = aggregator.compute(Some(NodeValue::None)).unwrap();

        assert_eq!(res, NodeValue::None);
    }

    #[test]
    fn min_aggregator_compute_with_non_numeric_value_fails() {
        let mut aggregator = MinAggregator::new();
        let err = aggregator.compute(Some(NodeValue::Bool(true))).unwrap_err();

        assert!(
            matches!(&err, AggregationError::NumericAggregationTypeMismatch { .. }),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    #[should_panic]
    fn min_aggregator_compute_without_node_value_panics() {
        let mut aggregator = MinAggregator::new();
        aggregator.compute(None).unwrap();
    }
}
