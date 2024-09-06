// Copyright 2024 Google LLC

use super::node_value_extension::NodeValueExtension;
use crate::expr::node_value::NodeValue;
use crate::metrics_config_orchestrator::message_builder::{AggregationError, ValueAggregator};

/// Field value type where numeric average is calculated.
#[cfg_attr(test, derive(std::fmt::Debug))]
pub(crate) struct AvgAggregator {
    // Stores the running average.
    current_avg: NodeValue,
    // Stores the number of samples.
    n: u32,
}

impl AvgAggregator {
    pub(crate) fn new() -> Self {
        Self { current_avg: NodeValue::Double(0.0), n: 0 }
    }
}

impl ValueAggregator for AvgAggregator {
    fn compute(&mut self, node_value: Option<NodeValue>) -> Result<NodeValue, AggregationError> {
        let node_value = node_value.expect("Aggregator should be provided with a value");
        if node_value.is_none() {
            return Ok(NodeValue::None);
        }
        self.current_avg.update_running_average(self.n, &node_value)?;
        self.n += 1;
        Ok(self.current_avg.clone())
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
        let mut aggregator = AvgAggregator::new();
        node_outputs
            .iter()
            .map(|value| aggregator.compute(Some((*value).into())).unwrap())
            .collect()
    }

    #[test]
    fn avg_aggregator_compute_with_int_ok() {
        let node_outputs: &[i32] = &[-2, 0, 2, 4, 6, 8, -32, 0];
        let expected_avg: &[f64] = &[-2.0, -1.0, 0.0, 1.0, 2.0, 3.0, -2.0, -1.75];

        let res = get_computed_values_from_aggregator(node_outputs);

        assert_eq!(res, expected_avg.iter().copied().map(NodeValue::from).collect::<Vec<_>>());
    }

    #[test]
    fn avg_aggregator_compute_with_float_ok() {
        let node_outputs: &[f32] = &[-1.5, 3.0, 9.0, 6.5, 0.0];
        let expected_avg: &[f64] = &[-1.5, 0.75, 3.5, 4.25, 3.4];

        let res = get_computed_values_from_aggregator(node_outputs);

        assert_eq!(res, expected_avg.iter().copied().map(NodeValue::from).collect::<Vec<_>>());
    }

    #[test]
    fn avg_aggregator_compute_with_none_value_ok() {
        let mut aggregator = AvgAggregator::new();
        let res = aggregator.compute(Some(NodeValue::None)).unwrap();

        assert_eq!(res, NodeValue::None);
    }

    #[test]
    fn avg_aggregator_compute_with_non_numeric_value_fails() {
        let mut aggregator = AvgAggregator::new();
        let err = aggregator.compute(Some(NodeValue::Bool(true))).unwrap_err();

        assert!(
            matches!(&err, AggregationError::NumericAggregationTypeMismatch { .. }),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    #[should_panic]
    fn avg_aggregator_compute_without_node_value_panics() {
        let mut aggregator = AvgAggregator::new();
        aggregator.compute(None).unwrap();
    }
}
