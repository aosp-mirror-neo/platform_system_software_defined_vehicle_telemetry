// Copyright 2024 Google LLC

use std::sync::Arc;

use super::node_value_extension::NodeValueExtension;
use crate::expr::ast::NodeEvaluationContext;
use crate::expr::{ast::Node, node_value::NodeValue};
use crate::metrics_config_orchestrator::message_builder::{AggregationError, ValueAggregator};

/// Field value type where numeric average is calculated.
pub struct AvgAggregator {
    // Produces the latest value.
    node: Arc<Node>,
    // Stores the running average.
    current_avg: NodeValue,
    // Stores the number of samples.
    n: u32,
}

impl AvgAggregator {
    pub fn new(node: &Arc<Node>) -> Self {
        Self { node: Arc::clone(node), current_avg: NodeValue::Double(0.0), n: 0 }
    }
}

impl ValueAggregator for AvgAggregator {
    fn compute(&mut self, context: &NodeEvaluationContext) -> Result<NodeValue, AggregationError> {
        let latest_value = self.node.evaluate(context)?;
        if latest_value.is_none() {
            return Ok(latest_value);
        }
        self.current_avg.update_running_average(self.n, &latest_value)?;
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
        let mut aggregator =
            AvgAggregator::new(&Arc::new(Node::ConstantLeaf { value: node_outputs[0].into() }));
        let mut result = Vec::new();

        let context = NodeEvaluationContext::new();
        for i in 0..node_outputs.len() - 1 {
            result.push(aggregator.compute(&context).unwrap());

            // update the next exrp eval result, so we can test correctness of compute()
            aggregator.node = Arc::new(Node::ConstantLeaf { value: node_outputs[i + 1].into() });
        }
        result
    }

    #[test]
    fn avg_aggregator_compute_with_int_ok() {
        let node_outputs: &[i32] = &[-2, 0, 2, 4, 6, 8, -32, 0];
        let expected_avg: &[f64] = &[-2.0, -1.0, 0.0, 1.0, 2.0, 3.0, -2.0];

        let res = get_computed_values_from_aggregator(node_outputs);

        assert_eq!(res, expected_avg.iter().copied().map(NodeValue::from).collect::<Vec<_>>());
    }

    #[test]
    fn avg_aggregator_compute_with_float_ok() {
        let node_outputs: &[f32] = &[-1.5, 3.0, 9.0, 6.5, 0.0];
        let expected_avg: &[f64] = &[-1.5, 0.75, 3.5, 4.25];

        let res = get_computed_values_from_aggregator(node_outputs);

        assert_eq!(res, expected_avg.iter().copied().map(NodeValue::from).collect::<Vec<_>>());
    }

    #[test]
    fn avg_aggregator_compute_with_none_value_ok() {
        let mut aggregator =
            AvgAggregator::new(&Arc::new(Node::ConstantLeaf { value: NodeValue::None }));

        let context = NodeEvaluationContext::new();
        let res = aggregator.compute(&context).unwrap();

        assert_eq!(res, NodeValue::None);
    }

    #[test]
    fn avg_aggregator_compute_with_non_numeric_value_fails() {
        let mut bad_avg =
            AvgAggregator::new(&Arc::new(Node::ConstantLeaf { value: NodeValue::Bool(true) }));

        let context = NodeEvaluationContext::new();
        let err = bad_avg.compute(&context).unwrap_err();

        assert!(
            matches!(&err, AggregationError::NumericAggregationTypeMismatch { .. }),
            "Unexpected error: {:?}",
            err
        );
    }
}
