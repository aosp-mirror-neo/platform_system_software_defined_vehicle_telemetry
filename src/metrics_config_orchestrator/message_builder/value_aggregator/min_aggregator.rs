use std::sync::Arc;

use super::node_value_extension::NodeValueExtension;
use crate::expr::ast::NodeEvaluationContext;
use crate::expr::{ast::Node, node_value::NodeValue};
use crate::metrics_config_orchestrator::message_builder::{AggregationError, ValueAggregator};

/// Field value type where aggregation for numeric minimum is performed.
pub struct MinAggregator {
    // Produces the latest value.
    node: Arc<Node>,
    // Stores the minimum so far.
    current_min: Option<NodeValue>,
}

impl MinAggregator {
    pub fn new(node: &Arc<Node>) -> Self {
        Self { node: node.clone(), current_min: None }
    }
}

impl ValueAggregator for MinAggregator {
    fn compute(&mut self, context: &NodeEvaluationContext) -> Result<NodeValue, AggregationError> {
        let latest_value = self.node.evaluate(context)?;
        if latest_value.is_none() {
            return Ok(latest_value);
        }
        if !latest_value.is_numeric() {
            return Err(AggregationError::NumericAggregationTypeMismatch {
                value: format!("{:?}", latest_value),
            });
        }
        if let Some(min) = &self.current_min {
            if latest_value.is_less_than(min)? {
                self.current_min = Some(latest_value.clone());
                Ok(latest_value)
            } else {
                Ok(min.clone())
            }
        } else {
            self.current_min = Some(latest_value.clone());
            Ok(latest_value)
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
        let mut aggregator =
            MinAggregator::new(&Arc::new(Node::ConstantLeaf { value: node_outputs[0].into() }));
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
    fn min_aggregator_compute_with_int_ok() {
        let node_outputs: &[i32] = &[0, 1, 2, 0, -1, -2, -1, 0];
        let expected_min: &[i32] = &[0, 0, 0, 0, -1, -2, -2];

        let res = get_computed_values_from_aggregator(node_outputs);

        assert_eq!(res, expected_min.iter().copied().map(NodeValue::from).collect::<Vec<_>>());
    }

    #[test]
    fn min_aggregator_compute_with_float_ok() {
        let node_outputs: &[f32] = &[5.0, 2.5, 1.25, 0.625, 0.625, 1.25, 2.5];
        let expected_min: &[f32] = &[5.0, 2.5, 1.25, 0.625, 0.625, 0.625];

        let res = get_computed_values_from_aggregator(node_outputs);

        assert_eq!(res, expected_min.iter().copied().map(NodeValue::from).collect::<Vec<_>>());
    }

    #[test]
    fn min_aggregator_compute_with_none_value_ok() {
        let mut aggregator =
            MinAggregator::new(&Arc::new(Node::ConstantLeaf { value: NodeValue::None }));

        let context = NodeEvaluationContext::new();
        let res = aggregator.compute(&context).unwrap();

        assert_eq!(res, NodeValue::None);
    }

    #[test]
    fn min_aggregator_compute_with_non_numeric_value_fails() {
        let mut bad_min =
            MinAggregator::new(&Arc::new(Node::ConstantLeaf { value: NodeValue::Bool(true) }));

        let context = NodeEvaluationContext::new();
        let err = bad_min.compute(&context).unwrap_err();

        assert!(
            matches!(&err, AggregationError::NumericAggregationTypeMismatch { .. }),
            "Unexpected error: {:?}",
            err
        );
    }
}
