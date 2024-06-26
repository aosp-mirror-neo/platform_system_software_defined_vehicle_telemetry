// Copyright 2024 Google LLC

use sdv_telemetry_utils::operation_result::OperationResult;
use std::sync::Arc;

use super::node_value_extension::NodeValueExtension;
use crate::expr::node::{Node, NodeEvaluationContext};
use crate::expr::node_value::NodeValue;
use crate::metrics_config_orchestrator::message_builder::{AggregationError, ValueAggregator};

/// Field value type where aggregation for numeric maximum is performed.
pub struct MaxAggregator {
    // Produces the latest value.
    node: Arc<Node>,
    // Stores the maximum so far.
    current_max: Option<NodeValue>,
}

impl MaxAggregator {
    pub fn new(node: &Arc<Node>) -> Self {
        Self { node: Arc::clone(node), current_max: None }
    }
}

impl ValueAggregator for MaxAggregator {
    fn compute<'a>(
        &'a mut self,
        context: &'a NodeEvaluationContext,
    ) -> OperationResult<NodeValue, AggregationError> {
        self.node.evaluate(context).err_into().and_then_sync(|latest_value| {
            if latest_value.is_none() {
                return Ok(NodeValue::None);
            }

            if !latest_value.is_numeric() {
                return Err(AggregationError::NumericAggregationTypeMismatch {
                    value: format!("{:?}", latest_value),
                });
            }

            if let Some(max) = &self.current_max {
                if max.clone().less_than(&latest_value)? {
                    self.current_max = Some(latest_value.clone());
                    Ok(latest_value)
                } else {
                    Ok(max.clone())
                }
            } else {
                self.current_max = Some(latest_value.clone());
                Ok(latest_value)
            }
        })
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
            MaxAggregator::new(&Arc::new(Node::ConstantLeaf { value: node_outputs[0].into() }));
        let mut result = Vec::new();

        let context = NodeEvaluationContext::new();
        for i in 0..node_outputs.len() - 1 {
            result.push(aggregator.compute(&context).unwrap_sync().unwrap());

            // update the next exrp eval result, so we can test correctness of compute()
            aggregator.node = Arc::new(Node::ConstantLeaf { value: node_outputs[i + 1].into() });
        }
        result
    }

    #[test]
    fn max_aggregator_compute_with_int_ok() {
        let node_outputs: &[i32] = &[0, 1, 2, 0, -1, 3, 0, -1];
        let expected_max: &[i32] = &[0, 1, 2, 2, 2, 3, 3];

        let res = get_computed_values_from_aggregator(node_outputs);

        assert_eq!(res, expected_max.iter().copied().map(NodeValue::from).collect::<Vec<_>>());
    }

    #[test]
    fn max_aggregator_compute_with_float_ok() {
        let node_outputs: &[f32] = &[-1.0, -0.5, 1.0, 2.5, 0.0, -1.0];
        let expected_max: &[f32] = &[-1.0, -0.5, 1.0, 2.5, 2.5];

        let res = get_computed_values_from_aggregator(node_outputs);

        assert_eq!(res, expected_max.iter().copied().map(NodeValue::from).collect::<Vec<_>>());
    }

    #[test]
    fn max_aggregator_compute_with_none_value_ok() {
        let mut aggregator =
            MaxAggregator::new(&Arc::new(Node::ConstantLeaf { value: NodeValue::None }));

        let context = NodeEvaluationContext::new();
        let res = aggregator.compute(&context).unwrap_sync().unwrap();

        assert_eq!(res, NodeValue::None);
    }

    #[test]
    fn max_aggregator_compute_with_non_numeric_value_fails() {
        let mut bad_max =
            MaxAggregator::new(&Arc::new(Node::ConstantLeaf { value: NodeValue::Bool(true) }));

        let context = NodeEvaluationContext::new();
        let err = bad_max.compute(&context).unwrap_sync().unwrap_err();

        assert!(
            matches!(&err, AggregationError::NumericAggregationTypeMismatch { .. }),
            "Unexpected error: {:?}",
            err
        );
    }
}
