// Copyright 2024 Google LLC

use sdv_telemetry_proto_rust::expression::combination_node::{ArithmeticOperator, Operator};
use sdv_telemetry_utils::operation_result::OperationResult;
use std::sync::Arc;

use crate::expr::node::{Node, NodeEvaluationContext};
use crate::expr::node_value::NodeValue;
use crate::metrics_config_orchestrator::message_builder::{AggregationError, ValueAggregator};

/// Field value type where numeric sum is calculated.
pub struct SumAggregator {
    // Produces the latest value.
    node: Arc<Node>,
    // Stores the sum of values so far.
    current_sum: NodeValue,
}

impl SumAggregator {
    pub fn new(node: &Arc<Node>) -> Self {
        Self { node: Arc::clone(node), current_sum: NodeValue::Int32(0) }
    }
}

impl ValueAggregator for SumAggregator {
    fn compute<'a>(
        &'a mut self,
        context: &'a NodeEvaluationContext,
    ) -> OperationResult<NodeValue, AggregationError> {
        self.node.evaluate(context).err_into().and_then_sync(|latest_value| {
            if latest_value.is_none() {
                return Ok(NodeValue::None);
            }

            self.current_sum = self.current_sum.clone().operate_sync(
                &Operator::ArithmeticOperator(ArithmeticOperator::ADD.into()),
                move || Ok(latest_value),
            )?;
            Ok(self.current_sum.clone())
        })
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
        let mut sum_aggregator =
            SumAggregator::new(&Arc::new(Node::ConstantLeaf { value: node_outputs[0].into() }));
        let mut result_sums = Vec::new();

        let context = NodeEvaluationContext::new();
        for i in 0..node_outputs.len() - 1 {
            let res = sum_aggregator.compute(&context).unwrap_sync().unwrap();
            result_sums.push(res);

            // update the next exrp eval result, so we can test correctness of compute()
            sum_aggregator.node =
                Arc::new(Node::ConstantLeaf { value: node_outputs[i + 1].into() });
        }

        result_sums
    }

    #[test]
    fn sum_aggregator_compute_with_int_ok() {
        let node_outputs: &[i64] = &[-2, 0, 2, 4, 6, 8, -32, 0];
        let expected_sum: &[i64] = &[-2, -2, 0, 4, 10, 18, -14];
        let res = get_computed_sums_from_aggregator(node_outputs);
        assert_eq!(res, expected_sum.iter().copied().map(NodeValue::from).collect::<Vec<_>>());
    }

    #[test]
    fn sum_aggregator_compute_with_float_ok() {
        let node_outputs: &[f64] = &[-2.0, 0.0, 2.0, 4.0, 6.0, 8.0, -32.0, 0.0];
        let expected_sum: &[f64] = &[-2.0, -2.0, 0.0, 4.0, 10.0, 18.0, -14.0];
        let res = get_computed_sums_from_aggregator(node_outputs);
        assert_eq!(res, expected_sum.iter().copied().map(NodeValue::from).collect::<Vec<_>>());
    }

    #[test]
    fn sum_aggregator_compute_with_none_value_ok() {
        let mut aggregator =
            SumAggregator::new(&Arc::new(Node::ConstantLeaf { value: NodeValue::None }));

        let context = NodeEvaluationContext::new();
        let res = aggregator.compute(&context).unwrap_sync().unwrap();
        assert_eq!(res, NodeValue::None);
    }

    #[test]
    #[should_panic]
    fn sum_aggregator_compute_with_non_numeric_value_panics() {
        let mut bad_sum_aggregator =
            SumAggregator::new(&Arc::new(Node::ConstantLeaf { value: NodeValue::Bool(true) }));

        let context = NodeEvaluationContext::new();
        bad_sum_aggregator.compute(&context).unwrap_sync().unwrap();
    }
}
