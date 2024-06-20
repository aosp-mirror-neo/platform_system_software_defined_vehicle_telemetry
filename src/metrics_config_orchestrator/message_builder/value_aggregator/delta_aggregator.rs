// Copyright 2024 Google LLC

use std::sync::Arc;

use crate::expr::ast::NodeEvaluationContext;
use crate::expr::{ast::Node, node_value::NodeValue};
use crate::metrics_config_orchestrator::message_builder::{AggregationError, ValueAggregator};
use sdv_telemetry_proto_rust::expression::combination_node::{ArithmeticOperator, Operator};

/// Field value type where numeric delta is calculated.
pub struct DeltaAggregator {
    // Produces the latest value.
    node: Arc<Node>,
    // Stores the last seen node to be used when calculating the delta for the next node.
    previous_value: NodeValue,
}

impl DeltaAggregator {
    pub fn new(node: &Arc<Node>) -> Self {
        Self { node: Arc::clone(node), previous_value: NodeValue::None }
    }
}

impl ValueAggregator for DeltaAggregator {
    fn compute(&mut self, context: &NodeEvaluationContext) -> Result<NodeValue, AggregationError> {
        let new_value = self.node.evaluate(context)?;

        if new_value.is_none() {
            return Ok(new_value);
        }

        if !new_value.is_numeric() {
            panic!(
                "Validation should have ensured that delta aggregation type \
              has numeric expression evaluation results"
            );
        }

        // Previous valus is none for the first received node.
        if self.previous_value.is_none() {
            self.previous_value = new_value;
            Ok(NodeValue::None)
        } else {
            let current_delta = new_value.operate(
                &Operator::ArithmeticOperator(ArithmeticOperator::SUBTRACT.into()),
                || Ok(self.previous_value.clone()),
            )?;
            self.previous_value = new_value;
            Ok(current_delta)
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
            DeltaAggregator::new(&Arc::new(Node::ConstantLeaf { value: node_outputs[0].into() }));
        let mut result = Vec::new();

        let context = NodeEvaluationContext::new();
        for i in 0..node_outputs.len() - 1 {
            let res = aggregator.compute(&context).unwrap();
            // Index 0 is ignored as delta for that is None which cannot be converted into a number.
            if i > 0 {
                result.push(res);
            }

            // update the next exrp eval result, so we can test correctness of compute()
            aggregator.node = Arc::new(Node::ConstantLeaf { value: node_outputs[i + 1].into() });
        }

        // To make sure the last node also gets calculated.
        result.push(aggregator.compute(&context).unwrap());

        result
    }

    #[test]
    fn delta_aggregator_compute_with_int_ok() {
        let node_outputs: &[i64] = &[-2, 0, 2, 4, 6, 8, -32, 0];
        let expected_delta: &[i64] = &[2, 2, 2, 2, 2, -40, 32];

        let res = get_computed_values_from_aggregator(node_outputs);

        assert_eq!(res, expected_delta.iter().copied().map(NodeValue::from).collect::<Vec<_>>());
    }

    #[test]
    fn delta_aggregator_compute_with_float_ok() {
        let node_outputs: &[f64] = &[-2.50, 0.0, 2.20, 4.0, 6.0, 8.70, -32.0, 0.0];
        let expected_delta: &[f64] = &[2.5, 2.2, 1.8, 2.0, 2.7, -40.7, 32.0];

        let res = get_computed_values_from_aggregator(node_outputs);

        assert_eq!(res, expected_delta.iter().copied().map(NodeValue::from).collect::<Vec<_>>());
    }

    #[test]
    fn delta_aggregator_compute_with_none_value_ok() {
        let mut aggregator =
            DeltaAggregator::new(&Arc::new(Node::ConstantLeaf { value: NodeValue::None }));

        let context = NodeEvaluationContext::new();
        let res = aggregator.compute(&context).unwrap();

        assert_eq!(res, NodeValue::None);
    }

    #[test]
    #[should_panic]
    fn delta_aggregator_compute_with_non_numeric_value_panics() {
        let mut bad_delta_aggregator =
            DeltaAggregator::new(&Arc::new(Node::ConstantLeaf { value: NodeValue::Bool(true) }));

        let context = NodeEvaluationContext::new();
        bad_delta_aggregator.compute(&context).unwrap();
    }
}
