use std::sync::Arc;

use super::node_value_extension::NodeValueExtension;
use crate::expr::{ast::Node, node_value::NodeValue};
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
        Self { node: node.clone(), current_sum: NodeValue::Int32(0) }
    }
}

impl ValueAggregator for SumAggregator {
    fn compute(&mut self) -> Result<NodeValue, AggregationError> {
        let latest_value = self.node.evaluate()?;
        if latest_value.is_none() {
            return Ok(latest_value);
        }
        if !latest_value.is_numeric() {
            panic!(
                "Validation should have ensured that sum aggregation type \
              has numeric expression evaluation results"
            );
        }

        self.current_sum.update_sum(&latest_value)?;
        Ok(self.current_sum.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sum_aggregator_compute_with_int_ok() {
        let node_outputs = [-2, 0, 2, 4, 6, 8, -32, 0];
        let expected_sum = [-2, -2, 0, 4, 10, 18, -14];
        let mut sum_aggregator = SumAggregator::new(&Arc::new(Node::ConstantLeaf {
            value: NodeValue::Int32(node_outputs[0]),
        }));

        // verify the sum is correctly calculated as the node output changes in each compute()
        for i in 0..expected_sum.len() {
            let res = sum_aggregator.compute().unwrap();
            assert_eq!(res, NodeValue::Int64(expected_sum[i]));

            // update the next exrp eval result, so we can test correctness of compute()
            sum_aggregator.node =
                Arc::new(Node::ConstantLeaf { value: NodeValue::Int32(node_outputs[i + 1]) });
        }
    }

    #[test]
    fn sum_aggregator_compute_with_float_ok() {
        let node_outputs = [-2.0, 0.0, 2.0, 4.0, 6.0, 8.0, -32.0, 0.0];
        let expected_sum = [-2.0, -2.0, 0.0, 4.0, 10.0, 18.0, -14.0];
        let mut sum_aggregator = SumAggregator::new(&Arc::new(Node::ConstantLeaf {
            value: NodeValue::Float(node_outputs[0]),
        }));

        // verify the sum is correctly calculated as the node output changes in each compute()
        for i in 0..expected_sum.len() {
            let res = sum_aggregator.compute().unwrap();
            assert_eq!(res, NodeValue::Double(expected_sum[i]));

            // update the next exrp eval result, so we can test correctness of compute()
            sum_aggregator.node =
                Arc::new(Node::ConstantLeaf { value: NodeValue::Float(node_outputs[i + 1]) });
        }
    }

    #[test]
    fn sum_aggregator_compute_with_none_value_ok() {
        let mut aggregator =
            SumAggregator::new(&Arc::new(Node::ConstantLeaf { value: NodeValue::None }));

        let res = aggregator.compute().unwrap();
        assert_eq!(res, NodeValue::None);
    }

    #[test]
    #[should_panic]
    fn sum_aggregator_compute_with_non_numeric_value_panics() {
        let mut bad_sum_aggregator =
            SumAggregator::new(&Arc::new(Node::ConstantLeaf { value: NodeValue::Bool(true) }));

        bad_sum_aggregator.compute().unwrap();
    }
}
