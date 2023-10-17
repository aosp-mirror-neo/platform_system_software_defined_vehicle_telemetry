use std::sync::Arc;

use super::node_value_extension::NodeValueExtension;
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
        MinAggregator { node: node.clone(), current_min: None }
    }
}

impl ValueAggregator for MinAggregator {
    fn compute(&mut self) -> Result<NodeValue, AggregationError> {
        let latest_value = self.node.evaluate()?;
        if !latest_value.is_numeric() {
            panic!(
                "Validation should have ensured that MIN aggregation type \
                has numeric expression evaluation results"
            );
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

    #[test]
    fn test_compute_integer_min_aggregation_ok() {
        let node_outputs = [0, 1, 2, 0, -1, -2, -1, 0];
        let expected_min = [0, 0, 0, 0, -1, -2, -2];
        let mut min_aggregator = MinAggregator::new(&Arc::new(Node::ConstantLeaf {
            value: NodeValue::Int32(node_outputs[0]),
        }));

        // verify the minimum is correctly tracked as the node output changes in each compute()
        for i in 0..expected_min.len() {
            // action
            let res = min_aggregator.compute().unwrap();

            // assert
            assert_eq!(res, NodeValue::Int32(expected_min[i]));

            // update the next exrp eval result, so we can test correctness of compute()
            min_aggregator.node =
                Arc::new(Node::ConstantLeaf { value: NodeValue::Int32(node_outputs[i + 1]) });
        }
    }

    #[test]
    fn test_compute_floating_point_min_aggregation_ok() {
        let node_outputs = [5.0, 2.5, 1.25, 0.625, 0.625, 1.25, 2.5];
        let expected_min = [5.0, 2.5, 1.25, 0.625, 0.625, 0.625];
        let mut min_aggregator = MinAggregator::new(&Arc::new(Node::ConstantLeaf {
            value: NodeValue::Float(node_outputs[0]),
        }));

        // verify the minimum is correctly tracked as the node output changes in each compute()
        for i in 0..expected_min.len() {
            // action
            let res = min_aggregator.compute().unwrap();

            // assert
            assert_eq!(res, NodeValue::Float(expected_min[i]));

            // update the next exrp eval result, so we can test correctness of compute()
            min_aggregator.node =
                Arc::new(Node::ConstantLeaf { value: NodeValue::Float(node_outputs[i + 1]) });
        }
    }

    #[test]
    #[should_panic]
    fn test_compute_node_output_type_is_not_numeric_panics() {
        let mut bad_min =
            MinAggregator::new(&Arc::new(Node::ConstantLeaf { value: NodeValue::Bool(true) }));

        bad_min.compute().unwrap();
    }
}
