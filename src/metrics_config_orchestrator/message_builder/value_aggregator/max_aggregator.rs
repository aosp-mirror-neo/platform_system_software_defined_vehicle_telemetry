use std::sync::Arc;

use super::node_value_extension::NodeValueExtension;
use crate::expr::{ast::Node, node_value::NodeValue};
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
        MaxAggregator { node: node.clone(), current_max: None }
    }
}

impl ValueAggregator for MaxAggregator {
    fn compute(&mut self) -> Result<NodeValue, AggregationError> {
        let latest_value = self.node.evaluate()?;
        if latest_value.is_none() {
            return Ok(latest_value);
        }
        if !latest_value.is_numeric() {
            panic!(
                "Validation should have ensured that MAX aggregation type \
                has numeric expression evaluation results"
            );
        }
        if let Some(max) = &self.current_max {
            if max.is_less_than(&latest_value)? {
                self.current_max = Some(latest_value.clone());
                Ok(latest_value)
            } else {
                Ok(max.clone())
            }
        } else {
            self.current_max = Some(latest_value.clone());
            Ok(latest_value)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_aggregator_compute_with_int_ok() {
        let node_outputs = [0, 1, 2, 0, -1, 3, 0, -1];
        let expected_max = [0, 1, 2, 2, 2, 3, 3];
        let mut max_aggregator = MaxAggregator::new(&Arc::new(Node::ConstantLeaf {
            value: NodeValue::Int32(node_outputs[0]),
        }));

        // verify the maximum is correctly tracked as the node output changes in each compute()
        for i in 0..expected_max.len() {
            // action
            let res = max_aggregator.compute().unwrap();

            // assert
            assert_eq!(res, NodeValue::Int32(expected_max[i]));

            // update the next exrp eval result, so we can test correctness of compute()
            max_aggregator.node =
                Arc::new(Node::ConstantLeaf { value: NodeValue::Int32(node_outputs[i + 1]) });
        }
    }

    #[test]
    fn max_aggregator_compute_with_float_ok() {
        let node_outputs = [-1.0, -0.5, 1.0, 2.5, 0.0, -1.0];
        let expected_max = [-1.0, -0.5, 1.0, 2.5, 2.5];
        let mut max_aggregator = MaxAggregator::new(&Arc::new(Node::ConstantLeaf {
            value: NodeValue::Float(node_outputs[0]),
        }));

        // verify the maximum is correctly tracked as the node output changes in each compute()
        for i in 0..expected_max.len() {
            // action
            let res = max_aggregator.compute().unwrap();

            // assert
            assert_eq!(res, NodeValue::Float(expected_max[i]));

            // update the next exrp eval result, so we can test correctness of compute()
            max_aggregator.node =
                Arc::new(Node::ConstantLeaf { value: NodeValue::Float(node_outputs[i + 1]) });
        }
    }

    #[test]
    fn max_aggregator_compute_with_none_value_ok() {
        let mut aggregator =
            MaxAggregator::new(&Arc::new(Node::ConstantLeaf { value: NodeValue::None }));

        let res = aggregator.compute().unwrap();

        assert_eq!(res, NodeValue::None);
    }

    #[test]
    #[should_panic]
    fn max_aggregator_compute_with_non_numeric_value_panics() {
        let mut bad_max =
            MaxAggregator::new(&Arc::new(Node::ConstantLeaf { value: NodeValue::Bool(true) }));

        bad_max.compute().unwrap();
    }
}
