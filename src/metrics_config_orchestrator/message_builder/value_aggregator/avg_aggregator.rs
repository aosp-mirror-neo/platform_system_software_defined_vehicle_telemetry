use std::sync::Arc;

use super::node_value_extension::NodeValueExtension;
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
        AvgAggregator { node: node.clone(), current_avg: NodeValue::Double(0.0), n: 0 }
    }
}

impl ValueAggregator for AvgAggregator {
    fn compute(&mut self) -> Result<NodeValue, AggregationError> {
        let latest_value = self.node.evaluate()?;
        if latest_value.is_none() {
            return Ok(latest_value);
        }
        if !latest_value.is_numeric() {
            panic!(
                "Validation should have ensured that avg aggregation type \
                has numeric expression evaluation results"
            );
        }
        self.current_avg.update_running_average(self.n, &latest_value)?;
        self.n += 1;
        Ok(self.current_avg.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn avg_aggregator_compute_with_int_ok() {
        let node_outputs = [-2, 0, 2, 4, 6, 8, -32, 0];
        let expected_avg = [-2, -1, 0, 1, 2, 3, -2];
        let mut avg_aggregator = AvgAggregator::new(&Arc::new(Node::ConstantLeaf {
            value: NodeValue::Int32(node_outputs[0]),
        }));

        // verify the average is correctly tracked as the node output changes in each compute()
        for i in 0..expected_avg.len() {
            // action
            let res = avg_aggregator.compute().unwrap();

            // assert
            assert_eq!(res, NodeValue::Double(expected_avg[i].into()));

            // update the next exrp eval result, so we can test correctness of compute()
            avg_aggregator.node =
                Arc::new(Node::ConstantLeaf { value: NodeValue::Int32(node_outputs[i + 1]) });
        }
    }

    #[test]
    fn avg_aggregator_compute_with_float_ok() {
        let node_outputs = [-1.5, 3.0, 9.0, 6.5, 0.0];
        let expected_avg = [-1.5, 0.75, 3.5, 4.25];
        let mut avg_aggregator = AvgAggregator::new(&Arc::new(Node::ConstantLeaf {
            value: NodeValue::Float(node_outputs[0]),
        }));

        // verify the average is correctly tracked as the node output changes in each compute()
        for i in 0..expected_avg.len() {
            // action
            let res = avg_aggregator.compute().unwrap();

            // assert
            assert_eq!(res, NodeValue::Double(expected_avg[i]));

            // update the next exrp eval result, so we can test correctness of compute()
            avg_aggregator.node =
                Arc::new(Node::ConstantLeaf { value: NodeValue::Float(node_outputs[i + 1]) });
        }
    }

    #[test]
    fn avg_aggregator_compute_with_none_value_ok() {
        let mut aggregator =
            AvgAggregator::new(&Arc::new(Node::ConstantLeaf { value: NodeValue::None }));

        let res = aggregator.compute().unwrap();

        assert_eq!(res, NodeValue::None);
    }

    #[test]
    #[should_panic]
    fn avg_aggregator_compute_with_non_numeric_value_panics() {
        let mut bad_avg =
            AvgAggregator::new(&Arc::new(Node::ConstantLeaf { value: NodeValue::Bool(true) }));

        bad_avg.compute().unwrap();
    }
}
