use std::sync::Arc;

use super::node_value_extension::NodeValueExtension;
use crate::expr::{ast::Node, node_value::NodeValue};
use crate::metrics_config_orchestrator::message_builder::{AggregationError, ValueAggregator};

/// Field value type where vector(concatenation) aggregation is performed.
pub struct VectorAggregator {
    // Produces the latest value.
    node: Arc<Node>,
    // Stores the existing vector. New values to be appended to the existing vector.
    current_vec: Option<NodeValue>,
}

impl VectorAggregator {
    pub fn new(node: &Arc<Node>) -> Self {
        VectorAggregator { node: node.clone(), current_vec: None }
    }
}

impl ValueAggregator for VectorAggregator {
    fn compute(&mut self) -> Result<NodeValue, AggregationError> {
        let latest_value = self.node.evaluate()?;
        if latest_value.is_none() {
            return Ok(latest_value);
        }
        if let Some(existing_vec) = &mut self.current_vec {
            existing_vec.push(&latest_value)?;
            Ok(existing_vec.clone())
        } else {
            // convert the single value NodeValue into a list type NodeValue
            let new_vec = latest_value.to_list()?;
            self.current_vec = Some(new_vec.clone());
            Ok(new_vec)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::mock::TEST_MESSAGE;

    #[test]
    fn test_compute_integer_vector_aggregation_ok() {
        let test_int = 100;
        let mut aggregate_int_value = VectorAggregator::new(&Arc::new(Node::ConstantLeaf {
            value: NodeValue::Int32(test_int),
        }));

        // compute 5 times to see the vector is building up
        for i in 0..5 {
            let expected_vec = vec![test_int; i + 1];

            // action
            let res = aggregate_int_value.compute().unwrap();

            // assert
            assert_eq!(res, NodeValue::Int32List(expected_vec));
        }
    }

    #[test]
    fn test_compute_floating_point_vector_aggregation_ok() {
        let test_double = 1.2_f64;
        let mut aggregate_double_value = VectorAggregator::new(&Arc::new(Node::ConstantLeaf {
            value: NodeValue::Double(test_double),
        }));

        // compute 5 times to see the vector is building up
        for i in 0..5 {
            let expected_vec = vec![test_double; i + 1];

            // action
            let res = aggregate_double_value.compute().unwrap();

            // assert
            assert_eq!(res, NodeValue::DoubleList(expected_vec));
        }
    }

    #[test]
    fn test_compute_handles_none_value() {
        let mut aggregator =
            VectorAggregator::new(&Arc::new(Node::ConstantLeaf { value: NodeValue::None }));

        let res = aggregator.compute().unwrap();

        assert_eq!(res, NodeValue::None);
    }

    #[test]
    fn test_compute_vector_aggregate_message_value_fails() {
        let mut aggregate_message_value = VectorAggregator::new(&Arc::new(Node::ConstantLeaf {
            value: NodeValue::Message(Box::new(TEST_MESSAGE.clone())),
        }));

        assert!(aggregate_message_value.compute().is_err());
    }

    #[test]
    fn test_compute_vector_aggregate_vector_type_fails() {
        let mut aggregate_list_of_list = VectorAggregator::new(&Arc::new(Node::ConstantLeaf {
            value: NodeValue::BoolList(vec![true, false]),
        }));

        assert!(aggregate_list_of_list.compute().is_err());
    }
}
