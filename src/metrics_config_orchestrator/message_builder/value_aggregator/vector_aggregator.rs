use std::sync::Arc;

use super::node_value_extension::NodeValueExtension;
use crate::expr::ast::NodeEvaluationContext;
use crate::expr::{ast::Node, node_value::NodeValue};
use crate::metrics_config_orchestrator::message_builder::{AggregationError, ValueAggregator};

/// Field value type where vector(concatenation) aggregation is performed.
pub struct VectorAggregator {
    // Produces the latest value.
    node: Arc<Node>,
    // Stores the existing vector. New values to be appended to the existing vector.
    current_vec: Option<NodeValue>,
    // Maximum length of the vector. If the vector is too long, the oldest value will be dropped.
    // If max_length is None, then the vector is not length-limited.
    max_length: Option<u32>,
}

impl VectorAggregator {
    pub fn new(node: &Arc<Node>, max_length: &Option<u32>) -> Self {
        Self { node: node.clone(), current_vec: None, max_length: *max_length }
    }
}

impl ValueAggregator for VectorAggregator {
    fn compute(&mut self, context: &NodeEvaluationContext) -> Result<NodeValue, AggregationError> {
        let latest_value = self.node.evaluate(context)?;
        if latest_value.is_none() {
            return Ok(latest_value);
        }
        if let Some(existing_vec) = &mut self.current_vec {
            existing_vec.push(&latest_value, &self.max_length)?;
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
    use protobuf::MessageDyn;

    use super::*;
    use crate::expr::mock::TEST_MESSAGE;

    #[test]
    fn vector_aggregator_compute_with_int_ok() {
        let test_int = 100;
        let mut aggregate_int_value = VectorAggregator::new(
            &Arc::new(Node::ConstantLeaf { value: NodeValue::Int32(test_int) }),
            &None,
        );

        let context = NodeEvaluationContext::new();
        // compute 5 times to see the vector is building up
        for i in 0..5 {
            let expected_vec = vec![test_int; i + 1];

            // action
            let res = aggregate_int_value.compute(&context).unwrap();

            // assert
            assert_eq!(res, NodeValue::Int32List(expected_vec));
        }
    }

    #[test]
    fn vector_aggregator_compute_with_int_and_limited_length_ok() {
        let mut aggregate_int_value = VectorAggregator::new(
            &Arc::new(Node::ConstantLeaf { value: NodeValue::Int64(0) }),
            &Some(3), // max length = 3
        );
        let expected_vectors = vec![
            vec![0],
            vec![0, 1],
            vec![0, 1, 2],
            // since max len = 3, the new value is appended and oldest value is dropped
            vec![1, 2, 3],
            vec![2, 3, 4],
        ];

        let context = NodeEvaluationContext::new();
        // compute 5 times to see the vector aggregation output is as expected
        for (i, expected_vec) in expected_vectors.into_iter().enumerate() {
            // make the next expression output equals to i
            aggregate_int_value.node =
                Arc::new(Node::ConstantLeaf { value: NodeValue::Int64(i as i64) });

            // action
            let res = aggregate_int_value.compute(&context).unwrap();

            // assert
            assert_eq!(res, NodeValue::Int64List(expected_vec));
        }
    }

    #[test]
    fn vector_aggregator_compute_with_float_ok() {
        let test_double = 1.2_f64;
        let mut aggregate_double_value = VectorAggregator::new(
            &Arc::new(Node::ConstantLeaf { value: NodeValue::Double(test_double) }),
            &None,
        );

        let context = NodeEvaluationContext::new();
        // compute 5 times to see the vector is building up
        for i in 0..5 {
            let expected_vec = vec![test_double; i + 1];

            // action
            let res = aggregate_double_value.compute(&context).unwrap();

            // assert
            assert_eq!(res, NodeValue::DoubleList(expected_vec));
        }
    }

    #[test]
    fn vector_aggregator_compute_with_float_and_limited_length_ok() {
        let mut aggregate_int_value = VectorAggregator::new(
            &Arc::new(Node::ConstantLeaf { value: NodeValue::Float(0.0) }),
            &Some(3), // max length = 3
        );
        let expected_vectors = vec![
            vec![0.0],
            vec![0.0, 0.2],
            vec![0.0, 0.2, 0.4],
            // since max len = 3, the new value is appended and oldest value is dropped
            vec![0.2, 0.4, 0.6],
            vec![0.4, 0.6, 0.8],
        ];

        let context = NodeEvaluationContext::new();
        // compute 5 times to see the vector aggregation output is as expected
        for (i, expected_vec) in expected_vectors.into_iter().enumerate() {
            // make the next expression output equals to i / 5 as a float
            aggregate_int_value.node =
                Arc::new(Node::ConstantLeaf { value: NodeValue::Float(i as f32 / 5.0) });

            // action
            let res = aggregate_int_value.compute(&context).unwrap();

            // assert
            assert_eq!(res, NodeValue::FloatList(expected_vec));
        }
    }

    #[test]
    fn vector_aggregator_compute_with_message_value_ok() {
        let boxed_message: Box<dyn MessageDyn> = Box::new(TEST_MESSAGE.clone());
        let mut aggregate_message_value = VectorAggregator::new(
            &Arc::new(Node::ConstantLeaf { value: NodeValue::Message(boxed_message.clone()) }),
            &None,
        );

        let context = NodeEvaluationContext::new();
        // compute 5 times to see the vector is building up
        for i in 0..5 {
            let expected_vec = vec![boxed_message.clone(); i + 1];

            // action
            let res = aggregate_message_value.compute(&context).unwrap();

            // assert
            assert_eq!(res, NodeValue::MessageList(expected_vec));
        }
    }

    #[test]
    fn vector_aggregator_compute_with_message_value_and_limited_length_ok() {
        let max_len: usize = 3;
        let boxed_message: Box<dyn MessageDyn> = Box::new(TEST_MESSAGE.clone());
        let mut aggregate_message_value = VectorAggregator::new(
            &Arc::new(Node::ConstantLeaf { value: NodeValue::Message(boxed_message.clone()) }),
            &Some(max_len as u32),
        );

        let context = NodeEvaluationContext::new();
        // compute 5 times and verify the output never exceeds max_len
        for i in 0..5 {
            let expected_vec = if i < max_len {
                vec![boxed_message.clone(); i + 1]
            } else {
                vec![boxed_message.clone(); max_len]
            };

            // action
            let res = aggregate_message_value.compute(&context).unwrap();

            // assert
            assert_eq!(res, NodeValue::MessageList(expected_vec));
        }
    }

    #[test]
    fn vector_aggregator_compute_with_none_value_ok() {
        let mut aggregator =
            VectorAggregator::new(&Arc::new(Node::ConstantLeaf { value: NodeValue::None }), &None);

        let context = NodeEvaluationContext::new();
        let res = aggregator.compute(&context).unwrap();

        assert_eq!(res, NodeValue::None);
    }

    #[test]
    fn vector_aggregator_compute_with_vector_fails() {
        let mut aggregate_list_of_list = VectorAggregator::new(
            &Arc::new(Node::ConstantLeaf { value: NodeValue::BoolList(vec![true, false]) }),
            &None,
        );

        let context = NodeEvaluationContext::new();
        assert!(aggregate_list_of_list.compute(&context).is_err());
    }
}
