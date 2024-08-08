// Copyright 2024 Google LLC

use super::node_value_extension::NodeValueExtension;
use crate::expr::node_value::NodeValue;
use crate::metrics_config_orchestrator::message_builder::{AggregationError, ValueAggregator};

/// Field value type where vector(concatenation) aggregation is performed.
pub struct VectorAggregator {
    // Stores the existing vector. New values to be appended to the existing vector.
    current_vec: Option<NodeValue>,
    // Maximum length of the vector. If the vector is too long, the oldest value will be dropped.
    // If max_length is None, then the vector is not length-limited.
    max_length: Option<u32>,
}

impl VectorAggregator {
    pub fn new(max_length: Option<u32>) -> Self {
        Self { current_vec: None, max_length }
    }
}

impl ValueAggregator for VectorAggregator {
    fn compute(&mut self, node_value: Option<NodeValue>) -> Result<NodeValue, AggregationError> {
        let node_value = node_value.expect("Aggregator should be provided with a value");
        if node_value.is_none() {
            return Ok(NodeValue::None);
        }

        if let Some(existing_vec) = &mut self.current_vec {
            existing_vec.push(&node_value, self.max_length)?;
            Ok(existing_vec.clone())
        } else {
            // convert the single value NodeValue into a list type NodeValue
            let new_vec = node_value.to_list()?;
            self.current_vec = Some(new_vec.clone());
            Ok(new_vec)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use protobuf::MessageDyn;
    use std::sync::Arc;

    use crate::expr::mock::TEST_MESSAGE;

    #[test]
    fn vector_aggregator_compute_with_int_ok() {
        let test_int = 100;
        let mut aggregator = VectorAggregator::new(None);

        // compute 5 times to see the vector is building up
        for i in 0..5 {
            let expected_vec = vec![test_int; i + 1];

            // action
            let res = aggregator.compute(Some(NodeValue::Int32(test_int))).unwrap();

            // assert
            assert_eq!(res, NodeValue::Int32List(expected_vec));
        }
    }

    #[test]
    fn vector_aggregator_compute_with_int_and_limited_length_ok() {
        let mut aggregator = VectorAggregator::new(
            Some(3), // max length = 3
        );
        let expected_vectors = vec![
            vec![0],
            vec![0, 1],
            vec![0, 1, 2],
            // since max len = 3, the new value is appended and oldest value is dropped
            vec![1, 2, 3],
            vec![2, 3, 4],
        ];

        // compute 5 times to see the vector aggregation output is as expected
        for (i, expected_vec) in expected_vectors.into_iter().enumerate() {
            // action
            let res = aggregator.compute(Some(NodeValue::Int64(i as i64))).unwrap();

            // assert
            assert_eq!(res, NodeValue::Int64List(expected_vec));
        }
    }

    #[test]
    fn vector_aggregator_compute_with_float_ok() {
        let test_double = 1.2_f64;
        let mut aggregator = VectorAggregator::new(None);

        // compute 5 times to see the vector is building up
        for i in 0..5 {
            let expected_vec = vec![test_double; i + 1];

            // action
            let res = aggregator.compute(Some(NodeValue::Double(test_double))).unwrap();

            // assert
            assert_eq!(res, NodeValue::DoubleList(expected_vec));
        }
    }

    #[test]
    fn vector_aggregator_compute_with_float_and_limited_length_ok() {
        let mut aggregator = VectorAggregator::new(
            Some(3), // max length = 3
        );
        let expected_vectors = vec![
            vec![0.0],
            vec![0.0, 0.2],
            vec![0.0, 0.2, 0.4],
            // since max len = 3, the new value is appended and oldest value is dropped
            vec![0.2, 0.4, 0.6],
            vec![0.4, 0.6, 0.8],
        ];

        // compute 5 times to see the vector aggregation output is as expected
        for (i, expected_vec) in expected_vectors.into_iter().enumerate() {
            // action
            let res = aggregator.compute(Some(NodeValue::Float(i as f32 / 5.0))).unwrap();

            // assert
            assert_eq!(res, NodeValue::FloatList(expected_vec));
        }
    }

    #[test]
    fn vector_aggregator_compute_with_message_value_ok() {
        let message = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let mut aggregator = VectorAggregator::new(None);

        // compute 5 times to see the vector is building up
        for i in 0..5 {
            let expected_vec = vec![Arc::clone(&message); i + 1];

            // action
            let res = aggregator.compute(Some(NodeValue::Message(Arc::clone(&message)))).unwrap();

            // assert
            assert_eq!(res, NodeValue::MessageList(expected_vec));
        }
    }

    #[test]
    fn vector_aggregator_compute_with_message_value_and_limited_length_ok() {
        let max_len: usize = 3;
        let message = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let mut aggregator = VectorAggregator::new(Some(max_len as u32));

        // compute 5 times and verify the output never exceeds max_len
        for i in 0..5 {
            let expected_vec = if i < max_len {
                vec![Arc::clone(&message); i + 1]
            } else {
                vec![Arc::clone(&message); max_len]
            };

            // action
            let res = aggregator.compute(Some(NodeValue::Message(Arc::clone(&message)))).unwrap();

            // assert
            assert_eq!(res, NodeValue::MessageList(expected_vec));
        }
    }

    #[test]
    fn vector_aggregator_compute_with_none_value_ok() {
        let mut aggregator = VectorAggregator::new(None);
        let res = aggregator.compute(Some(NodeValue::None)).unwrap();

        assert_eq!(res, NodeValue::None);
    }

    #[test]
    fn vector_aggregator_compute_with_vector_fails() {
        let mut aggregator = VectorAggregator::new(None);
        let err = aggregator.compute(Some(NodeValue::BoolList(vec![true, false]))).unwrap_err();

        assert!(
            matches!(&err, AggregationError::VectorInitializationError { .. }),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    #[should_panic]
    fn vector_aggregator_compute_without_node_value_panics() {
        let mut aggregator = VectorAggregator::new(None);
        aggregator.compute(None).unwrap();
    }
}
