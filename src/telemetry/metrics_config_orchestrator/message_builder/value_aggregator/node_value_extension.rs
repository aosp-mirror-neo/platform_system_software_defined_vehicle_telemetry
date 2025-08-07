// Copyright 2024 Google LLC

use sdv_telemetry_expr::node_value::NodeValue;
use sdv_telemetry_proto_rust::expression::combination_node::{Operator, RelationalOperator};
use std::{fmt::Debug, sync::Arc};

use crate::metrics_config_orchestrator::message_builder::value_aggregator::AggregationError;

/// Trait that defines helper functions for aggregating values.
pub(crate) trait NodeValueExtension {
    // Converts NodeValue of single value into NodeValue containing a list of 1 element.
    fn to_list(&self) -> Result<NodeValue, AggregationError>;

    // Appends a single value into an existing list.
    // Drops oldest value if vector exceeds max_length.
    fn push(&mut self, value: &NodeValue, max_length: Option<u32>) -> Result<(), AggregationError>;

    // Returns true if self is less than other.
    fn less_than(self, other: &NodeValue) -> Result<bool, AggregationError>;

    // Update the running average after learning the new sample.
    // Caller must be a NodeValue::Double.
    fn update_running_average(
        &mut self,
        n: u32,
        new_value: &NodeValue,
    ) -> Result<(), AggregationError>;
}

impl NodeValueExtension for NodeValue {
    fn to_list(&self) -> Result<NodeValue, AggregationError> {
        Ok(match self {
            NodeValue::Bool(val) => NodeValue::BoolList(vec![*val]),
            NodeValue::Int32(val) => NodeValue::Int32List(vec![*val]),
            NodeValue::Int64(val) => NodeValue::Int64List(vec![*val]),
            NodeValue::Float(val) => NodeValue::FloatList(vec![*val]),
            NodeValue::Double(val) => NodeValue::DoubleList(vec![*val]),
            NodeValue::Message(val) => NodeValue::MessageList(vec![Arc::clone(val)]),
            _ => {
                return Err(AggregationError::VectorInitializationError {
                    value: format!("{self:?}"),
                })
            }
        })
    }

    fn push(&mut self, value: &NodeValue, max_length: Option<u32>) -> Result<(), AggregationError> {
        // Creates error provider function to format values only when it's necessary to create an error.
        fn create_err_provider<'a, T: Debug>(
            v: &'a Vec<T>,
            value: &'a NodeValue,
        ) -> Box<dyn FnOnce() -> AggregationError + 'a> {
            let err_provider = move || AggregationError::VectorAppendTypeMismatch {
                list: format!("{v:?}"),
                value: format!("{value:?}"),
            };
            Box::new(err_provider)
        }

        match self {
            NodeValue::BoolList(v) => {
                v.push(value.get_bool().ok_or_else(create_err_provider(v, value))?);
                pop_front_to_size_n(v, max_length);
            }
            NodeValue::Int32List(v) => {
                v.push(value.get_int().ok_or_else(create_err_provider(v, value))? as i32);
                pop_front_to_size_n(v, max_length);
            }
            NodeValue::Int64List(v) => {
                v.push(value.get_int().ok_or_else(create_err_provider(v, value))?);
                pop_front_to_size_n(v, max_length);
            }
            NodeValue::FloatList(v) => {
                v.push(value.get_double().ok_or_else(create_err_provider(v, value))? as f32);
                pop_front_to_size_n(v, max_length);
            }
            NodeValue::DoubleList(v) => {
                v.push(value.get_double().ok_or_else(create_err_provider(v, value))?);
                pop_front_to_size_n(v, max_length);
            }
            NodeValue::MessageList(v) => {
                v.push(Arc::clone(value.get_message().ok_or_else(create_err_provider(v, value))?));
                pop_front_to_size_n(v, max_length);
            }
            _ => panic!("push() should never be called on non-list: {self:?}"),
        }
        Ok(())
    }

    fn less_than(self, other: &NodeValue) -> Result<bool, AggregationError> {
        if let NodeValue::Bool(res) = self.operate_sync(
            &Operator::RelationalOperator(RelationalOperator::LT.into()),
            move || other,
        )? {
            Ok(res)
        } else {
            panic!("Relational operation LT should produce a boolean output");
        }
    }

    fn update_running_average(
        &mut self,
        n: u32,
        new_value: &NodeValue,
    ) -> Result<(), AggregationError> {
        let NodeValue::Double(avg) = self else {
            panic!("update_running_average() should never be called on non-double");
        };

        let new_sample = new_value.get_double().ok_or_else(|| {
            AggregationError::NumericAggregationTypeMismatch { value: format!("{new_value:?}") }
        })?;

        let n = f64::from(n);
        let sum = *avg * n;
        *avg = (sum + new_sample) / (n + 1.0);
        Ok(())
    }
}

// Ensure the vector has size of at most n by removing from front of the vector.
fn pop_front_to_size_n<T: std::fmt::Debug>(vec: &mut Vec<T>, n: Option<u32>) {
    if let Some(max_len) = n {
        let max_len = max_len as usize;
        if vec.len() <= max_len {
            // vector size did not exceed the limit, no need to remove anything
        } else if vec.len() == max_len + 1 {
            // remove the oldest element in the front of the vector
            vec.remove(0);
        } else {
            panic!(
                "Everytime the vector exceeds max length of n, it should remove the oldest \
                element to keep the vector at size n. But either a previous aggregation \
                or a current aggregation caused the vector {vec:?} to exceed max length of {max_len} \
                by more than 1"
            );
        }
    }
}
