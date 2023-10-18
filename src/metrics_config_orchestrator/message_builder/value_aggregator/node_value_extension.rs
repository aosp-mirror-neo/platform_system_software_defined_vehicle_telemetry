use crate::expr::node_value::NodeValue;
use crate::metrics_config_orchestrator::message_builder::value_aggregator::AggregationError;

/// Trait that defines helper functions for aggregating values.
pub trait NodeValueExtension {
    // Converts NodeValue of single value into NodeValue containing a list of 1 element.
    fn to_list(&self) -> Result<NodeValue, AggregationError>;

    // Appends a single value into an existing list.
    fn push(&mut self, value: &NodeValue) -> Result<(), AggregationError>;
}

impl NodeValueExtension for NodeValue {
    fn to_list(&self) -> Result<NodeValue, AggregationError> {
        Ok(match self {
            NodeValue::Bool(val) => NodeValue::BoolList(vec![*val]),
            NodeValue::Int32(val) => NodeValue::Int32List(vec![*val]),
            NodeValue::Int64(val) => NodeValue::Int64List(vec![*val]),
            NodeValue::Float(val) => NodeValue::FloatList(vec![*val]),
            NodeValue::Double(val) => NodeValue::DoubleList(vec![*val]),
            _ => {
                return Err(AggregationError::VectorInitializationError {
                    value: format!("{:?}", self),
                })
            }
        })
    }

    fn push(&mut self, value: &NodeValue) -> Result<(), AggregationError> {
        // debug string is created now because a mutable borrow of self occurs later
        let list_debug_string = format!("{:?}", self);
        let err_provider = || AggregationError::VectorAppendTypeMismatch {
            list: list_debug_string.clone(),
            value: format!("{:?}", value),
        };
        match self {
            NodeValue::BoolList(v) => {
                v.push(value.get_bool(&err_provider)?);
            }
            NodeValue::Int32List(v) => {
                v.push(value.get_int(&err_provider)? as i32);
            }
            NodeValue::Int64List(v) => {
                v.push(value.get_int(&err_provider)?);
            }
            NodeValue::FloatList(v) => {
                v.push(value.get_double(&err_provider)? as f32);
            }
            NodeValue::DoubleList(v) => {
                v.push(value.get_double(&err_provider)?);
            }
            _ => panic!("push() should never be called on non-list: {:?}", self),
        }
        Ok(())
    }
}
