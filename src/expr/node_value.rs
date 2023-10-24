// Copyright 2023 Google LLC. All Rights Reserved.

//! Wrapper around a generic value.
//! Should be able to hold all the values in a ConstantLeafNode protobuf
//! message, as well as all the values that a FieldLeafNode can select.

mod protobuf_reflection_extensions;

use protobuf::{
    reflect::{ReflectFieldRef, ReflectValueRef},
    MessageDyn,
};
use sdvtelemetry_proto_rust::expression::{
    combination_node::{
        ArithmeticOperator, LogicalOperator, Operator as OperatorProto, RelationalOperator,
    },
    constant_leaf_node::Node_value as NodeValueProto,
    ConstantLeafNode as ConstantLeafNodeProto,
};
use std::error::Error;

use crate::expr::{
    node_errors::NodeError, node_value::protobuf_reflection_extensions::ToNodeValue,
};

const EPSILON: f32 = 1e-6;

/// Holds a generic value.
#[derive(Clone, Debug)]
pub enum NodeValue {
    /// No value.
    None,

    /// A boolean node value.
    Bool(bool),

    /// An int32 node value.
    Int32(i32),

    /// An int64 node value.
    Int64(i64),

    /// A float node value.
    Float(f32),

    /// A double node value.
    Double(f64),

    /// A protobuf message value.
    Message(Box<dyn MessageDyn>),

    /// Repeated boolean values.
    BoolList(Vec<bool>),

    /// Repeated int32 values.
    Int32List(Vec<i32>),

    /// Repeated int64 values.
    Int64List(Vec<i64>),

    /// Repeated float values.
    FloatList(Vec<f32>),

    /// Repeated double values.
    DoubleList(Vec<f64>),
}

/// Check if two floating point values are approximately equal.
fn approximately_equal(v1: f64, v2: f64) -> bool {
    (v1 - v2).abs() < EPSILON.into()
}

/// The PartialEq trait allows comparing to NodeValue's accurately, including
/// for floating point values.
impl PartialEq for NodeValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (NodeValue::None, NodeValue::None) => true,
            (NodeValue::Bool(v1), NodeValue::Bool(v2)) => *v1 == *v2,
            (NodeValue::Int32(v1), NodeValue::Int32(v2)) => *v1 == *v2,
            (NodeValue::Int64(v1), NodeValue::Int64(v2)) => *v1 == *v2,
            (NodeValue::Float(v1), NodeValue::Float(v2)) => {
                approximately_equal(*v1 as f64, *v2 as f64)
            }
            (NodeValue::Double(v1), NodeValue::Double(v2)) => approximately_equal(*v1, *v2),
            (NodeValue::Message(v1), NodeValue::Message(v2)) => v1 == v2,
            (NodeValue::BoolList(v1), NodeValue::BoolList(v2)) => *v1 == *v2,
            (NodeValue::Int32List(v1), NodeValue::Int32List(v2)) => *v1 == *v2,
            (NodeValue::Int64List(v1), NodeValue::Int64List(v2)) => *v1 == *v2,
            (NodeValue::FloatList(v1), NodeValue::FloatList(v2)) => {
                v1.len() == v2.len()
                    && v1.iter().zip(v2).all(|(a, b)| approximately_equal(*a as f64, *b as f64))
            }
            (NodeValue::DoubleList(v1), NodeValue::DoubleList(v2)) => {
                v1.len() == v2.len() && v1.iter().zip(v2).all(|(a, b)| approximately_equal(*a, *b))
            }
            _ => false,
        }
    }
}

impl NodeValue {
    /// Converts a ConstantLeafNode into a NodeValue.
    pub fn from_constant_leaf_node(
        constant_leaf_node: &ConstantLeafNodeProto,
    ) -> Result<NodeValue, NodeError> {
        Ok(match &constant_leaf_node.node_value {
            Some(NodeValueProto::BoolValue(val)) => NodeValue::Bool(*val),
            Some(NodeValueProto::Int32Value(val)) => NodeValue::Int32(*val),
            Some(NodeValueProto::Int64Value(val)) => NodeValue::Int64(*val),
            Some(NodeValueProto::FloatValue(val)) => NodeValue::Float(*val),
            Some(NodeValueProto::DoubleValue(val)) => NodeValue::Double(*val),
            _ => return Err(NodeError::ConstantValueNotSet),
        })
    }

    /// Performs operations using the operator.
    /// This call will short-circuit and skip evaluating the right branch if the
    /// left operand uniquely determines the result.
    ///
    /// # Arguments:
    ///
    /// * `&self` - The left operand.
    /// * `op` - The operator.
    ///          If unary operator, then operates on self.
    ///          If binary operator, then [self] <operator> [right].
    /// * `get_right_operand` - The function that provides the right operand when called.
    pub fn operate(
        &self,
        op: &OperatorProto,
        get_right_operand: impl FnOnce() -> Result<NodeValue, NodeError>,
    ) -> Result<NodeValue, NodeError> {
        match op {
            OperatorProto::ArithmeticOperator(op) => self.handle_arithmetic(
                &op.enum_value().map_err(|i| NodeError::UnknownOperator {
                    op: format!("Arithmetic operator with enum value {}", i),
                })?,
                &get_right_operand()?,
            ),
            OperatorProto::LogicalOperator(op) => self.handle_logical(
                &op.enum_value().map_err(|i| NodeError::UnknownOperator {
                    op: format!("Logical operator with enum value {}", i),
                })?,
                get_right_operand,
            ),
            OperatorProto::RelationalOperator(op) => self.handle_relational(
                &op.enum_value().map_err(|i| NodeError::UnknownOperator {
                    op: format!("Relational operator with enum value {}", i),
                })?,
                &get_right_operand()?,
            ),
            _ => Err(NodeError::UnknownOperator { op: format!("{:?}", op) }),
        }
    }

    /// Gets a NodeValue from a protobuf message and its fields.
    pub fn extract_value(
        msg: Box<dyn MessageDyn>,
        fields: &[String],
    ) -> Result<NodeValue, NodeError> {
        // if no field names, return the message itself as the NodeValue
        if fields.is_empty() {
            return Ok(NodeValue::Message(msg));
        }
        // recursively gets the field value from field names
        Self::extract_field_value(msg.as_ref(), fields)
    }

    /// Returns the bool value or a custom error message if NodeValue is not a bool.
    pub fn get_bool<E>(&self, err_provider: &impl Fn() -> E) -> Result<bool, E>
    where
        E: Error,
    {
        if let NodeValue::Bool(val) = self {
            return Ok(*val);
        }
        Err(err_provider())
    }

    /// Returns the int value or the provided error if NodeValue is not an int.
    pub fn get_int<E>(&self, err_provider: &impl Fn() -> E) -> Result<i64, E>
    where
        E: Error,
    {
        Ok(match self {
            NodeValue::Int32(val) => *val as i64,
            NodeValue::Int64(val) => *val,
            _ => return Err(err_provider()),
        })
    }

    /// Returns the double value or error if NodeValue is not numeric.
    pub fn get_double<E>(&self, err_provider: &impl Fn() -> E) -> Result<f64, E>
    where
        E: Error,
    {
        Ok(match self {
            NodeValue::Int32(val) => *val as f64,
            NodeValue::Int64(val) => *val as f64,
            NodeValue::Float(val) => *val as f64,
            NodeValue::Double(val) => *val,
            _ => return Err(err_provider()),
        })
    }

    /// Returns the Message value or the provided error if NodeValue is not a Message.
    pub fn get_message<E>(&self, err_provider: impl FnOnce() -> E) -> Result<&dyn MessageDyn, E>
    where
        E: Error,
    {
        if let NodeValue::Message(msg) = self {
            Ok(msg.as_ref())
        } else {
            Err(err_provider())
        }
    }

    /// Check if node holds a numeric value.
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            NodeValue::Int32(_) | NodeValue::Int64(_) | NodeValue::Float(_) | NodeValue::Double(_)
        )
    }

    /// Checks whether the node value is zero.
    pub fn is_zero(&self) -> Result<bool, NodeError> {
        Ok(match self {
            NodeValue::Int32(val) => *val == 0,
            NodeValue::Int64(val) => *val == 0,
            NodeValue::Float(val) => (*val).abs() < EPSILON,
            NodeValue::Double(val) => (*val).abs() < EPSILON.into(),
            _ => {
                return Err(NodeError::InternalError {
                    msg: format!("is_zero() invoked on non-numeric NodeValue: {:?}", self),
                })
            }
        })
    }

    // private functions and methods below

    /// Reads a field value from a protobuf message using the reflection.
    fn extract_field_value(
        msg: &dyn MessageDyn,
        fields: &[String],
    ) -> Result<NodeValue, NodeError> {
        let msg_descriptor = msg.descriptor_dyn();
        let field_name = &fields[0];
        let field_descriptor =
            msg_descriptor.field_by_name(field_name).ok_or(NodeError::FieldNotFound {
                field_name: field_name.clone(),
                message_name: msg_descriptor.name().to_string(),
            })?;
        let field_value = field_descriptor.get_reflect(msg);

        // Base case for recursion: one field is provided, simply read the value
        if fields.len() == 1 {
            return field_value.to_node_value();
        }

        // If fields length > 1, get nested field values iff the value is a single protobuf message,
        // otherwise return error
        if let ReflectFieldRef::Optional(singular_field) = field_value {
            match singular_field.value() {
                Some(nested_value) => {
                    if let ReflectValueRef::Message(msg) = nested_value {
                        // recursively read from nested messages
                        Self::extract_field_value(&(*msg), &fields[1..])
                    } else {
                        Err(NodeError::WrongNestedFieldType {
                            field_name: field_name.clone(),
                            e: "A non-terminal field is singular, but is not a \
                                protobuf message, cannot get nested fields"
                                .to_string(),
                        })
                    }
                }
                None => Ok(NodeValue::None),
            }
        } else {
            Err(NodeError::WrongNestedFieldType {
                field_name: field_name.clone(),
                e: "A non-terminal field is either repeated or a map".to_string(),
            })
        }
    }

    /// Handle logical operations.
    fn handle_logical(
        &self,
        op: &LogicalOperator,
        get_right_operand: impl FnOnce() -> Result<NodeValue, NodeError>,
    ) -> Result<NodeValue, NodeError> {
        let err_provider = || NodeError::LogicalOperatorIncompatibleOperands;
        if !self.is_bool() {
            return Err(err_provider());
        }
        Ok(NodeValue::Bool(match op {
            LogicalOperator::AND => {
                if !(self.get_bool(&err_provider)?) {
                    return Ok(self.clone());
                }
                self.get_bool(&err_provider)? && get_right_operand()?.get_bool(&err_provider)?
            }
            LogicalOperator::OR => {
                if self.get_bool(&err_provider)? {
                    return Ok(self.clone());
                }
                self.get_bool(&err_provider)? || get_right_operand()?.get_bool(&err_provider)?
            }
            LogicalOperator::XOR => {
                self.get_bool(&err_provider)? != get_right_operand()?.get_bool(&err_provider)?
            }
            LogicalOperator::NOT => !self.get_bool(&err_provider)?,
        }))
    }

    /// Handle arithmetic operations.
    fn handle_arithmetic(
        &self,
        op: &ArithmeticOperator,
        other: &NodeValue,
    ) -> Result<NodeValue, NodeError> {
        let err_provider = || NodeError::ArithmeticOperatorIncompatibleOperands;
        // assert that both operands are numeric types
        if !self.is_numeric() || !other.is_numeric() {
            return Err(err_provider());
        }
        match op {
            ArithmeticOperator::ADD => self.apply_numeric(
                other,
                |a, b| NodeValue::Int64(a + b),
                |a, b| NodeValue::Double(a + b),
                &err_provider,
            ),
            ArithmeticOperator::SUBTRACT => self.apply_numeric(
                other,
                |a, b| NodeValue::Int64(a - b),
                |a, b| NodeValue::Double(a - b),
                &err_provider,
            ),
            ArithmeticOperator::MULTIPLY => self.apply_numeric(
                other,
                |a, b| NodeValue::Int64(a * b),
                |a, b| NodeValue::Double(a * b),
                &err_provider,
            ),
            ArithmeticOperator::DIVIDE => {
                if other.is_zero()? {
                    return Err(NodeError::DivideByZero);
                }
                Ok(NodeValue::Double(
                    self.get_double(&err_provider)? / other.get_double(&err_provider)?,
                ))
            }
            ArithmeticOperator::MODULUS => {
                if !self.is_int() || !other.is_int() {
                    return Err(NodeError::ModulusOperatorIncompatibleOperands);
                }
                if other.is_zero()? {
                    return Err(NodeError::DivideByZero);
                }
                Ok(NodeValue::Int64(self.get_int(&err_provider)? % other.get_int(&err_provider)?))
            }
            ArithmeticOperator::POWER => self.apply_numeric(
                other,
                |a, b| NodeValue::Int64(a.pow(b as u32)),
                |a, b| NodeValue::Double(a.powf(b)),
                &err_provider,
            ),
        }
    }

    /// Handle relational operations.
    fn handle_relational(
        &self,
        op: &RelationalOperator,
        other: &NodeValue,
    ) -> Result<NodeValue, NodeError> {
        let err_provider = || NodeError::ComparisonOperatorIncomptaibleOperands;
        Ok(NodeValue::Bool(match op {
            RelationalOperator::EQ => {
                if !self.is_numeric() || !other.is_numeric() {
                    return Ok(NodeValue::Bool(self == other));
                }
                self.apply_numeric(other, |a, b| a == b, approximately_equal, &err_provider)
            }
            RelationalOperator::NOT_EQ => {
                if !self.is_numeric() || !other.is_numeric() {
                    return Ok(NodeValue::Bool(self != other));
                }
                self.apply_numeric(
                    other,
                    |a, b| a != b,
                    |a, b| !approximately_equal(a, b),
                    &err_provider,
                )
            }
            RelationalOperator::GT => self.apply_generic_operation(
                other,
                |a, b| a & !b,
                |a, b| a > b,
                |a, b| a > b,
                &err_provider,
            ),
            RelationalOperator::GT_OR_EQ => self.apply_generic_operation(
                other,
                |a, b| a >= b,
                |a, b| a >= b,
                |a, b| a >= b || approximately_equal(a, b),
                &err_provider,
            ),
            RelationalOperator::LT => self.apply_generic_operation(
                other,
                |a, b| !a & b,
                |a, b| a < b,
                |a, b| a < b,
                &err_provider,
            ),
            RelationalOperator::LT_OR_EQ => self.apply_generic_operation(
                other,
                |a, b| a <= b,
                |a, b| a <= b,
                |a, b| a <= b || approximately_equal(a, b),
                &err_provider,
            ),
            RelationalOperator::CONTAINS => self.contains(other),
            RelationalOperator::DOES_NOT_CONTAIN => {
                // negate the boolean in a Result<bool, E>
                self.contains(other).map(|x| !x)
            }
            RelationalOperator::ALL_EQ => self.all_equals(other),
        }?))
    }

    /// Check if list contains some element.
    /// Returns error if `self` is not a list value or `element` is not a compatible type.
    fn contains(&self, element: &NodeValue) -> Result<bool, NodeError> {
        let err_provider = || NodeError::ContainsOperatorIncompatibleOperands;
        Ok(match self {
            NodeValue::BoolList(v) => v.contains(&element.get_bool(&err_provider)?),
            NodeValue::Int32List(v) => {
                let target_value = element.get_int(&err_provider)? as i32;
                v.contains(&target_value)
            }
            NodeValue::Int64List(v) => v.contains(&element.get_int(&err_provider)?),
            NodeValue::FloatList(v) => {
                let target_value = element.get_double(&err_provider)?;
                v.iter().any(|x| approximately_equal(*x as f64, target_value))
            }
            NodeValue::DoubleList(v) => {
                let target_value = element.get_double(&err_provider)?;
                v.iter().any(|x| approximately_equal(*x, target_value))
            }
            _ => return Err(err_provider()),
        })
    }

    /// Check if all list items are equal to some element.
    /// Returns errors if `self` is not a list value or `element` is not a compatible type.
    fn all_equals(&self, element: &NodeValue) -> Result<bool, NodeError> {
        let err_provider = || NodeError::AllEqualsOperatorIncompatibleOperands;
        Ok(match self {
            NodeValue::BoolList(v) => {
                let target_value = element.get_bool(&err_provider)?;
                Self::all_equal_to(v, target_value)
            }
            NodeValue::Int32List(v) => {
                let target_value = element.get_int(&err_provider)? as i32;
                Self::all_equal_to(v, target_value)
            }
            NodeValue::Int64List(v) => {
                let target_value = element.get_int(&err_provider)?;
                Self::all_equal_to(v, target_value)
            }
            NodeValue::FloatList(v) => {
                let target_value = element.get_double(&err_provider)?;
                v.iter().all(|x| approximately_equal(*x as f64, target_value))
            }
            NodeValue::DoubleList(v) => {
                let target_value = element.get_double(&err_provider)?;
                v.iter().all(|x| approximately_equal(*x, target_value))
            }
            _ => return Err(err_provider()),
        })
    }

    /// Check if all vector elements are equal to target_value.
    fn all_equal_to<T: PartialEq>(v: &[T], target_value: T) -> bool {
        v.iter().all(|x| *x == target_value)
    }

    /// Check if node holds a boolean value.
    fn is_bool(&self) -> bool {
        matches!(self, NodeValue::Bool(_))
    }

    /// Check if node holds an integer value.
    fn is_int(&self) -> bool {
        matches!(self, NodeValue::Int32(_) | NodeValue::Int64(_))
    }

    /// Applies the provided function on two numeric NodeValues based on
    /// the numeric type (integer or floating point).
    /// Returns an error if the NodeValues are not numeric types.
    fn apply_numeric<R>(
        &self,
        other: &NodeValue,
        int_func: impl FnOnce(i64, i64) -> R,
        float_func: impl FnOnce(f64, f64) -> R,
        err_provider: &impl Fn() -> NodeError,
    ) -> Result<R, NodeError> {
        if self.is_int() && other.is_int() {
            Ok(int_func(self.get_int(err_provider)?, other.get_int(err_provider)?))
        } else if self.is_numeric() && other.is_numeric() {
            Ok(float_func(self.get_double(err_provider)?, other.get_double(err_provider)?))
        } else {
            Err(err_provider())
        }
    }

    /// Applies the provided function on two NodeValues based on their types,
    /// supported generic types are booleans, integers or floating points.
    /// Returns an error if the NodeValues are not one of those types.
    fn apply_generic_operation<R>(
        &self,
        other: &NodeValue,
        bool_func: impl FnOnce(bool, bool) -> R,
        int_func: impl FnOnce(i64, i64) -> R,
        float_func: impl FnOnce(f64, f64) -> R,
        err_provider: &impl Fn() -> NodeError,
    ) -> Result<R, NodeError> {
        if self.is_bool() && other.is_bool() {
            Ok(bool_func(self.get_bool(err_provider)?, other.get_bool(err_provider)?))
        } else {
            self.apply_numeric(other, int_func, float_func, err_provider)
        }
    }
}

#[cfg(test)]
mod tests {

    use protobuf::Message;
    use sdvtelemetry_proto_rust::expression::ConstantLeafNode;
    use std::sync::Mutex;

    use super::*;
    use crate::expr::mock::*;

    fn provide(value: &NodeValue) -> Result<NodeValue, NodeError> {
        Ok(value.clone())
    }

    #[test]
    fn test_from_constant_leaf_node() -> Result<(), NodeError> {
        // setup
        let mut constant_leaf_node_bool = ConstantLeafNode::new();
        constant_leaf_node_bool.set_bool_value(true);

        let mut constant_leaf_node_i32 = ConstantLeafNode::new();
        constant_leaf_node_i32.set_int32_value(32);

        let mut constant_leaf_node_i64 = ConstantLeafNode::new();
        constant_leaf_node_i64.set_int64_value(64);

        let mut constant_leaf_node_float = ConstantLeafNode::new();
        constant_leaf_node_float.set_float_value(1.5);

        let mut constant_leaf_node_double = ConstantLeafNode::new();
        constant_leaf_node_double.set_double_value(1.5);

        // assert
        assert_eq!(
            NodeValue::from_constant_leaf_node(&constant_leaf_node_bool)?,
            NodeValue::Bool(true)
        );
        assert_eq!(
            NodeValue::from_constant_leaf_node(&constant_leaf_node_i32)?,
            NodeValue::Int32(32)
        );
        assert_eq!(
            NodeValue::from_constant_leaf_node(&constant_leaf_node_i64)?,
            NodeValue::Int64(64)
        );
        assert_eq!(
            NodeValue::from_constant_leaf_node(&constant_leaf_node_float)?,
            NodeValue::Float(1.5)
        );
        assert_eq!(
            NodeValue::from_constant_leaf_node(&constant_leaf_node_double)?,
            NodeValue::Double(1.5)
        );
        Ok(())
    }

    #[test]
    fn test_extract_value_no_field_names_ok() -> Result<(), NodeError> {
        // setup
        let msg = TEST_MESSAGE.clone();
        let expected_msg_bytes =
            msg.write_to_bytes().unwrap_or_else(|_| panic!("Failed to serialize TestMessage"));

        // action
        let node_value = NodeValue::extract_value(Box::new(msg), &[])?;

        // assert
        if let NodeValue::Message(node_msg) = node_value {
            let bytes = node_msg
                .write_to_bytes_dyn()
                .unwrap_or_else(|_| panic!("Failed to serialize NodeValue's protobuf message"));
            assert_eq!(bytes, expected_msg_bytes);
        } else {
            panic!("Expect Message variant of NodeValue, got {:?}", node_value);
        }
        Ok(())
    }

    #[test]
    fn test_extract_value_field_not_set_ok() -> Result<(), NodeError> {
        let field_names = ["int_field".to_string()];

        assert_eq!(
            NodeValue::extract_value(Box::new(EMPTY_TEST_MESSAGE.clone()), &field_names)?,
            NodeValue::None
        );
        Ok(())
    }

    #[test]
    fn test_extract_value_int_field_ok() -> Result<(), NodeError> {
        let field_names = ["int_field".to_string()];

        assert_eq!(
            NodeValue::extract_value(Box::new(TEST_MESSAGE.clone()), &field_names)?,
            NodeValue::Int32(80)
        );
        Ok(())
    }

    #[test]
    fn test_extract_value_float_field_ok() -> Result<(), NodeError> {
        let field_names = ["float_field".to_string()];

        assert_eq!(
            NodeValue::extract_value(Box::new(TEST_MESSAGE.clone()), &field_names)?,
            NodeValue::Float(100.0)
        );
        Ok(())
    }

    #[test]
    fn test_extract_value_enum_field_ok() -> Result<(), NodeError> {
        let field_names = ["enum_field".to_string()];

        assert_eq!(
            NodeValue::extract_value(Box::new(TEST_MESSAGE.clone()), &field_names)?,
            NodeValue::Int32(1)
        );
        Ok(())
    }

    #[test]
    fn test_extract_field_value_message_field_ok() -> Result<(), NodeError> {
        let msg = TEST_MESSAGE.clone();
        let field_names = ["message_field".to_string()];
        let expected_value = NodeValue::Message(Box::new(msg.message_field.clone().unwrap()));

        // action
        let node_value = NodeValue::extract_value(Box::new(msg), &field_names)?;

        // assert
        assert_eq!(node_value, expected_value);
        Ok(())
    }

    #[test]
    fn test_extract_value_nested_field_not_set_ok() -> Result<(), NodeError> {
        let field_names = ["message_field".to_string(), "int_field".to_string()];

        assert_eq!(
            NodeValue::extract_value(Box::new(EMPTY_TEST_MESSAGE.clone()), &field_names)?,
            NodeValue::None
        );
        Ok(())
    }

    #[test]
    fn test_extract_value_nested_singular_field_ok() -> Result<(), NodeError> {
        let field_names = ["message_field".to_string(), "bool_field".to_string()];

        assert_eq!(
            NodeValue::extract_value(Box::new(TEST_MESSAGE.clone()), &field_names)?,
            NodeValue::Bool(true)
        );
        Ok(())
    }

    #[test]
    fn test_extract_value_repeated_bool_field_ok() -> Result<(), NodeError> {
        let msg = TEST_MESSAGE.clone();
        let field_names = ["repeated_bools".to_string()];
        let repeated_bools = msg.repeated_bools.clone();

        assert_eq!(
            NodeValue::extract_value(Box::new(msg), &field_names)?,
            NodeValue::BoolList(repeated_bools)
        );
        Ok(())
    }

    #[test]
    fn test_extract_value_repeated_int_field_ok() -> Result<(), NodeError> {
        let msg = TEST_MESSAGE.clone();
        let field_names = ["repeated_i64s".to_string()];
        let repeated_i64s = msg.repeated_i64s.clone();

        assert_eq!(
            NodeValue::extract_value(Box::new(msg), &field_names)?,
            NodeValue::Int64List(repeated_i64s)
        );
        Ok(())
    }

    #[test]
    fn test_extract_value_repeated_double_field_ok() -> Result<(), NodeError> {
        let msg = TEST_MESSAGE.clone();
        let field_names = ["repeated_doubles".to_string()];
        let repeated_doubles = msg.repeated_doubles.clone();

        assert_eq!(
            NodeValue::extract_value(Box::new(msg), &field_names)?,
            NodeValue::DoubleList(repeated_doubles)
        );
        Ok(())
    }

    #[test]
    fn test_extract_value_repeated_enum_field_ok() -> Result<(), NodeError> {
        let msg = TEST_MESSAGE.clone();
        let field_names = ["repeated_enums".to_string()];
        let repeated_i32s = msg.repeated_enums.iter().map(|x| x.value()).collect();

        assert_eq!(
            NodeValue::extract_value(Box::new(msg), &field_names)?,
            NodeValue::Int32List(repeated_i32s)
        );
        Ok(())
    }

    #[test]
    fn test_extract_value_nested_repeated_field_ok() -> Result<(), NodeError> {
        let msg = TEST_MESSAGE.clone();
        let field_names = ["message_field".to_string(), "repeated_i32s".to_string()];
        let repeated_i32s = msg.message_field.repeated_i32s.clone();

        assert_eq!(
            NodeValue::extract_value(Box::new(msg), &field_names)?,
            NodeValue::Int32List(repeated_i32s)
        );
        Ok(())
    }

    #[test]
    fn test_extract_value_string_field_should_err() {
        let field_names = ["string_field".to_string()];

        assert!(matches!(
            NodeValue::extract_value(Box::new(TEST_MESSAGE.clone()), &field_names).err().unwrap(),
            NodeError::UnsupportedSingularFieldType { .. }
        ));
    }

    #[test]
    fn test_extract_value_repeated_message_field_should_err() {
        let field_names = ["repeated_messages".to_string()];

        assert!(matches!(
            NodeValue::extract_value(Box::new(TEST_MESSAGE.clone()), &field_names).err().unwrap(),
            NodeError::UnsupportedRepeatedFieldType { .. }
        ));
    }

    #[test]
    fn test_extract_value_repeated_message_as_intermediate_field_name_should_err() {
        let field_names = ["repeated_messages".to_string(), "bool_field".to_string()];

        assert!(matches!(
            NodeValue::extract_value(Box::new(TEST_MESSAGE.clone()), &field_names).err().unwrap(),
            NodeError::WrongNestedFieldType { .. }
        ));
    }

    #[test]
    fn test_extract_value_nonexistent_field_should_err() {
        let field_names = ["foo".to_string()];

        assert!(matches!(
            NodeValue::extract_value(Box::new(TEST_MESSAGE.clone()), &field_names).err().unwrap(),
            NodeError::FieldNotFound { .. }
        ));
    }

    #[test]
    fn test_operate_logical_requires_boolean_types() {
        let true_val = NodeValue::Bool(true);
        let int_val = NodeValue::Int32(1);

        assert!(matches!(
            true_val.operate(&OP_AND, || provide(&int_val)).err().unwrap(),
            NodeError::LogicalOperatorIncompatibleOperands
        ));
        assert!(matches!(
            int_val.operate(&OP_AND, || provide(&true_val)).err().unwrap(),
            NodeError::LogicalOperatorIncompatibleOperands
        ));
    }

    #[test]
    fn test_operate_logical_and() -> Result<(), NodeError> {
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        // true && true = true
        assert_eq!(true_val.operate(&OP_AND, || provide(&true_val))?, NodeValue::Bool(true));
        // true && false = false
        assert_eq!(true_val.operate(&OP_AND, || provide(&false_val))?, NodeValue::Bool(false));
        // false && false = false
        assert_eq!(false_val.operate(&OP_AND, || provide(&false_val))?, NodeValue::Bool(false));
        Ok(())
    }

    #[test]
    fn test_operate_logical_and_short_circuits() -> Result<(), NodeError> {
        let false_val = NodeValue::Bool(false);
        let num_calls = Mutex::new(0);
        let right_operand_provider = || {
            *num_calls.lock().unwrap() += 1;
            provide(&false_val)
        };

        let res = false_val.operate(&OP_AND, right_operand_provider)?;

        // false && anything = false
        assert_eq!(res, NodeValue::Bool(false));
        // should not compute right operand because of short circuiting
        assert_eq!(*num_calls.lock().unwrap(), 0);
        Ok(())
    }

    #[test]
    fn test_operate_logical_and_short_circuiting_attempted() -> Result<(), NodeError> {
        let true_val = NodeValue::Bool(true);
        let num_calls = Mutex::new(0);
        let right_operand_provider = || {
            *num_calls.lock().unwrap() += 1;
            provide(&true_val)
        };

        // true && [right_operand] = [right_operand]
        let res = true_val.operate(&OP_AND, right_operand_provider)?;

        assert_eq!(res, NodeValue::Bool(true));
        // should compute right operand because left operand does not uniquely determine result
        assert_eq!(*num_calls.lock().unwrap(), 1);
        Ok(())
    }

    #[test]
    fn test_operate_logical_or() -> Result<(), NodeError> {
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        // true || true = true
        assert_eq!(true_val.operate(&OP_OR, || provide(&true_val))?, NodeValue::Bool(true));
        // true || false = true
        assert_eq!(true_val.operate(&OP_OR, || provide(&false_val))?, NodeValue::Bool(true));
        // false || false = false
        assert_eq!(false_val.operate(&OP_OR, || provide(&false_val))?, NodeValue::Bool(false));
        Ok(())
    }

    #[test]
    fn test_operate_logical_or_short_circuits() -> Result<(), NodeError> {
        let true_val = NodeValue::Bool(true);
        let num_calls = Mutex::new(0);
        let right_operand_provider = || {
            *num_calls.lock().unwrap() += 1;
            provide(&true_val)
        };

        let res = true_val.operate(&OP_OR, right_operand_provider)?;

        // true || anything = true
        assert_eq!(res, NodeValue::Bool(true));
        // should not compute right operand because of short circuiting
        assert_eq!(*num_calls.lock().unwrap(), 0);
        Ok(())
    }

    #[test]
    fn test_operate_logical_or_short_circuiting_attempted() -> Result<(), NodeError> {
        let false_val = NodeValue::Bool(false);
        let num_calls = Mutex::new(0);
        let right_operand_provider = || {
            *num_calls.lock().unwrap() += 1;
            provide(&false_val)
        };

        // false || [right_operand] = [right_operand]
        let res = false_val.operate(&OP_OR, right_operand_provider)?;

        assert_eq!(res, NodeValue::Bool(false));
        // should compute right operand because left operand does not uniquely determine result
        assert_eq!(*num_calls.lock().unwrap(), 1);
        Ok(())
    }

    #[test]
    fn test_operate_logical_xor() -> Result<(), NodeError> {
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        // true XOR true = false
        assert_eq!(true_val.operate(&OP_XOR, || provide(&true_val))?, NodeValue::Bool(false));
        // true XOR false = true
        assert_eq!(true_val.operate(&OP_XOR, || provide(&false_val))?, NodeValue::Bool(true));
        // false XOR false = false
        assert_eq!(false_val.operate(&OP_XOR, || provide(&false_val))?, NodeValue::Bool(false));
        Ok(())
    }

    #[test]
    fn test_operate_logical_not() -> Result<(), NodeError> {
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        // !true = false
        assert_eq!(
            true_val.operate(&OP_NOT, || panic!(
                "Unary operator should not call right operand provider"
            ))?,
            NodeValue::Bool(false)
        );
        // !false = true
        assert_eq!(
            false_val.operate(&OP_NOT, || panic!(
                "Unary operator should not call right operand provider"
            ))?,
            NodeValue::Bool(true)
        );
        Ok(())
    }

    #[test]
    fn test_operate_arithmetic_requires_numeric_types() {
        let true_val = NodeValue::Bool(true);
        let int_val = NodeValue::Int32(1);

        assert!(matches!(
            true_val.operate(&OP_ADD, || provide(&int_val)).err().unwrap(),
            NodeError::ArithmeticOperatorIncompatibleOperands
        ));
        assert!(matches!(
            int_val.operate(&OP_ADD, || provide(&true_val)).err().unwrap(),
            NodeError::ArithmeticOperatorIncompatibleOperands
        ));
    }

    #[test]
    fn test_operate_arithmetic_add_integers() -> Result<(), NodeError> {
        let five = NodeValue::Int32(5);
        let twenty = NodeValue::Int32(20);

        // 5 + 20 = 25
        assert_eq!(five.operate(&OP_ADD, || provide(&twenty))?, NodeValue::Int64(25));
        Ok(())
    }

    #[test]
    fn test_operate_arithmetic_add_floats() -> Result<(), NodeError> {
        let five = NodeValue::Float(5.2);
        let twenty = NodeValue::Double(20.1);

        // 5.2 + 20.1 = 25.3
        assert_eq!(five.operate(&OP_ADD, || provide(&twenty))?, NodeValue::Double(25.3));
        Ok(())
    }

    #[test]
    fn test_operate_arithmetic_subtract_integers() -> Result<(), NodeError> {
        let five = NodeValue::Int32(5);
        let twenty = NodeValue::Int32(20);

        // 5 - 20 = -15
        assert_eq!(five.operate(&OP_SUBTRACT, || provide(&twenty))?, NodeValue::Int64(-15));
        Ok(())
    }

    #[test]
    fn test_operate_arithmetic_subtract_floats() -> Result<(), NodeError> {
        let five = NodeValue::Float(5.2);
        let twenty = NodeValue::Double(20.1);

        // 5.2 - 20.1 = -14.9
        assert_eq!(five.operate(&OP_SUBTRACT, || provide(&twenty))?, NodeValue::Double(-14.9));
        Ok(())
    }

    #[test]
    fn test_operate_arithmetic_multiply() -> Result<(), NodeError> {
        let five = NodeValue::Int32(5);
        let point_five = NodeValue::Float(0.5);

        // 5 * 0.5 = 2.5
        assert_eq!(five.operate(&OP_MULTIPLY, || provide(&point_five))?, NodeValue::Double(2.5));
        Ok(())
    }

    #[test]
    fn test_operate_arithmetic_divide_by_zero() {
        let five = NodeValue::Int32(5);
        let zero = NodeValue::Int32(0);

        assert!(matches!(
            five.operate(&OP_DIVIDE, || provide(&zero)).err().unwrap(),
            NodeError::DivideByZero
        ));
    }

    #[test]
    fn test_operate_arithmetic_divide() -> Result<(), NodeError> {
        let five = NodeValue::Int32(5);
        let two = NodeValue::Int32(2);

        // 5 / 2 = 2.5
        assert_eq!(five.operate(&OP_DIVIDE, || provide(&two))?, NodeValue::Double(2.5));
        Ok(())
    }

    #[test]
    fn test_operate_arithmetic_modulus_by_zero() {
        let five = NodeValue::Int32(5);
        let zero = NodeValue::Int32(0);

        assert!(matches!(
            five.operate(&OP_MODULUS, || provide(&zero)).err().unwrap(),
            NodeError::DivideByZero
        ));
    }

    #[test]
    fn test_operate_arithmetic_modulus() -> Result<(), NodeError> {
        let five = NodeValue::Int32(5);
        let two = NodeValue::Int32(2);

        // 5 % 2 = 1
        assert_eq!(five.operate(&OP_MODULUS, || provide(&two))?, NodeValue::Int64(1));
        Ok(())
    }

    #[test]
    fn test_operate_arithmetic_modulus_no_floating_points() {
        let five = NodeValue::Float(5.0);
        let two = NodeValue::Int32(2);

        // both operands must be integers
        assert!(matches!(
            five.operate(&OP_MODULUS, || provide(&two)).err().unwrap(),
            NodeError::ModulusOperatorIncompatibleOperands
        ));
    }

    #[test]
    fn test_operate_arithmetic_power_integers() -> Result<(), NodeError> {
        let two = NodeValue::Int32(2);

        // 2^2 = 4
        assert_eq!(two.operate(&OP_POWER, || provide(&two))?, NodeValue::Int64(4));
        Ok(())
    }

    #[test]
    fn test_operate_arithmetic_power_floats() -> Result<(), NodeError> {
        let val = 2.5_f64;
        let expected = val.powf(val);
        let two_point_five = NodeValue::Double(val);

        assert_eq!(
            two_point_five.operate(&OP_POWER, || provide(&two_point_five))?,
            NodeValue::Double(expected)
        );
        Ok(())
    }

    #[test]
    fn test_operate_relational_eq() -> Result<(), NodeError> {
        let two = NodeValue::Int32(2);
        let two_i64 = NodeValue::Int64(2);
        let two_f32 = NodeValue::Float(2.0);
        let two_f64 = NodeValue::Double(2.0);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(two.operate(&OP_EQ, || provide(&two))?, NodeValue::Bool(true));
        assert_eq!(two.operate(&OP_EQ, || provide(&two_i64))?, NodeValue::Bool(true));
        assert_eq!(two.operate(&OP_EQ, || provide(&two_f32))?, NodeValue::Bool(true));
        assert_eq!(two.operate(&OP_EQ, || provide(&two_f64))?, NodeValue::Bool(true));
        assert_eq!(two.operate(&OP_EQ, || provide(&five))?, NodeValue::Bool(false));
        assert_eq!(two.operate(&OP_EQ, || provide(&true_val))?, NodeValue::Bool(false));
        Ok(())
    }

    #[test]
    fn test_operate_relational_not_eq() -> Result<(), NodeError> {
        let two = NodeValue::Int32(2);
        let two_f32 = NodeValue::Float(2.0);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(two.operate(&OP_NOT_EQ, || provide(&two))?, NodeValue::Bool(false));
        assert_eq!(two.operate(&OP_NOT_EQ, || provide(&two_f32))?, NodeValue::Bool(false));
        assert_eq!(two.operate(&OP_NOT_EQ, || provide(&five))?, NodeValue::Bool(true));
        assert_eq!(two.operate(&OP_NOT_EQ, || provide(&true_val))?, NodeValue::Bool(true));
        Ok(())
    }

    #[test]
    fn test_operate_relational_gt() -> Result<(), NodeError> {
        let two = NodeValue::Int32(2);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        assert_eq!(true_val.operate(&OP_GT, || provide(&false_val))?, NodeValue::Bool(true));
        assert_eq!(two.operate(&OP_GT, || provide(&two))?, NodeValue::Bool(false));
        assert_eq!(two.operate(&OP_GT, || provide(&five))?, NodeValue::Bool(false));
        assert_eq!(five.operate(&OP_GT, || provide(&two))?, NodeValue::Bool(true));
        assert!(matches!(
            five.operate(&OP_GT, || provide(&true_val)).err().unwrap(),
            NodeError::ComparisonOperatorIncomptaibleOperands
        ));
        Ok(())
    }

    #[test]
    fn test_operate_relational_gt_or_eq() -> Result<(), NodeError> {
        let two = NodeValue::Int32(2);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(true_val.operate(&OP_GT_OR_EQ, || provide(&true_val))?, NodeValue::Bool(true));
        assert_eq!(two.operate(&OP_GT_OR_EQ, || provide(&two))?, NodeValue::Bool(true));
        assert_eq!(two.operate(&OP_GT_OR_EQ, || provide(&five))?, NodeValue::Bool(false));
        assert_eq!(five.operate(&OP_GT_OR_EQ, || provide(&two))?, NodeValue::Bool(true));
        assert!(matches!(
            five.operate(&OP_GT_OR_EQ, || provide(&true_val)).err().unwrap(),
            NodeError::ComparisonOperatorIncomptaibleOperands
        ));
        Ok(())
    }

    #[test]
    fn test_operate_relational_lt() -> Result<(), NodeError> {
        let two = NodeValue::Int32(2);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        assert_eq!(true_val.operate(&OP_LT, || provide(&false_val))?, NodeValue::Bool(false));
        assert_eq!(two.operate(&OP_LT, || provide(&two))?, NodeValue::Bool(false));
        assert_eq!(two.operate(&OP_LT, || provide(&five))?, NodeValue::Bool(true));
        assert_eq!(five.operate(&OP_LT, || provide(&two))?, NodeValue::Bool(false));
        assert!(matches!(
            five.operate(&OP_LT, || provide(&true_val)).err().unwrap(),
            NodeError::ComparisonOperatorIncomptaibleOperands
        ));
        Ok(())
    }

    #[test]
    fn test_operate_relational_lt_or_eq() -> Result<(), NodeError> {
        let two = NodeValue::Int32(2);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(true_val.operate(&OP_LT_OR_EQ, || provide(&true_val))?, NodeValue::Bool(true));
        assert_eq!(two.operate(&OP_LT_OR_EQ, || provide(&two))?, NodeValue::Bool(true));
        assert_eq!(two.operate(&OP_LT_OR_EQ, || provide(&five))?, NodeValue::Bool(true));
        assert_eq!(five.operate(&OP_LT_OR_EQ, || provide(&two))?, NodeValue::Bool(false));
        assert!(matches!(
            five.operate(&OP_LT_OR_EQ, || provide(&true_val)).err().unwrap(),
            NodeError::ComparisonOperatorIncomptaibleOperands
        ));
        Ok(())
    }

    #[test]
    fn test_operate_relational_contains_errs_if_left_operand_not_a_list() {
        let two = NodeValue::Int32(2);

        assert!(matches!(
            two.operate(&OP_CONTAINS, || provide(&two)).err().unwrap(),
            NodeError::ContainsOperatorIncompatibleOperands
        ));
    }

    #[test]
    fn test_operate_relational_contains_errs_if_operands_have_incompatible_types() {
        let bool_list = NodeValue::BoolList(vec![true, true, false]);
        let five = NodeValue::Float(5.5);

        assert!(matches!(
            bool_list.operate(&OP_CONTAINS, || provide(&five)).err().unwrap(),
            NodeError::ContainsOperatorIncompatibleOperands
        ));
    }

    #[test]
    fn test_operate_relational_contains_bool_type_ok() -> Result<(), NodeError> {
        let bool_list = NodeValue::BoolList(vec![true, true, true]);
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        assert_eq!(bool_list.operate(&OP_CONTAINS, || provide(&true_val))?, NodeValue::Bool(true));
        assert_eq!(
            bool_list.operate(&OP_CONTAINS, || provide(&false_val))?,
            NodeValue::Bool(false)
        );
        Ok(())
    }

    #[test]
    fn test_operate_relational_contains_int_type_ok() -> Result<(), NodeError> {
        let int_list = NodeValue::Int32List(vec![3, 30, 300]);
        let zero = NodeValue::Int32(0);
        let three = NodeValue::Int32(3);
        let thirty = NodeValue::Int64(30);
        // not int
        let three_hundred = NodeValue::Double(300.0);

        assert_eq!(int_list.operate(&OP_CONTAINS, || provide(&zero))?, NodeValue::Bool(false));
        assert_eq!(int_list.operate(&OP_CONTAINS, || provide(&three))?, NodeValue::Bool(true));
        assert_eq!(int_list.operate(&OP_CONTAINS, || provide(&thirty))?, NodeValue::Bool(true));
        assert!(matches!(
            int_list.operate(&OP_CONTAINS, || provide(&three_hundred)).err().unwrap(),
            NodeError::ContainsOperatorIncompatibleOperands
        ));
        Ok(())
    }

    #[test]
    fn test_operate_relational_contains_float_type_ok() -> Result<(), NodeError> {
        let float_list = NodeValue::FloatList(vec![0.5, std::f32::consts::PI, 10.0]);
        let point_five = NodeValue::Float(0.5);
        // f64 PI, not f32 PI
        let pi = NodeValue::Double(std::f64::consts::PI);
        // not float but is a numeric type
        let ten = NodeValue::Int32(10);
        let thirty = NodeValue::Float(30.0);

        assert_eq!(
            float_list.operate(&OP_CONTAINS, || provide(&point_five))?,
            NodeValue::Bool(true)
        );
        assert_eq!(float_list.operate(&OP_CONTAINS, || provide(&pi))?, NodeValue::Bool(true));
        assert_eq!(float_list.operate(&OP_CONTAINS, || provide(&ten))?, NodeValue::Bool(true));
        assert_eq!(float_list.operate(&OP_CONTAINS, || provide(&thirty))?, NodeValue::Bool(false));
        Ok(())
    }

    #[test]
    fn test_operate_relational_does_not_contain_errs_if_left_operand_not_a_list() {
        let two = NodeValue::Int32(2);

        assert!(matches!(
            two.operate(&OP_DOES_NOT_CONTAIN, || provide(&two)).err().unwrap(),
            NodeError::ContainsOperatorIncompatibleOperands
        ));
    }

    #[test]
    fn test_operate_relational_does_not_contain_errs_if_operands_have_incompatible_types() {
        let bool_list = NodeValue::BoolList(vec![true, true, false]);
        let five = NodeValue::Float(5.5);

        assert!(matches!(
            bool_list.operate(&OP_DOES_NOT_CONTAIN, || provide(&five)).err().unwrap(),
            NodeError::ContainsOperatorIncompatibleOperands
        ));
    }

    #[test]
    fn test_operate_relational_does_not_contain_bool_type_ok() -> Result<(), NodeError> {
        let bool_list = NodeValue::BoolList(vec![true, true, true]);
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        assert_eq!(
            bool_list.operate(&OP_DOES_NOT_CONTAIN, || provide(&true_val))?,
            NodeValue::Bool(false)
        );
        assert_eq!(
            bool_list.operate(&OP_DOES_NOT_CONTAIN, || provide(&false_val))?,
            NodeValue::Bool(true)
        );
        Ok(())
    }

    #[test]
    fn test_operate_relational_does_not_contain_int_type_ok() -> Result<(), NodeError> {
        let int_list = NodeValue::Int32List(vec![3, 30, 300]);
        let zero = NodeValue::Int32(0);
        let three = NodeValue::Int32(3);
        let thirty = NodeValue::Int64(30);
        // not int
        let three_hundred = NodeValue::Double(300.0);

        assert_eq!(
            int_list.operate(&OP_DOES_NOT_CONTAIN, || provide(&zero))?,
            NodeValue::Bool(true)
        );
        assert_eq!(
            int_list.operate(&OP_DOES_NOT_CONTAIN, || provide(&three))?,
            NodeValue::Bool(false)
        );
        assert_eq!(
            int_list.operate(&OP_DOES_NOT_CONTAIN, || provide(&thirty))?,
            NodeValue::Bool(false)
        );
        assert!(matches!(
            int_list.operate(&OP_DOES_NOT_CONTAIN, || provide(&three_hundred)).err().unwrap(),
            NodeError::ContainsOperatorIncompatibleOperands
        ));
        Ok(())
    }

    #[test]
    fn test_operate_relational_does_not_contain_float_type_ok() -> Result<(), NodeError> {
        let float_list = NodeValue::FloatList(vec![0.5, std::f32::consts::PI, 10.0]);
        let point_five = NodeValue::Float(0.5);
        // f64 PI, not f32 PI
        let pi = NodeValue::Double(std::f64::consts::PI);
        // not float but is a numeric type
        let ten = NodeValue::Int32(10);
        let thirty = NodeValue::Float(30.0);

        assert_eq!(
            float_list.operate(&OP_DOES_NOT_CONTAIN, || provide(&point_five))?,
            NodeValue::Bool(false)
        );
        assert_eq!(
            float_list.operate(&OP_DOES_NOT_CONTAIN, || provide(&pi))?,
            NodeValue::Bool(false)
        );
        assert_eq!(
            float_list.operate(&OP_DOES_NOT_CONTAIN, || provide(&ten))?,
            NodeValue::Bool(false)
        );
        assert_eq!(
            float_list.operate(&OP_DOES_NOT_CONTAIN, || provide(&thirty))?,
            NodeValue::Bool(true)
        );
        Ok(())
    }

    #[test]
    fn test_operate_relational_all_eq_errs_if_left_operand_not_a_list() {
        let two = NodeValue::Int32(2);

        assert!(matches!(
            two.operate(&OP_ALL_EQ, || provide(&two)).err().unwrap(),
            NodeError::AllEqualsOperatorIncompatibleOperands
        ));
    }

    #[test]
    fn test_operate_relational_all_eq_errs_if_operands_have_incompatible_types() {
        let bool_list = NodeValue::BoolList(vec![true, true, false]);
        let five = NodeValue::Float(5.5);

        assert!(matches!(
            bool_list.operate(&OP_ALL_EQ, || provide(&five)).err().unwrap(),
            NodeError::AllEqualsOperatorIncompatibleOperands
        ));
    }

    #[test]
    fn test_operate_relational_all_eq_bool_type_ok() -> Result<(), NodeError> {
        let bool_list_1 = NodeValue::BoolList(vec![true, true, true]);
        let bool_list_2 = NodeValue::BoolList(vec![true, false, false]);
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        assert_eq!(bool_list_1.operate(&OP_ALL_EQ, || provide(&true_val))?, NodeValue::Bool(true));
        assert_eq!(
            bool_list_1.operate(&OP_ALL_EQ, || provide(&false_val))?,
            NodeValue::Bool(false)
        );
        assert_eq!(bool_list_2.operate(&OP_ALL_EQ, || provide(&true_val))?, NodeValue::Bool(false));
        assert_eq!(
            bool_list_2.operate(&OP_ALL_EQ, || provide(&false_val))?,
            NodeValue::Bool(false)
        );
        Ok(())
    }

    #[test]
    fn test_operate_relational_all_eq_int_type_ok() -> Result<(), NodeError> {
        let int_list_1 = NodeValue::Int64List(vec![3, 30, 300]);
        let int_list_2 = NodeValue::Int64List(vec![0, 0]);
        let zero = NodeValue::Int32(0);

        assert_eq!(int_list_1.operate(&OP_ALL_EQ, || provide(&zero))?, NodeValue::Bool(false));
        assert_eq!(int_list_2.operate(&OP_ALL_EQ, || provide(&zero))?, NodeValue::Bool(true));
        Ok(())
    }

    #[test]
    fn test_operate_relational_all_eq_float_type_ok() -> Result<(), NodeError> {
        let float_list_1 = NodeValue::FloatList(vec![0.5, std::f32::consts::PI, 10.0]);
        let float_list_2 = NodeValue::FloatList(vec![0.5, 0.5]);

        let point_five = NodeValue::Double(0.5);

        assert_eq!(
            float_list_1.operate(&OP_ALL_EQ, || provide(&point_five))?,
            NodeValue::Bool(false)
        );
        assert_eq!(
            float_list_2.operate(&OP_ALL_EQ, || provide(&point_five))?,
            NodeValue::Bool(true)
        );
        Ok(())
    }
}
