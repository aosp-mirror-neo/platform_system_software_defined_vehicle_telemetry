// Copyright 2023 Google LLC

//! Wrapper around a generic value.
//! Should be able to hold all the values in a ConstantLeafNode protobuf
//! message, as well as all the values that a FieldLeafNode can select.

use protobuf::reflect::{ReflectEqMode, ReflectFieldRef, ReflectValueRef};
use protobuf::MessageDyn;
use sdv_telemetry_proto_rust::expression::combination_node::{
    ArithmeticOperator, LogicalOperator, Operator as OperatorProto, RelationalOperator,
    RoundingOperator,
};
use sdv_telemetry_proto_rust::expression::constant_leaf_node::Node_value as NodeValueProto;
use sdv_telemetry_proto_rust::expression::ConstantLeafNode as ConstantLeafNodeProto;
use sdv_telemetry_utils::operation_result::OperationResult;
use std::{borrow::Cow, convert::From, sync::Arc};

use crate::expr::node_error::NodeEvaluationError;
use crate::expr::node_evaluation_state::{NodeEvaluationResult, NodeEvaluationState};
use crate::expr::node_evaluation_state_ext::WithNodeEvaluationState;
use crate::expr::proto_node_value_converter::protobuf_reflection_to_node_value::MessageDescriptorExt;

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
    Message(Arc<dyn MessageDyn>),

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

    /// Repeated protobuf messages.
    MessageList(Vec<Arc<dyn MessageDyn>>),

    /// A string node value.
    String(String),

    /// A byte array node value.
    ByteList(Vec<u8>),
}

/// Check if two floating point values are approximately equal.
fn approximately_equal(v1: f64, v2: f64) -> bool {
    const EPSILON: f32 = 1e-6;
    (v1 - v2).abs() < EPSILON.into()
}

/// The PartialEq trait allows comparing to NodeValue's accurately, including
/// for floating point values.
impl PartialEq for NodeValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (NodeValue::None, NodeValue::None) => true,
            (NodeValue::Bool(v1), NodeValue::Bool(v2)) => *v1 == *v2,
            (NodeValue::String(v1), NodeValue::String(v2)) => *v1 == *v2,
            (NodeValue::Int32(v1), NodeValue::Int32(v2)) => *v1 == *v2,
            (NodeValue::Int64(v1), NodeValue::Int64(v2)) => *v1 == *v2,
            (NodeValue::Float(v1), NodeValue::Float(v2)) => {
                approximately_equal(f64::from(*v1), f64::from(*v2))
            }
            (NodeValue::Double(v1), NodeValue::Double(v2)) => approximately_equal(*v1, *v2),
            (NodeValue::Message(v1), NodeValue::Message(v2)) => {
                v1.reflect_eq_dyn(v2.as_ref(), &ReflectEqMode::default())
            }
            (NodeValue::BoolList(v1), NodeValue::BoolList(v2)) => *v1 == *v2,
            (NodeValue::Int32List(v1), NodeValue::Int32List(v2)) => *v1 == *v2,
            (NodeValue::Int64List(v1), NodeValue::Int64List(v2)) => *v1 == *v2,
            (NodeValue::FloatList(v1), NodeValue::FloatList(v2)) => {
                v1.len() == v2.len()
                    && v1
                        .iter()
                        .zip(v2)
                        .all(|(a, b)| approximately_equal(f64::from(*a), f64::from(*b)))
            }
            (NodeValue::DoubleList(v1), NodeValue::DoubleList(v2)) => {
                v1.len() == v2.len() && v1.iter().zip(v2).all(|(a, b)| approximately_equal(*a, *b))
            }
            (NodeValue::MessageList(v1), NodeValue::MessageList(v2)) => {
                let mode = ReflectEqMode::default();
                v1.len() == v2.len()
                    && v1.iter().zip(v2).all(|(a, b)| a.reflect_eq_dyn(b.as_ref(), &mode))
            }
            (NodeValue::ByteList(v1), NodeValue::ByteList(v2)) => *v1 == *v2,
            _ => false,
        }
    }
}

impl NodeValue {
    /// Performs operation synchronously using the operator and synchronous right operand provider.
    /// This call will short-circuit and skip evaluating the right branch if the
    /// left operand uniquely determines the result.
    ///
    /// # Arguments:
    ///
    /// * `self` - The left operand.
    /// * `op` - The operator.
    ///          If unary operator, then operates on self.
    ///          If binary operator, then [self] <operator> [right].
    /// * `get_right_operand` - The function that provides the right operand when called.
    pub fn operate_sync(
        self,
        op: &OperatorProto,
        get_right_operand: impl FnOnce() -> Result<NodeValue, NodeEvaluationError> + Send,
    ) -> Result<NodeValue, NodeEvaluationError> {
        self.operate(op, NodeEvaluationState::default(), move |state| {
            OperationResult::Synchronous(get_right_operand().with_state(state))
        })
        .unwrap_sync()
        .map(|(value, _state)| value)
    }

    /// Returns the bool value or `None` if `Self` is not a bool.
    pub fn get_bool(&self) -> Option<bool> {
        if let NodeValue::Bool(val) = self {
            Some(*val)
        } else {
            None
        }
    }

    /// Returns the int value or `None` if `Self` is not an int.
    pub fn get_int(&self) -> Option<i64> {
        Some(match self {
            NodeValue::Int32(val) => i64::from(*val),
            NodeValue::Int64(val) => *val,
            _ => return None,
        })
    }

    /// Returns the double value or `None` if `Self` is not numeric.
    pub fn get_double(&self) -> Option<f64> {
        Some(match self {
            NodeValue::Int32(val) => f64::from(*val),
            NodeValue::Int64(val) => *val as f64,
            NodeValue::Float(val) => f64::from(*val),
            NodeValue::Double(val) => *val,
            _ => return None,
        })
    }

    /// Returns the Message value or `None` if `Self` is not a Message.
    pub fn get_message(&self) -> Option<&Arc<dyn MessageDyn>> {
        if let NodeValue::Message(msg) = self {
            Some(msg)
        } else {
            None
        }
    }

    /// Returns the string value or `None` if `Self` is not a string.
    pub fn get_string(&self) -> Option<&str> {
        if let NodeValue::String(val) = self {
            Some(val)
        } else {
            None
        }
    }

    /// Returns the bytes value or `None` if `Self` is not bytes.
    pub fn get_bytes(&self) -> Option<&[u8]> {
        if let NodeValue::ByteList(val) = self {
            Some(val)
        } else {
            None
        }
    }

    /// Check if node value is none.
    pub fn is_none(&self) -> bool {
        matches!(self, NodeValue::None)
    }

    /// Check if node holds a numeric value.
    pub fn is_numeric(&self) -> bool {
        self.get_double().is_some()
    }

    /// Check if node holds a list.
    fn is_list(&self) -> bool {
        matches!(
            self,
            NodeValue::BoolList(_)
                | NodeValue::Int32List(_)
                | NodeValue::Int64List(_)
                | NodeValue::FloatList(_)
                | NodeValue::DoubleList(_)
                | NodeValue::MessageList(_)
        )
    }

    /// Checks whether the node value is zero.
    pub fn is_zero(&self) -> Result<bool, NodeEvaluationError> {
        Ok(match self {
            NodeValue::Int32(val) => *val == 0,
            NodeValue::Int64(val) => *val == 0,
            NodeValue::Float(val) => approximately_equal(f64::from(*val), 0.0),
            NodeValue::Double(val) => approximately_equal(*val, 0.0),
            _ => {
                return Err(NodeEvaluationError::InternalError {
                    msg: format!("is_zero() invoked on non-numeric NodeValue: {:?}", self),
                })
            }
        })
    }

    /// Converts a ConstantLeafNode into a NodeValue.
    pub(super) fn from_constant_leaf_node(
        constant_leaf_node: &ConstantLeafNodeProto,
    ) -> Result<NodeValue, NodeEvaluationError> {
        Ok(match &constant_leaf_node.node_value {
            Some(NodeValueProto::BoolValue(val)) => NodeValue::Bool(*val),
            Some(NodeValueProto::Int32Value(val)) => NodeValue::Int32(*val),
            Some(NodeValueProto::Int64Value(val)) => NodeValue::Int64(*val),
            Some(NodeValueProto::FloatValue(val)) => NodeValue::Float(*val),
            Some(NodeValueProto::DoubleValue(val)) => NodeValue::Double(*val),
            Some(node_value) => {
                return Err(NodeEvaluationError::ConstantValueNotSupported {
                    node_value: node_value.clone(),
                })
            }
            None => return Err(NodeEvaluationError::ConstantValueNotSet),
        })
    }

    /// Gets a NodeValue from a protobuf message and its fields.
    pub(super) fn extract_value(
        msg: Cow<Arc<dyn MessageDyn>>,
        fields: &[String],
    ) -> Result<NodeValue, NodeEvaluationError> {
        // if no field names, return the message itself as the NodeValue
        if fields.is_empty() {
            return Ok(NodeValue::Message(msg.into_owned()));
        }
        // recursively gets the field value from field names
        Self::extract_field_value(msg.as_ref().as_ref(), fields)
    }

    pub(super) fn into_operation_result(
        self,
        state: NodeEvaluationState,
    ) -> OperationResult<'static, NodeEvaluationResult, NodeEvaluationError> {
        Ok((self, state)).into()
    }

    /// Performs operation using the operator and right operand provider.
    /// This call will short-circuit and skip evaluating the right branch if the
    /// left operand uniquely determines the result.
    ///
    /// # Arguments:
    ///
    /// * `self` - The left operand.
    /// * `op` - The operator.
    ///          If unary operator, then operates on self.
    ///          If binary operator, then [self] <operator> [right].
    /// * `right_operand_provider` - The function that provides the right operand when called.
    pub(super) fn operate<'a>(
        self,
        op: &OperatorProto,
        state: NodeEvaluationState,
        right_operand_provider: impl FnOnce(
                NodeEvaluationState,
            ) -> OperationResult<'a, NodeEvaluationResult, NodeEvaluationError>
            + Send,
    ) -> OperationResult<'a, NodeEvaluationResult, NodeEvaluationError> {
        // None occurs when message is missing or incomplete, and None propagates up.
        if self.is_none() {
            return NodeValue::None.into_operation_result(state);
        }

        match op {
            OperatorProto::ArithmeticOperator(op) => {
                let op = match op.enum_value() {
                    Ok(value) => value,
                    Err(i) => {
                        return Err(NodeEvaluationError::UnknownOperator {
                            op: format!("Arithmetic operator with enum value {}", i),
                        })
                        .into()
                    }
                };

                self.handle_arithmetic(&op, state, right_operand_provider)
            }
            OperatorProto::LogicalOperator(op) => {
                let op = match op.enum_value() {
                    Ok(value) => value,
                    Err(i) => {
                        return Err(NodeEvaluationError::UnknownOperator {
                            op: format!("Logical operator with enum value {}", i),
                        })
                        .into()
                    }
                };

                self.handle_logical(&op, state, right_operand_provider)
            }
            OperatorProto::RelationalOperator(op) => {
                let op = match op.enum_value() {
                    Ok(value) => value,
                    Err(i) => {
                        return Err(NodeEvaluationError::UnknownOperator {
                            op: format!("Relational operator with enum value {}", i),
                        })
                        .into()
                    }
                };

                self.handle_binary(state, right_operand_provider, move |left, right| {
                    left.handle_relational(&op, right)
                })
            }
            OperatorProto::RoundingOperator(op) => self.handle_unary(state, |value| {
                let op = match op.enum_value() {
                    Ok(value) => value,
                    Err(i) => {
                        return Err(NodeEvaluationError::UnknownOperator {
                            op: format!("Rounding operator with enum value {}", i),
                        })
                    }
                };

                value.handle_rounding(&op)
            }),
            _ => Err(NodeEvaluationError::UnknownOperator { op: format!("{:?}", op) }).into(),
        }
    }

    // private functions and methods below

    /// Reads a field value from a protobuf message using the reflection.
    fn extract_field_value(
        msg: &dyn MessageDyn,
        fields: &[String],
    ) -> Result<NodeValue, NodeEvaluationError> {
        let msg_descriptor = msg.descriptor_dyn();
        let field_name = &fields[0];
        let field_descriptor =
            msg_descriptor.field_by_name(field_name).ok_or(NodeEvaluationError::FieldNotFound {
                field_name: field_name.clone(),
                message_name: msg_descriptor.name().to_string(),
            })?;

        // Base case for recursion: one field is provided, simply read the value
        if fields.len() == 1 {
            return msg_descriptor.get_field_value(&field_descriptor, msg);
        }

        // If fields length > 1, get nested field values iff the value is a single protobuf message,
        // otherwise return error
        if let ReflectFieldRef::Optional(singular_field) = field_descriptor.get_reflect(msg) {
            match singular_field.value() {
                Some(nested_value) => {
                    if let ReflectValueRef::Message(msg) = nested_value {
                        // recursively read from nested messages
                        Self::extract_field_value(&(*msg), &fields[1..])
                    } else {
                        Err(NodeEvaluationError::WrongNestedFieldType {
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
            Err(NodeEvaluationError::WrongNestedFieldType {
                field_name: field_name.clone(),
                e: "A non-terminal field is either repeated or a map".to_string(),
            })
        }
    }

    /// Handle arithmetic operations.
    fn handle_arithmetic<'a>(
        self,
        op: &ArithmeticOperator,
        state: NodeEvaluationState,
        right_operand_provider: impl FnOnce(
                NodeEvaluationState,
            ) -> OperationResult<'a, NodeEvaluationResult, NodeEvaluationError>
            + Send,
    ) -> OperationResult<'a, NodeEvaluationResult, NodeEvaluationError> {
        let err_provider = || NodeEvaluationError::ArithmeticOperatorIncompatibleOperands;

        match op {
            ArithmeticOperator::ABSOLUTE => self.handle_unary(state, |value| {
                value
                    .apply_numeric_unary(
                        |a| NodeValue::Int64(a.abs()),
                        |a| NodeValue::Double(a.abs()),
                    )
                    .ok_or_else(err_provider)
            }),
            ArithmeticOperator::ADD => {
                self.handle_binary(state, right_operand_provider, move |left, right| {
                    left.apply_numeric_binary(
                        right,
                        |a, b| NodeValue::Int64(a + b),
                        |a, b| NodeValue::Double(a + b),
                    )
                    .ok_or_else(err_provider)
                })
            }
            ArithmeticOperator::DIVIDE => {
                self.handle_binary(state, right_operand_provider, move |left, right| {
                    let left_double = left.get_double().ok_or_else(err_provider)?;
                    let right_double = right.get_double().ok_or_else(err_provider)?;

                    if approximately_equal(right_double, 0.0) {
                        return Err(NodeEvaluationError::DivideByZero);
                    }

                    Ok(NodeValue::Double(left_double / right_double))
                })
            }
            ArithmeticOperator::MODULO_TRUNC => {
                self.handle_binary(state, right_operand_provider, |left, right| {
                    // First, verify that both operands have the corrected data
                    // types, and only then perform further checks.
                    let err_provider = || NodeEvaluationError::ModuloOperatorIncompatibleOperands;
                    let dividend = left.get_int().ok_or_else(err_provider)?;
                    let divisor = right.get_int().ok_or_else(err_provider)?;

                    if divisor == 0 {
                        return Err(NodeEvaluationError::DivideByZero);
                    }

                    Ok(NodeValue::Int64(dividend % divisor))
                })
            }
            ArithmeticOperator::MULTIPLY => {
                self.handle_binary(state, right_operand_provider, move |left, right| {
                    left.apply_numeric_binary(
                        right,
                        |a, b| NodeValue::Int64(a * b),
                        |a, b| NodeValue::Double(a * b),
                    )
                    .ok_or_else(err_provider)
                })
            }
            ArithmeticOperator::POWER => {
                self.handle_binary(state, right_operand_provider, move |left, right| {
                    left.apply_numeric_binary(
                        right,
                        |a: i64, b: i64| {
                            if let Ok(b) = b.try_into() {
                                // If `b` is positive and fits into `u32`, calculate the power as an
                                // integer (i64::pow requires a `u32` exponent).
                                NodeValue::Int64(a.pow(b))
                            } else {
                                // Otherwise, fall back to using floats to calculate the power.
                                NodeValue::Double((a as f64).powf(b as f64))
                            }
                        },
                        |a, b| NodeValue::Double(a.powf(b)),
                    )
                    .ok_or_else(err_provider)
                })
            }
            ArithmeticOperator::SUBTRACT => {
                self.handle_binary(state, right_operand_provider, move |left, right| {
                    left.apply_numeric_binary(
                        right,
                        |a, b| NodeValue::Int64(a - b),
                        |a, b| NodeValue::Double(a - b),
                    )
                    .ok_or_else(err_provider)
                })
            }
            ArithmeticOperator::UNARY_MINUS => self.handle_unary(state, |value| {
                value
                    .apply_numeric_unary(|a| NodeValue::Int64(-a), |a| NodeValue::Double(-a))
                    .ok_or_else(err_provider)
            }),
        }
    }

    /// Handle logical operations.
    fn handle_logical<'a>(
        self,
        op: &LogicalOperator,
        state: NodeEvaluationState,
        right_operand_provider: impl FnOnce(
                NodeEvaluationState,
            ) -> OperationResult<'a, NodeEvaluationResult, NodeEvaluationError>
            + Send,
    ) -> OperationResult<'a, NodeEvaluationResult, NodeEvaluationError> {
        let err_provider = || NodeEvaluationError::LogicalOperatorIncompatibleOperands;

        let Some(left_value) = self.get_bool() else { return Err(err_provider()).into() };

        match op {
            LogicalOperator::AND => {
                if !left_value {
                    // Short-circuit and don't evaluate right operand if the left operand is false.
                    NodeValue::Bool(false).into_operation_result(state)
                } else {
                    // If left is true, the result depends on the right operand.
                    self.handle_binary(state, right_operand_provider, move |_left, right| {
                        Ok(NodeValue::Bool(right.get_bool().ok_or_else(err_provider)?))
                    })
                }
            }
            LogicalOperator::OR => {
                if left_value {
                    // Short-circuit and don't evaluate right operand if the left operand is true.
                    NodeValue::Bool(true).into_operation_result(state)
                } else {
                    // If left is false, the result depends on the right operand.
                    self.handle_binary(state, right_operand_provider, move |_left, right| {
                        Ok(NodeValue::Bool(right.get_bool().ok_or_else(err_provider)?))
                    })
                }
            }
            LogicalOperator::XOR => {
                self.handle_binary(state, right_operand_provider, move |_left, right| {
                    Ok(NodeValue::Bool(left_value != right.get_bool().ok_or_else(err_provider)?))
                })
            }
            LogicalOperator::NOT => NodeValue::Bool(!left_value).into_operation_result(state),
        }
    }

    /// Handle relational operations.
    fn handle_relational(
        &self,
        op: &RelationalOperator,
        other: &NodeValue,
    ) -> Result<NodeValue, NodeEvaluationError> {
        // None occurs when message is missing or incomplete, and None propagates up.
        if other.is_none() {
            return Ok(NodeValue::None);
        }
        let err_provider = || NodeEvaluationError::ComparisonOperatorIncomptaibleOperands;
        Ok(NodeValue::Bool(match op {
            RelationalOperator::EQ => {
                if self.is_list() {
                    return Ok(NodeValue::Bool(self.all_equals(other)?));
                }
                if !self.is_numeric() || !other.is_numeric() {
                    return Ok(NodeValue::Bool(self == other));
                }
                self.apply_numeric_binary(other, |a, b| a == b, approximately_equal)
                    .ok_or_else(err_provider)
            }
            RelationalOperator::NOT_EQ => {
                if !self.is_numeric() || !other.is_numeric() {
                    return Ok(NodeValue::Bool(self != other));
                }
                self.apply_numeric_binary(other, |a, b| a != b, |a, b| !approximately_equal(a, b))
                    .ok_or_else(err_provider)
            }
            RelationalOperator::GT => self
                .apply_generic_operation(
                    other,
                    |a, b| a & !b,
                    |a, b| a > b,
                    |a, b| a > b && !approximately_equal(a, b),
                )
                .ok_or_else(err_provider),
            RelationalOperator::GT_OR_EQ => self
                .apply_generic_operation(
                    other,
                    |a, b| a >= b,
                    |a, b| a >= b,
                    |a, b| a >= b || approximately_equal(a, b),
                )
                .ok_or_else(err_provider),
            RelationalOperator::LT => self
                .apply_generic_operation(
                    other,
                    |a, b| !a & b,
                    |a, b| a < b,
                    |a, b| a < b && !approximately_equal(a, b),
                )
                .ok_or_else(err_provider),
            RelationalOperator::LT_OR_EQ => self
                .apply_generic_operation(
                    other,
                    |a, b| a <= b,
                    |a, b| a <= b,
                    |a, b| a <= b || approximately_equal(a, b),
                )
                .ok_or_else(err_provider),
            RelationalOperator::CONTAINS => self.contains(other),
            RelationalOperator::DOES_NOT_CONTAIN => {
                // negate the boolean in a Result<bool, E>
                self.contains(other).map(|x| !x)
            }
            RelationalOperator::ALL_EQ => {
                panic!("ALL_EQ operator usage: this expression should have been skipped after validation");
            }
        }?))
    }

    /// Handle rounding operations.
    fn handle_rounding(&self, op: &RoundingOperator) -> Result<NodeValue, NodeEvaluationError> {
        let err_provider = || NodeEvaluationError::RoundingOperatorIncompatibleOperands;
        if !matches!(self, NodeValue::Float(_) | NodeValue::Double(_)) {
            return Err(err_provider());
        }
        let value = self.get_double().ok_or_else(err_provider)?;
        Ok(NodeValue::Int64(match op {
            RoundingOperator::FLOOR => value.floor() as i64,
            RoundingOperator::ROUND => value.round() as i64,
            RoundingOperator::CEIL => value.ceil() as i64,
        }))
    }

    /// Handles unary operation on self using provided node value handler.
    fn handle_unary(
        &self,
        state: NodeEvaluationState,
        handler: impl FnOnce(&NodeValue) -> Result<NodeValue, NodeEvaluationError>,
    ) -> OperationResult<'static, NodeEvaluationResult, NodeEvaluationError> {
        handler(self).with_state(state).into()
    }

    /// Handles binary operation on self using provided node value handler.
    fn handle_binary<'a>(
        self,
        state: NodeEvaluationState,
        right_operand_provider: impl FnOnce(
                NodeEvaluationState,
            ) -> OperationResult<'a, NodeEvaluationResult, NodeEvaluationError>
            + Send,
        handler: impl FnOnce(&NodeValue, &NodeValue) -> Result<NodeValue, NodeEvaluationError>
            + Send
            + 'a,
    ) -> OperationResult<'a, NodeEvaluationResult, NodeEvaluationError> {
        right_operand_provider(state).and_then_sync(move |(right, state)| {
            if right.is_none() {
                // None occurs when message is missing or incomplete, and None propagates up.
                return Ok((NodeValue::None, state));
            }

            handler(&self, &right).with_state(state)
        })
    }

    /// Check if list contains some element.
    /// Returns error if `self` is not a list value or `element` is not a compatible type.
    fn contains(&self, element: &NodeValue) -> Result<bool, NodeEvaluationError> {
        let err_provider = || NodeEvaluationError::ContainsOperatorIncompatibleOperands;
        Ok(match self {
            NodeValue::BoolList(v) => v.contains(&element.get_bool().ok_or_else(err_provider)?),
            NodeValue::Int32List(v) => {
                let target_value = element.get_int().ok_or_else(err_provider)? as i32;
                v.contains(&target_value)
            }
            NodeValue::Int64List(v) => v.contains(&element.get_int().ok_or_else(err_provider)?),
            NodeValue::FloatList(v) => {
                let target_value = element.get_double().ok_or_else(err_provider)?;
                v.iter().any(|x| approximately_equal(f64::from(*x), target_value))
            }
            NodeValue::DoubleList(v) => {
                let target_value = element.get_double().ok_or_else(err_provider)?;
                v.iter().any(|x| approximately_equal(*x, target_value))
            }
            NodeValue::MessageList(v) => {
                let target_value = element.get_message().ok_or_else(err_provider)?;
                let mode = ReflectEqMode::default();
                v.iter().any(|x| x.reflect_eq_dyn(target_value.as_ref(), &mode))
            }
            _ => return Err(err_provider()),
        })
    }

    /// Check if all list items are equal to some element.
    /// Returns errors if `self` is not a list value or `element` is not a compatible type.
    fn all_equals(&self, element: &NodeValue) -> Result<bool, NodeEvaluationError> {
        let err_provider = || NodeEvaluationError::ComparisonOperatorListIncompatibleOperands;
        Ok(match self {
            NodeValue::BoolList(v) => {
                let target_value = element.get_bool().ok_or_else(err_provider)?;
                v.iter().all(|x| *x == target_value)
            }
            NodeValue::Int32List(v) => {
                let target_value = element.get_int().ok_or_else(err_provider)? as i32;
                v.iter().all(|x| *x == target_value)
            }
            NodeValue::Int64List(v) => {
                let target_value = element.get_int().ok_or_else(err_provider)?;
                v.iter().all(|x| *x == target_value)
            }
            NodeValue::FloatList(v) => {
                let target_value = element.get_double().ok_or_else(err_provider)?;
                v.iter().all(|x| approximately_equal(f64::from(*x), target_value))
            }
            NodeValue::DoubleList(v) => {
                let target_value = element.get_double().ok_or_else(err_provider)?;
                v.iter().all(|x| approximately_equal(*x, target_value))
            }
            NodeValue::MessageList(v) => {
                let target_value = element.get_message().ok_or_else(err_provider)?;
                let mode = ReflectEqMode::default();
                v.iter().all(|x| x.reflect_eq_dyn(target_value.as_ref(), &mode))
            }
            _ => return Err(err_provider()),
        })
    }

    /// Applies the provided function on two numeric NodeValues based on
    /// the numeric type (integer or floating point).
    /// Returns an error if the NodeValues are not numeric types.
    fn apply_numeric_binary<R>(
        &self,
        other: &NodeValue,
        int_func: impl FnOnce(i64, i64) -> R,
        float_func: impl FnOnce(f64, f64) -> R,
    ) -> Option<R> {
        if let (Some(l), Some(r)) = (self.get_int(), other.get_int()) {
            Some(int_func(l, r))
        } else if let (Some(l), Some(r)) = (self.get_double(), other.get_double()) {
            Some(float_func(l, r))
        } else {
            None
        }
    }

    /// Applies the provided function on a numeric NodeValue based on
    /// the numeric type (integer or floating point).
    /// Returns an error if the NodeValues are not numeric types.
    fn apply_numeric_unary<R>(
        &self,
        int_func: impl FnOnce(i64) -> R,
        float_func: impl FnOnce(f64) -> R,
    ) -> Option<R> {
        self.get_int().map(int_func).or_else(|| self.get_double().map(float_func))
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
    ) -> Option<R> {
        if let (Some(l), Some(r)) = (self.get_bool(), other.get_bool()) {
            Some(bool_func(l, r))
        } else {
            self.apply_numeric_binary(other, int_func, float_func)
        }
    }
}

impl From<i32> for NodeValue {
    fn from(item: i32) -> Self {
        Self::Int32(item)
    }
}

impl From<i64> for NodeValue {
    fn from(item: i64) -> Self {
        Self::Int64(item)
    }
}

impl From<f32> for NodeValue {
    fn from(item: f32) -> Self {
        Self::Float(item)
    }
}

impl From<f64> for NodeValue {
    fn from(item: f64) -> Self {
        Self::Double(item)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::assert_that;
    use googletest::matchers::{eq, err, ok, pat};
    use protobuf::reflect::ReflectEqMode;
    use sdv_telemetry_proto_rust::expression::ConstantLeafNode;
    use sdv_telemetry_test_proto::test_message::{TestEnum, TestMessage};
    use sdv_telemetry_test_utils::{msg_dyn_eq, parameterized_test};
    use std::f32::consts::PI as PIf32;
    use std::f64::consts::PI as PIf64;

    use crate::expr::mock::*;

    const ONE: NodeValue = NodeValue::Double(1.0);
    const ONE_EPSILON: NodeValue = NodeValue::Double(1.0 + f64::EPSILON);

    fn unary_right_operand_provider() -> Result<NodeValue, NodeEvaluationError> {
        panic!("Unary operator should not call right operand provider");
    }

    #[test]
    fn one_and_one_epsilon_are_valid() {
        assert_ne!(
            ONE.get_double().unwrap(),
            ONE_EPSILON.get_double().unwrap(),
            "These values must be very close but not equal so that the following tests can verify the correct handling of approximately equal values."
        );
    }

    #[test]
    fn from_constant_leaf_node() {
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
            NodeValue::from_constant_leaf_node(&constant_leaf_node_bool).unwrap(),
            NodeValue::Bool(true)
        );
        assert_eq!(
            NodeValue::from_constant_leaf_node(&constant_leaf_node_i32).unwrap(),
            NodeValue::Int32(32)
        );
        assert_eq!(
            NodeValue::from_constant_leaf_node(&constant_leaf_node_i64).unwrap(),
            NodeValue::Int64(64)
        );
        assert_eq!(
            NodeValue::from_constant_leaf_node(&constant_leaf_node_float).unwrap(),
            NodeValue::Float(1.5)
        );
        assert_eq!(
            NodeValue::from_constant_leaf_node(&constant_leaf_node_double).unwrap(),
            NodeValue::Double(1.5)
        );
    }

    #[test]
    fn extract_value_no_field_names_ok() {
        // setup
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;

        // action
        let node_value = NodeValue::extract_value(Cow::Borrowed(&msg), &[]).unwrap();

        // assert
        assert_that!(
            node_value,
            pat!(NodeValue::Message(msg_dyn_eq(msg.as_ref(), &ReflectEqMode::default())))
        );
    }

    #[test]
    fn extract_value_explicitly_optional_field_not_set_returns_none() {
        let msg = Arc::new(TestMessage::new()) as Arc<dyn MessageDyn>;

        assert_eq!(
            NodeValue::extract_value(Cow::Borrowed(&msg), &["optional_bool_field".to_string()])
                .unwrap(),
            NodeValue::None
        );
        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &["optional_int64_field".to_string()])
                .unwrap(),
            NodeValue::None
        );
    }

    #[test]
    fn extract_value_primitive_field_not_set_uses_default_values() {
        let msg = Arc::new(TestMessage::new()) as Arc<dyn MessageDyn>;

        assert_eq!(
            NodeValue::extract_value(Cow::Borrowed(&msg), &["int_field".to_string()]).unwrap(),
            NodeValue::Int32(0)
        );
        assert_eq!(
            NodeValue::extract_value(Cow::Borrowed(&msg), &["float_field".to_string()]).unwrap(),
            NodeValue::Float(0.0)
        );
        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &["enum_field".to_string()]).unwrap(),
            NodeValue::Int32(0)
        );
    }

    #[test]
    fn extract_value_message_field_not_set_returns_none() {
        let msg = Arc::new(TestMessage::new()) as Arc<dyn MessageDyn>;

        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &["message_field".to_string()]).unwrap(),
            NodeValue::None
        );
    }

    #[test]
    fn extract_value_repeated_field_not_set_returns_empty_list() {
        let msg = Arc::new(TestMessage::new()) as Arc<dyn MessageDyn>;

        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &["repeated_bools".to_string()]).unwrap(),
            NodeValue::BoolList(vec![])
        );
    }

    #[test]
    fn extract_value_int_field_ok() {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["int_field".to_string()];

        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &field_names).unwrap(),
            NodeValue::Int32(80)
        );
    }

    #[test]
    fn extract_value_float_field_ok() {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["float_field".to_string()];

        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &field_names).unwrap(),
            NodeValue::Float(100.0)
        );
    }

    #[test]
    fn extract_value_enum_field_ok() {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["enum_field".to_string()];

        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &field_names).unwrap(),
            NodeValue::Int32(1)
        );
    }

    #[test]
    fn extract_value_message_field_ok() {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["message_field".to_string()];
        let expected_value =
            NodeValue::Message(Arc::new(TEST_MESSAGE.message_field.clone().unwrap()));

        // action
        let node_value = NodeValue::extract_value(Cow::Owned(msg), &field_names).unwrap();

        // assert
        assert_eq!(node_value, expected_value);
    }

    #[test]
    fn extract_value_string_field_ok() {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["string_field".to_string()];

        let extracted = NodeValue::extract_value(Cow::Owned(msg), &field_names).unwrap();

        assert_eq!(extracted, NodeValue::String("Hello world".to_string()));
    }

    #[test]
    fn extract_value_bytes_field_ok() {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["bytes_field".to_string()];

        let extracted = NodeValue::extract_value(Cow::Owned(msg), &field_names).unwrap();

        assert_eq!(extracted, NodeValue::ByteList("Hello world".as_bytes().to_vec()));
    }

    #[test]
    fn extract_value_field_has_default_value_ok() {
        let mut msg = TestMessage::new();
        msg.int_field = 0;
        msg.float_field = 0.0;
        msg.enum_field = TestEnum::NONE.into(); // this enum is equal to int 0
        msg.optional_bool_field = Some(false);
        let msg = Arc::new(msg) as Arc<dyn MessageDyn>;

        assert_eq!(
            NodeValue::extract_value(Cow::Borrowed(&msg), &["int_field".to_string()]).unwrap(),
            NodeValue::Int32(0)
        );
        assert_eq!(
            NodeValue::extract_value(Cow::Borrowed(&msg), &["float_field".to_string()]).unwrap(),
            NodeValue::Float(0.0)
        );
        assert_eq!(
            NodeValue::extract_value(Cow::Borrowed(&msg), &["enum_field".to_string()]).unwrap(),
            NodeValue::Int32(0)
        );
        assert_eq!(
            NodeValue::extract_value(Cow::Borrowed(&msg), &["optional_bool_field".to_string()])
                .unwrap(),
            NodeValue::Bool(false)
        );
        assert_eq!(
            NodeValue::extract_value(Cow::Borrowed(&msg), &["string_field".to_string()]).unwrap(),
            NodeValue::String(String::default())
        );
        assert_eq!(
            NodeValue::extract_value(Cow::Borrowed(&msg), &["bytes_field".to_string()]).unwrap(),
            NodeValue::ByteList(vec![])
        );
    }

    #[test]
    fn extract_value_nested_field_not_set_ok() {
        let msg = Arc::new(TestMessage::new()) as Arc<dyn MessageDyn>;
        let field_names = ["message_field".to_string(), "int_field".to_string()];

        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &field_names).unwrap(),
            NodeValue::None
        );
    }

    #[test]
    fn extract_value_nested_singular_field_ok() {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["message_field".to_string(), "bool_field".to_string()];

        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &field_names).unwrap(),
            NodeValue::Bool(true)
        );
    }

    #[test]
    fn extract_value_repeated_bool_field_ok() {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["repeated_bools".to_string()];
        let repeated_bools = TEST_MESSAGE.repeated_bools.clone();

        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &field_names).unwrap(),
            NodeValue::BoolList(repeated_bools)
        );
    }

    #[test]
    fn extract_value_repeated_int_field_ok() {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["repeated_i64s".to_string()];
        let repeated_i64s = TEST_MESSAGE.repeated_i64s.clone();

        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &field_names).unwrap(),
            NodeValue::Int64List(repeated_i64s)
        );
    }

    #[test]
    fn extract_value_repeated_double_field_ok() {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["repeated_doubles".to_string()];
        let repeated_doubles = TEST_MESSAGE.repeated_doubles.clone();

        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &field_names).unwrap(),
            NodeValue::DoubleList(repeated_doubles)
        );
    }

    #[test]
    fn extract_value_repeated_enum_field_ok() {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["repeated_enums".to_string()];
        let repeated_i32s = TEST_MESSAGE.repeated_enums.iter().map(|x| x.value()).collect();

        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &field_names).unwrap(),
            NodeValue::Int32List(repeated_i32s)
        );
    }

    #[test]
    fn extract_value_repeated_message_field_ok() {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["repeated_messages".to_string()];
        let repeated_msgs = TEST_MESSAGE
            .repeated_messages
            .iter()
            .map(|x| Arc::new(x.clone()) as Arc<dyn MessageDyn>)
            .collect::<Vec<_>>();

        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &field_names).unwrap(),
            NodeValue::MessageList(repeated_msgs)
        );
    }

    #[test]
    fn extract_value_nested_repeated_field_ok() {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["message_field".to_string(), "repeated_i32s".to_string()];
        let repeated_i32s = TEST_MESSAGE.message_field.repeated_i32s.clone();

        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &field_names).unwrap(),
            NodeValue::Int32List(repeated_i32s)
        );
    }

    #[test]
    fn extract_value_repeated_message_as_intermediate_field_name_should_err() {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["repeated_messages".to_string(), "bool_field".to_string()];

        assert!(matches!(
            NodeValue::extract_value(Cow::Owned(msg), &field_names).unwrap_err(),
            NodeEvaluationError::WrongNestedFieldType { .. }
        ));
    }

    #[test]
    fn extract_value_nonexistent_field_should_err() {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["foo".to_string()];

        assert!(matches!(
            NodeValue::extract_value(Cow::Owned(msg), &field_names).unwrap_err(),
            NodeEvaluationError::FieldNotFound { .. }
        ));
    }

    #[test]
    #[should_panic]
    fn all_eq_operator_fail() {
        let list = NodeValue::BoolList(vec![true, true, true]);
        let value = NodeValue::Bool(true);
        let _ = list.operate_sync(&OP_ALL_EQ, || Ok(value));
    }

    parameterized_test!(
        #[test]
        operate_sync_logical,

        |operator, a: NodeValue, b: NodeValue, matcher| {
            assert_that!(
                a.operate_sync(operator, || Ok(b)),
                matcher,
            );
        },

        and_1: (&OP_AND, NodeValue::Bool(true), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(true))),),
        and_2: (&OP_AND, NodeValue::Bool(false), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(false))),),
        and_3: (&OP_AND, NodeValue::Bool(true), NodeValue::Bool(false), ok(eq(&NodeValue::Bool(false))),),
        and_4: (&OP_AND, NodeValue::Bool(false), NodeValue::Bool(false), ok(eq(&NodeValue::Bool(false))),),
        and_none_left: (&OP_AND, NodeValue::None, NodeValue::Bool(true), ok(eq(&NodeValue::None)),),
        and_none_right: (&OP_AND, NodeValue::Bool(true), NodeValue::None, ok(eq(&NodeValue::None)),),
        and_integers_fails: (&OP_AND, NodeValue::Int32(1), NodeValue::Int32(0), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),
        and_non_bool_left_fails_1: (&OP_AND, NodeValue::Int32(1), NodeValue::Bool(true), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),
        and_non_bool_left_fails_2: (&OP_AND, NodeValue::BoolList(vec![true]), NodeValue::Bool(true), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),
        and_non_bool_right_fails_1: (&OP_AND, NodeValue::Bool(true), NodeValue::Int32(1), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),
        and_non_bool_right_fails_2: (&OP_AND, NodeValue::Bool(true), NodeValue::BoolList(vec![true]), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),

        or_1: (&OP_OR, NodeValue::Bool(true), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(true))),),
        or_2: (&OP_OR, NodeValue::Bool(false), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(true))),),
        or_3: (&OP_OR, NodeValue::Bool(true), NodeValue::Bool(false), ok(eq(&NodeValue::Bool(true))),),
        or_4: (&OP_OR, NodeValue::Bool(false), NodeValue::Bool(false), ok(eq(&NodeValue::Bool(false))),),
        or_none_left: (&OP_OR, NodeValue::None, NodeValue::Bool(true), ok(eq(&NodeValue::None)),),
        or_none_right: (&OP_OR, NodeValue::Bool(true), NodeValue::None, ok(eq(&NodeValue::Bool(true))),),
        or_integers_fails: (&OP_OR, NodeValue::Int32(1), NodeValue::Int32(0), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),
        or_non_bool_left_fails_1: (&OP_OR, NodeValue::Int32(1), NodeValue::Bool(false), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),
        or_non_bool_left_fails_2: (&OP_OR, NodeValue::BoolList(vec![false]), NodeValue::Bool(false), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),
        or_non_bool_right_fails_1: (&OP_OR, NodeValue::Bool(false), NodeValue::Int32(1), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),
        or_non_bool_right_fails_2: (&OP_OR, NodeValue::Bool(false), NodeValue::BoolList(vec![false]), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),

        xor_1: (&OP_XOR, NodeValue::Bool(true), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(false))),),
        xor_2: (&OP_XOR, NodeValue::Bool(false), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(true))),),
        xor_3: (&OP_XOR, NodeValue::Bool(true), NodeValue::Bool(false), ok(eq(&NodeValue::Bool(true))),),
        xor_4: (&OP_XOR, NodeValue::Bool(false), NodeValue::Bool(false), ok(eq(&NodeValue::Bool(false))),),
        xor_none_left: (&OP_XOR, NodeValue::None, NodeValue::Bool(true), ok(eq(&NodeValue::None)),),
        xor_none_right: (&OP_XOR, NodeValue::Bool(true), NodeValue::None, ok(eq(&NodeValue::None)),),
        xor_integers_fails: (&OP_XOR, NodeValue::Int32(1), NodeValue::Int32(0), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),
        xor_non_bool_left_fails_1: (&OP_XOR, NodeValue::Int32(1), NodeValue::Bool(true), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),
        xor_non_bool_left_fails_2: (&OP_XOR, NodeValue::BoolList(vec![true]), NodeValue::Bool(true), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),
        xor_non_bool_right_fails_1: (&OP_XOR, NodeValue::Bool(true), NodeValue::Int32(1), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),
        xor_non_bool_right_fails_2: (&OP_XOR, NodeValue::Bool(true), NodeValue::BoolList(vec![true]), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),
    );

    parameterized_test!(
        // Regardless of the right operand, even when the right operand is an incompatible type
        // (e.g., Int, List, etc.), or is `NodeValue::None`, the short-circuiting behavior will
        // ensure that the right operand isn't accessed nor checked.
        #[test]
        operate_sync_logical_short_circuit,

        |operator, left: NodeValue, matcher| {
            assert_that!(
                left.operate_sync(operator, || {
                    panic!("Right operator should not be accessed due to short-circuiting behavior.");
                }),
                matcher,
            );
        },

        and: (&OP_AND, NodeValue::Bool(false), /* right operand panics */ ok(eq(&NodeValue::Bool(false))),),
        or: (&OP_OR, NodeValue::Bool(true), /* right operand panics */ ok(eq(&NodeValue::Bool(true))),),
    );

    parameterized_test!(
        #[test]
        operate_sync_arithmetic,

        |operator, a: NodeValue, b: NodeValue, matcher| {
            assert_that!(
                a.operate_sync(operator, || Ok(b)),
                matcher,
            );
        },

        add_integers: (&OP_ADD, NodeValue::Int32(5), NodeValue::Int32(20), ok(eq(&NodeValue::Int64(25))),),
        add_floats: (&OP_ADD, NodeValue::Float(5.2), NodeValue::Double(20.1), ok(eq(&NodeValue::Double(25.3))),),
        add_none_left: (&OP_ADD, NodeValue::None, NodeValue::Int32(1), ok(eq(&NodeValue::None)),),
        add_none_right: (&OP_ADD, NodeValue::Int32(1), NodeValue::None, ok(eq(&NodeValue::None)),),
        add_non_numeric_left_fails: (&OP_ADD, NodeValue::Bool(true), NodeValue::Int32(1), err(pat!(NodeEvaluationError::ArithmeticOperatorIncompatibleOperands)),),
        add_non_numeric_right_fails: (&OP_ADD, NodeValue::Int32(1), NodeValue::Bool(true), err(pat!(NodeEvaluationError::ArithmeticOperatorIncompatibleOperands)),),

        sub_integers: (&OP_SUBTRACT, NodeValue::Int32(5), NodeValue::Int32(20), ok(eq(&NodeValue::Int64(-15))),),
        sub_floats: (&OP_SUBTRACT, NodeValue::Float(5.2), NodeValue::Double(20.1), ok(eq(&NodeValue::Double(-14.9))),),
        sub_none_left: (&OP_SUBTRACT, NodeValue::None, NodeValue::Int32(1), ok(eq(&NodeValue::None)),),
        sub_none_right: (&OP_SUBTRACT, NodeValue::Int32(1), NodeValue::None, ok(eq(&NodeValue::None)),),
        sub_non_numeric_left_fails: (&OP_SUBTRACT, NodeValue::Bool(true), NodeValue::Int32(1), err(pat!(NodeEvaluationError::ArithmeticOperatorIncompatibleOperands)),),
        sub_non_numeric_right_fails: (&OP_SUBTRACT, NodeValue::Int32(1), NodeValue::Bool(true), err(pat!(NodeEvaluationError::ArithmeticOperatorIncompatibleOperands)),),

        multiply: (&OP_MULTIPLY, NodeValue::Int32(5), NodeValue::Float(0.5), ok(eq(&NodeValue::Double(2.5))),),
        multiply_none_left: (&OP_MULTIPLY, NodeValue::None, NodeValue::Int32(1), ok(eq(&NodeValue::None)),),
        multiply_none_right: (&OP_MULTIPLY, NodeValue::Int32(1), NodeValue::None, ok(eq(&NodeValue::None)),),
        multiply_non_numeric_left_fails: (&OP_MULTIPLY, NodeValue::Bool(true), NodeValue::Int32(1), err(pat!(NodeEvaluationError::ArithmeticOperatorIncompatibleOperands)),),
        multiply_non_numeric_right_fails: (&OP_MULTIPLY, NodeValue::Int32(1), NodeValue::Bool(true), err(pat!(NodeEvaluationError::ArithmeticOperatorIncompatibleOperands)),),

        divide: (&OP_DIVIDE, NodeValue::Int32(5), NodeValue::Int32(2), ok(eq(&NodeValue::Double(2.5))),),
        divide_by_zero_fails: (&OP_DIVIDE, NodeValue::Int32(5), NodeValue::Int32(0), err(pat!(NodeEvaluationError::DivideByZero)),),
        divide_none_left: (&OP_DIVIDE, NodeValue::None, NodeValue::Int32(1), ok(eq(&NodeValue::None)),),
        divide_none_right: (&OP_DIVIDE, NodeValue::Int32(1), NodeValue::None, ok(eq(&NodeValue::None)),),
        divide_non_numeric_left_fails: (&OP_DIVIDE, NodeValue::Bool(true), NodeValue::Int32(1), err(pat!(NodeEvaluationError::ArithmeticOperatorIncompatibleOperands)),),
        divide_non_numeric_right_fails: (&OP_DIVIDE, NodeValue::Int32(1), NodeValue::Bool(true), err(pat!(NodeEvaluationError::ArithmeticOperatorIncompatibleOperands)),),

        modulo_trunc_1: (&OP_MODULO_TRUNC, NodeValue::Int32(8), NodeValue::Int32(3), ok(eq(&NodeValue::Int64(2))),),
        modulo_trunc_2: (&OP_MODULO_TRUNC, NodeValue::Int32(8), NodeValue::Int32(-3), ok(eq(&NodeValue::Int64(2))),),
        modulo_trunc_3: (&OP_MODULO_TRUNC, NodeValue::Int32(-8), NodeValue::Int32(3), ok(eq(&NodeValue::Int64(-2))),),
        modulo_trunc_4: (&OP_MODULO_TRUNC, NodeValue::Int32(-8), NodeValue::Int32(-3), ok(eq(&NodeValue::Int64(-2))),),
        modulo_trunc_by_zero_fails: (&OP_MODULO_TRUNC, NodeValue::Int32(5), NodeValue::Int32(0), err(pat!(NodeEvaluationError::DivideByZero)),),
        modulo_trunc_none_left: (&OP_MODULO_TRUNC, NodeValue::None, NodeValue::Int32(1), ok(eq(&NodeValue::None)),),
        modulo_trunc_none_right: (&OP_MODULO_TRUNC, NodeValue::Int32(1), NodeValue::None, ok(eq(&NodeValue::None)),),
        modulo_trunc_float_left_fails: (&OP_MODULO_TRUNC, NodeValue::Float(5.0), NodeValue::Int32(1), err(pat!(NodeEvaluationError::ModuloOperatorIncompatibleOperands)),),
        modulo_trunc_float_right_fails: (&OP_MODULO_TRUNC, NodeValue::Int32(1), NodeValue::Float(5.0), err(pat!(NodeEvaluationError::ModuloOperatorIncompatibleOperands)),),
        modulo_trunc_non_numeric_left_fails: (&OP_MODULO_TRUNC, NodeValue::Bool(true), NodeValue::Int32(1), err(pat!(NodeEvaluationError::ModuloOperatorIncompatibleOperands)),),
        modulo_trunc_non_numeric_right_fails: (&OP_MODULO_TRUNC, NodeValue::Int32(1), NodeValue::Bool(true), err(pat!(NodeEvaluationError::ModuloOperatorIncompatibleOperands)),),

        power_integers: (&OP_POWER, NodeValue::Int32(2), NodeValue::Int32(2), ok(eq(&NodeValue::Int64(4))),), // 2^2 = 4
        power_negative_integer_base: (&OP_POWER, NodeValue::Int32(-2), NodeValue::Int32(2), ok(eq(&NodeValue::Int64(4))),), // (-2)^2 = 4
        power_negative_integer_power: (&OP_POWER, NodeValue::Int32(2), NodeValue::Int32(-2), ok(eq(&NodeValue::Double(0.25))),), // 2^(-2) = 0.25
        power_floats: (&OP_POWER, NodeValue::Double(2.5), NodeValue::Double(2.5), ok(eq(&NodeValue::Double(9.88211768803))),), // 2.5^2.5 = 9.88...
        power_none_left: (&OP_POWER, NodeValue::None, NodeValue::Int32(1), ok(eq(&NodeValue::None)),),
        power_none_right: (&OP_POWER, NodeValue::Int32(1), NodeValue::None, ok(eq(&NodeValue::None)),),
        power_non_numeric_left_fails: (&OP_POWER, NodeValue::Bool(true), NodeValue::Int32(1), err(pat!(NodeEvaluationError::ArithmeticOperatorIncompatibleOperands)),),
        power_non_numeric_right_fails: (&OP_POWER, NodeValue::Int32(1), NodeValue::Bool(true), err(pat!(NodeEvaluationError::ArithmeticOperatorIncompatibleOperands)),),
    );

    parameterized_test!(
        #[test]
        operate_sync_relational,

        |operator, a: NodeValue, b: NodeValue, matcher| {
            assert_that!(
                a.operate_sync(operator, || Ok(b)),
                matcher
            );
        },

        eq_1: (&OP_EQ, ONE, ONE_EPSILON, ok(eq(&NodeValue::Bool(true))),),
        eq_2: (&OP_EQ, NodeValue::Int32(2), NodeValue::Int32(2), ok(eq(&NodeValue::Bool(true))),),
        eq_3: (&OP_EQ, NodeValue::Int32(2), NodeValue::Int64(2), ok(eq(&NodeValue::Bool(true))),),
        eq_4: (&OP_EQ, NodeValue::Int32(2), NodeValue::Float(2.0), ok(eq(&NodeValue::Bool(true))),),
        eq_5: (&OP_EQ, NodeValue::Int32(2), NodeValue::Double(2.0), ok(eq(&NodeValue::Bool(true))),),
        eq_6: (&OP_EQ, NodeValue::Int32(2), NodeValue::Float(5.5), ok(eq(&NodeValue::Bool(false))),),
        eq_7: (&OP_EQ, NodeValue::Int32(2), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(false))),),
        eq_none_left: (&OP_EQ, NodeValue::None, NodeValue::Int32(1), ok(eq(&NodeValue::None)),),
        eq_none_right: (&OP_EQ, NodeValue::Int32(1), NodeValue::None, ok(eq(&NodeValue::None)),),

        eq_list_1: (&OP_EQ, NodeValue::BoolList(vec![true, true, true]), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(true))),),
        eq_list_2: (&OP_EQ, NodeValue::BoolList(vec![true, true, true]), NodeValue::Bool(false), ok(eq(&NodeValue::Bool(false))),),
        eq_list_3: (&OP_EQ, NodeValue::BoolList(vec![true, false, false]), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(false))),),
        eq_list_4: (&OP_EQ, NodeValue::BoolList(vec![true, false, false]), NodeValue::Bool(false), ok(eq(&NodeValue::Bool(false))),),
        eq_list_5: (&OP_EQ, NodeValue::Int64List(vec![3, 30, 300]), NodeValue::Int32(0), ok(eq(&NodeValue::Bool(false))),),
        eq_list_6: (&OP_EQ, NodeValue::Int64List(vec![0, 0]), NodeValue::Int32(0), ok(eq(&NodeValue::Bool(true))),),
        eq_list_7: (&OP_EQ, NodeValue::FloatList(vec![0.5, std::f32::consts::PI, 10.0]), NodeValue::Double(0.5), ok(eq(&NodeValue::Bool(false))),),
        eq_list_8: (&OP_EQ, NodeValue::FloatList(vec![0.5, 0.5]), NodeValue::Double(0.5), ok(eq(&NodeValue::Bool(true))),),
        eq_list_9: (&OP_EQ, NodeValue::MessageList(vec![Arc::new(TEST_MESSAGE.clone()), Arc::new(TEST_MESSAGE.clone())]), NodeValue::Message(Arc::new(TEST_MESSAGE.clone())), ok(eq(&NodeValue::Bool(true))),),
        eq_list_10: (&OP_EQ, NodeValue::MessageList(vec![Arc::new(TEST_MESSAGE.clone()), Arc::new(TEST_MESSAGE.clone())]), NodeValue::Message(Arc::new(TestMessage::new())), ok(eq(&NodeValue::Bool(false))),),
        eq_list_none_left: (&OP_EQ, NodeValue::None, NodeValue::Bool(false), ok(eq(&NodeValue::None)),),
        eq_list_none_right: (&OP_EQ, NodeValue::Int64List(vec![42]), NodeValue::None, ok(eq(&NodeValue::None)),),
        eq_list_incompatible_types: (&OP_EQ, NodeValue::BoolList(vec![true, true, false]), NodeValue::Float(5.5), err(pat!(NodeEvaluationError::ComparisonOperatorListIncompatibleOperands)),),

        ne_1: (&OP_NOT_EQ, ONE, ONE_EPSILON, ok(eq(&NodeValue::Bool(false))),),
        ne_2: (&OP_NOT_EQ, NodeValue::Int32(2), NodeValue::Int32(2), ok(eq(&NodeValue::Bool(false))),),
        ne_3: (&OP_NOT_EQ, NodeValue::Int32(2), NodeValue::Int64(2), ok(eq(&NodeValue::Bool(false))),),
        ne_4: (&OP_NOT_EQ, NodeValue::Int32(2), NodeValue::Float(2.0), ok(eq(&NodeValue::Bool(false))),),
        ne_5: (&OP_NOT_EQ, NodeValue::Int32(2), NodeValue::Double(2.0), ok(eq(&NodeValue::Bool(false))),),
        ne_6: (&OP_NOT_EQ, NodeValue::Int32(2), NodeValue::Float(5.5), ok(eq(&NodeValue::Bool(true))),),
        ne_7: (&OP_NOT_EQ, NodeValue::Int32(2), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(true))),),
        ne_none_left: (&OP_NOT_EQ, NodeValue::None, NodeValue::Int32(1), ok(eq(&NodeValue::None)),),
        ne_none_right: (&OP_NOT_EQ, NodeValue::Int32(1), NodeValue::None, ok(eq(&NodeValue::None)),),

        gt_1: (&OP_GT, NodeValue::Bool(true), NodeValue::Bool(false), ok(eq(&NodeValue::Bool(true))),),
        gt_2: (&OP_GT, NodeValue::Bool(false), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(false))),),
        gt_3: (&OP_GT, ONE_EPSILON, ONE, ok(eq(&NodeValue::Bool(false))),),
        gt_4: (&OP_GT, NodeValue::Int32(2), NodeValue::Int32(2), ok(eq(&NodeValue::Bool(false))),),
        gt_5: (&OP_GT, NodeValue::Int32(2), NodeValue::Float(5.5), ok(eq(&NodeValue::Bool(false))),),
        gt_6: (&OP_GT, NodeValue::Float(5.5), NodeValue::Int32(2), ok(eq(&NodeValue::Bool(true))),),
        gt_7: (&OP_GT, NodeValue::Float(5.5), NodeValue::Bool(true), err(pat!(NodeEvaluationError::ComparisonOperatorIncomptaibleOperands)),),
        gt_none_left: (&OP_GT, NodeValue::None, NodeValue::Int32(1), ok(eq(&NodeValue::None)),),
        gt_none_right: (&OP_GT, NodeValue::Int32(1), NodeValue::None, ok(eq(&NodeValue::None)),),

        gte_1: (&OP_GT_OR_EQ, NodeValue::Bool(true), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(true))),),
        gte_2: (&OP_GT_OR_EQ, ONE_EPSILON, ONE, ok(eq(&NodeValue::Bool(true))),),
        gte_3: (&OP_GT_OR_EQ, NodeValue::Int32(2), NodeValue::Int32(2), ok(eq(&NodeValue::Bool(true))),),
        gte_4: (&OP_GT_OR_EQ, NodeValue::Int32(2), NodeValue::Float(5.5), ok(eq(&NodeValue::Bool(false))),),
        gte_5: (&OP_GT_OR_EQ, NodeValue::Float(5.5), NodeValue::Int32(2), ok(eq(&NodeValue::Bool(true))),),
        gte_6: (&OP_GT_OR_EQ, NodeValue::Float(5.5), NodeValue::Bool(true), err(pat!(NodeEvaluationError::ComparisonOperatorIncomptaibleOperands)),),
        gte_none_left: (&OP_GT_OR_EQ, NodeValue::None, NodeValue::Int32(1), ok(eq(&NodeValue::None)),),
        gte_none_right: (&OP_GT_OR_EQ, NodeValue::Int32(1), NodeValue::None, ok(eq(&NodeValue::None)),),

        lt_1: (&OP_LT, NodeValue::Bool(true), NodeValue::Bool(false), ok(eq(&NodeValue::Bool(false))),),
        lt_2: (&OP_LT, NodeValue::Bool(false), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(true))),),
        lt_3: (&OP_LT, ONE, ONE_EPSILON, ok(eq(&NodeValue::Bool(false))),),
        lt_4: (&OP_LT, NodeValue::Int32(2), NodeValue::Int32(2), ok(eq(&NodeValue::Bool(false))),),
        lt_5: (&OP_LT, NodeValue::Int32(2), NodeValue::Float(5.5), ok(eq(&NodeValue::Bool(true))),),
        lt_6: (&OP_LT, NodeValue::Float(5.5), NodeValue::Int32(2), ok(eq(&NodeValue::Bool(false))),),
        lt_7: (&OP_LT, NodeValue::Float(5.5), NodeValue::Bool(true), err(pat!(NodeEvaluationError::ComparisonOperatorIncomptaibleOperands)),),
        lt_none_left: (&OP_LT, NodeValue::None, NodeValue::Int32(1), ok(eq(&NodeValue::None)),),
        lt_none_right: (&OP_LT, NodeValue::Int32(1), NodeValue::None, ok(eq(&NodeValue::None)),),

        lte_1: (&OP_LT_OR_EQ, NodeValue::Bool(true), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(true))),),
        lte_2: (&OP_LT_OR_EQ, ONE_EPSILON, ONE, ok(eq(&NodeValue::Bool(true))),),
        lte_3: (&OP_LT_OR_EQ, NodeValue::Int32(2), NodeValue::Int32(2), ok(eq(&NodeValue::Bool(true))),),
        lte_4: (&OP_LT_OR_EQ, NodeValue::Int32(2), NodeValue::Float(5.5), ok(eq(&NodeValue::Bool(true))),),
        lte_5: (&OP_LT_OR_EQ, NodeValue::Float(5.5), NodeValue::Int32(2), ok(eq(&NodeValue::Bool(false))),),
        lte_6: (&OP_LT_OR_EQ, NodeValue::Float(5.5), NodeValue::Bool(true), err(pat!(NodeEvaluationError::ComparisonOperatorIncomptaibleOperands)),),
        lte_none_left: (&OP_LT_OR_EQ, NodeValue::None, NodeValue::Int32(1), ok(eq(&NodeValue::None)),),
        lte_none_right: (&OP_LT_OR_EQ, NodeValue::Int32(1), NodeValue::None, ok(eq(&NodeValue::None)),),

        contains_bool_1: (&OP_CONTAINS, NodeValue::BoolList(vec![true, true, true]), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(true))),),
        contains_bool_2: (&OP_CONTAINS, NodeValue::BoolList(vec![true, true, true]), NodeValue::Bool(false), ok(eq(&NodeValue::Bool(false))),),
        contains_int_1: (&OP_CONTAINS, NodeValue::Int32List(vec![3, 30, 300]), NodeValue::Int32(30), ok(eq(&NodeValue::Bool(true))),),
        contains_int_2: (&OP_CONTAINS, NodeValue::Int32List(vec![3, 30, 300]), NodeValue::Int64(30), ok(eq(&NodeValue::Bool(true))),),
        contains_int_3: (&OP_CONTAINS, NodeValue::Int32List(vec![3, 30, 300]), NodeValue::Int32(0), ok(eq(&NodeValue::Bool(false))),),
        contains_float_1: (&OP_CONTAINS, NodeValue::FloatList(vec![0.5, PIf32, 10.0]), NodeValue::Float(0.5), ok(eq(&NodeValue::Bool(true))),),
        contains_float_2: (&OP_CONTAINS, NodeValue::FloatList(vec![0.5, PIf32, 10.0]), NodeValue::Double(PIf64), ok(eq(&NodeValue::Bool(true))),),
        contains_float_3: (&OP_CONTAINS, NodeValue::FloatList(vec![0.5, PIf32, 10.0]), NodeValue::Int32(10), ok(eq(&NodeValue::Bool(true))),),
        contains_float_4: (&OP_CONTAINS, NodeValue::FloatList(vec![0.5, PIf32, 10.0]), NodeValue::Float(30.0), ok(eq(&NodeValue::Bool(false))),),
        contains_approximately_equal: (&OP_CONTAINS, NodeValue::DoubleList(vec![ONE.get_double().unwrap()]), ONE_EPSILON, ok(eq(&NodeValue::Bool(true))),),
        contains_message_1: (&OP_CONTAINS, NodeValue::MessageList(vec![Arc::new(TEST_MESSAGE.clone())]), NodeValue::Message(Arc::new(TEST_MESSAGE.clone())), ok(eq(&NodeValue::Bool(true))),),
        contains_message_2: (&OP_CONTAINS, NodeValue::MessageList(vec![Arc::new(TEST_MESSAGE.clone())]), NodeValue::Message(Arc::new(TestMessage::new())), ok(eq(&NodeValue::Bool(false))),),
        contains_none_left: (&OP_CONTAINS, NodeValue::None, NodeValue::Int32(2), ok(eq(&NodeValue::None)),),
        contains_none_right: (&OP_CONTAINS, NodeValue::Int32List(vec![2]), NodeValue::None, ok(eq(&NodeValue::None)),),
        contains_not_a_list: (&OP_CONTAINS, NodeValue::Int32(2), NodeValue::Int32(2), err(pat!(NodeEvaluationError::ContainsOperatorIncompatibleOperands)),),
        contains_incompatible_types_1: (&OP_CONTAINS, NodeValue::BoolList(vec![true, true, false]), NodeValue::Float(5.5), err(pat!(NodeEvaluationError::ContainsOperatorIncompatibleOperands)),),
        contains_incompatible_types_2: (&OP_CONTAINS, NodeValue::Int32List(vec![1, 2, 3]), NodeValue::Float(2.0), err(pat!(NodeEvaluationError::ContainsOperatorIncompatibleOperands)),),

        not_contains_bool_1: (&OP_DOES_NOT_CONTAIN, NodeValue::BoolList(vec![true, true, true]), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(false))),),
        not_contains_bool_2: (&OP_DOES_NOT_CONTAIN, NodeValue::BoolList(vec![true, true, true]), NodeValue::Bool(false), ok(eq(&NodeValue::Bool(true))),),
        not_contains_int_1: (&OP_DOES_NOT_CONTAIN, NodeValue::Int32List(vec![3, 30, 300]), NodeValue::Int32(30), ok(eq(&NodeValue::Bool(false))),),
        not_contains_int_2: (&OP_DOES_NOT_CONTAIN, NodeValue::Int32List(vec![3, 30, 300]), NodeValue::Int64(30), ok(eq(&NodeValue::Bool(false))),),
        not_contains_int_3: (&OP_DOES_NOT_CONTAIN, NodeValue::Int32List(vec![3, 30, 300]), NodeValue::Int32(0), ok(eq(&NodeValue::Bool(true))),),
        not_contains_float_1: (&OP_DOES_NOT_CONTAIN, NodeValue::FloatList(vec![0.5, PIf32, 10.0]), NodeValue::Float(0.5), ok(eq(&NodeValue::Bool(false))),),
        not_contains_float_2: (&OP_DOES_NOT_CONTAIN, NodeValue::FloatList(vec![0.5, PIf32, 10.0]), NodeValue::Double(PIf64), ok(eq(&NodeValue::Bool(false))),),
        not_contains_float_3: (&OP_DOES_NOT_CONTAIN, NodeValue::FloatList(vec![0.5, PIf32, 10.0]), NodeValue::Int32(10), ok(eq(&NodeValue::Bool(false))),),
        not_contains_float_4: (&OP_DOES_NOT_CONTAIN, NodeValue::FloatList(vec![0.5, PIf32, 10.0]), NodeValue::Float(30.0), ok(eq(&NodeValue::Bool(true))),),
        not_contains_approximately_equal: (&OP_DOES_NOT_CONTAIN, NodeValue::DoubleList(vec![ONE.get_double().unwrap()]), ONE_EPSILON, ok(eq(&NodeValue::Bool(false))),),
        not_contains_message_1: (&OP_DOES_NOT_CONTAIN, NodeValue::MessageList(vec![Arc::new(TEST_MESSAGE.clone())]), NodeValue::Message(Arc::new(TEST_MESSAGE.clone())), ok(eq(&NodeValue::Bool(false))),),
        not_contains_message_2: (&OP_DOES_NOT_CONTAIN, NodeValue::MessageList(vec![Arc::new(TEST_MESSAGE.clone())]), NodeValue::Message(Arc::new(TestMessage::new())), ok(eq(&NodeValue::Bool(true))),),
        not_contains_none_left: (&OP_DOES_NOT_CONTAIN, NodeValue::None, NodeValue::Int32(2), ok(eq(&NodeValue::None)),),
        not_contains_none_right: (&OP_DOES_NOT_CONTAIN, NodeValue::Int32List(vec![2]), NodeValue::None, ok(eq(&NodeValue::None)),),
        not_contains_not_a_list: (&OP_DOES_NOT_CONTAIN, NodeValue::Int32(2), NodeValue::Int32(2), err(pat!(NodeEvaluationError::ContainsOperatorIncompatibleOperands)),),
        not_contains_incompatible_types_1: (&OP_DOES_NOT_CONTAIN, NodeValue::BoolList(vec![true, true, false]), NodeValue::Float(5.5), err(pat!(NodeEvaluationError::ContainsOperatorIncompatibleOperands)),),
        not_contains_incompatible_types_2: (&OP_DOES_NOT_CONTAIN, NodeValue::Int32List(vec![1, 2, 3]), NodeValue::Float(2.0), err(pat!(NodeEvaluationError::ContainsOperatorIncompatibleOperands)),),
    );

    parameterized_test!(
        #[test]
        operate_sync_unary,

        |operator, value: NodeValue, matcher| {
            assert_that!(
                value.operate_sync(operator, unary_right_operand_provider),
                matcher,
            );
        },

        not_true: (&OP_NOT, NodeValue::Bool(true), ok(eq(&NodeValue::Bool(false))),),
        not_false: (&OP_NOT, NodeValue::Bool(false), ok(eq(&NodeValue::Bool(true))),),
        not_none: (&OP_NOT, NodeValue::None, ok(eq(&NodeValue::None)),),
        not_non_bool_fails: (&OP_NOT, NodeValue::Int32(3), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),

        minus_1: (&OP_UNARY_MINUS, NodeValue::Int32(2), ok(eq(&NodeValue::Int64(-2))),),
        minus_2: (&OP_UNARY_MINUS, NodeValue::Float(5.5), ok(eq(&NodeValue::Double(-5.5))),),
        minus_none: (&OP_UNARY_MINUS, NodeValue::None, ok(eq(&NodeValue::None)),),
        minus_non_numeric_fails: (&OP_UNARY_MINUS, NodeValue::Bool(true), err(pat!(NodeEvaluationError::ArithmeticOperatorIncompatibleOperands)),),

        absolute_1: (&OP_ABSOLUTE, NodeValue::Int64(1), ok(eq(&NodeValue::Int64(1))),),
        absolute_2: (&OP_ABSOLUTE, NodeValue::Int64(-3), ok(eq(&NodeValue::Int64(3))),),
        absolute_3: (&OP_ABSOLUTE, NodeValue::Double(2.7), ok(eq(&NodeValue::Double(2.7))),),
        absolute_4: (&OP_ABSOLUTE, NodeValue::Double(-2.7), ok(eq(&NodeValue::Double(2.7))),),
        absolute_5: (&OP_ABSOLUTE, NodeValue::Double(0.0), ok(eq(&NodeValue::Double(0.0))),),
        absolute_none: (&OP_ABSOLUTE, NodeValue::None, ok(eq(&NodeValue::None)),),
        absolute_non_numeric_fails: (&OP_ABSOLUTE, NodeValue::Bool(true), err(pat!(NodeEvaluationError::ArithmeticOperatorIncompatibleOperands)),),

        floor_1: (&OP_FLOOR, NodeValue::Double(0.1), ok(eq(&NodeValue::Int64(0))),),
        floor_2: (&OP_FLOOR, NodeValue::Float(2.9), ok(eq(&NodeValue::Int64(2))),),
        floor_3: (&OP_FLOOR, NodeValue::Float(5.0), ok(eq(&NodeValue::Int64(5))),),
        floor_4: (&OP_FLOOR, NodeValue::Double(-0.33), ok(eq(&NodeValue::Int64(-1))),),
        floor_5: (&OP_FLOOR, NodeValue::Double(-0.99), ok(eq(&NodeValue::Int64(-1))),),
        floor_none: (&OP_FLOOR, NodeValue::None, ok(eq(&NodeValue::None)),),
        floor_non_numeric_fails: (&OP_FLOOR, NodeValue::Bool(true), err(pat!(NodeEvaluationError::RoundingOperatorIncompatibleOperands)),),

        round_1: (&OP_ROUND, NodeValue::Double(0.1), ok(eq(&NodeValue::Int64(0))),),
        round_2: (&OP_ROUND, NodeValue::Float(2.9), ok(eq(&NodeValue::Int64(3))),),
        round_3: (&OP_ROUND, NodeValue::Float(5.0), ok(eq(&NodeValue::Int64(5))),),
        round_4: (&OP_ROUND, NodeValue::Double(-0.33), ok(eq(&NodeValue::Int64(0))),),
        round_5: (&OP_ROUND, NodeValue::Double(-0.99), ok(eq(&NodeValue::Int64(-1))),),
        round_none: (&OP_ROUND, NodeValue::None, ok(eq(&NodeValue::None)),),
        round_non_numeric_fails: (&OP_ROUND, NodeValue::Bool(true), err(pat!(NodeEvaluationError::RoundingOperatorIncompatibleOperands)),),

        ceil_1: (&OP_CEIL, NodeValue::Double(0.1), ok(eq(&NodeValue::Int64(1))),),
        ceil_2: (&OP_CEIL, NodeValue::Float(2.9), ok(eq(&NodeValue::Int64(3))),),
        ceil_3: (&OP_CEIL, NodeValue::Float(5.0), ok(eq(&NodeValue::Int64(5))),),
        ceil_4: (&OP_CEIL, NodeValue::Double(-0.33), ok(eq(&NodeValue::Int64(0))),),
        ceil_5: (&OP_CEIL, NodeValue::Double(-0.99), ok(eq(&NodeValue::Int64(0))),),
        ceil_none: (&OP_ROUND, NodeValue::None, ok(eq(&NodeValue::None)),),
        ceil_non_numeric_fails: (&OP_CEIL, NodeValue::Bool(true), err(pat!(NodeEvaluationError::RoundingOperatorIncompatibleOperands)),),
    );
}
