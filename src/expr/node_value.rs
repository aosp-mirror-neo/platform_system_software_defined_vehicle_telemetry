// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Wrapper around a generic value.
//! Should be able to hold all the values in a ConstantLeafNode protobuf
//! message, as well as all the values that a FieldLeafNode can select.

use sdv_telemetry_proto_rust::expression::combination_node::{
    ArithmeticOperator, LogicalOperator, Operator as OperatorProto, RelationalOperator,
    RoundingOperator,
};
use sdv_telemetry_proto_rust::expression::constant_leaf_node::Node_value as NodeValueProto;
use sdv_telemetry_proto_rust::expression::ConstantLeafNode as ConstantLeafNodeProto;
use std::{borrow::Cow, convert::From, sync::Arc};

use crate::dynamic_message::DynamicMessage;
use crate::node_error::NodeEvaluationError;
use crate::node_value_type::NodeValueType;

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

    /// A message value.
    Message(Arc<dyn DynamicMessage>),

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

    /// Repeated messages.
    MessageList(Vec<Arc<dyn DynamicMessage>>),

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
            (NodeValue::Message(v1), NodeValue::Message(v2)) => v1.equals(v2.as_ref()),
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
                v1.len() == v2.len() && v1.iter().zip(v2).all(|(a, b)| a.equals(b.as_ref()))
            }
            (NodeValue::ByteList(v1), NodeValue::ByteList(v2)) => *v1 == *v2,
            _ => false,
        }
    }
}

pub(crate) enum OperationState<'a> {
    /// Operation finished without the need for a right operator (either due to being a unary
    /// operation, or due to short-circuiting).
    Finished(NodeValue),
    /// Operation cannot be finished without providing a right operand.
    RightOperandRequired(
        Box<dyn FnOnce(&NodeValue) -> Result<NodeValue, NodeEvaluationError> + 'a + Send>,
    ),
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
    ///   If unary operator, then operates on self.
    ///   If binary operator, then [self] <operator> [right].
    /// * `get_right_operand` - The function that provides the right operand when called.
    pub fn operate_sync<'a>(
        &self,
        op: &OperatorProto,
        get_right_operand: impl FnOnce() -> &'a NodeValue + Send,
    ) -> Result<NodeValue, NodeEvaluationError> {
        Self::operate(Cow::Borrowed(self), op).and_then(|result| match result {
            OperationState::Finished(result) => Ok(result),
            OperationState::RightOperandRequired(operate) => operate(get_right_operand()),
        })
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
    pub fn get_message(&self) -> Option<&Arc<dyn DynamicMessage>> {
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

    pub fn get_node_value_type(&self) -> Option<NodeValueType> {
        match self {
            NodeValue::None => None,
            NodeValue::Bool(_) => Some(NodeValueType::Bool),
            NodeValue::Int32(_) | NodeValue::Int64(_) => Some(NodeValueType::Int),
            NodeValue::Float(_) | NodeValue::Double(_) => Some(NodeValueType::Float),
            NodeValue::Message(msg) => {
                Some(NodeValueType::Message { descriptor: Some(msg.get_descriptor().to_owned()) })
            }
            NodeValue::BoolList(_) => Some(NodeValueType::BoolList),
            NodeValue::Int32List(_) | NodeValue::Int64List(_) => Some(NodeValueType::IntList),
            NodeValue::FloatList(_) | NodeValue::DoubleList(_) => Some(NodeValueType::FloatList),
            NodeValue::MessageList(vec) => Some(NodeValueType::MessageList {
                descriptor: vec.first().map(|msg| msg.get_descriptor().to_owned()),
            }),
            NodeValue::String(_) => Some(NodeValueType::String),
            NodeValue::ByteList(_) => Some(NodeValueType::ByteList),
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
                    msg: format!("is_zero() invoked on non-numeric NodeValue: {self:?}"),
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
        msg: Arc<dyn DynamicMessage>,
        fields: &[String],
    ) -> Result<NodeValue, NodeEvaluationError> {
        // if no field names, return the message itself as the NodeValue
        if fields.is_empty() {
            return Ok(NodeValue::Message(msg));
        }
        // recursively gets the field value from field names
        Self::extract_field_value(msg.as_ref(), fields)
    }

    /// Performs operation using the operator. If the operation can be performed without a right
    /// operator, returns `OperationState::Finished`. Otherwise returns
    /// `OperationState::RightOperatorNeeded`, which needs to be called with the right operator to
    /// finish the operation.
    ///
    /// # Arguments:
    ///
    /// * `self` - The left operand.
    /// * `op` - The operator.
    ///   If unary operator, then operates on self.
    ///   If binary operator, then [self] <operator> [right].
    pub(super) fn operate<'a>(
        self_cow: Cow<'a, Self>,
        op: &OperatorProto,
    ) -> Result<OperationState<'a>, NodeEvaluationError> {
        match op {
            OperatorProto::ArithmeticOperator(op) => {
                let op = match op.enum_value() {
                    Ok(value) => value,
                    Err(i) => {
                        return Err(NodeEvaluationError::UnknownOperator {
                            op: format!("Arithmetic operator with enum value {i}"),
                        })
                    }
                };

                // None occurs when message is missing or incomplete, and None propagates up.
                if self_cow.is_none() {
                    return Ok(OperationState::Finished(NodeValue::None));
                }
                Self::handle_arithmetic(self_cow, op)
            }
            OperatorProto::LogicalOperator(op) => {
                let op = match op.enum_value() {
                    Ok(value) => value,
                    Err(i) => {
                        return Err(NodeEvaluationError::UnknownOperator {
                            op: format!("Logical operator with enum value {i}"),
                        })
                    }
                };

                // None is handled inside `handle_logical`.
                Self::handle_logical(self_cow, op)
            }
            OperatorProto::RelationalOperator(op) => {
                let op = match op.enum_value() {
                    Ok(value) => value,
                    Err(i) => {
                        return Err(NodeEvaluationError::UnknownOperator {
                            op: format!("Relational operator with enum value {i}"),
                        })
                    }
                };

                // None occurs when message is missing or incomplete, and None propagates up.
                if self_cow.is_none() {
                    return Ok(OperationState::Finished(NodeValue::None));
                }
                Self::handle_binary(self_cow, move |left, right| left.handle_relational(&op, right))
            }
            OperatorProto::RoundingOperator(op) => {
                let op = match op.enum_value() {
                    Ok(value) => value,
                    Err(i) => {
                        return Err(NodeEvaluationError::UnknownOperator {
                            op: format!("Rounding operator with enum value {i}"),
                        })
                    }
                };

                // None occurs when message is missing or incomplete, and None propagates up.
                if self_cow.is_none() {
                    return Ok(OperationState::Finished(NodeValue::None));
                }
                self_cow.handle_unary(|value| value.handle_rounding(&op))
            }
            _ => Err(NodeEvaluationError::UnknownOperator { op: format!("{op:?}") }),
        }
    }

    // private functions and methods below

    /// Reads a field value from the message.
    fn extract_field_value(
        msg: &dyn DynamicMessage,
        fields: &[String],
    ) -> Result<NodeValue, NodeEvaluationError> {
        let field_name = &fields[0];
        let field_value = msg.get_field_value(field_name)?;

        // Base case for recursion: one field is provided, simply return the value.
        if fields.len() == 1 {
            return Ok(field_value);
        }

        // If fields length > 1, get nested field values if the value is a single message or None,
        // otherwise return an error.
        match field_value {
            NodeValue::Message(msg) => Self::extract_field_value(msg.as_ref(), &fields[1..]),
            val @ NodeValue::None => Ok(val),
            node_value => Err(NodeEvaluationError::WrongNestedFieldType {
                field_name: field_name.clone(),
                e: format!("Unable to get nested fields from node value {node_value:?}"),
            }),
        }
    }

    /// Handle arithmetic operations.
    fn handle_arithmetic(
        self_cow: Cow<'_, Self>,
        op: ArithmeticOperator,
    ) -> Result<OperationState<'_>, NodeEvaluationError> {
        let err_provider = || NodeEvaluationError::ArithmeticOperatorIncompatibleOperands;

        match op {
            ArithmeticOperator::ABSOLUTE => self_cow.handle_unary(|value| {
                value
                    .apply_numeric_unary(
                        |a| NodeValue::Int64(a.abs()),
                        |a| NodeValue::Double(a.abs()),
                    )
                    .ok_or_else(err_provider)
            }),
            ArithmeticOperator::ADD => Self::handle_binary(self_cow, move |left, right| {
                left.apply_numeric_binary(
                    right,
                    |a, b| NodeValue::Int64(a + b),
                    |a, b| NodeValue::Double(a + b),
                )
                .ok_or_else(err_provider)
            }),
            ArithmeticOperator::DIVIDE => Self::handle_binary(self_cow, move |left, right| {
                let left_double = left.get_double().ok_or_else(err_provider)?;
                let right_double = right.get_double().ok_or_else(err_provider)?;

                if approximately_equal(right_double, 0.0) {
                    return Err(NodeEvaluationError::DivideByZero);
                }

                Ok(NodeValue::Double(left_double / right_double))
            }),
            ArithmeticOperator::MODULO_TRUNC => {
                Self::handle_binary(self_cow, |left, right| {
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
            ArithmeticOperator::MULTIPLY => Self::handle_binary(self_cow, move |left, right| {
                left.apply_numeric_binary(
                    right,
                    |a, b| NodeValue::Int64(a * b),
                    |a, b| NodeValue::Double(a * b),
                )
                .ok_or_else(err_provider)
            }),
            ArithmeticOperator::POWER => {
                Self::handle_binary(self_cow, move |left, right| {
                    left.apply_numeric_binary(
                        right,
                        |a, b| {
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
            ArithmeticOperator::SUBTRACT => Self::handle_binary(self_cow, move |left, right| {
                left.apply_numeric_binary(
                    right,
                    |a, b| NodeValue::Int64(a - b),
                    |a, b| NodeValue::Double(a - b),
                )
                .ok_or_else(err_provider)
            }),
            ArithmeticOperator::UNARY_MINUS => self_cow.handle_unary(|value| {
                value
                    .apply_numeric_unary(|a| NodeValue::Int64(-a), |a| NodeValue::Double(-a))
                    .ok_or_else(err_provider)
            }),
        }
    }

    /// Handle logical operations.
    fn handle_logical(
        self_cow: Cow<'_, Self>,
        op: LogicalOperator,
    ) -> Result<OperationState<'_>, NodeEvaluationError> {
        let err_provider = || NodeEvaluationError::LogicalOperatorIncompatibleOperands;

        let left_value = match self_cow.get_bool() {
            // `left_value` is a boolean
            Some(left_value) => Some(left_value),
            // `left_value` is not a boolean (e.g., a string), or it is none (= unknown).
            None => {
                if self_cow.is_none() {
                    // `left_value` is none (= unknown).
                    None
                } else {
                    // `left_value` is not a boolean nor none -> incompatible operand.
                    return Err(err_provider());
                }
            }
        };

        match op {
            LogicalOperator::AND => match left_value {
                // Short-circuit and don't evaluate right operand if the left operand is false.
                Some(false) => Ok(OperationState::Finished(NodeValue::Bool(false))),
                // If left is none, the result depends on the right operand, but can only be none or
                // false. It can never be true, because we don't know whether left is true or false.
                None => Self::handle_binary(self_cow, move |_left, right| {
                    Ok(if !right.get_bool().ok_or_else(err_provider)? {
                        NodeValue::Bool(false)
                    } else {
                        NodeValue::None
                    })
                }),
                // If left is true, the result depends on the right operand.
                Some(true) => Self::handle_binary(self_cow, move |_left, right| {
                    // We don't need to deal with the case where right is none here, because
                    // `Self::handle_binary` has a guard clause that returns none if right is none.
                    Ok(NodeValue::Bool(right.get_bool().ok_or_else(err_provider)?))
                }),
            },
            LogicalOperator::OR => match left_value {
                // Short-circuit and don't evaluate right operand if the left operand is true.
                Some(true) => Ok(OperationState::Finished(NodeValue::Bool(true))),
                // If left is none, the result depends on the right operand, but can only be none or
                // true. It can never be false, because we don't know whether left is true or false.
                None => Self::handle_binary(self_cow, move |_left, right| {
                    Ok(if right.get_bool().ok_or_else(err_provider)? {
                        NodeValue::Bool(true)
                    } else {
                        NodeValue::None
                    })
                }),
                // If left is false, the result depends on the right operand.
                Some(false) => Self::handle_binary(self_cow, move |_left, right| {
                    // We don't need to deal with the case where right is none here, because
                    // `Self::handle_binary` has a guard clause that returns none if right is none.
                    Ok(NodeValue::Bool(right.get_bool().ok_or_else(err_provider)?))
                }),
            },
            LogicalOperator::XOR => match left_value {
                None => Ok(OperationState::Finished(NodeValue::None)),
                Some(left_value) => Self::handle_binary(self_cow, move |_left, right| {
                    // We don't need to deal with the case where right is none here, because
                    // `Self::handle_binary` has a guard clause that returns none if right is none.
                    Ok(NodeValue::Bool(left_value != right.get_bool().ok_or_else(err_provider)?))
                }),
            },
            LogicalOperator::NOT => Ok(OperationState::Finished(match left_value {
                None => NodeValue::None,
                Some(left_value) => NodeValue::Bool(!left_value),
            })),
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
        Ok(match self {
            val @ NodeValue::Int32(_) | val @ NodeValue::Int64(_) => val.clone(),
            NodeValue::Float(_) | NodeValue::Double(_) => {
                let value = self.get_double().ok_or_else(err_provider)?;
                NodeValue::Int64(match op {
                    RoundingOperator::FLOOR => value.floor() as i64,
                    RoundingOperator::ROUND => value.round() as i64,
                    RoundingOperator::CEIL => value.ceil() as i64,
                })
            }
            _ => return Err(err_provider()),
        })
    }

    /// Handles unary operation on self using provided node value handler.
    fn handle_unary(
        &self,
        handler: impl FnOnce(&NodeValue) -> Result<NodeValue, NodeEvaluationError>,
    ) -> Result<OperationState<'static>, NodeEvaluationError> {
        Ok(OperationState::Finished(handler(self)?))
    }

    /// Handles binary operation on self using provided node value handler.
    fn handle_binary<'a>(
        self_cow: Cow<'a, Self>,
        handler: impl FnOnce(&NodeValue, &NodeValue) -> Result<NodeValue, NodeEvaluationError>
            + Send
            + 'a,
    ) -> Result<OperationState<'a>, NodeEvaluationError> {
        Ok(OperationState::RightOperandRequired(Box::new(move |right| {
            if right.is_none() {
                // None occurs when message is missing or incomplete, and None propagates up.
                return Ok(NodeValue::None);
            }

            handler(&self_cow, right)
        })))
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
                v.iter().any(|x| x.equals(target_value.as_ref()))
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
                v.iter().all(|x| x.equals(target_value.as_ref()))
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
    use protobuf::MessageFull;
    use sdv_telemetry_proto_rust::expression::ConstantLeafNode;
    use sdv_telemetry_test_proto::test_message::TestMessage;
    use sdv_telemetry_test_utils::assert_matches;
    use sdv_telemetry_test_utils::parameterized_test;
    use std::f32::consts::PI as PIf32;
    use std::f64::consts::PI as PIf64;

    use crate::mock::*;

    const ONE: NodeValue = NodeValue::Double(1.0);
    const ONE_EPSILON: NodeValue = NodeValue::Double(1.0 + f64::EPSILON);

    fn unary_right_operand_provider() -> &'static NodeValue {
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

    parameterized_test!(
        #[test]
        is_zero,

        |a: NodeValue, expected| {
            assert_that!(a.is_zero(), expected);
        },

        int32_zero: (NodeValue::Int32(0), ok(eq(&true))),
        int64_zero: (NodeValue::Int64(0), ok(eq(&true))),
        float_zero: (NodeValue::Float(0.0), ok(eq(&true))),
        double_zero: (NodeValue::Double(0.0), ok(eq(&true))),
        float_approx_zero: (NodeValue::Float(1e-7), ok(eq(&true))),
        double_approx_zero: (NodeValue::Double(-1e-9), ok(eq(&true))),

        int32_non_zero: (NodeValue::Int32(42), ok(eq(&false))),
        int64_non_zero: (NodeValue::Int64(-42), ok(eq(&false))),
        float_non_zero: (NodeValue::Float(42.0), ok(eq(&false))),
        double_non_zero: (NodeValue::Double(-42.0), ok(eq(&false))),
        float_approx_non_zero: (NodeValue::Float(0.1), ok(eq(&false))),
        double_approx_non_zero: (NodeValue::Double(-0.0001), ok(eq(&false))),

        bool_err: (NodeValue::Bool(false), err(pat!(NodeEvaluationError::InternalError { .. }))),
        string_err: (NodeValue::String("".to_owned()), err(pat!(NodeEvaluationError::InternalError { .. }))),
    );

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

        let constant_leaf_node_none = ConstantLeafNode::new();

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
        assert_matches!(
            NodeValue::from_constant_leaf_node(&constant_leaf_node_none).unwrap_err(),
            NodeEvaluationError::ConstantValueNotSet
        );
    }

    #[test]
    fn extract_value_no_field_names_ok() {
        let msg = Arc::clone(&*TEST_DYNAMIC_MESSAGE) as Arc<dyn DynamicMessage>;

        let node_value = NodeValue::extract_value(Arc::clone(&msg), &[]).unwrap();

        assert_matches!(
            &node_value,
            NodeValue::Message(inner_msg) if Arc::ptr_eq(inner_msg, &msg)
        );
    }

    #[test]
    fn extract_value_nested_field_not_set_ok() {
        let mut msg = MockDynamicMessage::new(TestMessage::descriptor());
        msg.set_field_value("message_field".to_owned(), NodeValue::None);
        let msg = Arc::new(msg);
        let field_names = ["message_field".to_owned(), "int_field".to_owned()];

        assert_eq!(NodeValue::extract_value(msg, &field_names).unwrap(), NodeValue::None);
    }

    #[test]
    fn extract_value_nested_singular_field_ok() {
        let msg = Arc::clone(&*TEST_DYNAMIC_MESSAGE);
        let field_names = ["message_field".to_owned(), "bool_field".to_owned()];

        assert_eq!(NodeValue::extract_value(msg, &field_names).unwrap(), NodeValue::Bool(true));
    }

    #[test]
    fn extract_value_nested_repeated_field_ok() {
        let msg = Arc::clone(&*TEST_DYNAMIC_MESSAGE);
        let field_names = ["message_field".to_owned(), "repeated_i32s".to_owned()];

        assert_eq!(
            NodeValue::extract_value(msg, &field_names).unwrap(),
            NodeValue::Int32List(vec![1, 2])
        );
    }

    #[test]
    fn extract_value_repeated_message_as_intermediate_field_name_should_err() {
        let msg = Arc::clone(&*TEST_DYNAMIC_MESSAGE);
        let field_names = ["repeated_messages".to_owned(), "bool_field".to_owned()];

        assert_matches!(
            NodeValue::extract_value(msg, &field_names).unwrap_err(),
            NodeEvaluationError::WrongNestedFieldType { .. }
        );
    }

    #[test]
    fn extract_value_propagates_error() {
        let msg = Arc::new(MockDynamicMessage::new(TestMessage::descriptor()));
        msg.set_next_error(NodeEvaluationError::ConstantValueNotSet);
        let field_names = ["foo".to_owned()];

        assert_matches!(
            NodeValue::extract_value(msg, &field_names).unwrap_err(),
            NodeEvaluationError::ConstantValueNotSet
        );
    }

    #[test]
    #[should_panic]
    fn all_eq_operator_fail() {
        let list = NodeValue::BoolList(vec![true, true, true]);
        let value = NodeValue::Bool(true);
        let _ = list.operate_sync(&OP_ALL_EQ, || &value);
    }

    parameterized_test!(
        #[test]
        operate_sync_logical,

        |operator, a: NodeValue, b: NodeValue, matcher| {
            assert_that!(
                a.operate_sync(operator, || &b),
                matcher,
            );
        },

        and_1: (&OP_AND, NodeValue::Bool(true), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(true))),),
        and_2: (&OP_AND, NodeValue::Bool(false), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(false))),),
        and_3: (&OP_AND, NodeValue::Bool(true), NodeValue::Bool(false), ok(eq(&NodeValue::Bool(false))),),
        and_4: (&OP_AND, NodeValue::Bool(false), NodeValue::Bool(false), ok(eq(&NodeValue::Bool(false))),),
        and_none_left_true_right: (&OP_AND, NodeValue::None, NodeValue::Bool(true), ok(eq(&NodeValue::None)),),
        and_none_left_false_right: (&OP_AND, NodeValue::None, NodeValue::Bool(false), ok(eq(&NodeValue::Bool(false))),),
        and_true_left_none_right: (&OP_AND, NodeValue::Bool(true), NodeValue::None, ok(eq(&NodeValue::None)),),
        and_false_left_none_right: (&OP_AND, NodeValue::Bool(false), NodeValue::None, ok(eq(&NodeValue::Bool(false))),),
        and_integers_fails: (&OP_AND, NodeValue::Int32(1), NodeValue::Int32(0), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),
        and_non_bool_left_fails_1: (&OP_AND, NodeValue::Int32(1), NodeValue::Bool(true), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),
        and_non_bool_left_fails_2: (&OP_AND, NodeValue::BoolList(vec![true]), NodeValue::Bool(true), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),
        and_non_bool_right_fails_1: (&OP_AND, NodeValue::Bool(true), NodeValue::Int32(1), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),
        and_non_bool_right_fails_2: (&OP_AND, NodeValue::Bool(true), NodeValue::BoolList(vec![true]), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),

        or_1: (&OP_OR, NodeValue::Bool(true), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(true))),),
        or_2: (&OP_OR, NodeValue::Bool(false), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(true))),),
        or_3: (&OP_OR, NodeValue::Bool(true), NodeValue::Bool(false), ok(eq(&NodeValue::Bool(true))),),
        or_4: (&OP_OR, NodeValue::Bool(false), NodeValue::Bool(false), ok(eq(&NodeValue::Bool(false))),),
        or_none_left_true_right: (&OP_OR, NodeValue::None, NodeValue::Bool(true), ok(eq(&NodeValue::Bool(true))),),
        or_none_left_false_right: (&OP_OR, NodeValue::None, NodeValue::Bool(false), ok(eq(&NodeValue::None)),),
        or_true_left_none_right: (&OP_OR, NodeValue::Bool(true), NodeValue::None, ok(eq(&NodeValue::Bool(true))),),
        or_false_left_none_right: (&OP_OR, NodeValue::Bool(false), NodeValue::None, ok(eq(&NodeValue::None)),),
        or_integers_fails: (&OP_OR, NodeValue::Int32(1), NodeValue::Int32(0), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),
        or_non_bool_left_fails_1: (&OP_OR, NodeValue::Int32(1), NodeValue::Bool(false), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),
        or_non_bool_left_fails_2: (&OP_OR, NodeValue::BoolList(vec![false]), NodeValue::Bool(false), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),
        or_non_bool_right_fails_1: (&OP_OR, NodeValue::Bool(false), NodeValue::Int32(1), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),
        or_non_bool_right_fails_2: (&OP_OR, NodeValue::Bool(false), NodeValue::BoolList(vec![false]), err(pat!(NodeEvaluationError::LogicalOperatorIncompatibleOperands)),),

        xor_1: (&OP_XOR, NodeValue::Bool(true), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(false))),),
        xor_2: (&OP_XOR, NodeValue::Bool(false), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(true))),),
        xor_3: (&OP_XOR, NodeValue::Bool(true), NodeValue::Bool(false), ok(eq(&NodeValue::Bool(true))),),
        xor_4: (&OP_XOR, NodeValue::Bool(false), NodeValue::Bool(false), ok(eq(&NodeValue::Bool(false))),),
        xor_none_left_true_right: (&OP_XOR, NodeValue::None, NodeValue::Bool(true), ok(eq(&NodeValue::None)),),
        xor_none_left_false_right: (&OP_XOR, NodeValue::None, NodeValue::Bool(false), ok(eq(&NodeValue::None)),),
        xor_true_left_none_right: (&OP_XOR, NodeValue::Bool(true), NodeValue::None, ok(eq(&NodeValue::None)),),
        xor_false_left_none_right: (&OP_XOR, NodeValue::Bool(false), NodeValue::None, ok(eq(&NodeValue::None)),),
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
                a.operate_sync(operator, || &b),
                matcher,
            );
        },

        add_integers: (&OP_ADD, NodeValue::Int32(5), NodeValue::Int32(20), ok(eq(&NodeValue::Int64(25))),),
        add_floats: (&OP_ADD, NodeValue::Float(5.2), NodeValue::Double(20.1), ok(eq(&NodeValue::Double(25.3))),),
        add_none_left: (&OP_ADD, NodeValue::None, NodeValue::Int32(1), ok(eq(&NodeValue::None)),),
        add_none_right: (&OP_ADD, NodeValue::Int32(1), NodeValue::None, ok(eq(&NodeValue::None)),),
        add_non_numeric_left_fails: (&OP_ADD, NodeValue::Bool(true), NodeValue::Int32(1), err(pat!(NodeEvaluationError::ArithmeticOperatorIncompatibleOperands { .. })),),
        add_non_numeric_right_fails: (&OP_ADD, NodeValue::Int32(1), NodeValue::Bool(true), err(pat!(NodeEvaluationError::ArithmeticOperatorIncompatibleOperands { .. })),),

        sub_integers: (&OP_SUBTRACT, NodeValue::Int32(5), NodeValue::Int32(20), ok(eq(&NodeValue::Int64(-15))),),
        sub_floats: (&OP_SUBTRACT, NodeValue::Float(5.2), NodeValue::Double(20.1), ok(eq(&NodeValue::Double(-14.9))),),
        sub_none_left: (&OP_SUBTRACT, NodeValue::None, NodeValue::Int32(1), ok(eq(&NodeValue::None)),),
        sub_none_right: (&OP_SUBTRACT, NodeValue::Int32(1), NodeValue::None, ok(eq(&NodeValue::None)),),
        sub_non_numeric_left_fails: (&OP_SUBTRACT, NodeValue::Bool(true), NodeValue::Int32(1), err(pat!(NodeEvaluationError::ArithmeticOperatorIncompatibleOperands { .. })),),
        sub_non_numeric_right_fails: (&OP_SUBTRACT, NodeValue::Int32(1), NodeValue::Bool(true), err(pat!(NodeEvaluationError::ArithmeticOperatorIncompatibleOperands)),),

        multiply: (&OP_MULTIPLY, NodeValue::Int32(5), NodeValue::Float(0.5), ok(eq(&NodeValue::Double(2.5))),),
        multiply_none_left: (&OP_MULTIPLY, NodeValue::None, NodeValue::Int32(1), ok(eq(&NodeValue::None)),),
        multiply_none_right: (&OP_MULTIPLY, NodeValue::Int32(1), NodeValue::None, ok(eq(&NodeValue::None)),),
        multiply_non_numeric_left_fails: (&OP_MULTIPLY, NodeValue::Bool(true), NodeValue::Int32(1), err(pat!(NodeEvaluationError::ArithmeticOperatorIncompatibleOperands)),),
        multiply_non_numeric_right_fails: (&OP_MULTIPLY, NodeValue::Int32(1), NodeValue::Bool(true), err(pat!(NodeEvaluationError::ArithmeticOperatorIncompatibleOperands)),),

        divide: (&OP_DIVIDE, NodeValue::Int32(5), NodeValue::Int32(2), ok(eq(&NodeValue::Double(2.5))),),
        divide_by_zero_fails: (&OP_DIVIDE, NodeValue::Int32(5), NodeValue::Int32(0), err(pat!(NodeEvaluationError::DivideByZero { .. })),),
        divide_none_left: (&OP_DIVIDE, NodeValue::None, NodeValue::Int32(1), ok(eq(&NodeValue::None)),),
        divide_none_right: (&OP_DIVIDE, NodeValue::Int32(1), NodeValue::None, ok(eq(&NodeValue::None)),),
        divide_non_numeric_left_fails: (&OP_DIVIDE, NodeValue::Bool(true), NodeValue::Int32(1), err(pat!(NodeEvaluationError::ArithmeticOperatorIncompatibleOperands)),),
        divide_non_numeric_right_fails: (&OP_DIVIDE, NodeValue::Int32(1), NodeValue::Bool(true), err(pat!(NodeEvaluationError::ArithmeticOperatorIncompatibleOperands)),),

        modulo_trunc_1: (&OP_MODULO_TRUNC, NodeValue::Int32(8), NodeValue::Int32(3), ok(eq(&NodeValue::Int64(2))),),
        modulo_trunc_2: (&OP_MODULO_TRUNC, NodeValue::Int32(8), NodeValue::Int32(-3), ok(eq(&NodeValue::Int64(2))),),
        modulo_trunc_3: (&OP_MODULO_TRUNC, NodeValue::Int32(-8), NodeValue::Int32(3), ok(eq(&NodeValue::Int64(-2))),),
        modulo_trunc_4: (&OP_MODULO_TRUNC, NodeValue::Int32(-8), NodeValue::Int32(-3), ok(eq(&NodeValue::Int64(-2))),),
        modulo_trunc_by_zero_fails: (&OP_MODULO_TRUNC, NodeValue::Int32(5), NodeValue::Int32(0), err(pat!(NodeEvaluationError::DivideByZero { .. })),),
        modulo_trunc_none_left: (&OP_MODULO_TRUNC, NodeValue::None, NodeValue::Int32(1), ok(eq(&NodeValue::None)),),
        modulo_trunc_none_right: (&OP_MODULO_TRUNC, NodeValue::Int32(1), NodeValue::None, ok(eq(&NodeValue::None)),),
        modulo_trunc_float_left_fails: (&OP_MODULO_TRUNC, NodeValue::Float(5.0), NodeValue::Int32(1), err(pat!(NodeEvaluationError::ModuloOperatorIncompatibleOperands { .. })),),
        modulo_trunc_float_right_fails: (&OP_MODULO_TRUNC, NodeValue::Int32(1), NodeValue::Float(5.0), err(pat!(NodeEvaluationError::ModuloOperatorIncompatibleOperands { .. })),),
        modulo_trunc_non_numeric_left_fails: (&OP_MODULO_TRUNC, NodeValue::Bool(true), NodeValue::Int32(1), err(pat!(NodeEvaluationError::ModuloOperatorIncompatibleOperands { .. })),),
        modulo_trunc_non_numeric_right_fails: (&OP_MODULO_TRUNC, NodeValue::Int32(1), NodeValue::Bool(true), err(pat!(NodeEvaluationError::ModuloOperatorIncompatibleOperands { .. })),),

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
                a.operate_sync(operator, || &b),
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

        eq_list_bool_all_eq: (&OP_EQ, NodeValue::BoolList(vec![true, true, true]), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(true))),),
        eq_list_bool_none_eq: (&OP_EQ, NodeValue::BoolList(vec![true, true, true]), NodeValue::Bool(false), ok(eq(&NodeValue::Bool(false))),),
        eq_list_bool_some_eq_true: (&OP_EQ, NodeValue::BoolList(vec![true, false, false]), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(false))),),
        eq_list_bool_some_eq_false: (&OP_EQ, NodeValue::BoolList(vec![true, false, false]), NodeValue::Bool(false), ok(eq(&NodeValue::Bool(false))),),
        eq_list_int32_not_eq: (&OP_EQ, NodeValue::Int32List(vec![3, 30, 300]), NodeValue::Int32(0), ok(eq(&NodeValue::Bool(false))),),
        eq_list_int32_all_eq: (&OP_EQ, NodeValue::Int32List(vec![0, 0]), NodeValue::Int32(0), ok(eq(&NodeValue::Bool(true))),),
        eq_list_int64_not_eq: (&OP_EQ, NodeValue::Int64List(vec![3, 30, 300]), NodeValue::Int32(0), ok(eq(&NodeValue::Bool(false))),),
        eq_list_int64_all_eq: (&OP_EQ, NodeValue::Int64List(vec![0, 0]), NodeValue::Int32(0), ok(eq(&NodeValue::Bool(true))),),
        eq_list_float_not_eq: (&OP_EQ, NodeValue::FloatList(vec![0.5, std::f32::consts::PI, 10.0]), NodeValue::Double(0.5), ok(eq(&NodeValue::Bool(false))),),
        eq_list_float_all_eq: (&OP_EQ, NodeValue::FloatList(vec![0.5, 0.5]), NodeValue::Double(0.5), ok(eq(&NodeValue::Bool(true))),),
        eq_list_double_not_eq: (&OP_EQ, NodeValue::DoubleList(vec![0.5, std::f64::consts::PI, 10.0]), NodeValue::Double(0.5), ok(eq(&NodeValue::Bool(false))),),
        eq_list_double_all_eq: (&OP_EQ, NodeValue::DoubleList(vec![0.5, 0.5]), NodeValue::Float(0.5), ok(eq(&NodeValue::Bool(true))),),
        eq_list_msg_all_eq: (&OP_EQ, NodeValue::MessageList(vec![Arc::<MockDynamicMessage>::clone(&*TEST_DYNAMIC_MESSAGE), Arc::<MockDynamicMessage>::clone(&*TEST_DYNAMIC_MESSAGE)]), NodeValue::Message(Arc::<MockDynamicMessage>::clone(&*TEST_DYNAMIC_MESSAGE)), ok(eq(&NodeValue::Bool(true))),),
        eq_list_msg_not_eq: (&OP_EQ, NodeValue::MessageList(vec![Arc::<MockDynamicMessage>::clone(&*TEST_DYNAMIC_MESSAGE), Arc::<MockDynamicMessage>::clone(&*TEST_DYNAMIC_MESSAGE)]), NodeValue::Message(Arc::new(MockDynamicMessage::new(TestMessage::descriptor()))), ok(eq(&NodeValue::Bool(false))),),
        eq_list_none_left: (&OP_EQ, NodeValue::None, NodeValue::Bool(false), ok(eq(&NodeValue::None)),),
        eq_list_none_right: (&OP_EQ, NodeValue::Int64List(vec![42]), NodeValue::None, ok(eq(&NodeValue::None)),),
        eq_list_incompatible_types: (&OP_EQ, NodeValue::BoolList(vec![true, true, false]), NodeValue::Float(5.5), err(pat!(NodeEvaluationError::ComparisonOperatorListIncompatibleOperands { .. })),),
        eq_list_list: (&OP_EQ, NodeValue::BoolList(vec![true]), NodeValue::BoolList(vec![true]), err(pat!(NodeEvaluationError::ComparisonOperatorListIncompatibleOperands { .. })),),

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
        gt_7: (&OP_GT, NodeValue::Float(5.5), NodeValue::Bool(true), err(pat!(NodeEvaluationError::ComparisonOperatorIncomptaibleOperands { .. })),),
        gt_none_left: (&OP_GT, NodeValue::None, NodeValue::Int32(1), ok(eq(&NodeValue::None)),),
        gt_none_right: (&OP_GT, NodeValue::Int32(1), NodeValue::None, ok(eq(&NodeValue::None)),),

        gte_1: (&OP_GT_OR_EQ, NodeValue::Bool(true), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(true))),),
        gte_2: (&OP_GT_OR_EQ, ONE_EPSILON, ONE, ok(eq(&NodeValue::Bool(true))),),
        gte_3: (&OP_GT_OR_EQ, NodeValue::Int32(2), NodeValue::Int32(2), ok(eq(&NodeValue::Bool(true))),),
        gte_4: (&OP_GT_OR_EQ, NodeValue::Int32(2), NodeValue::Float(5.5), ok(eq(&NodeValue::Bool(false))),),
        gte_5: (&OP_GT_OR_EQ, NodeValue::Float(5.5), NodeValue::Int32(2), ok(eq(&NodeValue::Bool(true))),),
        gte_6: (&OP_GT_OR_EQ, NodeValue::Float(5.5), NodeValue::Bool(true), err(pat!(NodeEvaluationError::ComparisonOperatorIncomptaibleOperands { .. })),),
        gte_none_left: (&OP_GT_OR_EQ, NodeValue::None, NodeValue::Int32(1), ok(eq(&NodeValue::None)),),
        gte_none_right: (&OP_GT_OR_EQ, NodeValue::Int32(1), NodeValue::None, ok(eq(&NodeValue::None)),),

        lt_1: (&OP_LT, NodeValue::Bool(true), NodeValue::Bool(false), ok(eq(&NodeValue::Bool(false))),),
        lt_2: (&OP_LT, NodeValue::Bool(false), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(true))),),
        lt_3: (&OP_LT, ONE, ONE_EPSILON, ok(eq(&NodeValue::Bool(false))),),
        lt_4: (&OP_LT, NodeValue::Int32(2), NodeValue::Int32(2), ok(eq(&NodeValue::Bool(false))),),
        lt_5: (&OP_LT, NodeValue::Int32(2), NodeValue::Float(5.5), ok(eq(&NodeValue::Bool(true))),),
        lt_6: (&OP_LT, NodeValue::Float(5.5), NodeValue::Int32(2), ok(eq(&NodeValue::Bool(false))),),
        lt_7: (&OP_LT, NodeValue::Float(5.5), NodeValue::Bool(true), err(pat!(NodeEvaluationError::ComparisonOperatorIncomptaibleOperands { .. })),),
        lt_none_left: (&OP_LT, NodeValue::None, NodeValue::Int32(1), ok(eq(&NodeValue::None)),),
        lt_none_right: (&OP_LT, NodeValue::Int32(1), NodeValue::None, ok(eq(&NodeValue::None)),),

        lte_1: (&OP_LT_OR_EQ, NodeValue::Bool(true), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(true))),),
        lte_2: (&OP_LT_OR_EQ, ONE_EPSILON, ONE, ok(eq(&NodeValue::Bool(true))),),
        lte_3: (&OP_LT_OR_EQ, NodeValue::Int32(2), NodeValue::Int32(2), ok(eq(&NodeValue::Bool(true))),),
        lte_4: (&OP_LT_OR_EQ, NodeValue::Int32(2), NodeValue::Float(5.5), ok(eq(&NodeValue::Bool(true))),),
        lte_5: (&OP_LT_OR_EQ, NodeValue::Float(5.5), NodeValue::Int32(2), ok(eq(&NodeValue::Bool(false))),),
        lte_6: (&OP_LT_OR_EQ, NodeValue::Float(5.5), NodeValue::Bool(true), err(pat!(NodeEvaluationError::ComparisonOperatorIncomptaibleOperands { .. })),),
        lte_none_left: (&OP_LT_OR_EQ, NodeValue::None, NodeValue::Int32(1), ok(eq(&NodeValue::None)),),
        lte_none_right: (&OP_LT_OR_EQ, NodeValue::Int32(1), NodeValue::None, ok(eq(&NodeValue::None)),),

        contains_bool_1: (&OP_CONTAINS, NodeValue::BoolList(vec![true, true, true]), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(true))),),
        contains_bool_2: (&OP_CONTAINS, NodeValue::BoolList(vec![true, true, true]), NodeValue::Bool(false), ok(eq(&NodeValue::Bool(false))),),
        contains_int_1: (&OP_CONTAINS, NodeValue::Int32List(vec![3, 30, 300]), NodeValue::Int32(30), ok(eq(&NodeValue::Bool(true))),),
        contains_int_2: (&OP_CONTAINS, NodeValue::Int32List(vec![3, 30, 300]), NodeValue::Int64(30), ok(eq(&NodeValue::Bool(true))),),
        contains_int_3: (&OP_CONTAINS, NodeValue::Int32List(vec![3, 30, 300]), NodeValue::Int32(0), ok(eq(&NodeValue::Bool(false))),),
        contains_int64_1: (&OP_CONTAINS, NodeValue::Int64List(vec![3, 30, 300]), NodeValue::Int32(30), ok(eq(&NodeValue::Bool(true))),),
        contains_int64_2: (&OP_CONTAINS, NodeValue::Int64List(vec![3, 30, 300]), NodeValue::Int64(30), ok(eq(&NodeValue::Bool(true))),),
        contains_int64_3: (&OP_CONTAINS, NodeValue::Int64List(vec![3, 30, 300]), NodeValue::Int32(0), ok(eq(&NodeValue::Bool(false))),),
        contains_float_1: (&OP_CONTAINS, NodeValue::FloatList(vec![0.5, PIf32, 10.0]), NodeValue::Float(0.5), ok(eq(&NodeValue::Bool(true))),),
        contains_float_2: (&OP_CONTAINS, NodeValue::FloatList(vec![0.5, PIf32, 10.0]), NodeValue::Double(PIf64), ok(eq(&NodeValue::Bool(true))),),
        contains_float_3: (&OP_CONTAINS, NodeValue::FloatList(vec![0.5, PIf32, 10.0]), NodeValue::Int32(10), ok(eq(&NodeValue::Bool(true))),),
        contains_float_4: (&OP_CONTAINS, NodeValue::FloatList(vec![0.5, PIf32, 10.0]), NodeValue::Float(30.0), ok(eq(&NodeValue::Bool(false))),),
        contains_approximately_equal: (&OP_CONTAINS, NodeValue::DoubleList(vec![ONE.get_double().unwrap()]), ONE_EPSILON, ok(eq(&NodeValue::Bool(true))),),
        contains_message_1: (&OP_CONTAINS, NodeValue::MessageList(vec![Arc::<MockDynamicMessage>::clone(&*TEST_DYNAMIC_MESSAGE)]), NodeValue::Message(Arc::<MockDynamicMessage>::clone(&*TEST_DYNAMIC_MESSAGE)), ok(eq(&NodeValue::Bool(true))),),
        contains_message_2: (&OP_CONTAINS, NodeValue::MessageList(vec![Arc::<MockDynamicMessage>::clone(&*TEST_DYNAMIC_MESSAGE)]), NodeValue::Message(Arc::new(MockDynamicMessage::new(TestMessage::descriptor()))), ok(eq(&NodeValue::Bool(false))),),
        contains_none_left: (&OP_CONTAINS, NodeValue::None, NodeValue::Int32(2), ok(eq(&NodeValue::None)),),
        contains_none_right: (&OP_CONTAINS, NodeValue::Int32List(vec![2]), NodeValue::None, ok(eq(&NodeValue::None)),),
        contains_not_a_list: (&OP_CONTAINS, NodeValue::Int32(2), NodeValue::Int32(2), err(pat!(NodeEvaluationError::ContainsOperatorIncompatibleOperands { .. })),),
        contains_incompatible_types_1: (&OP_CONTAINS, NodeValue::BoolList(vec![true, true, false]), NodeValue::Float(5.5), err(pat!(NodeEvaluationError::ContainsOperatorIncompatibleOperands { .. })),),
        contains_incompatible_types_2: (&OP_CONTAINS, NodeValue::Int32List(vec![1, 2, 3]), NodeValue::Float(2.0), err(pat!(NodeEvaluationError::ContainsOperatorIncompatibleOperands { .. })),),
        contains_incompatible_types_3: (&OP_CONTAINS, NodeValue::MessageList(vec![Arc::<MockDynamicMessage>::clone(&*TEST_DYNAMIC_MESSAGE)]), NodeValue::Bool(false), err(pat!(NodeEvaluationError::ContainsOperatorIncompatibleOperands { .. })),),

        not_contains_bool_1: (&OP_DOES_NOT_CONTAIN, NodeValue::BoolList(vec![true, true, true]), NodeValue::Bool(true), ok(eq(&NodeValue::Bool(false))),),
        not_contains_bool_2: (&OP_DOES_NOT_CONTAIN, NodeValue::BoolList(vec![true, true, true]), NodeValue::Bool(false), ok(eq(&NodeValue::Bool(true))),),
        not_contains_int_1: (&OP_DOES_NOT_CONTAIN, NodeValue::Int32List(vec![3, 30, 300]), NodeValue::Int32(30), ok(eq(&NodeValue::Bool(false))),),
        not_contains_int_2: (&OP_DOES_NOT_CONTAIN, NodeValue::Int32List(vec![3, 30, 300]), NodeValue::Int64(30), ok(eq(&NodeValue::Bool(false))),),
        not_contains_int_3: (&OP_DOES_NOT_CONTAIN, NodeValue::Int32List(vec![3, 30, 300]), NodeValue::Int32(0), ok(eq(&NodeValue::Bool(true))),),
        not_contains_int64_1: (&OP_DOES_NOT_CONTAIN, NodeValue::Int64List(vec![3, 30, 300]), NodeValue::Int32(30), ok(eq(&NodeValue::Bool(false))),),
        not_contains_int64_2: (&OP_DOES_NOT_CONTAIN, NodeValue::Int64List(vec![3, 30, 300]), NodeValue::Int64(30), ok(eq(&NodeValue::Bool(false))),),
        not_contains_int64_3: (&OP_DOES_NOT_CONTAIN, NodeValue::Int64List(vec![3, 30, 300]), NodeValue::Int32(0), ok(eq(&NodeValue::Bool(true))),),
        not_contains_float_1: (&OP_DOES_NOT_CONTAIN, NodeValue::FloatList(vec![0.5, PIf32, 10.0]), NodeValue::Float(0.5), ok(eq(&NodeValue::Bool(false))),),
        not_contains_float_2: (&OP_DOES_NOT_CONTAIN, NodeValue::FloatList(vec![0.5, PIf32, 10.0]), NodeValue::Double(PIf64), ok(eq(&NodeValue::Bool(false))),),
        not_contains_float_3: (&OP_DOES_NOT_CONTAIN, NodeValue::FloatList(vec![0.5, PIf32, 10.0]), NodeValue::Int32(10), ok(eq(&NodeValue::Bool(false))),),
        not_contains_float_4: (&OP_DOES_NOT_CONTAIN, NodeValue::FloatList(vec![0.5, PIf32, 10.0]), NodeValue::Float(30.0), ok(eq(&NodeValue::Bool(true))),),
        not_contains_approximately_equal: (&OP_DOES_NOT_CONTAIN, NodeValue::DoubleList(vec![ONE.get_double().unwrap()]), ONE_EPSILON, ok(eq(&NodeValue::Bool(false))),),
        not_contains_message_1: (&OP_DOES_NOT_CONTAIN, NodeValue::MessageList(vec![Arc::<MockDynamicMessage>::clone(&*TEST_DYNAMIC_MESSAGE)]), NodeValue::Message(Arc::<MockDynamicMessage>::clone(&*TEST_DYNAMIC_MESSAGE)), ok(eq(&NodeValue::Bool(false))),),
        not_contains_message_2: (&OP_DOES_NOT_CONTAIN, NodeValue::MessageList(vec![Arc::<MockDynamicMessage>::clone(&*TEST_DYNAMIC_MESSAGE)]), NodeValue::Message(Arc::new(MockDynamicMessage::new(TestMessage::descriptor()))), ok(eq(&NodeValue::Bool(true))),),
        not_contains_none_left: (&OP_DOES_NOT_CONTAIN, NodeValue::None, NodeValue::Int32(2), ok(eq(&NodeValue::None)),),
        not_contains_none_right: (&OP_DOES_NOT_CONTAIN, NodeValue::Int32List(vec![2]), NodeValue::None, ok(eq(&NodeValue::None)),),
        not_contains_not_a_list: (&OP_DOES_NOT_CONTAIN, NodeValue::Int32(2), NodeValue::Int32(2), err(pat!(NodeEvaluationError::ContainsOperatorIncompatibleOperands { .. })),),
        not_contains_incompatible_types_1: (&OP_DOES_NOT_CONTAIN, NodeValue::BoolList(vec![true, true, false]), NodeValue::Float(5.5), err(pat!(NodeEvaluationError::ContainsOperatorIncompatibleOperands { .. })),),
        not_contains_incompatible_types_2: (&OP_DOES_NOT_CONTAIN, NodeValue::Int32List(vec![1, 2, 3]), NodeValue::Float(2.0), err(pat!(NodeEvaluationError::ContainsOperatorIncompatibleOperands { .. })),),
        not_contains_incompatible_types_3: (&OP_DOES_NOT_CONTAIN, NodeValue::MessageList(vec![Arc::<MockDynamicMessage>::clone(&*TEST_DYNAMIC_MESSAGE)]), NodeValue::Bool(false), err(pat!(NodeEvaluationError::ContainsOperatorIncompatibleOperands { .. })),),
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
        floor_6: (&OP_FLOOR, NodeValue::Int64(-1), ok(eq(&NodeValue::Int64(-1))),),
        floor_none: (&OP_FLOOR, NodeValue::None, ok(eq(&NodeValue::None)),),
        floor_non_numeric_fails: (&OP_FLOOR, NodeValue::Bool(true), err(pat!(NodeEvaluationError::RoundingOperatorIncompatibleOperands { .. })),),

        round_1: (&OP_ROUND, NodeValue::Double(0.1), ok(eq(&NodeValue::Int64(0))),),
        round_2: (&OP_ROUND, NodeValue::Float(2.9), ok(eq(&NodeValue::Int64(3))),),
        round_3: (&OP_ROUND, NodeValue::Float(5.0), ok(eq(&NodeValue::Int64(5))),),
        round_4: (&OP_ROUND, NodeValue::Double(-0.33), ok(eq(&NodeValue::Int64(0))),),
        round_5: (&OP_ROUND, NodeValue::Double(-0.99), ok(eq(&NodeValue::Int64(-1))),),
        round_6: (&OP_ROUND, NodeValue::Int64(-1), ok(eq(&NodeValue::Int64(-1))),),
        round_none: (&OP_ROUND, NodeValue::None, ok(eq(&NodeValue::None)),),
        round_non_numeric_fails: (&OP_ROUND, NodeValue::Bool(true), err(pat!(NodeEvaluationError::RoundingOperatorIncompatibleOperands { .. })),),

        ceil_1: (&OP_CEIL, NodeValue::Double(0.1), ok(eq(&NodeValue::Int64(1))),),
        ceil_2: (&OP_CEIL, NodeValue::Float(2.9), ok(eq(&NodeValue::Int64(3))),),
        ceil_3: (&OP_CEIL, NodeValue::Float(5.0), ok(eq(&NodeValue::Int64(5))),),
        ceil_4: (&OP_CEIL, NodeValue::Double(-0.33), ok(eq(&NodeValue::Int64(0))),),
        ceil_5: (&OP_CEIL, NodeValue::Double(-0.99), ok(eq(&NodeValue::Int64(0))),),
        ceil_6: (&OP_CEIL, NodeValue::Int64(-1), ok(eq(&NodeValue::Int64(-1))),),
        ceil_none: (&OP_ROUND, NodeValue::None, ok(eq(&NodeValue::None)),),
        ceil_non_numeric_fails: (&OP_CEIL, NodeValue::Bool(true), err(pat!(NodeEvaluationError::RoundingOperatorIncompatibleOperands { .. })),),
    );
}
