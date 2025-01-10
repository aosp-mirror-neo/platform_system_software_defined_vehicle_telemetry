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
use std::{borrow::Cow, convert::From, error::Error, sync::Arc};

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

    /// Returns the bool value or a custom error message if NodeValue is not a bool.
    pub fn get_bool<E>(&self, err_provider: &impl Fn() -> E) -> Result<bool, E>
    where
        E: Error,
    {
        if let NodeValue::Bool(val) = self {
            Ok(*val)
        } else {
            Err(err_provider())
        }
    }

    /// Returns the int value or the provided error if NodeValue is not an int.
    pub fn get_int<E>(&self, err_provider: &impl Fn() -> E) -> Result<i64, E>
    where
        E: Error,
    {
        Ok(match self {
            NodeValue::Int32(val) => i64::from(*val),
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
            NodeValue::Int32(val) => f64::from(*val),
            NodeValue::Int64(val) => *val as f64,
            NodeValue::Float(val) => f64::from(*val),
            NodeValue::Double(val) => *val,
            _ => return Err(err_provider()),
        })
    }

    /// Returns the Message value or the provided error if NodeValue is not a Message.
    pub fn get_message<E>(
        &self,
        err_provider: impl FnOnce() -> E,
    ) -> Result<&Arc<dyn MessageDyn>, E>
    where
        E: Error,
    {
        if let NodeValue::Message(msg) = self {
            Ok(msg)
        } else {
            Err(err_provider())
        }
    }

    /// Returns the string value or the provided error if NodeValue is not a string.
    pub fn get_string<E>(&self, err_provider: &impl Fn() -> E) -> Result<&str, E>
    where
        E: Error,
    {
        if let NodeValue::String(val) = self {
            Ok(val)
        } else {
            Err(err_provider())
        }
    }

    /// Returns the bytes value or the provided error if NodeValue is not bytes.
    pub fn get_bytes<E>(&self, err_provider: &impl Fn() -> E) -> Result<&[u8], E>
    where
        E: Error,
    {
        if let NodeValue::ByteList(val) = self {
            Ok(val)
        } else {
            Err(err_provider())
        }
    }

    /// Check if node value is none.
    pub fn is_none(&self) -> bool {
        matches!(self, NodeValue::None)
    }

    /// Check if node holds a numeric value.
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            NodeValue::Int32(_) | NodeValue::Int64(_) | NodeValue::Float(_) | NodeValue::Double(_)
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

        if !self.is_numeric() {
            return Err(err_provider()).into();
        }

        match op {
            ArithmeticOperator::ABSOLUTE => self.handle_unary(state, |value| {
                value.apply_numeric_unary(
                    |a| NodeValue::Int64(a.abs()),
                    |a| NodeValue::Double(a.abs()),
                    &err_provider,
                )
            }),
            ArithmeticOperator::ADD => {
                self.handle_binary(state, right_operand_provider, move |left, right| {
                    left.apply_numeric_binary(
                        right,
                        |a, b| NodeValue::Int64(a + b),
                        |a, b| NodeValue::Double(a + b),
                        &err_provider,
                    )
                })
            }
            ArithmeticOperator::DIVIDE => {
                self.handle_binary(state, right_operand_provider, move |left, right| {
                    let left_double = left.get_double(&err_provider)?;
                    let right_double = right.get_double(&err_provider)?;

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
                    let dividend = left.get_int(&err_provider)?;
                    let divisor = right.get_int(&err_provider)?;

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
                        &err_provider,
                    )
                })
            }
            ArithmeticOperator::POWER => {
                self.handle_binary(state, right_operand_provider, move |left, right| {
                    left.apply_numeric_binary(
                        right,
                        |a, b| NodeValue::Int64(a.pow(b as u32)),
                        |a, b| NodeValue::Double(a.powf(b)),
                        &err_provider,
                    )
                })
            }
            ArithmeticOperator::SUBTRACT => {
                self.handle_binary(state, right_operand_provider, move |left, right| {
                    left.apply_numeric_binary(
                        right,
                        |a, b| NodeValue::Int64(a - b),
                        |a, b| NodeValue::Double(a - b),
                        &err_provider,
                    )
                })
            }
            ArithmeticOperator::UNARY_MINUS => self.handle_unary(state, |value| {
                value.apply_numeric_unary(
                    |a| NodeValue::Int64(-a),
                    |a| NodeValue::Double(-a),
                    &err_provider,
                )
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

        let left_value = match self.get_bool(&err_provider) {
            Ok(value) => value,
            Err(err) => return Err(err).into(),
        };

        match op {
            LogicalOperator::AND => {
                if !left_value {
                    // Short-circuit and don't evaluate right operand if the left operand is false.
                    NodeValue::Bool(false).into_operation_result(state)
                } else {
                    // If left is true, the result depends on the right operand.
                    self.handle_binary(state, right_operand_provider, move |_left, right| {
                        Ok(NodeValue::Bool(right.get_bool(&err_provider)?))
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
                        Ok(NodeValue::Bool(right.get_bool(&err_provider)?))
                    })
                }
            }
            LogicalOperator::XOR => {
                self.handle_binary(state, right_operand_provider, move |_left, right| {
                    Ok(NodeValue::Bool(left_value != right.get_bool(&err_provider)?))
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
                if !self.is_numeric() || !other.is_numeric() {
                    return Ok(NodeValue::Bool(self == other));
                }
                self.apply_numeric_binary(other, |a, b| a == b, approximately_equal, &err_provider)
            }
            RelationalOperator::NOT_EQ => {
                if !self.is_numeric() || !other.is_numeric() {
                    return Ok(NodeValue::Bool(self != other));
                }
                self.apply_numeric_binary(
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
                |a, b| a > b && !approximately_equal(a, b),
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
                |a, b| a < b && !approximately_equal(a, b),
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

    /// Handle rounding operations.
    fn handle_rounding(&self, op: &RoundingOperator) -> Result<NodeValue, NodeEvaluationError> {
        let err_provider = || NodeEvaluationError::RoundingOperatorIncompatibleOperands;
        if !matches!(self, NodeValue::Float(_) | NodeValue::Double(_)) {
            return Err(err_provider());
        }
        let value = self.get_double(&err_provider)?;
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
            NodeValue::BoolList(v) => v.contains(&element.get_bool(&err_provider)?),
            NodeValue::Int32List(v) => {
                let target_value = element.get_int(&err_provider)? as i32;
                v.contains(&target_value)
            }
            NodeValue::Int64List(v) => v.contains(&element.get_int(&err_provider)?),
            NodeValue::FloatList(v) => {
                let target_value = element.get_double(&err_provider)?;
                v.iter().any(|x| approximately_equal(f64::from(*x), target_value))
            }
            NodeValue::DoubleList(v) => {
                let target_value = element.get_double(&err_provider)?;
                v.iter().any(|x| approximately_equal(*x, target_value))
            }
            NodeValue::MessageList(v) => {
                let target_value = element.get_message(err_provider)?;
                let mode = ReflectEqMode::default();
                v.iter().any(|x| x.reflect_eq_dyn(target_value.as_ref(), &mode))
            }
            _ => return Err(err_provider()),
        })
    }

    /// Check if all list items are equal to some element.
    /// Returns errors if `self` is not a list value or `element` is not a compatible type.
    fn all_equals(&self, element: &NodeValue) -> Result<bool, NodeEvaluationError> {
        let err_provider = || NodeEvaluationError::AllEqualsOperatorIncompatibleOperands;
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
                v.iter().all(|x| approximately_equal(f64::from(*x), target_value))
            }
            NodeValue::DoubleList(v) => {
                let target_value = element.get_double(&err_provider)?;
                v.iter().all(|x| approximately_equal(*x, target_value))
            }
            NodeValue::MessageList(v) => {
                let target_value = element.get_message(err_provider)?;
                let mode = ReflectEqMode::default();
                v.iter().all(|x| x.reflect_eq_dyn(target_value.as_ref(), &mode))
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
    fn apply_numeric_binary<R>(
        &self,
        other: &NodeValue,
        int_func: impl FnOnce(i64, i64) -> R,
        float_func: impl FnOnce(f64, f64) -> R,
        err_provider: &impl Fn() -> NodeEvaluationError,
    ) -> Result<R, NodeEvaluationError> {
        if self.is_int() && other.is_int() {
            Ok(int_func(self.get_int(err_provider)?, other.get_int(err_provider)?))
        } else if self.is_numeric() && other.is_numeric() {
            Ok(float_func(self.get_double(err_provider)?, other.get_double(err_provider)?))
        } else {
            Err(err_provider())
        }
    }

    /// Applies the provided function on a numeric NodeValue based on
    /// the numeric type (integer or floating point).
    /// Returns an error if the NodeValues are not numeric types.
    fn apply_numeric_unary<R>(
        &self,
        int_func: impl FnOnce(i64) -> R,
        float_func: impl FnOnce(f64) -> R,
        err_provider: &impl Fn() -> NodeEvaluationError,
    ) -> Result<R, NodeEvaluationError> {
        if self.is_int() {
            Ok(int_func(self.get_int(err_provider)?))
        } else if self.is_numeric() {
            Ok(float_func(self.get_double(err_provider)?))
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
        err_provider: &impl Fn() -> NodeEvaluationError,
    ) -> Result<R, NodeEvaluationError> {
        if self.is_bool() && other.is_bool() {
            Ok(bool_func(self.get_bool(err_provider)?, other.get_bool(err_provider)?))
        } else {
            self.apply_numeric_binary(other, int_func, float_func, err_provider)
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

    use parking_lot::Mutex;
    use protobuf::reflect::ReflectEqMode;
    use sdv_telemetry_proto_rust::expression::ConstantLeafNode;
    use sdv_telemetry_test_proto::test_message::{TestEnum, TestMessage};

    use crate::expr::mock::*;

    fn provide(value: &NodeValue) -> Result<NodeValue, NodeEvaluationError> {
        Ok(value.clone())
    }

    #[test]
    fn from_constant_leaf_node() -> Result<(), NodeEvaluationError> {
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
    fn extract_value_no_field_names_ok() -> Result<(), NodeEvaluationError> {
        // setup
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;

        // action
        let node_value = NodeValue::extract_value(Cow::Borrowed(&msg), &[])?;

        // assert
        if let NodeValue::Message(node_msg) = node_value {
            assert!(node_msg.reflect_eq_dyn(msg.as_ref(), &ReflectEqMode::default()));
        } else {
            panic!("Expect Message variant of NodeValue, got {:?}", node_value);
        }
        Ok(())
    }

    #[test]
    fn extract_value_explicitly_optional_field_not_set_returns_none(
    ) -> Result<(), NodeEvaluationError> {
        let msg = Arc::new(TestMessage::new()) as Arc<dyn MessageDyn>;

        assert_eq!(
            NodeValue::extract_value(Cow::Borrowed(&msg), &["optional_bool_field".to_string()])?,
            NodeValue::None
        );
        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &["optional_int64_field".to_string()])?,
            NodeValue::None
        );
        Ok(())
    }

    #[test]
    fn extract_value_primitive_field_not_set_uses_default_values() -> Result<(), NodeEvaluationError>
    {
        let msg = Arc::new(TestMessage::new()) as Arc<dyn MessageDyn>;

        assert_eq!(
            NodeValue::extract_value(Cow::Borrowed(&msg), &["int_field".to_string()])?,
            NodeValue::Int32(0)
        );
        assert_eq!(
            NodeValue::extract_value(Cow::Borrowed(&msg), &["float_field".to_string()])?,
            NodeValue::Float(0.0)
        );
        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &["enum_field".to_string()])?,
            NodeValue::Int32(0)
        );
        Ok(())
    }

    #[test]
    fn extract_value_message_field_not_set_returns_none() -> Result<(), NodeEvaluationError> {
        let msg = Arc::new(TestMessage::new()) as Arc<dyn MessageDyn>;

        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &["message_field".to_string()])?,
            NodeValue::None
        );
        Ok(())
    }

    #[test]
    fn extract_value_repeated_field_not_set_returns_empty_list() -> Result<(), NodeEvaluationError>
    {
        let msg = Arc::new(TestMessage::new()) as Arc<dyn MessageDyn>;

        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &["repeated_bools".to_string()])?,
            NodeValue::BoolList(vec![])
        );
        Ok(())
    }

    #[test]
    fn extract_value_int_field_ok() -> Result<(), NodeEvaluationError> {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["int_field".to_string()];

        assert_eq!(NodeValue::extract_value(Cow::Owned(msg), &field_names)?, NodeValue::Int32(80));
        Ok(())
    }

    #[test]
    fn extract_value_float_field_ok() -> Result<(), NodeEvaluationError> {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["float_field".to_string()];

        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &field_names)?,
            NodeValue::Float(100.0)
        );
        Ok(())
    }

    #[test]
    fn extract_value_enum_field_ok() -> Result<(), NodeEvaluationError> {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["enum_field".to_string()];

        assert_eq!(NodeValue::extract_value(Cow::Owned(msg), &field_names)?, NodeValue::Int32(1));
        Ok(())
    }

    #[test]
    fn extract_value_message_field_ok() -> Result<(), NodeEvaluationError> {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["message_field".to_string()];
        let expected_value =
            NodeValue::Message(Arc::new(TEST_MESSAGE.message_field.clone().unwrap()));

        // action
        let node_value = NodeValue::extract_value(Cow::Owned(msg), &field_names)?;

        // assert
        assert_eq!(node_value, expected_value);
        Ok(())
    }

    #[test]
    fn extract_value_string_field_ok() -> Result<(), NodeEvaluationError> {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["string_field".to_string()];

        let extracted = NodeValue::extract_value(Cow::Owned(msg), &field_names)?;

        assert_eq!(extracted, NodeValue::String("Hello world".to_string()));
        Ok(())
    }

    #[test]
    fn extract_value_bytes_field_ok() -> Result<(), NodeEvaluationError> {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["bytes_field".to_string()];

        let extracted = NodeValue::extract_value(Cow::Owned(msg), &field_names)?;

        assert_eq!(extracted, NodeValue::ByteList("Hello world".as_bytes().to_vec()));
        Ok(())
    }

    #[test]
    fn extract_value_field_has_default_value_ok() -> Result<(), NodeEvaluationError> {
        let mut msg = TestMessage::new();
        msg.int_field = 0;
        msg.float_field = 0.0;
        msg.enum_field = TestEnum::NONE.into(); // this enum is equal to int 0
        msg.optional_bool_field = Some(false);
        let msg = Arc::new(msg) as Arc<dyn MessageDyn>;

        assert_eq!(
            NodeValue::extract_value(Cow::Borrowed(&msg), &["int_field".to_string()])?,
            NodeValue::Int32(0)
        );
        assert_eq!(
            NodeValue::extract_value(Cow::Borrowed(&msg), &["float_field".to_string()])?,
            NodeValue::Float(0.0)
        );
        assert_eq!(
            NodeValue::extract_value(Cow::Borrowed(&msg), &["enum_field".to_string()])?,
            NodeValue::Int32(0)
        );
        assert_eq!(
            NodeValue::extract_value(Cow::Borrowed(&msg), &["optional_bool_field".to_string()])?,
            NodeValue::Bool(false)
        );
        assert_eq!(
            NodeValue::extract_value(Cow::Borrowed(&msg), &["string_field".to_string()])?,
            NodeValue::String(String::default())
        );
        assert_eq!(
            NodeValue::extract_value(Cow::Borrowed(&msg), &["bytes_field".to_string()])?,
            NodeValue::ByteList(vec![])
        );
        Ok(())
    }

    #[test]
    fn extract_value_nested_field_not_set_ok() -> Result<(), NodeEvaluationError> {
        let msg = Arc::new(TestMessage::new()) as Arc<dyn MessageDyn>;
        let field_names = ["message_field".to_string(), "int_field".to_string()];

        assert_eq!(NodeValue::extract_value(Cow::Owned(msg), &field_names)?, NodeValue::None);
        Ok(())
    }

    #[test]
    fn extract_value_nested_singular_field_ok() -> Result<(), NodeEvaluationError> {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["message_field".to_string(), "bool_field".to_string()];

        assert_eq!(NodeValue::extract_value(Cow::Owned(msg), &field_names)?, NodeValue::Bool(true));
        Ok(())
    }

    #[test]
    fn extract_value_repeated_bool_field_ok() -> Result<(), NodeEvaluationError> {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["repeated_bools".to_string()];
        let repeated_bools = TEST_MESSAGE.repeated_bools.clone();

        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &field_names)?,
            NodeValue::BoolList(repeated_bools)
        );
        Ok(())
    }

    #[test]
    fn extract_value_repeated_int_field_ok() -> Result<(), NodeEvaluationError> {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["repeated_i64s".to_string()];
        let repeated_i64s = TEST_MESSAGE.repeated_i64s.clone();

        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &field_names)?,
            NodeValue::Int64List(repeated_i64s)
        );
        Ok(())
    }

    #[test]
    fn extract_value_repeated_double_field_ok() -> Result<(), NodeEvaluationError> {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["repeated_doubles".to_string()];
        let repeated_doubles = TEST_MESSAGE.repeated_doubles.clone();

        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &field_names)?,
            NodeValue::DoubleList(repeated_doubles)
        );
        Ok(())
    }

    #[test]
    fn extract_value_repeated_enum_field_ok() -> Result<(), NodeEvaluationError> {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["repeated_enums".to_string()];
        let repeated_i32s = TEST_MESSAGE.repeated_enums.iter().map(|x| x.value()).collect();

        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &field_names)?,
            NodeValue::Int32List(repeated_i32s)
        );
        Ok(())
    }

    #[test]
    fn extract_value_repeated_message_field_ok() -> Result<(), NodeEvaluationError> {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["repeated_messages".to_string()];
        let repeated_msgs = TEST_MESSAGE
            .repeated_messages
            .iter()
            .map(|x| Arc::new(x.clone()) as Arc<dyn MessageDyn>)
            .collect::<Vec<_>>();

        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &field_names)?,
            NodeValue::MessageList(repeated_msgs)
        );
        Ok(())
    }

    #[test]
    fn extract_value_nested_repeated_field_ok() -> Result<(), NodeEvaluationError> {
        let msg = Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>;
        let field_names = ["message_field".to_string(), "repeated_i32s".to_string()];
        let repeated_i32s = TEST_MESSAGE.message_field.repeated_i32s.clone();

        assert_eq!(
            NodeValue::extract_value(Cow::Owned(msg), &field_names)?,
            NodeValue::Int32List(repeated_i32s)
        );
        Ok(())
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
    fn operate_sync_logical_requires_boolean_types() {
        let true_val = NodeValue::Bool(true);
        let int_val = NodeValue::Int32(1);

        assert!(matches!(
            true_val.clone().operate_sync(&OP_AND, || provide(&int_val)).unwrap_err(),
            NodeEvaluationError::LogicalOperatorIncompatibleOperands
        ));
        assert!(matches!(
            int_val.operate_sync(&OP_AND, || provide(&true_val)).unwrap_err(),
            NodeEvaluationError::LogicalOperatorIncompatibleOperands
        ));
    }

    #[test]
    fn operate_sync_logical_and() -> Result<(), NodeEvaluationError> {
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);
        let none_val = NodeValue::None;

        // true && true = true
        assert_eq!(
            true_val.clone().operate_sync(&OP_AND, || provide(&true_val))?,
            NodeValue::Bool(true)
        );
        // true && false = false
        assert_eq!(
            true_val.clone().operate_sync(&OP_AND, || provide(&false_val))?,
            NodeValue::Bool(false)
        );
        // false && false = false
        assert_eq!(
            false_val.clone().operate_sync(&OP_AND, || provide(&false_val))?,
            NodeValue::Bool(false)
        );
        // true && None = None
        assert_eq!(true_val.operate_sync(&OP_AND, || provide(&none_val))?, NodeValue::None);
        Ok(())
    }

    #[test]
    fn operate_sync_logical_and_short_circuits() -> Result<(), NodeEvaluationError> {
        let false_val = NodeValue::Bool(false);
        let num_calls = Mutex::new(0);
        let right_operand_provider = || {
            *num_calls.lock() += 1;
            provide(&false_val)
        };

        let res = false_val.clone().operate_sync(&OP_AND, right_operand_provider)?;

        // false && anything = false
        assert_eq!(res, NodeValue::Bool(false));
        // should not compute right operand because of short circuiting
        assert_eq!(*num_calls.lock(), 0);
        Ok(())
    }

    #[test]
    fn operate_sync_logical_and_short_circuiting_attempted() -> Result<(), NodeEvaluationError> {
        let true_val = NodeValue::Bool(true);
        let num_calls = Mutex::new(0);
        let right_operand_provider = || {
            *num_calls.lock() += 1;
            provide(&true_val)
        };

        // true && [right_operand] = [right_operand]
        let res = true_val.clone().operate_sync(&OP_AND, right_operand_provider)?;

        assert_eq!(res, NodeValue::Bool(true));
        // should compute right operand because left operand does not uniquely determine result
        assert_eq!(*num_calls.lock(), 1);
        Ok(())
    }

    #[test]
    fn operate_sync_logical_or() -> Result<(), NodeEvaluationError> {
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);
        let none_val = NodeValue::None;

        // true || true = true
        assert_eq!(
            true_val.clone().operate_sync(&OP_OR, || provide(&true_val))?,
            NodeValue::Bool(true)
        );
        // true || false = true
        assert_eq!(true_val.operate_sync(&OP_OR, || provide(&false_val))?, NodeValue::Bool(true));
        // false || false = false
        assert_eq!(
            false_val.clone().operate_sync(&OP_OR, || provide(&false_val))?,
            NodeValue::Bool(false)
        );
        // false || None = None
        assert_eq!(false_val.operate_sync(&OP_OR, || provide(&none_val))?, NodeValue::None);
        Ok(())
    }

    #[test]
    fn operate_sync_logical_or_short_circuits() -> Result<(), NodeEvaluationError> {
        let true_val = NodeValue::Bool(true);
        let num_calls = Mutex::new(0);
        let right_operand_provider = || {
            *num_calls.lock() += 1;
            provide(&true_val)
        };

        let res = true_val.clone().operate_sync(&OP_OR, right_operand_provider)?;

        // true || anything = true
        assert_eq!(res, NodeValue::Bool(true));
        // should not compute right operand because of short circuiting
        assert_eq!(*num_calls.lock(), 0);
        Ok(())
    }

    #[test]
    fn operate_sync_logical_or_short_circuiting_attempted() -> Result<(), NodeEvaluationError> {
        let false_val = NodeValue::Bool(false);
        let num_calls = Mutex::new(0);
        let right_operand_provider = || {
            *num_calls.lock() += 1;
            provide(&false_val)
        };

        // false || [right_operand] = [right_operand]
        let res = false_val.clone().operate_sync(&OP_OR, right_operand_provider)?;

        assert_eq!(res, NodeValue::Bool(false));
        // should compute right operand because left operand does not uniquely determine result
        assert_eq!(*num_calls.lock(), 1);
        Ok(())
    }

    #[test]
    fn operate_sync_logical_xor() -> Result<(), NodeEvaluationError> {
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);
        let none_val = NodeValue::None;

        // true XOR true = false
        assert_eq!(
            true_val.clone().operate_sync(&OP_XOR, || provide(&true_val))?,
            NodeValue::Bool(false)
        );
        // true XOR false = true
        assert_eq!(true_val.operate_sync(&OP_XOR, || provide(&false_val))?, NodeValue::Bool(true));
        // false XOR false = false
        assert_eq!(
            false_val.clone().operate_sync(&OP_XOR, || provide(&false_val))?,
            NodeValue::Bool(false)
        );
        // false || None = None
        assert_eq!(false_val.operate_sync(&OP_XOR, || provide(&none_val))?, NodeValue::None);
        Ok(())
    }

    #[test]
    fn operate_sync_logical_not() -> Result<(), NodeEvaluationError> {
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        // !true = false
        assert_eq!(
            true_val.operate_sync(&OP_NOT, || panic!(
                "Unary operator should not call right operand provider"
            ))?,
            NodeValue::Bool(false)
        );
        // !false = true
        assert_eq!(
            false_val.operate_sync(&OP_NOT, || panic!(
                "Unary operator should not call right operand provider"
            ))?,
            NodeValue::Bool(true)
        );
        Ok(())
    }

    #[test]
    fn operate_sync_arithmetic_requires_numeric_types() {
        let true_val = NodeValue::Bool(true);
        let int_val = NodeValue::Int32(1);

        assert!(matches!(
            true_val.clone().operate_sync(&OP_ADD, || provide(&int_val)).unwrap_err(),
            NodeEvaluationError::ArithmeticOperatorIncompatibleOperands
        ));
        assert!(matches!(
            int_val.operate_sync(&OP_ADD, || provide(&true_val)).unwrap_err(),
            NodeEvaluationError::ArithmeticOperatorIncompatibleOperands
        ));
    }

    #[test]
    fn operate_sync_arithmetic_propagates_up_none_values() {
        let int_val = NodeValue::Int32(1);
        let none_val = NodeValue::None;

        // 1 + None = None
        assert_eq!(
            int_val.clone().operate_sync(&OP_ADD, || provide(&none_val)).unwrap(),
            NodeValue::None
        );
        // None + 1 = None
        assert_eq!(
            none_val.clone().operate_sync(&OP_ADD, || provide(&int_val)).unwrap(),
            NodeValue::None
        );
        // - None = None
        assert_eq!(
            none_val
                .clone()
                .operate_sync(&OP_UNARY_MINUS, || panic!(
                    "Unary operator should not call right operand provider"
                ))
                .unwrap(),
            NodeValue::None
        );
        // |None| = None
        assert_eq!(
            none_val
                .operate_sync(&OP_ABSOLUTE, || panic!(
                    "Unary operator should not call right operand provider"
                ))
                .unwrap(),
            NodeValue::None
        );
    }

    #[test]
    fn operate_sync_arithmetic_add_integers() -> Result<(), NodeEvaluationError> {
        let five = NodeValue::Int32(5);
        let twenty = NodeValue::Int32(20);

        // 5 + 20 = 25
        assert_eq!(five.operate_sync(&OP_ADD, || provide(&twenty))?, NodeValue::Int64(25));
        Ok(())
    }

    #[test]
    fn operate_sync_arithmetic_add_floats() -> Result<(), NodeEvaluationError> {
        let five = NodeValue::Float(5.2);
        let twenty = NodeValue::Double(20.1);

        // 5.2 + 20.1 = 25.3
        assert_eq!(five.operate_sync(&OP_ADD, || provide(&twenty))?, NodeValue::Double(25.3));
        Ok(())
    }

    #[test]
    fn operate_sync_arithmetic_subtract_integers() -> Result<(), NodeEvaluationError> {
        let five = NodeValue::Int32(5);
        let twenty = NodeValue::Int32(20);

        // 5 - 20 = -15
        assert_eq!(five.operate_sync(&OP_SUBTRACT, || provide(&twenty))?, NodeValue::Int64(-15));
        Ok(())
    }

    #[test]
    fn operate_sync_arithmetic_subtract_floats() -> Result<(), NodeEvaluationError> {
        let five = NodeValue::Float(5.2);
        let twenty = NodeValue::Double(20.1);

        // 5.2 - 20.1 = -14.9
        assert_eq!(five.operate_sync(&OP_SUBTRACT, || provide(&twenty))?, NodeValue::Double(-14.9));
        Ok(())
    }

    #[test]
    fn operate_sync_arithmetic_multiply() -> Result<(), NodeEvaluationError> {
        let five = NodeValue::Int32(5);
        let point_five = NodeValue::Float(0.5);

        // 5 * 0.5 = 2.5
        assert_eq!(
            five.operate_sync(&OP_MULTIPLY, || provide(&point_five))?,
            NodeValue::Double(2.5)
        );
        Ok(())
    }

    #[test]
    fn operate_sync_arithmetic_divide_by_zero() {
        let five = NodeValue::Int32(5);
        let zero = NodeValue::Int32(0);

        assert!(matches!(
            five.operate_sync(&OP_DIVIDE, || provide(&zero)).unwrap_err(),
            NodeEvaluationError::DivideByZero
        ));
    }

    #[test]
    fn operate_sync_arithmetic_divide() -> Result<(), NodeEvaluationError> {
        let five = NodeValue::Int32(5);
        let two = NodeValue::Int32(2);

        // 5 / 2 = 2.5
        assert_eq!(five.operate_sync(&OP_DIVIDE, || provide(&two))?, NodeValue::Double(2.5));
        Ok(())
    }

    #[test]
    fn operate_sync_arithmetic_modulo_trunc_by_zero() {
        let five = NodeValue::Int32(5);
        let zero = NodeValue::Int32(0);

        assert!(matches!(
            five.operate_sync(&OP_MODULO_TRUNC, || provide(&zero)).unwrap_err(),
            NodeEvaluationError::DivideByZero
        ));
    }

    #[test]
    fn operate_sync_arithmetic_modulo_trunc_ok() -> Result<(), NodeEvaluationError> {
        let cases = [
            (8, /*%*/ 3, /*=*/ 2),
            (8, /*%*/ -3, /*=*/ 2),
            (-8, /*%*/ 3, /*=*/ -2),
            (-8, /*%*/ -3, /*=*/ -2),
        ];
        for (dividend, divisor, expected) in cases {
            assert_eq!(
                NodeValue::Int32(dividend)
                    .operate_sync(&OP_MODULO_TRUNC, || provide(&NodeValue::Int32(divisor)))?,
                NodeValue::Int64(expected)
            );
        }
        Ok(())
    }

    #[test]
    fn operate_sync_arithmetic_modulo_trunc_no_floating_points() {
        let five = NodeValue::Float(5.0);
        let two = NodeValue::Int32(2);

        // both operands must be integers
        assert!(matches!(
            five.clone().operate_sync(&OP_MODULO_TRUNC, || provide(&two)).unwrap_err(),
            NodeEvaluationError::ModuloOperatorIncompatibleOperands
        ));
        assert!(matches!(
            two.operate_sync(&OP_MODULO_TRUNC, || provide(&five)).unwrap_err(),
            NodeEvaluationError::ModuloOperatorIncompatibleOperands
        ));
    }

    #[test]
    fn operate_sync_arithmetic_power_integers() -> Result<(), NodeEvaluationError> {
        let two = NodeValue::Int32(2);

        // 2^2 = 4
        assert_eq!(two.clone().operate_sync(&OP_POWER, || provide(&two))?, NodeValue::Int64(4));
        Ok(())
    }

    #[test]
    fn operate_sync_arithmetic_power_floats() -> Result<(), NodeEvaluationError> {
        let val = 2.5_f64;
        let expected = val.powf(val);
        let two_point_five = NodeValue::Double(val);

        assert_eq!(
            two_point_five.clone().operate_sync(&OP_POWER, || provide(&two_point_five))?,
            NodeValue::Double(expected)
        );
        Ok(())
    }

    #[test]
    fn operate_sync_arithmetic_unary_minus() -> Result<(), NodeEvaluationError> {
        let two = NodeValue::Int32(2);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(
            two.operate_sync(&OP_UNARY_MINUS, || panic!(
                "Unary operator should not call right operand provider"
            ))?,
            NodeValue::Int64(-2)
        );
        assert_eq!(
            five.operate_sync(&OP_UNARY_MINUS, || panic!(
                "Unary operator should not call right operand provider"
            ))?,
            NodeValue::Double(-5.5)
        );
        // fails for non-numeric operand
        assert!(matches!(
            true_val
                .operate_sync(&OP_UNARY_MINUS, || panic!(
                    "Unary operator should not call right operand provider"
                ))
                .unwrap_err(),
            NodeEvaluationError::ArithmeticOperatorIncompatibleOperands
        ));

        Ok(())
    }

    #[test]
    fn operate_sync_arithmetic_absolute() -> Result<(), NodeEvaluationError> {
        let one = NodeValue::Int64(1);
        let negative_three = NodeValue::Int64(-3);
        let two_point_seven = NodeValue::Float(2.7);
        let negative_two_point_seven = NodeValue::Float(-2.7);
        let zero = NodeValue::Float(0.0);

        assert_eq!(
            one.operate_sync(&OP_ABSOLUTE, || provide(&NodeValue::None))?,
            NodeValue::Int64(1)
        );
        assert_eq!(
            negative_three.operate_sync(&OP_ABSOLUTE, || provide(&NodeValue::None))?,
            NodeValue::Int64(3)
        );
        assert_eq!(
            two_point_seven.operate_sync(&OP_ABSOLUTE, || provide(&NodeValue::None))?,
            NodeValue::Double(2.7)
        );
        assert_eq!(
            negative_two_point_seven.operate_sync(&OP_ABSOLUTE, || provide(&NodeValue::None))?,
            NodeValue::Double(2.7)
        );
        assert_eq!(
            zero.operate_sync(&OP_ABSOLUTE, || provide(&NodeValue::None))?,
            NodeValue::Double(0.0)
        );
        Ok(())
    }

    #[test]
    fn operate_sync_relational_propagates_up_none_values() {
        let int_val = NodeValue::Int32(1);
        let none_val = NodeValue::None;

        // 1 > None = None
        assert_eq!(
            int_val.clone().operate_sync(&OP_GT, || provide(&none_val)).unwrap(),
            NodeValue::None
        );
        // None > 1 = None
        assert_eq!(none_val.operate_sync(&OP_GT, || provide(&int_val)).unwrap(), NodeValue::None);
    }

    #[test]
    fn operate_sync_relational_eq() -> Result<(), NodeEvaluationError> {
        let one = NodeValue::Double(1.0);
        let one_epsilon = NodeValue::Double(1.0 + f64::EPSILON);
        assert_ne!(
            one.get_double(&|| -> NodeEvaluationError { unreachable!() })?,
            one_epsilon.get_double(&|| -> NodeEvaluationError { unreachable!() })?,
            "These values must be very close but not equal so that this test can verify the correct handling of approximately equal values."
        );
        let two = NodeValue::Int32(2);
        let two_i64 = NodeValue::Int64(2);
        let two_f32 = NodeValue::Float(2.0);
        let two_f64 = NodeValue::Double(2.0);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(one.operate_sync(&OP_EQ, || provide(&one_epsilon))?, NodeValue::Bool(true));
        assert_eq!(two.clone().operate_sync(&OP_EQ, || provide(&two))?, NodeValue::Bool(true));
        assert_eq!(two.clone().operate_sync(&OP_EQ, || provide(&two_i64))?, NodeValue::Bool(true));
        assert_eq!(two.clone().operate_sync(&OP_EQ, || provide(&two_f32))?, NodeValue::Bool(true));
        assert_eq!(two.clone().operate_sync(&OP_EQ, || provide(&two_f64))?, NodeValue::Bool(true));
        assert_eq!(two.clone().operate_sync(&OP_EQ, || provide(&five))?, NodeValue::Bool(false));
        assert_eq!(two.operate_sync(&OP_EQ, || provide(&true_val))?, NodeValue::Bool(false));
        Ok(())
    }

    #[test]
    fn operate_sync_relational_not_eq() -> Result<(), NodeEvaluationError> {
        let one = NodeValue::Double(1.0);
        let one_epsilon = NodeValue::Double(1.0 + f64::EPSILON);
        assert_ne!(
            one.get_double(&|| -> NodeEvaluationError { unreachable!() })?,
            one_epsilon.get_double(&|| -> NodeEvaluationError { unreachable!() })?,
            "These values must be very close but not equal so that this test can verify the correct handling of approximately equal values."
        );
        let two = NodeValue::Int32(2);
        let two_f32 = NodeValue::Float(2.0);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(one.operate_sync(&OP_NOT_EQ, || provide(&one_epsilon))?, NodeValue::Bool(false));
        assert_eq!(two.clone().operate_sync(&OP_NOT_EQ, || provide(&two))?, NodeValue::Bool(false));
        assert_eq!(
            two.clone().operate_sync(&OP_NOT_EQ, || provide(&two_f32))?,
            NodeValue::Bool(false)
        );
        assert_eq!(two.clone().operate_sync(&OP_NOT_EQ, || provide(&five))?, NodeValue::Bool(true));
        assert_eq!(two.operate_sync(&OP_NOT_EQ, || provide(&true_val))?, NodeValue::Bool(true));
        Ok(())
    }

    #[test]
    fn operate_sync_relational_gt() -> Result<(), NodeEvaluationError> {
        let one = NodeValue::Double(1.0);
        let one_epsilon = NodeValue::Double(1.0 + f64::EPSILON);
        assert_ne!(
          one.get_double(&|| -> NodeEvaluationError { unreachable!() })?,
          one_epsilon.get_double(&|| -> NodeEvaluationError { unreachable!() })?,
          "These values must be very close but not equal so that this test can verify the correct handling of approximately equal values."
        );
        let two = NodeValue::Int32(2);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        assert_eq!(
            true_val.clone().operate_sync(&OP_GT, || provide(&false_val))?,
            NodeValue::Bool(true)
        );
        assert_eq!(one_epsilon.operate_sync(&OP_GT, || provide(&one))?, NodeValue::Bool(false));
        assert_eq!(two.clone().operate_sync(&OP_GT, || provide(&two))?, NodeValue::Bool(false));
        assert_eq!(two.clone().operate_sync(&OP_GT, || provide(&five))?, NodeValue::Bool(false));
        assert_eq!(five.clone().operate_sync(&OP_GT, || provide(&two))?, NodeValue::Bool(true));
        assert!(matches!(
            five.operate_sync(&OP_GT, || provide(&true_val)).unwrap_err(),
            NodeEvaluationError::ComparisonOperatorIncomptaibleOperands
        ));
        Ok(())
    }

    #[test]
    fn operate_sync_relational_gt_or_eq() -> Result<(), NodeEvaluationError> {
        let one = NodeValue::Double(1.0);
        let one_epsilon = NodeValue::Double(1.0 + f64::EPSILON);
        assert_ne!(
            one.get_double(&|| -> NodeEvaluationError { unreachable!() })?,
            one_epsilon.get_double(&|| -> NodeEvaluationError { unreachable!() })?,
            "These values must be very close but not equal so that this test can verify the correct handling of approximately equal values."
        );
        let two = NodeValue::Int32(2);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(
            true_val.clone().operate_sync(&OP_GT_OR_EQ, || provide(&true_val))?,
            NodeValue::Bool(true)
        );
        assert_eq!(
            one.operate_sync(&OP_GT_OR_EQ, || provide(&one_epsilon))?,
            NodeValue::Bool(true)
        );
        assert_eq!(
            two.clone().operate_sync(&OP_GT_OR_EQ, || provide(&two))?,
            NodeValue::Bool(true)
        );
        assert_eq!(
            two.clone().operate_sync(&OP_GT_OR_EQ, || provide(&five))?,
            NodeValue::Bool(false)
        );
        assert_eq!(
            five.clone().operate_sync(&OP_GT_OR_EQ, || provide(&two))?,
            NodeValue::Bool(true)
        );
        assert!(matches!(
            five.operate_sync(&OP_GT_OR_EQ, || provide(&true_val)).unwrap_err(),
            NodeEvaluationError::ComparisonOperatorIncomptaibleOperands
        ));
        Ok(())
    }

    #[test]
    fn operate_sync_relational_lt() -> Result<(), NodeEvaluationError> {
        let one = NodeValue::Double(1.0);
        let one_epsilon = NodeValue::Double(1.0 + f64::EPSILON);
        assert_ne!(
            one.get_double(&|| -> NodeEvaluationError { unreachable!() })?,
            one_epsilon.get_double(&|| -> NodeEvaluationError { unreachable!() })?,
            "These values must be very close but not equal so that this test can verify the correct handling of approximately equal values."
        );
        let two = NodeValue::Int32(2);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        assert_eq!(
            true_val.clone().operate_sync(&OP_LT, || provide(&false_val))?,
            NodeValue::Bool(false)
        );
        assert_eq!(one.operate_sync(&OP_LT, || provide(&one_epsilon))?, NodeValue::Bool(false));
        assert_eq!(two.clone().operate_sync(&OP_LT, || provide(&two))?, NodeValue::Bool(false));
        assert_eq!(two.clone().operate_sync(&OP_LT, || provide(&five))?, NodeValue::Bool(true));
        assert_eq!(five.clone().operate_sync(&OP_LT, || provide(&two))?, NodeValue::Bool(false));
        assert!(matches!(
            five.operate_sync(&OP_LT, || provide(&true_val)).unwrap_err(),
            NodeEvaluationError::ComparisonOperatorIncomptaibleOperands
        ));
        Ok(())
    }

    #[test]
    fn operate_sync_relational_lt_or_eq() -> Result<(), NodeEvaluationError> {
        let one = NodeValue::Double(1.0);
        let one_epsilon = NodeValue::Double(1.0 + f64::EPSILON);
        assert_ne!(
            one.get_double(&|| -> NodeEvaluationError { unreachable!() })?,
            one_epsilon.get_double(&|| -> NodeEvaluationError { unreachable!() })?,
            "These values must be very close but not equal so that this test can verify the correct handling of approximately equal values."
        );
        let two = NodeValue::Int32(2);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(
            true_val.clone().operate_sync(&OP_LT_OR_EQ, || provide(&true_val))?,
            NodeValue::Bool(true)
        );
        assert_eq!(
            one_epsilon.operate_sync(&OP_LT_OR_EQ, || provide(&one))?,
            NodeValue::Bool(true)
        );
        assert_eq!(
            two.clone().operate_sync(&OP_LT_OR_EQ, || provide(&two))?,
            NodeValue::Bool(true)
        );
        assert_eq!(
            two.clone().operate_sync(&OP_LT_OR_EQ, || provide(&five))?,
            NodeValue::Bool(true)
        );
        assert_eq!(
            five.clone().operate_sync(&OP_LT_OR_EQ, || provide(&two))?,
            NodeValue::Bool(false)
        );
        assert!(matches!(
            five.operate_sync(&OP_LT_OR_EQ, || provide(&true_val)).unwrap_err(),
            NodeEvaluationError::ComparisonOperatorIncomptaibleOperands
        ));
        Ok(())
    }

    #[test]
    fn operate_sync_relational_contains_errs_if_left_operand_not_a_list() {
        let two = NodeValue::Int32(2);

        assert!(matches!(
            two.clone().operate_sync(&OP_CONTAINS, || provide(&two)).unwrap_err(),
            NodeEvaluationError::ContainsOperatorIncompatibleOperands
        ));
    }

    #[test]
    fn operate_sync_relational_contains_errs_if_operands_have_incompatible_types() {
        let bool_list = NodeValue::BoolList(vec![true, true, false]);
        let five = NodeValue::Float(5.5);

        assert!(matches!(
            bool_list.operate_sync(&OP_CONTAINS, || provide(&five)).unwrap_err(),
            NodeEvaluationError::ContainsOperatorIncompatibleOperands
        ));
    }

    #[test]
    fn operate_sync_relational_contains_bool_type_ok() -> Result<(), NodeEvaluationError> {
        let bool_list = NodeValue::BoolList(vec![true, true, true]);
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        assert_eq!(
            bool_list.clone().operate_sync(&OP_CONTAINS, || provide(&true_val))?,
            NodeValue::Bool(true)
        );
        assert_eq!(
            bool_list.operate_sync(&OP_CONTAINS, || provide(&false_val))?,
            NodeValue::Bool(false)
        );
        Ok(())
    }

    #[test]
    fn operate_sync_relational_contains_int_type_ok() -> Result<(), NodeEvaluationError> {
        let int_list = NodeValue::Int32List(vec![3, 30, 300]);
        let zero = NodeValue::Int32(0);
        let three = NodeValue::Int32(3);
        let thirty = NodeValue::Int64(30);
        // not int
        let three_hundred = NodeValue::Double(300.0);

        assert_eq!(
            int_list.clone().operate_sync(&OP_CONTAINS, || provide(&zero))?,
            NodeValue::Bool(false)
        );
        assert_eq!(
            int_list.clone().operate_sync(&OP_CONTAINS, || provide(&three))?,
            NodeValue::Bool(true)
        );
        assert_eq!(
            int_list.clone().operate_sync(&OP_CONTAINS, || provide(&thirty))?,
            NodeValue::Bool(true)
        );
        assert!(matches!(
            int_list.operate_sync(&OP_CONTAINS, || provide(&three_hundred)).unwrap_err(),
            NodeEvaluationError::ContainsOperatorIncompatibleOperands
        ));
        Ok(())
    }

    #[test]
    fn operate_sync_relational_contains_float_type_ok() -> Result<(), NodeEvaluationError> {
        let float_list = NodeValue::FloatList(vec![0.5, std::f32::consts::PI, 10.0]);
        let point_five = NodeValue::Float(0.5);
        // f64 PI, not f32 PI
        let pi = NodeValue::Double(std::f64::consts::PI);
        // not float but is a numeric type
        let ten = NodeValue::Int32(10);
        let thirty = NodeValue::Float(30.0);

        assert_eq!(
            float_list.clone().operate_sync(&OP_CONTAINS, || provide(&point_five))?,
            NodeValue::Bool(true)
        );
        assert_eq!(
            float_list.clone().operate_sync(&OP_CONTAINS, || provide(&pi))?,
            NodeValue::Bool(true)
        );
        assert_eq!(
            float_list.clone().operate_sync(&OP_CONTAINS, || provide(&ten))?,
            NodeValue::Bool(true)
        );
        assert_eq!(
            float_list.operate_sync(&OP_CONTAINS, || provide(&thirty))?,
            NodeValue::Bool(false)
        );
        Ok(())
    }

    #[test]
    fn operate_sync_relational_contains_message_type_ok() -> Result<(), NodeEvaluationError> {
        let message: Arc<dyn MessageDyn> = Arc::new(TEST_MESSAGE.clone());
        let msg_list = NodeValue::MessageList(vec![Arc::clone(&message)]);
        let msg_1 = NodeValue::Message(message);
        let msg_2 = NodeValue::Message(Arc::new(TestMessage::new()));

        assert_eq!(
            msg_list.clone().operate_sync(&OP_CONTAINS, || provide(&msg_1))?,
            NodeValue::Bool(true)
        );
        assert_eq!(
            msg_list.operate_sync(&OP_CONTAINS, || provide(&msg_2))?,
            NodeValue::Bool(false)
        );
        Ok(())
    }

    #[test]
    fn operate_sync_relational_does_not_contain_errs_if_left_operand_not_a_list() {
        let two = NodeValue::Int32(2);

        assert!(matches!(
            two.clone().operate_sync(&OP_DOES_NOT_CONTAIN, || provide(&two)).unwrap_err(),
            NodeEvaluationError::ContainsOperatorIncompatibleOperands
        ));
    }

    #[test]
    fn operate_sync_relational_does_not_contain_errs_if_operands_have_incompatible_types() {
        let bool_list = NodeValue::BoolList(vec![true, true, false]);
        let five = NodeValue::Float(5.5);

        assert!(matches!(
            bool_list.operate_sync(&OP_DOES_NOT_CONTAIN, || provide(&five)).unwrap_err(),
            NodeEvaluationError::ContainsOperatorIncompatibleOperands
        ));
    }

    #[test]
    fn operate_sync_relational_does_not_contain_bool_type_ok() -> Result<(), NodeEvaluationError> {
        let bool_list = NodeValue::BoolList(vec![true, true, true]);
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        assert_eq!(
            bool_list.clone().operate_sync(&OP_DOES_NOT_CONTAIN, || provide(&true_val))?,
            NodeValue::Bool(false)
        );
        assert_eq!(
            bool_list.operate_sync(&OP_DOES_NOT_CONTAIN, || provide(&false_val))?,
            NodeValue::Bool(true)
        );
        Ok(())
    }

    #[test]
    fn operate_sync_relational_does_not_contain_int_type_ok() -> Result<(), NodeEvaluationError> {
        let int_list = NodeValue::Int32List(vec![3, 30, 300]);
        let zero = NodeValue::Int32(0);
        let three = NodeValue::Int32(3);
        let thirty = NodeValue::Int64(30);
        // not int
        let three_hundred = NodeValue::Double(300.0);

        assert_eq!(
            int_list.clone().operate_sync(&OP_DOES_NOT_CONTAIN, || provide(&zero))?,
            NodeValue::Bool(true)
        );
        assert_eq!(
            int_list.clone().operate_sync(&OP_DOES_NOT_CONTAIN, || provide(&three))?,
            NodeValue::Bool(false)
        );
        assert_eq!(
            int_list.clone().operate_sync(&OP_DOES_NOT_CONTAIN, || provide(&thirty))?,
            NodeValue::Bool(false)
        );
        assert!(matches!(
            int_list.operate_sync(&OP_DOES_NOT_CONTAIN, || provide(&three_hundred)).unwrap_err(),
            NodeEvaluationError::ContainsOperatorIncompatibleOperands
        ));
        Ok(())
    }

    #[test]
    fn operate_sync_relational_does_not_contain_float_type_ok() -> Result<(), NodeEvaluationError> {
        let float_list = NodeValue::FloatList(vec![0.5, std::f32::consts::PI, 10.0]);
        let point_five = NodeValue::Float(0.5);
        // f64 PI, not f32 PI
        let pi = NodeValue::Double(std::f64::consts::PI);
        // not float but is a numeric type
        let ten = NodeValue::Int32(10);
        let thirty = NodeValue::Float(30.0);

        assert_eq!(
            float_list.clone().operate_sync(&OP_DOES_NOT_CONTAIN, || provide(&point_five))?,
            NodeValue::Bool(false)
        );
        assert_eq!(
            float_list.clone().operate_sync(&OP_DOES_NOT_CONTAIN, || provide(&pi))?,
            NodeValue::Bool(false)
        );
        assert_eq!(
            float_list.clone().operate_sync(&OP_DOES_NOT_CONTAIN, || provide(&ten))?,
            NodeValue::Bool(false)
        );
        assert_eq!(
            float_list.operate_sync(&OP_DOES_NOT_CONTAIN, || provide(&thirty))?,
            NodeValue::Bool(true)
        );
        Ok(())
    }

    #[test]
    fn operate_sync_relational_does_not_contain_message_type_ok() -> Result<(), NodeEvaluationError>
    {
        let message: Arc<dyn MessageDyn> = Arc::new(TEST_MESSAGE.clone());
        let msg_list = NodeValue::MessageList(vec![Arc::clone(&message)]);
        let msg_1 = NodeValue::Message(message);
        let msg_2 = NodeValue::Message(Arc::new(TestMessage::new()));

        assert_eq!(
            msg_list.clone().operate_sync(&OP_DOES_NOT_CONTAIN, || provide(&msg_1))?,
            NodeValue::Bool(false)
        );
        assert_eq!(
            msg_list.operate_sync(&OP_DOES_NOT_CONTAIN, || provide(&msg_2))?,
            NodeValue::Bool(true)
        );
        Ok(())
    }

    #[test]
    fn operate_sync_relational_all_eq_errs_if_left_operand_not_a_list() {
        let two = NodeValue::Int32(2);

        assert!(matches!(
            two.clone().operate_sync(&OP_ALL_EQ, || provide(&two)).unwrap_err(),
            NodeEvaluationError::AllEqualsOperatorIncompatibleOperands
        ));
    }

    #[test]
    fn operate_sync_relational_all_eq_errs_if_operands_have_incompatible_types() {
        let bool_list = NodeValue::BoolList(vec![true, true, false]);
        let five = NodeValue::Float(5.5);

        assert!(matches!(
            bool_list.operate_sync(&OP_ALL_EQ, || provide(&five)).unwrap_err(),
            NodeEvaluationError::AllEqualsOperatorIncompatibleOperands
        ));
    }

    #[test]
    fn operate_sync_relational_all_eq_bool_type_ok() -> Result<(), NodeEvaluationError> {
        let bool_list_1 = NodeValue::BoolList(vec![true, true, true]);
        let bool_list_2 = NodeValue::BoolList(vec![true, false, false]);
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        assert_eq!(
            bool_list_1.clone().operate_sync(&OP_ALL_EQ, || provide(&true_val))?,
            NodeValue::Bool(true)
        );
        assert_eq!(
            bool_list_1.operate_sync(&OP_ALL_EQ, || provide(&false_val))?,
            NodeValue::Bool(false)
        );
        assert_eq!(
            bool_list_2.clone().operate_sync(&OP_ALL_EQ, || provide(&true_val))?,
            NodeValue::Bool(false)
        );
        assert_eq!(
            bool_list_2.operate_sync(&OP_ALL_EQ, || provide(&false_val))?,
            NodeValue::Bool(false)
        );
        Ok(())
    }

    #[test]
    fn operate_sync_relational_all_eq_int_type_ok() -> Result<(), NodeEvaluationError> {
        let int_list_1 = NodeValue::Int64List(vec![3, 30, 300]);
        let int_list_2 = NodeValue::Int64List(vec![0, 0]);
        let zero = NodeValue::Int32(0);

        assert_eq!(int_list_1.operate_sync(&OP_ALL_EQ, || provide(&zero))?, NodeValue::Bool(false));
        assert_eq!(int_list_2.operate_sync(&OP_ALL_EQ, || provide(&zero))?, NodeValue::Bool(true));
        Ok(())
    }

    #[test]
    fn operate_sync_relational_all_eq_float_type_ok() -> Result<(), NodeEvaluationError> {
        let float_list_1 = NodeValue::FloatList(vec![0.5, std::f32::consts::PI, 10.0]);
        let float_list_2 = NodeValue::FloatList(vec![0.5, 0.5]);

        let point_five = NodeValue::Double(0.5);

        assert_eq!(
            float_list_1.operate_sync(&OP_ALL_EQ, || provide(&point_five))?,
            NodeValue::Bool(false)
        );
        assert_eq!(
            float_list_2.operate_sync(&OP_ALL_EQ, || provide(&point_five))?,
            NodeValue::Bool(true)
        );
        Ok(())
    }

    #[test]
    fn operate_sync_relational_all_eq_message_type_ok() -> Result<(), NodeEvaluationError> {
        let message: Arc<dyn MessageDyn> = Arc::new(TEST_MESSAGE.clone());
        let msg_list = NodeValue::MessageList(vec![Arc::clone(&message); 2]);
        let msg_1 = NodeValue::Message(message);
        let msg_2 = NodeValue::Message(Arc::new(TestMessage::new()));

        assert_eq!(
            msg_list.clone().operate_sync(&OP_ALL_EQ, || provide(&msg_1))?,
            NodeValue::Bool(true)
        );
        assert_eq!(msg_list.operate_sync(&OP_ALL_EQ, || provide(&msg_2))?, NodeValue::Bool(false));
        Ok(())
    }

    #[test]
    fn operate_sync_rounding_errs_if_operand_is_incompatible() {
        let true_val = NodeValue::Bool(true);
        let one = NodeValue::Int32(1);

        assert!(matches!(
            one.clone().operate_sync(&OP_FLOOR, || provide(&NodeValue::None)).unwrap_err(),
            NodeEvaluationError::RoundingOperatorIncompatibleOperands
        ));
        assert!(matches!(
            true_val.operate_sync(&OP_ROUND, || provide(&NodeValue::None)).unwrap_err(),
            NodeEvaluationError::RoundingOperatorIncompatibleOperands
        ));
        assert!(matches!(
            one.operate_sync(&OP_CEIL, || provide(&NodeValue::None)).unwrap_err(),
            NodeEvaluationError::RoundingOperatorIncompatibleOperands
        ));
    }

    #[test]
    fn operate_sync_rounding_floor_ok() -> Result<(), NodeEvaluationError> {
        let point_one = NodeValue::Double(0.1);
        let two_point_nine = NodeValue::Float(2.9);
        let five = NodeValue::Float(5.0);
        let negative_point_three = NodeValue::Double(-0.33);
        let negative_point_nine = NodeValue::Double(-0.99);

        assert_eq!(
            point_one.operate_sync(&OP_FLOOR, || provide(&NodeValue::None))?,
            NodeValue::Int64(0)
        );
        assert_eq!(
            two_point_nine.operate_sync(&OP_FLOOR, || provide(&NodeValue::None))?,
            NodeValue::Int64(2)
        );
        assert_eq!(
            five.operate_sync(&OP_FLOOR, || provide(&NodeValue::None))?,
            NodeValue::Int64(5)
        );
        assert_eq!(
            negative_point_three.operate_sync(&OP_FLOOR, || provide(&NodeValue::None))?,
            NodeValue::Int64(-1)
        );
        assert_eq!(
            negative_point_nine.operate_sync(&OP_FLOOR, || provide(&NodeValue::None))?,
            NodeValue::Int64(-1)
        );
        Ok(())
    }

    #[test]
    fn operate_sync_rounding_round_ok() -> Result<(), NodeEvaluationError> {
        let point_one = NodeValue::Double(0.1);
        let two_point_nine = NodeValue::Float(2.9);
        let five = NodeValue::Float(5.0);
        let negative_point_three = NodeValue::Double(-0.33);
        let negative_point_nine = NodeValue::Double(-0.99);

        assert_eq!(
            point_one.operate_sync(&OP_ROUND, || provide(&NodeValue::None))?,
            NodeValue::Int64(0)
        );
        assert_eq!(
            two_point_nine.operate_sync(&OP_ROUND, || provide(&NodeValue::None))?,
            NodeValue::Int64(3)
        );
        assert_eq!(
            five.operate_sync(&OP_ROUND, || provide(&NodeValue::None))?,
            NodeValue::Int64(5)
        );
        assert_eq!(
            negative_point_three.operate_sync(&OP_ROUND, || provide(&NodeValue::None))?,
            NodeValue::Int64(0)
        );
        assert_eq!(
            negative_point_nine.operate_sync(&OP_ROUND, || provide(&NodeValue::None))?,
            NodeValue::Int64(-1)
        );
        Ok(())
    }

    #[test]
    fn operate_sync_rounding_ceil_ok() -> Result<(), NodeEvaluationError> {
        let point_one = NodeValue::Double(0.1);
        let two_point_nine = NodeValue::Float(2.9);
        let five = NodeValue::Float(5.0);
        let negative_point_three = NodeValue::Double(-0.33);
        let negative_point_nine = NodeValue::Double(-0.99);

        assert_eq!(
            point_one.operate_sync(&OP_CEIL, || provide(&NodeValue::None))?,
            NodeValue::Int64(1)
        );
        assert_eq!(
            two_point_nine.operate_sync(&OP_CEIL, || provide(&NodeValue::None))?,
            NodeValue::Int64(3)
        );
        assert_eq!(five.operate_sync(&OP_CEIL, || provide(&NodeValue::None))?, NodeValue::Int64(5));
        assert_eq!(
            negative_point_three.operate_sync(&OP_CEIL, || provide(&NodeValue::None))?,
            NodeValue::Int64(0)
        );
        assert_eq!(
            negative_point_nine.operate_sync(&OP_CEIL, || provide(&NodeValue::None))?,
            NodeValue::Int64(0)
        );
        Ok(())
    }
}
