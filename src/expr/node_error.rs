// Copyright 2024 Google LLC

//! Errors that can arise from incorrect node setup and from AST traversal.

use sdv_telemetry_proto_rust::expression::constant_leaf_node::Node_value as NodeValueProto;
use std::sync::Arc;
use thiserror::Error;

use crate::service_publisher_accessor::GetLatestMessageError;

#[derive(Error, Debug)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum NodeError {
    // general errors
    #[error("Node at index {i} is out of bounds, expression nodes have size {n}")]
    IndexOutOfBound { i: usize, n: usize },
    #[error("Expression nodes contain a cycle at index {i}")]
    CyclicDependency { i: usize },
    #[error("Node at index {i} did not set node_type")]
    NodeTypeNotSet { i: usize },
    #[error("Node at index {i} has an unsupported node_type")]
    NodeTypeNotSupported { i: usize },
    #[error("Node evaluation type mismatch: {msg}")]
    TypeMismatch { msg: String },
    #[error("Internal implementation error: {msg}")]
    InternalError { msg: String },

    // errors related to ConstantLeafNode
    #[error("Constant node value is not set")]
    ConstantValueNotSet,
    #[error("Constant node has an unsupported value: {node_value:?}")]
    ConstantValueNotSupported { node_value: NodeValueProto },

    // errors related to FieldLeafNode
    #[error("Field leaf node encountered error: {e}")]
    MessageProviderError {
        #[from]
        e: Arc<MessageProviderError>,
    },
    #[error("Descriptor not found: {e}")]
    DescriptorProviderError {
        #[from]
        e: DescriptorProviderError,
    },
    #[error("Field {field_name} does not exist in message {message_name}")]
    FieldNotFound { field_name: String, message_name: String },
    #[error("Nested field {field_name} is illegal: {e}")]
    WrongNestedFieldType { field_name: String, e: String },
    #[error("Map fields are not supported")]
    UnsupportedSingularFieldType,
    #[error("Only repeated bools or repeated numbers are supported")]
    UnsupportedRepeatedFieldType,

    // Errors related to `FunctionLeafNode`.
    #[error("Function node did not set a function_type")]
    FunctionLeafNodeNotSet,
    #[error("Function node did not have a supported function_type")]
    FunctionLeafNodeNotSupported,
    #[error("Function node did not have valid arguments: {error}")]
    FunctionLeafNodeArgumentError { error: FunctionLeafNodeArgumentError },

    // error related to CombinationNode
    #[error("Combination node at index {i} did not set operator")]
    OperatorNotSet { i: usize },
    #[error("Unknown operator type: {op}")]
    UnknownOperator { op: String },
    #[error("Combination node at index {i} did not set left child")]
    LeftChildNotSet { i: usize },
    #[error("Unary operator has 2 child nodes")]
    UnaryOperatorIncorrectChildren,
    #[error("Binary operator has 1 child")]
    BinaryOperatorIncorrectChildren,

    // errors related to operator and operand compatibility
    #[error("Logical operator requires boolean operands")]
    LogicalOperatorIncompatibleOperands,
    #[error("Comparison operators requires numeric operands")]
    ComparisonOperatorIncomptaibleOperands,
    #[error("Arithmetic operator requires numeric operands")]
    ArithmeticOperatorIncompatibleOperands,
    #[error("Rounding operator requires a floating point operand")]
    RoundingOperatorIncompatibleOperands,
    #[error("Modulo operator requires integer operands")]
    ModuloOperatorIncompatibleOperands,
    #[error("Arithmetic operation cannot divide or mod by zero")]
    DivideByZero,
    #[error("Contains operator requires left operand is a list, right operand is a scalar of compatible type to the list value")]
    ContainsOperatorIncompatibleOperands,
    #[error("ALL_EQ operator requires left operand is a list, right operand is a scalar of compatible type to the list value")]
    AllEqualsOperatorIncompatibleOperands,

    // errors related to converting a node value into a protobuf reflection object
    #[error("Cannot assign {value} to a repeated field")]
    RepeatedFieldWrongAssignment { value: String },
    #[error("Cannot convert {value} to a protobuf reflection of type {refl_type}")]
    ValueToProtoConversionError { value: String, refl_type: String },
}

/// Error returned by MessageProvider
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum MessageProviderError {
    /// Publisher name is invalid.
    #[error("Publisher name is invalid")]
    InvalidPublisherName,
    #[error("Error when trying to retrieve latest message: {e}")]
    MessageRetrievalError {
        #[from]
        e: GetLatestMessageError,
    },
}

/// Error returned by DescriptorProvider
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum DescriptorProviderError {
    /// Publisher name is invalid.
    #[error("Publisher name is invalid")]
    InvalidPublisherName,
}

#[derive(Error, Debug)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum FunctionLeafNodeArgumentError {
    #[error("GetCurrentTimestampError: {0}")]
    GetCurrentTimestampError(FunctionLeafNodeGetCurrentTimestampArgumentError),
}

#[derive(Error, Debug)]
#[non_exhaustive]
#[allow(missing_docs)]
pub enum FunctionLeafNodeGetCurrentTimestampArgumentError {
    #[error("Time source is not set")]
    TimeSourceNotSet,
    #[error("Time source is unsupported")]
    TimeSourceNotSupported,
}
