// Copyright 2023 Google LLC. All Rights Reserved.

//! Helper functions used for AST tree validation.

use protobuf::reflect::{MessageDescriptor, RuntimeFieldType, RuntimeType};
use sdvtelemetry_proto_rust::expression::{
    combination_node::Operator as OperatorProto,
    combination_node::{ArithmeticOperator, LogicalOperator, RelationalOperator},
    constant_leaf_node::Node_value as NodeValueProto,
    ConstantLeafNode, FieldLeafNode,
};

use crate::expr::{ast::DescriptorProvider, node_errors::NodeError};

/// Used for cycle detection in the expression nodes.
#[derive(Clone, PartialEq)]
pub enum VisitStatus {
    NotVisited,
    Visiting,
    Visited,
}

/// Used for compatibility type checking in expression nodes, such as
/// operator-operand compatibility and protobuf field assignment compatibility.
#[derive(Clone, PartialEq, Debug)]
#[allow(missing_docs)]
pub enum NodeValueType {
    None,
    Bool,
    Int,
    Float,
    Message,
    BoolList,
    IntList,
    FloatList,
}

impl NodeValueType {
    fn is_bool(&self) -> bool {
        matches!(self, NodeValueType::Bool)
    }

    fn is_int(&self) -> bool {
        matches!(self, NodeValueType::Int)
    }

    fn is_numeric(&self) -> bool {
        matches!(self, NodeValueType::Int | NodeValueType::Float)
    }

    // returns true iff `self` is a list type, and `other` is a singular & compatible
    // type to the list
    fn can_apply_list_operator(&self, other: &NodeValueType) -> bool {
        match self {
            NodeValueType::BoolList => other.is_bool(),
            NodeValueType::IntList | NodeValueType::FloatList => other.is_numeric(),
            _ => false,
        }
    }
}

/// Implementing types will be able to get its corresponding NodeValueType.
pub trait NodeValueTypeExtension {
    /// Get self's NodeValueType.
    fn get_node_value_type(&self) -> Result<NodeValueType, NodeError>;
}

impl NodeValueTypeExtension for ConstantLeafNode {
    fn get_node_value_type(&self) -> Result<NodeValueType, NodeError> {
        if self.node_value.is_none() {
            return Err(NodeError::ConstantValueNotSet);
        }
        Ok(match self.node_value.as_ref().unwrap() {
            NodeValueProto::BoolValue(_) => NodeValueType::Bool,
            NodeValueProto::Int32Value(_) | NodeValueProto::Int64Value(_) => NodeValueType::Int,
            NodeValueProto::FloatValue(_) | NodeValueProto::DoubleValue(_) => NodeValueType::Float,
            _ => {
                return Err(NodeError::ConstantUnknownValue { v: format!("{:?}", self.node_value) })
            }
        })
    }
}

pub trait FieldLeafNodeExtension {
    fn get_node_value_type(
        &self,
        descriptor_provider: &dyn DescriptorProvider,
    ) -> Result<NodeValueType, NodeError>;
}

impl FieldLeafNodeExtension for FieldLeafNode {
    fn get_node_value_type(
        &self,
        descriptor_provider: &dyn DescriptorProvider,
    ) -> Result<NodeValueType, NodeError> {
        if self.field_names.is_empty() {
            return Ok(NodeValueType::Message);
        }
        let descriptor = &descriptor_provider
            .get_descriptor(&self.publisher_name)
            .map_err(|e| NodeError::DescriptorProviderError { e })?;
        get_nested_field_type(descriptor, &self.field_names)
    }
}

fn get_nested_field_type(
    message_descriptor: &MessageDescriptor,
    field_names: &[String],
) -> Result<NodeValueType, NodeError> {
    let field_name = &field_names[0];
    // look up field by name
    if let Some(field_descriptor) = message_descriptor.field_by_name(field_name) {
        if field_names.len() == 1 {
            // final field in the chain, return its type
            field_descriptor.runtime_field_type().get_node_value_type()
        } else if let RuntimeFieldType::Singular(RuntimeType::Message(nested_msg_descriptor)) =
            field_descriptor.runtime_field_type()
        {
            // intermediate field must be a singular Message field
            get_nested_field_type(&nested_msg_descriptor, &field_names[1..])
        } else {
            // all other intermediate field type is wrong
            Err(NodeError::WrongNestedFieldType {
                field_name: field_name.clone(),
                e: "A non-terminal field must be a singular message".to_string(),
            })
        }
    } else {
        Err(NodeError::FieldNotFound {
            field_name: field_name.clone(),
            message_name: message_descriptor.full_name().to_string(),
        })
    }
}

pub trait OperatorProtoExtension {
    /// Helper function to check if operator is unary.
    fn is_unary_operator(&self) -> bool;

    /// Helper function to check if operator requires a right child.
    fn check_right_child(&self, right_index: &Option<u32>) -> Result<(), NodeError>;

    /// Checks if operands are compatible with operator, and return the node value type
    fn get_node_value_type(
        &self,
        node_value_types: &[NodeValueType],
        left: usize,
        right: &Option<u32>,
    ) -> Result<NodeValueType, NodeError>;
}

impl OperatorProtoExtension for OperatorProto {
    fn is_unary_operator(&self) -> bool {
        if let OperatorProto::LogicalOperator(op) = self {
            return op.enum_value_or_default() == LogicalOperator::NOT;
        }
        false
    }

    fn check_right_child(&self, right_index: &Option<u32>) -> Result<(), NodeError> {
        let unary_op = self.is_unary_operator();
        let has_right_child = right_index.is_some();
        if unary_op && has_right_child {
            Err(NodeError::UnaryOperatorIncorrectChildren)
        } else if !unary_op && !has_right_child {
            Err(NodeError::BinaryOperatorIncorrectChildren)
        } else {
            Ok(())
        }
    }

    fn get_node_value_type(
        &self,
        node_value_types: &[NodeValueType],
        left_idx: usize,
        right_idx: &Option<u32>,
    ) -> Result<NodeValueType, NodeError> {
        let left = &node_value_types[left_idx];
        let right = if let Some(right_idx) = right_idx {
            &node_value_types[*right_idx as usize]
        } else {
            &NodeValueType::None
        };
        match self {
            // relational operator output type = boolean
            OperatorProto::RelationalOperator(op) => {
                match op.enum_value().map_err(|i| NodeError::UnknownOperator {
                    op: format!("Relational operator with enum value {}", i),
                })? {
                    RelationalOperator::EQ | RelationalOperator::NOT_EQ => Ok(NodeValueType::Bool),
                    RelationalOperator::GT
                    | RelationalOperator::GT_OR_EQ
                    | RelationalOperator::LT
                    | RelationalOperator::LT_OR_EQ => {
                        if left.is_numeric() && right.is_numeric() {
                            Ok(NodeValueType::Bool)
                        } else {
                            Err(NodeError::ComparisonOperatorIncomptaibleOperands)
                        }
                    }
                    RelationalOperator::CONTAINS | RelationalOperator::DOES_NOT_CONTAIN => {
                        if left.can_apply_list_operator(right) {
                            Ok(NodeValueType::Bool)
                        } else {
                            Err(NodeError::ContainsOperatorIncompatibleOperands)
                        }
                    }
                    RelationalOperator::ALL_EQ => {
                        if left.can_apply_list_operator(right) {
                            Ok(NodeValueType::Bool)
                        } else {
                            Err(NodeError::AllEqualsOperatorIncompatibleOperands)
                        }
                    }
                }
            }
            // logical operator output type = boolean
            OperatorProto::LogicalOperator(op) => match op.enum_value().map_err(|i| {
                NodeError::UnknownOperator { op: format!("Logical operator with enum value {}", i) }
            })? {
                LogicalOperator::AND | LogicalOperator::OR | LogicalOperator::XOR => {
                    if left.is_bool() && right.is_bool() {
                        Ok(NodeValueType::Bool)
                    } else {
                        Err(NodeError::LogicalOperatorIncompatibleOperands)
                    }
                }
                LogicalOperator::NOT => {
                    if right != &NodeValueType::None {
                        Err(NodeError::UnaryOperatorIncorrectChildren)
                    } else if left.is_bool() {
                        Ok(NodeValueType::Bool)
                    } else {
                        Err(NodeError::LogicalOperatorIncompatibleOperands)
                    }
                }
            },
            // arithmetic operator output type = number
            OperatorProto::ArithmeticOperator(op) => {
                match op.enum_value().map_err(|i| NodeError::UnknownOperator {
                    op: format!("Arithmetic operator with enum value {}", i),
                })? {
                    ArithmeticOperator::ADD
                    | ArithmeticOperator::SUBTRACT
                    | ArithmeticOperator::MULTIPLY
                    | ArithmeticOperator::POWER => {
                        if left.is_int() && right.is_int() {
                            Ok(NodeValueType::Int)
                        } else if left.is_numeric() && right.is_numeric() {
                            Ok(NodeValueType::Float)
                        } else {
                            Err(NodeError::ArithmeticOperatorIncompatibleOperands)
                        }
                    }
                    ArithmeticOperator::DIVIDE => {
                        if left.is_numeric() && right.is_numeric() {
                            Ok(NodeValueType::Float)
                        } else {
                            Err(NodeError::ArithmeticOperatorIncompatibleOperands)
                        }
                    }
                    ArithmeticOperator::MODULUS => {
                        if left.is_int() && right.is_int() {
                            Ok(NodeValueType::Int)
                        } else {
                            Err(NodeError::ArithmeticOperatorIncompatibleOperands)
                        }
                    }
                }
            }
            _ => Err(NodeError::UnknownOperator { op: format!("{:?}", self) }),
        }
    }
}

impl NodeValueTypeExtension for RuntimeFieldType {
    fn get_node_value_type(&self) -> Result<NodeValueType, NodeError> {
        match self {
            RuntimeFieldType::Singular(runtime_type) => runtime_type.get_node_value_type(),
            RuntimeFieldType::Repeated(runtime_type) => {
                match runtime_type
                    .get_node_value_type()
                    .map_err(|_| NodeError::UnsupportedRepeatedFieldType)?
                {
                    NodeValueType::Bool => Ok(NodeValueType::BoolList),
                    NodeValueType::Int => Ok(NodeValueType::IntList),
                    NodeValueType::Float => Ok(NodeValueType::FloatList),
                    _ => Err(NodeError::UnsupportedRepeatedFieldType),
                }
            }
            RuntimeFieldType::Map(..) => Err(NodeError::UnsupportedSingularFieldType),
        }
    }
}

impl NodeValueTypeExtension for RuntimeType {
    fn get_node_value_type(&self) -> Result<NodeValueType, NodeError> {
        match self {
            RuntimeType::I32
            | RuntimeType::I64
            | RuntimeType::U32
            | RuntimeType::U64
            | RuntimeType::Enum(_) => Ok(NodeValueType::Int),
            RuntimeType::F32 | RuntimeType::F64 => Ok(NodeValueType::Float),
            RuntimeType::Bool => Ok(NodeValueType::Bool),
            RuntimeType::Message(_) => Ok(NodeValueType::Message),
            RuntimeType::String | RuntimeType::VecU8 => {
                Err(NodeError::UnsupportedSingularFieldType)
            }
        }
    }
}
