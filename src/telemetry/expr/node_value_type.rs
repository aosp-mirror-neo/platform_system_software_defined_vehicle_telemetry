// Copyright 2023 Google LLC

//! Helper functions used for AST tree validation.

use protobuf::reflect::{MessageDescriptor, RuntimeFieldType, RuntimeType};
use sdv_telemetry_proto_rust::expression::{
    combination_node::Operator as OperatorProto,
    combination_node::{ArithmeticOperator, LogicalOperator, RelationalOperator},
    constant_leaf_node::Node_value as NodeValueProto,
    function_leaf_node::Function_type as FunctionLeafNodeTypeProto,
    ConstantLeafNode, FieldLeafNode, FunctionLeafNode,
};

use crate::expr::{node_error::NodeEvaluationError, node_factory::DescriptorProvider};

/// Used for compatibility type checking in expression nodes, such as
/// operator-operand compatibility and protobuf field assignment compatibility.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) enum NodeValueType {
    Bool,
    Int,
    Float,
    Message { descriptor: Option<MessageDescriptor> },
    String,
    ByteList,
    BoolList,
    IntList,
    FloatList,
    MessageList { descriptor: Option<MessageDescriptor> },
}

impl NodeValueType {
    pub fn is_bool(&self) -> bool {
        matches!(self, NodeValueType::Bool)
    }

    pub fn is_numeric(&self) -> bool {
        self.is_int() || self.is_float()
    }

    fn is_float(&self) -> bool {
        matches!(self, NodeValueType::Float)
    }

    fn is_int(&self) -> bool {
        matches!(self, NodeValueType::Int)
    }

    fn is_list(&self) -> bool {
        matches!(
            self,
            NodeValueType::BoolList
                | NodeValueType::IntList
                | NodeValueType::FloatList
                | NodeValueType::MessageList { .. }
        )
    }

    // returns true if `self` is a list type, and `other` is a singular & compatible
    // type to the list
    fn can_apply_list_operator(&self, other: Option<&NodeValueType>) -> bool {
        match self {
            NodeValueType::BoolList => other.is_none_or(|o| o.is_bool()),
            NodeValueType::IntList | NodeValueType::FloatList => {
                other.is_none_or(|o| o.is_numeric())
            }
            NodeValueType::MessageList { descriptor } => {
                other.is_none_or(|o| {
                    // Compatible if the other type is a message and the descriptors
                    // match or not specified in either.
                    let NodeValueType::Message { descriptor: other_descriptor } = o else {
                        return false;
                    };
                    if let (Some(descriptor), Some(other_descriptor)) =
                        (descriptor, other_descriptor)
                    {
                        descriptor == other_descriptor
                    } else {
                        true
                    }
                })
            }
            _ => false,
        }
    }
}

/// Implementing types will be able to get its corresponding NodeValueType.
pub trait NodeValueTypeExtension {
    /// Get self's NodeValueType.
    fn get_node_value_type(&self) -> Result<NodeValueType, NodeEvaluationError>;
}

impl NodeValueTypeExtension for ConstantLeafNode {
    fn get_node_value_type(&self) -> Result<NodeValueType, NodeEvaluationError> {
        Ok(match &self.node_value {
            Some(node_value) => match node_value {
                NodeValueProto::BoolValue(_) => NodeValueType::Bool,
                NodeValueProto::Int32Value(_) | NodeValueProto::Int64Value(_) => NodeValueType::Int,
                NodeValueProto::FloatValue(_) | NodeValueProto::DoubleValue(_) => {
                    NodeValueType::Float
                }
                _ => {
                    return Err(NodeEvaluationError::ConstantValueNotSupported {
                        node_value: node_value.clone(),
                    })
                }
            },
            None => return Err(NodeEvaluationError::ConstantValueNotSet),
        })
    }
}

impl NodeValueTypeExtension for FunctionLeafNode {
    fn get_node_value_type(&self) -> Result<NodeValueType, NodeEvaluationError> {
        let function_type =
            self.function_type.as_ref().ok_or(NodeEvaluationError::FunctionLeafNodeNotSet)?;
        Ok(match function_type {
            FunctionLeafNodeTypeProto::GetCurrentTimestamp(_) => NodeValueType::Int,
            _ => return Err(NodeEvaluationError::FunctionLeafNodeNotSupported),
        })
    }
}

pub trait FieldLeafNodeExtension {
    fn get_node_value_type(
        &self,
        descriptor_provider: &dyn DescriptorProvider,
    ) -> Result<Option<NodeValueType>, NodeEvaluationError>;
}

impl FieldLeafNodeExtension for FieldLeafNode {
    fn get_node_value_type(
        &self,
        descriptor_provider: &dyn DescriptorProvider,
    ) -> Result<Option<NodeValueType>, NodeEvaluationError> {
        let maybe_descriptor = descriptor_provider.get_descriptor(&self.publisher_name)?;

        if self.field_names.is_empty() {
            Ok(Some(NodeValueType::Message { descriptor: maybe_descriptor.cloned() }))
        } else if let Some(descriptor) = maybe_descriptor {
            Ok(Some(get_nested_field_type(descriptor, &self.field_names)?))
        } else {
            Ok(None)
        }
    }
}

fn get_nested_field_type(
    message_descriptor: &MessageDescriptor,
    field_names: &[String],
) -> Result<NodeValueType, NodeEvaluationError> {
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
            Err(NodeEvaluationError::WrongNestedFieldType {
                field_name: field_name.clone(),
                e: "A non-terminal field must be a singular message".to_string(),
            })
        }
    } else {
        Err(NodeEvaluationError::FieldNotFound {
            field_name: field_name.clone(),
            message_name: message_descriptor.full_name().to_string(),
        })
    }
}

pub trait OperatorProtoExtension {
    /// Helper function to check if operator is unary.
    fn is_unary_operator(&self) -> bool;

    /// Helper function to check if operator requires a right child.
    fn check_right_child(&self, right_index: Option<u32>) -> Result<(), NodeEvaluationError>;

    /// Checks if operands are compatible with operator, and return the node value type
    fn get_node_value_type(
        &self,
        node_value_types: &[Option<NodeValueType>],
        left: usize,
        right: Option<u32>,
    ) -> Result<Option<NodeValueType>, NodeEvaluationError>;
}

impl OperatorProtoExtension for OperatorProto {
    fn is_unary_operator(&self) -> bool {
        match self {
            OperatorProto::RoundingOperator(_) => true,
            OperatorProto::LogicalOperator(op) => {
                op.enum_value_or_default() == LogicalOperator::NOT
            }
            OperatorProto::ArithmeticOperator(op) => matches!(
                op.enum_value_or_default(),
                ArithmeticOperator::UNARY_MINUS | ArithmeticOperator::ABSOLUTE
            ),
            _ => false,
        }
    }

    fn check_right_child(&self, right_index: Option<u32>) -> Result<(), NodeEvaluationError> {
        let unary_op = self.is_unary_operator();
        let has_right_child = right_index.is_some();
        if unary_op && has_right_child {
            Err(NodeEvaluationError::UnaryOperatorIncorrectChildren)
        } else if !unary_op && !has_right_child {
            Err(NodeEvaluationError::BinaryOperatorIncorrectChildren)
        } else {
            Ok(())
        }
    }

    fn get_node_value_type(
        &self,
        node_value_types: &[Option<NodeValueType>],
        left_idx: usize,
        right_idx: Option<u32>,
    ) -> Result<Option<NodeValueType>, NodeEvaluationError> {
        let left = &node_value_types[left_idx];
        let get_right = || {
            let idx = right_idx.expect("check_right_child should verify right child exists");
            node_value_types[idx as usize].as_ref()
        };

        match self {
            // relational operator output type = boolean
            OperatorProto::RelationalOperator(op) => {
                match op.enum_value().map_err(|i| NodeEvaluationError::UnknownOperator {
                    op: format!("Relational operator with enum value {}", i),
                })? {
                    RelationalOperator::EQ if left.as_ref().is_none_or(|l| l.is_list()) => {
                        if left.as_ref().is_none_or(|l| l.can_apply_list_operator(get_right())) {
                            Ok(Some(NodeValueType::Bool))
                        } else {
                            Err(NodeEvaluationError::ComparisonOperatorListIncompatibleOperands)
                        }
                    }
                    RelationalOperator::EQ | RelationalOperator::NOT_EQ => {
                        Ok(Some(NodeValueType::Bool))
                    }
                    RelationalOperator::GT
                    | RelationalOperator::GT_OR_EQ
                    | RelationalOperator::LT
                    | RelationalOperator::LT_OR_EQ => {
                        if (left.as_ref().is_none_or(|l| l.is_numeric()))
                            && get_right().is_none_or(|r| r.is_numeric())
                        {
                            Ok(Some(NodeValueType::Bool))
                        } else {
                            Err(NodeEvaluationError::ComparisonOperatorIncomptaibleOperands)
                        }
                    }
                    RelationalOperator::CONTAINS | RelationalOperator::DOES_NOT_CONTAIN => {
                        if left.as_ref().is_none_or(|l| l.can_apply_list_operator(get_right())) {
                            Ok(Some(NodeValueType::Bool))
                        } else {
                            Err(NodeEvaluationError::ContainsOperatorIncompatibleOperands)
                        }
                    }
                    RelationalOperator::ALL_EQ => {
                        Err(NodeEvaluationError::DeprecatedOperator { op: self.clone() })
                    }
                }
            }
            // logical operator output type = boolean
            OperatorProto::LogicalOperator(op) => {
                match op.enum_value().map_err(|i| NodeEvaluationError::UnknownOperator {
                    op: format!("Logical operator with enum value {}", i),
                })? {
                    LogicalOperator::AND | LogicalOperator::OR | LogicalOperator::XOR => {
                        if left.as_ref().is_none_or(|l| l.is_bool())
                            && get_right().is_none_or(|r| r.is_bool())
                        {
                            Ok(Some(NodeValueType::Bool))
                        } else {
                            Err(NodeEvaluationError::LogicalOperatorIncompatibleOperands)
                        }
                    }
                    LogicalOperator::NOT => {
                        if left.as_ref().is_none_or(|l| l.is_bool()) {
                            Ok(Some(NodeValueType::Bool))
                        } else {
                            Err(NodeEvaluationError::LogicalOperatorIncompatibleOperands)
                        }
                    }
                }
            }
            // arithmetic operator output type = number
            OperatorProto::ArithmeticOperator(op) => {
                match op.enum_value().map_err(|i| NodeEvaluationError::UnknownOperator {
                    op: format!("Arithmetic operator with enum value {}", i),
                })? {
                    ArithmeticOperator::ADD
                    | ArithmeticOperator::SUBTRACT
                    | ArithmeticOperator::MULTIPLY
                    | ArithmeticOperator::POWER => {
                        if left.as_ref().is_some_and(|l| l.is_int())
                            && get_right().is_some_and(|r| r.is_int())
                        {
                            // TODO: b/389623348 - For `ArithmeticOperator::POWER`, ideally, we'd
                            // return `Int` if `get_right()` fits into a `u32`, and only return
                            // `Float` if it doesn't, to match the implementation. However, at this
                            // point we don't have any runtime value information and no concept of
                            // "unsigned" integers.
                            //
                            // Therefore, the best we can do is continue to return `Int` (which is
                            // assignable to `Float`). This is not perfect, because validation will
                            // not catch the case when `2^(-2) = 0.25` is being assigned to an Int
                            // field (which will fail with a runtime error).
                            //
                            // See ag/31234259 for the CL that made this change.
                            Ok(Some(NodeValueType::Int))
                        } else if left.as_ref().is_some_and(|l| l.is_numeric())
                            && get_right().is_some_and(|l| l.is_numeric())
                        {
                            Ok(Some(NodeValueType::Float))
                        } else if left.as_ref().is_none_or(|l| l.is_int())
                            && get_right().is_none_or(|r| r.is_int())
                        {
                            // Type cannot be determined because it can be either int or float.
                            Ok(None)
                        } else if left.as_ref().is_none_or(|l| l.is_numeric())
                            && get_right().is_none_or(|r| r.is_numeric())
                        {
                            // If the known argument is a float, the result will
                            // always be float, not int.
                            Ok(Some(NodeValueType::Float))
                        } else {
                            Err(NodeEvaluationError::ArithmeticOperatorIncompatibleOperands)
                        }
                    }
                    ArithmeticOperator::DIVIDE => {
                        if left.as_ref().is_none_or(|l| l.is_numeric())
                            && get_right().is_none_or(|r| r.is_numeric())
                        {
                            Ok(Some(NodeValueType::Float))
                        } else {
                            Err(NodeEvaluationError::ArithmeticOperatorIncompatibleOperands)
                        }
                    }
                    ArithmeticOperator::MODULO_TRUNC => {
                        if left.as_ref().is_none_or(|l| l.is_int())
                            && get_right().is_none_or(|r| r.is_int())
                        {
                            Ok(Some(NodeValueType::Int))
                        } else {
                            Err(NodeEvaluationError::ArithmeticOperatorIncompatibleOperands)
                        }
                    }
                    ArithmeticOperator::UNARY_MINUS => match left {
                        None => Ok(None),
                        Some(left) => {
                            if left.is_int() {
                                Ok(Some(NodeValueType::Int))
                            } else if left.is_numeric() {
                                Ok(Some(NodeValueType::Float))
                            } else {
                                Err(NodeEvaluationError::ArithmeticOperatorIncompatibleOperands)
                            }
                        }
                    },
                    ArithmeticOperator::ABSOLUTE => match left {
                        None => Ok(None),
                        Some(left) => {
                            if left.is_int() {
                                Ok(Some(NodeValueType::Int))
                            } else if left.is_numeric() {
                                Ok(Some(NodeValueType::Float))
                            } else {
                                Err(NodeEvaluationError::ArithmeticOperatorIncompatibleOperands)
                            }
                        }
                    },
                }
            }
            // rounding operator output type = int
            OperatorProto::RoundingOperator(_) => {
                if left.as_ref().is_none_or(|l| l.is_float()) {
                    Ok(Some(NodeValueType::Int))
                } else {
                    Err(NodeEvaluationError::RoundingOperatorIncompatibleOperands)
                }
            }
            _ => Err(NodeEvaluationError::UnknownOperator { op: format!("{:?}", self) }),
        }
    }
}

impl NodeValueTypeExtension for RuntimeFieldType {
    fn get_node_value_type(&self) -> Result<NodeValueType, NodeEvaluationError> {
        match self {
            RuntimeFieldType::Singular(runtime_type) => runtime_type.get_node_value_type(),
            RuntimeFieldType::Repeated(runtime_type) => {
                match runtime_type
                    .get_node_value_type()
                    .map_err(|_| NodeEvaluationError::UnsupportedRepeatedFieldType)?
                {
                    NodeValueType::Bool => Ok(NodeValueType::BoolList),
                    NodeValueType::Int => Ok(NodeValueType::IntList),
                    NodeValueType::Float => Ok(NodeValueType::FloatList),
                    NodeValueType::Message { descriptor } => {
                        Ok(NodeValueType::MessageList { descriptor: descriptor.clone() })
                    }
                    _ => Err(NodeEvaluationError::UnsupportedRepeatedFieldType),
                }
            }
            RuntimeFieldType::Map(..) => Err(NodeEvaluationError::UnsupportedSingularFieldType),
        }
    }
}

impl NodeValueTypeExtension for RuntimeType {
    fn get_node_value_type(&self) -> Result<NodeValueType, NodeEvaluationError> {
        match self {
            RuntimeType::I32
            | RuntimeType::I64
            | RuntimeType::U32
            | RuntimeType::U64
            | RuntimeType::Enum(_) => Ok(NodeValueType::Int),
            RuntimeType::F32 | RuntimeType::F64 => Ok(NodeValueType::Float),
            RuntimeType::Bool => Ok(NodeValueType::Bool),
            RuntimeType::String => Ok(NodeValueType::String),
            RuntimeType::Message(descriptor) => {
                Ok(NodeValueType::Message { descriptor: Some(descriptor.clone()) })
            }
            RuntimeType::VecU8 => Ok(NodeValueType::ByteList),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use protobuf::{
        well_known_types::wrappers::FloatValue, well_known_types::wrappers::Int32Value, MessageFull,
    };
    use sdv_telemetry_test_utils::parameterized_test;

    parameterized_test!(
        #[test]
        is_bool,

        |expected, value_type: NodeValueType| {
            assert_eq!(
                value_type.is_bool(),
                expected,
                "{value_type:?}.is_bool() should be {expected:?}"
            );
        },

        bool: (true, NodeValueType::Bool,),

        int: (false, NodeValueType::Int,),
        float: (false, NodeValueType::Float,),
        string: (false, NodeValueType::String,),
        float_list: (false, NodeValueType::FloatList,),
    );

    parameterized_test!(
        #[test]
        is_numeric,

        |expected, value_type: NodeValueType| {
            assert_eq!(
                value_type.is_numeric(),
                expected,
                "{value_type:?}.is_numeric() should be {expected:?}"
            );
        },

        int: (true, NodeValueType::Int,),
        float: (true, NodeValueType::Float,),

        bool: (false, NodeValueType::Bool,),
        int_list: (false, NodeValueType::IntList,),
        float_list: (false, NodeValueType::FloatList,),
    );

    parameterized_test!(
        #[test]
        is_int,

        |expected, value_type: NodeValueType| {
            assert_eq!(
                value_type.is_int(),
                expected,
                "{value_type:?}.is_int() should be {expected:?}"
            );
        },

        int: (true, NodeValueType::Int,),

        bool: (false, NodeValueType::Bool,),
        float: (false, NodeValueType::Float,),
        string: (false, NodeValueType::String,),
        int_list: (false, NodeValueType::IntList,),
    );

    parameterized_test!(
        #[test]
        is_float,

        |expected, value_type: NodeValueType| {
            assert_eq!(
                value_type.is_float(),
                expected,
                "{value_type:?}.is_float() should be {expected:?}"
            );
        },

        float: (true, NodeValueType::Float,),

        bool: (false, NodeValueType::Bool,),
        int: (false, NodeValueType::Int,),
        string: (false, NodeValueType::String,),
        float_list: (false, NodeValueType::FloatList,),
    );

    parameterized_test!(
        #[test]
        can_apply_list_operator_for,

        |expected, value_type: NodeValueType, other: Option<&NodeValueType>| {
            assert_eq!(
                value_type.can_apply_list_operator(other),
                expected,
                "{value_type:?}.can_apply_list_operator({other:?}) should be {expected:?}"
            );
        },

        bool_list_on_none: (true, NodeValueType::BoolList, None,),
        bool_list_on_bool: (true, NodeValueType::BoolList, Some(&NodeValueType::Bool),),
        bool_list_on_int: (false, NodeValueType::BoolList, Some(&NodeValueType::Int),),

        int_list_on_none: (true, NodeValueType::IntList, None,),
        int_list_on_int: (true, NodeValueType::IntList, Some(&NodeValueType::Float),),
        int_list_on_float: (true, NodeValueType::IntList, Some(&NodeValueType::Int),),
        int_list_on_bool: (false, NodeValueType::IntList, Some(&NodeValueType::Bool),),

        float_list_on_none: (true, NodeValueType::FloatList, None,),
        float_list_on_int: (true, NodeValueType::FloatList, Some(&NodeValueType::Int),),
        float_list_on_float: (true, NodeValueType::FloatList, Some(&NodeValueType::Float),),
        float_list_on_bool: (false, NodeValueType::FloatList, Some(&NodeValueType::Bool),),

        message_list_with_none_descriptor_on_none: (
            true,
            NodeValueType::MessageList { descriptor: None },
            None,
        ),
        message_list_with_some_descriptor_on_none: (
            true,
            NodeValueType::MessageList { descriptor: Some(Int32Value::descriptor()) },
            None,
        ),
        message_list_with_int_descriptor_on_message_with_int_descriptor: (
            true,
            NodeValueType::MessageList { descriptor: Some(Int32Value::descriptor()) },
            Some(&NodeValueType::Message { descriptor: Some(Int32Value::descriptor()) }),
        ),
        message_list_with_int_descriptor_on_message_with_none_descriptor: (
            true,
            NodeValueType::MessageList { descriptor: Some(Int32Value::descriptor()) },
            Some(&NodeValueType::Message { descriptor: None }),
        ),
        message_list_with_none_descriptor_on_message_with_int_descriptor: (
            true,
            NodeValueType::MessageList { descriptor: None },
            Some(&NodeValueType::Message { descriptor: Some(Int32Value::descriptor()) }),
        ),
        message_list_with_none_descriptor_on_message_with_none_descriptor: (
            true,
            NodeValueType::MessageList { descriptor: None },
            Some(&NodeValueType::Message { descriptor: None }),
        ),
        message_list_with_float_descriptor_on_message_with_int_descriptor: (
            false,
            NodeValueType::MessageList { descriptor: Some(FloatValue::descriptor()) },
            Some(&NodeValueType::Message { descriptor: Some(Int32Value::descriptor()) }),
        ),
        message_list_on_bool: (
            false,
            NodeValueType::MessageList { descriptor: Some(FloatValue::descriptor()) },
            Some(&NodeValueType::Bool),
        ),
        message_list_on_int: (
            false,
            NodeValueType::MessageList { descriptor: Some(FloatValue::descriptor()) },
            Some(&NodeValueType::Int),
        ),

        bool_on_none: (false, NodeValueType::Bool, None,),
        int_on_none: (false, NodeValueType::Int, None,),
        float_on_none: (false, NodeValueType::Float, None,),
        message_with_none_descriptor_on_none: (false, NodeValueType::Message { descriptor: None }, None,),
        message_with_some_descriptor_on_none: (false, NodeValueType::Message { descriptor: Some(Int32Value::descriptor()) }, None,),
        // `ByteList` does not currently support edge processing.
        byte_list_on_none: (false, NodeValueType::ByteList, None,),
    );
}
