// Copyright 2023 Google LLC. All Rights Reserved.

use protobuf::reflect::{FileDescriptor, MessageDescriptor};
use sdvtelemetry_proto_rust::metrics_configuration::{
    proto_message_builder::field_assignment::AggregationType, ProtoMessageBuilder,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use thiserror::Error;

use crate::expr::ast::Node;
use crate::expr::ast_extensions::{NodeValueType, NodeValueTypeExtension};
use crate::metrics_config_orchestrator::message_builder::{
    AggregationFactory, MessageBuilder, MessageBuilderImpl,
};
use crate::utils::collection_extension::HashMapExtension;

/// Factory that validates MetricsConfig's ProtoMessageBuilder and creates
/// corresponding Rust representations.
pub(crate) trait MessageBuilderFactory: Send + Sync {
    fn validate(
        &self,
        proto_message_builder: &ProtoMessageBuilder,
        context: &MessageBuilderFactoryValidateContext,
    ) -> Result<(), MessageBuilderFactoryError>;

    fn create(
        &self,
        proto_message_builder: &ProtoMessageBuilder,
        context: &MessageBuilderFactoryCreateContext,
    ) -> Result<Box<dyn MessageBuilder>, MessageBuilderFactoryError>;
}

pub(crate) struct MessageBuilderFactoryCreateContext<'a> {
    pub file_descriptors: &'a [FileDescriptor],
    pub nodes: &'a [Arc<Node>],
    pub node_types: &'a [NodeValueType],
}

pub(crate) struct MessageBuilderFactoryValidateContext<'a> {
    pub file_descriptors: &'a [FileDescriptor],
    pub node_types: &'a [NodeValueType],
}

#[allow(dead_code)]
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum MessageBuilderFactoryError {
    #[error("Message {message_type} is not found in the provided file descriptor protos")]
    MessageNotFound { message_type: String },
    #[error("Field {field_name} not found in message {message_type}")]
    FieldNotFound { field_name: String, message_type: String },
    #[error("Multiple assignments found for field {field_name} in message {message_type}")]
    DuplicateFieldAssignment { field_name: String, message_type: String },
    #[error("Required field selector_node_index is not set for field assignment {field_name}")]
    MissingNodeIndex { field_name: String },
    #[error("Field assignment for {field_name} is an expression node at invalid index {i}")]
    InvalidNodeIndex { field_name: String, i: u32 },
    #[error("Field {field_name} of message {message_type} has unsupported type")]
    UnsupportedFieldType { field_name: String, message_type: String },
    #[error(
        "Field {field_name} of message {message_type} has incompatible value assignment: {msg}"
    )]
    IncompatibleFieldAssignment { field_name: String, message_type: String, msg: String },
    #[error("Field {field_name} of message {message_type} has unrecognized aggregation type {i}")]
    UnknownAggregationType { field_name: String, message_type: String, i: i32 },
    #[error("Field {field_name} of message {message_type} did not specify aggregation type")]
    AggregationTypeNotSet { field_name: String, message_type: String },
    #[error("Cannot perform aggregation type {aggr_type} on node value type {node_type}")]
    UnsupportedAggregation { aggr_type: String, node_type: String },
}

impl<'a> MessageBuilderFactoryCreateContext<'a> {
    /// Constructor for the context used by factory's create() method
    pub(crate) fn new(
        file_descriptors: &'a [FileDescriptor],
        nodes: &'a [Arc<Node>],
        node_types: &'a [NodeValueType],
    ) -> Self {
        MessageBuilderFactoryCreateContext { file_descriptors, nodes, node_types }
    }
}

#[allow(dead_code)]
impl<'a> MessageBuilderFactoryValidateContext<'a> {
    /// Constructor for the context used by factory's validate() method
    pub(crate) fn new(
        file_descriptors: &'a [FileDescriptor],
        node_types: &'a [NodeValueType],
    ) -> Self {
        MessageBuilderFactoryValidateContext { file_descriptors, node_types }
    }
}

/// Concrete implementation of the factory trait.
pub(crate) struct MessageBuilderFactoryImpl {}

impl MessageBuilderFactory for MessageBuilderFactoryImpl {
    fn validate(
        &self,
        proto_message_builder: &ProtoMessageBuilder,
        context: &MessageBuilderFactoryValidateContext,
    ) -> Result<(), MessageBuilderFactoryError> {
        let message_descriptor = Self::get_message_descriptor(
            context.file_descriptors,
            &proto_message_builder.message_type,
        )?;

        // validation does not provide the nodes to the helper function
        Self::map_field_assignments(
            proto_message_builder,
            &message_descriptor,
            None,
            context.node_types,
        )?;
        Ok(())
    }

    fn create(
        &self,
        proto_message_builder: &ProtoMessageBuilder,
        context: &MessageBuilderFactoryCreateContext,
    ) -> Result<Box<dyn MessageBuilder>, MessageBuilderFactoryError> {
        let message_descriptor = Self::get_message_descriptor(
            context.file_descriptors,
            &proto_message_builder.message_type,
        )?;
        // maps field_name to an optional tuple that is required for creating field values
        let map = Self::map_field_assignments(
            proto_message_builder,
            &message_descriptor,
            Some(context.nodes),
            context.node_types,
        )?;

        // iterate through raw assignments to build ValueAggregator's
        let field_assignments = map
            .into_iter()
            .map(|(field_name, node_and_aggregation_type_option)| {
                let (node, aggregation_type) = node_and_aggregation_type_option.expect(
                    "All field names should map to valid expression nodes and aggregation types",
                );
                let aggregated_field_value =
                    Mutex::new(AggregationFactory::create(aggregation_type, node));
                // hashmap key = field_name, value = aggregated_field_value
                (field_name, aggregated_field_value)
            })
            .collect();

        Ok(Box::new(MessageBuilderImpl::new(message_descriptor, field_assignments)))
    }
}

impl MessageBuilderFactoryImpl {
    pub fn new() -> Self {
        MessageBuilderFactoryImpl {}
    }

    fn get_message_descriptor(
        file_descriptors: &[FileDescriptor],
        message_type: &str,
    ) -> Result<MessageDescriptor, MessageBuilderFactoryError> {
        // iterate through all file_descriptors, find MessageDescriptor for the given message_type
        // message_by_full_name() method requires the name to start with dot, it returns a clone
        // of the MessageDescriptor, so we can return it as-is without cloning again
        file_descriptors.iter().find_map(|fd| fd.message_by_full_name(message_type)).ok_or(
            MessageBuilderFactoryError::MessageNotFound { message_type: message_type.to_string() },
        )
    }

    fn get_node(nodes: Option<&[Arc<Node>]>, idx: usize) -> Option<&Arc<Node>> {
        nodes?.get(idx)
    }

    /// Helper function to map ProtoMessageBuilder's field assignment to Node, if the Nodes
    /// are provided. Otherwise this function only performs validation.
    ///
    /// Returns a hashmap, where the key is the field_name, the value is an optional tuple
    /// consisting of the expression node (if provided) and the aggregation type.
    fn map_field_assignments<'a>(
        proto_message_builder: &'a ProtoMessageBuilder,
        message_descriptor: &'a MessageDescriptor,
        nodes: Option<&'a [Arc<Node>]>,
        node_types: &'a [NodeValueType],
    ) -> Result<HashMap<String, Option<(&'a Arc<Node>, AggregationType)>>, MessageBuilderFactoryError>
    {
        let get_msg_type = || proto_message_builder.message_type.clone();

        // maps field_name to an optional tuple (&Arc<Node>, AggregationType)
        let mut field_assignments_map = HashMap::new();
        for field_assignment in &proto_message_builder.field_assignments {
            let get_field_name = || field_assignment.field_name.clone();

            // validate the enum value is a known value
            let aggregation_type = field_assignment.aggregation_type.enum_value().map_err(|i| {
                MessageBuilderFactoryError::UnknownAggregationType {
                    field_name: get_field_name(),
                    message_type: get_msg_type(),
                    i,
                }
            })?;

            if aggregation_type == AggregationType::AGGREGATION_TYPE_UNSPECIFIED {
                return Err(MessageBuilderFactoryError::AggregationTypeNotSet {
                    field_name: get_field_name(),
                    message_type: get_msg_type(),
                });
            }

            // validate the field value references a valid node index
            let node_index = field_assignment.expression_node_index.ok_or(
                MessageBuilderFactoryError::MissingNodeIndex { field_name: get_field_name() },
            )?;
            let node_type = node_types.get(node_index as usize).ok_or(
                MessageBuilderFactoryError::InvalidNodeIndex {
                    field_name: get_field_name(),
                    i: node_index,
                },
            )?;

            // validate this field exists in the given message
            let field_descriptor = message_descriptor
                .field_by_name(&field_assignment.field_name)
                .ok_or(MessageBuilderFactoryError::FieldNotFound {
                field_name: get_field_name(),
                message_type: get_msg_type(),
            })?;

            // validate this field has a supported type (no strings, maps, bytes)
            let field_type =
                field_descriptor.runtime_field_type().get_node_value_type().map_err(|_| {
                    MessageBuilderFactoryError::UnsupportedFieldType {
                        field_name: get_field_name(),
                        message_type: get_msg_type(),
                    }
                })?;

            // TODO: b/304597877 - Check compatibility using aggregation type & node type
            // validate this field's type is consistent with the aggregated output's type
            let aggregated_type = Self::get_aggregated_type(node_type, &aggregation_type)?;
            if field_type != aggregated_type {
                return Err(MessageBuilderFactoryError::IncompatibleFieldAssignment {
                    field_name: get_field_name(),
                    message_type: get_msg_type(),
                    msg: format!(
                        "Field expects type {:?} but expression node's output type is {:?}, \
                        and after aggregation, the type will be {:?}",
                        field_type, node_type, aggregated_type,
                    ),
                });
            }

            // validate there is only 1 assignment for each field
            field_assignments_map
                .try_insert_value(
                    get_field_name(),
                    Self::get_node(nodes, node_index as usize).map(|node| (node, aggregation_type)),
                ) // `try_insert_value` returns error if key already exists
                .map_err(|_| MessageBuilderFactoryError::DuplicateFieldAssignment {
                    field_name: get_field_name(),
                    message_type: get_msg_type(),
                })?;
        }
        Ok(field_assignments_map)
    }

    fn get_aggregated_type(
        node_type: &NodeValueType,
        aggregation_type: &AggregationType,
    ) -> Result<NodeValueType, MessageBuilderFactoryError> {
        let err_provider = || {
            Err(MessageBuilderFactoryError::UnsupportedAggregation {
                aggr_type: format!("{:?}", aggregation_type),
                node_type: format!("{:?}", node_type),
            })
        };
        Ok(match aggregation_type {
            AggregationType::AGGREGATION_TYPE_UNSPECIFIED => {
                panic!("AggregationType should be validated to be set")
            }
            AggregationType::NO_AGGREGATION => node_type.clone(),
            AggregationType::VECTOR => match node_type {
                NodeValueType::Bool => NodeValueType::BoolList,
                NodeValueType::Int => NodeValueType::IntList,
                NodeValueType::Float => NodeValueType::FloatList,
                _ => {
                    return err_provider();
                }
            },
            AggregationType::MAX | AggregationType::MIN => {
                if matches!(node_type, NodeValueType::Int | NodeValueType::Float) {
                    node_type.clone()
                } else {
                    return err_provider();
                }
            }
            AggregationType::AVG => {
                if matches!(node_type, NodeValueType::Int | NodeValueType::Float) {
                    NodeValueType::Float
                } else {
                    return err_provider();
                }
            }
            AggregationType::COUNT => NodeValueType::Int,
        })
    }
}

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
mod tests {
    use mock::get_test_file_descriptors;
    use sdvtelemetry_proto_rust::metrics_configuration::proto_message_builder::FieldAssignment;

    use super::*;
    use crate::expr::node_value::NodeValue;

    // Package and message name are in to src/test_utils/test_message.proto
    const TEST_MESSAGE_FULL_NAME: &str = ".android.sdv.telemetry.test.TestMessage";
    const INT_FIELD_NAME: &str = "int_field";
    const FLOAT_FIELD_NAME: &str = "float_field";
    const REPEATED_INT_FIELD_NAME: &str = "repeated_i64s";
    const STRING_FIELD_NAME: &str = "string_field";

    fn create_test_file_descriptors() -> Vec<FileDescriptor> {
        get_test_file_descriptors().to_vec()
    }

    fn create_nodes_with_size(n: usize) -> (Vec<Arc<Node>>, Vec<NodeValueType>) {
        let node = Arc::new(Node::ConstantLeaf { value: NodeValue::Int32(0) });
        let node_type = NodeValueType::Int;
        (vec![node; n], vec![node_type; n])
    }

    fn create_field_assignment_proto(field_name: &str, node_index: u32) -> FieldAssignment {
        create_field_assignment_proto_with_aggregation(
            field_name,
            node_index,
            AggregationType::NO_AGGREGATION,
        )
    }

    fn create_field_assignment_proto_without_node_index() -> FieldAssignment {
        let mut field_assignment = FieldAssignment::new();
        field_assignment.aggregation_type = AggregationType::NO_AGGREGATION.into();
        field_assignment
    }

    fn create_field_assignment_proto_with_aggregation(
        field_name: &str,
        node_index: u32,
        aggregation_type: AggregationType,
    ) -> FieldAssignment {
        let mut field_assignment = FieldAssignment::new();
        field_assignment.field_name = field_name.to_string();
        field_assignment.expression_node_index = Some(node_index);
        field_assignment.aggregation_type = aggregation_type.into();
        field_assignment
    }

    fn create_proto_message_builder(
        message_type: &str,
        field_assignments: Vec<FieldAssignment>,
    ) -> ProtoMessageBuilder {
        let mut builder = ProtoMessageBuilder::new();
        builder.message_type = message_type.to_string();
        builder.field_assignments = field_assignments;
        builder
    }

    #[test]
    fn test_factory_validate_ok() {
        let factory = MessageBuilderFactoryImpl::new();
        let fds = create_test_file_descriptors();
        let node_types = vec![NodeValueType::Int];
        let context = MessageBuilderFactoryValidateContext::new(&fds, &node_types);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            vec![create_field_assignment_proto(INT_FIELD_NAME, 0)],
        );

        assert!(factory.validate(&proto_message_builder, &context).is_ok());
    }

    #[test]
    fn test_factory_validate_no_aggregation_type_fails() {
        let factory = MessageBuilderFactoryImpl::new();
        let fds = create_test_file_descriptors();
        let node_types = vec![NodeValueType::Int];
        let context = MessageBuilderFactoryValidateContext::new(&fds, &node_types);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            vec![{
                let mut field_assignment = FieldAssignment::new();
                field_assignment.field_name = INT_FIELD_NAME.to_string();
                field_assignment.expression_node_index = Some(0);
                // error - didn't set aggregation_type
                field_assignment
            }],
        );

        let err = factory.validate(&proto_message_builder, &context).err().unwrap();

        assert!(
            matches!(err, MessageBuilderFactoryError::AggregationTypeNotSet { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_factory_validate_message_not_found_fails() {
        let factory = MessageBuilderFactoryImpl::new();
        let fds = create_test_file_descriptors();
        let node_types = vec![NodeValueType::Int];
        let context = MessageBuilderFactoryValidateContext::new(&fds, &node_types);
        let proto_message_builder = create_proto_message_builder(
            // not a valid message in test_message.proto
            ".com.example.foo.BarMessage",
            vec![create_field_assignment_proto(INT_FIELD_NAME, 0)],
        );

        let err = factory.validate(&proto_message_builder, &context).err().unwrap();

        assert!(
            matches!(err, MessageBuilderFactoryError::MessageNotFound { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_factory_validate_field_assignment_node_index_not_set_fails() {
        let factory = MessageBuilderFactoryImpl::new();
        let fds = create_test_file_descriptors();
        let node_types = vec![];
        let context = MessageBuilderFactoryValidateContext::new(&fds, &node_types);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            vec![create_field_assignment_proto_without_node_index()],
        );

        let err = factory.validate(&proto_message_builder, &context).err().unwrap();

        assert!(
            matches!(err, MessageBuilderFactoryError::MissingNodeIndex { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_factory_validate_field_assignment_node_index_out_of_bound_fails() {
        let factory = MessageBuilderFactoryImpl::new();
        let fds = create_test_file_descriptors();
        // only 10 nodes in context
        let node_types = vec![NodeValueType::Int; 10];
        let context = MessageBuilderFactoryValidateContext::new(&fds, &node_types);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            // field assignment requires node at index 20
            vec![create_field_assignment_proto(INT_FIELD_NAME, 20)],
        );

        let err = factory.validate(&proto_message_builder, &context).err().unwrap();

        assert!(
            matches!(err, MessageBuilderFactoryError::InvalidNodeIndex { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_factory_validate_duplicate_field_assignment_fails() {
        let factory = MessageBuilderFactoryImpl::new();
        let fds = create_test_file_descriptors();
        let node_types = vec![NodeValueType::Int; 2];
        let context = MessageBuilderFactoryValidateContext::new(&fds, &node_types);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            vec![
                // the same field has 2 assignments
                create_field_assignment_proto(INT_FIELD_NAME, 0),
                create_field_assignment_proto(INT_FIELD_NAME, 1),
            ],
        );

        let err = factory.validate(&proto_message_builder, &context).err().unwrap();

        assert!(
            matches!(err, MessageBuilderFactoryError::DuplicateFieldAssignment { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_factory_validate_field_name_not_found_fails() {
        let factory = MessageBuilderFactoryImpl::new();
        let fds = create_test_file_descriptors();
        let node_types = vec![NodeValueType::Int; 2];
        let context = MessageBuilderFactoryValidateContext::new(&fds, &node_types);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            vec![
                create_field_assignment_proto(INT_FIELD_NAME, 0),
                // not a valid field_name in TestMessage
                create_field_assignment_proto("random_field_name", 1),
            ],
        );

        let err = factory.validate(&proto_message_builder, &context).err().unwrap();

        assert!(
            matches!(err, MessageBuilderFactoryError::FieldNotFound { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_factory_validate_field_has_unsupported_fails() {
        let factory = MessageBuilderFactoryImpl::new();
        let fds = create_test_file_descriptors();
        let node_types = vec![NodeValueType::Bool];
        let context = MessageBuilderFactoryValidateContext::new(&fds, &node_types);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            // String field is not supported
            vec![create_field_assignment_proto(STRING_FIELD_NAME, 0)],
        );

        let err = factory.validate(&proto_message_builder, &context).err().unwrap();

        assert!(
            matches!(err, MessageBuilderFactoryError::UnsupportedFieldType { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_factory_validate_field_has_incompatible_node_output_fails() {
        let factory = MessageBuilderFactoryImpl::new();
        let fds = create_test_file_descriptors();
        // Node at index 0 will produce a boolean output
        let node_types = vec![NodeValueType::Bool];
        let context = MessageBuilderFactoryValidateContext::new(&fds, &node_types);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            // Some int field uses node at index 0 as its value assignment
            vec![create_field_assignment_proto(INT_FIELD_NAME, 0)],
        );

        let err = factory.validate(&proto_message_builder, &context).err().unwrap();

        assert!(
            matches!(err, MessageBuilderFactoryError::IncompatibleFieldAssignment { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_factory_validate_incompatible_node_and_aggregation_type_fails() {
        let factory = MessageBuilderFactoryImpl::new();
        let fds = create_test_file_descriptors();
        let node_types = vec![NodeValueType::IntList];
        let context = MessageBuilderFactoryValidateContext::new(&fds, &node_types);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            // cannot apply VECTOR on IntList node outputs
            vec![create_field_assignment_proto_with_aggregation(
                REPEATED_INT_FIELD_NAME,
                0,
                AggregationType::VECTOR,
            )],
        );

        let err = factory.validate(&proto_message_builder, &context).err().unwrap();

        assert!(
            matches!(err, MessageBuilderFactoryError::UnsupportedAggregation { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_factory_validate_field_has_incompatible_aggregated_output_fails() {
        let factory = MessageBuilderFactoryImpl::new();
        let fds = create_test_file_descriptors();
        let node_types = vec![NodeValueType::Int];
        let context = MessageBuilderFactoryValidateContext::new(&fds, &node_types);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            // AVG will produce a float output, cannot assign float to int field
            vec![create_field_assignment_proto_with_aggregation(
                INT_FIELD_NAME,
                0,
                AggregationType::AVG,
            )],
        );

        let err = factory.validate(&proto_message_builder, &context).err().unwrap();

        assert!(
            matches!(err, MessageBuilderFactoryError::IncompatibleFieldAssignment { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_factory_create_ok() {
        let factory = MessageBuilderFactoryImpl::new();
        let fds = create_test_file_descriptors();
        let (nodes, node_types) = create_nodes_with_size(1);
        let context = MessageBuilderFactoryCreateContext::new(&fds, &nodes, &node_types);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            vec![create_field_assignment_proto(INT_FIELD_NAME, 0)],
        );

        factory.create(&proto_message_builder, &context).unwrap();
    }

    #[test]
    fn test_factory_create_no_aggregation_type_fails() {
        let factory = MessageBuilderFactoryImpl::new();
        let fds = create_test_file_descriptors();
        let (nodes, node_types) = create_nodes_with_size(1);
        let context = MessageBuilderFactoryCreateContext::new(&fds, &nodes, &node_types);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            vec![{
                let mut field_assignment = FieldAssignment::new();
                field_assignment.field_name = INT_FIELD_NAME.to_string();
                field_assignment.expression_node_index = Some(0);
                // error - didn't set aggregation_type
                field_assignment
            }],
        );

        let err = factory.create(&proto_message_builder, &context).err().unwrap();

        assert!(
            matches!(err, MessageBuilderFactoryError::AggregationTypeNotSet { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_factory_create_message_not_found_fails() {
        let factory = MessageBuilderFactoryImpl::new();
        let fds = create_test_file_descriptors();
        let (nodes, node_types) = create_nodes_with_size(2);
        let context = MessageBuilderFactoryCreateContext::new(&fds, &nodes, &node_types);
        let proto_message_builder = create_proto_message_builder(
            // not a valid message in test_message.proto
            ".com.example.foo.BarMessage",
            vec![create_field_assignment_proto(INT_FIELD_NAME, 0)],
        );

        let err = factory.create(&proto_message_builder, &context).err().unwrap();

        assert!(
            matches!(err, MessageBuilderFactoryError::MessageNotFound { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_factory_create_field_assignment_node_index_not_set_fails() {
        let factory = MessageBuilderFactoryImpl::new();
        let fds = create_test_file_descriptors();
        let nodes = vec![];
        let node_types = vec![];
        let context = MessageBuilderFactoryCreateContext::new(&fds, &nodes, &node_types);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            vec![create_field_assignment_proto_without_node_index()],
        );

        let err = factory.create(&proto_message_builder, &context).err().unwrap();

        assert!(
            matches!(err, MessageBuilderFactoryError::MissingNodeIndex { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_factory_create_field_assignment_node_index_out_of_bound_fails() {
        let factory = MessageBuilderFactoryImpl::new();
        // create 10 expression nodes and put it in context
        let fds = create_test_file_descriptors();
        let (nodes, node_types) = create_nodes_with_size(10);
        let context = MessageBuilderFactoryCreateContext::new(&fds, &nodes, &node_types);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            // field assignment requires node at index 20
            vec![create_field_assignment_proto(INT_FIELD_NAME, 20)],
        );

        let err = factory.create(&proto_message_builder, &context).err().unwrap();

        assert!(
            matches!(err, MessageBuilderFactoryError::InvalidNodeIndex { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_factory_create_duplicate_field_assignment_fails() {
        let factory = MessageBuilderFactoryImpl::new();
        let fds = create_test_file_descriptors();
        let (nodes, node_types) = create_nodes_with_size(2);
        let context = MessageBuilderFactoryCreateContext::new(&fds, &nodes, &node_types);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            vec![
                // the same field has 2 assignments
                create_field_assignment_proto(INT_FIELD_NAME, 0),
                create_field_assignment_proto(INT_FIELD_NAME, 1),
            ],
        );

        let err = factory.create(&proto_message_builder, &context).err().unwrap();

        assert!(
            matches!(err, MessageBuilderFactoryError::DuplicateFieldAssignment { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_factory_create_field_name_not_found_fails() {
        let factory = MessageBuilderFactoryImpl::new();
        let fds = create_test_file_descriptors();
        let (nodes, node_types) = create_nodes_with_size(2);
        let context = MessageBuilderFactoryCreateContext::new(&fds, &nodes, &node_types);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            vec![
                create_field_assignment_proto(INT_FIELD_NAME, 0),
                // not a valid field_name in TestMessage
                create_field_assignment_proto("random_field_name", 1),
            ],
        );

        let err = factory.create(&proto_message_builder, &context).err().unwrap();

        assert!(
            matches!(err, MessageBuilderFactoryError::FieldNotFound { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_factory_create_field_has_unsupported_fails() {
        let factory = MessageBuilderFactoryImpl::new();
        let fds = create_test_file_descriptors();
        let (nodes, node_types) = create_nodes_with_size(2);
        let context = MessageBuilderFactoryCreateContext::new(&fds, &nodes, &node_types);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            // String field is not supported
            vec![create_field_assignment_proto(STRING_FIELD_NAME, 0)],
        );

        let err = factory.create(&proto_message_builder, &context).err().unwrap();

        assert!(
            matches!(err, MessageBuilderFactoryError::UnsupportedFieldType { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_factory_create_field_has_incompatible_node_output_fails() {
        let factory = MessageBuilderFactoryImpl::new();
        let fds = create_test_file_descriptors();
        // Node at index 0 will produce an int output
        let (nodes, node_types) = create_nodes_with_size(2);
        let context = MessageBuilderFactoryCreateContext::new(&fds, &nodes, &node_types);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            // Some float field uses node at index 0 as its value assignment
            vec![create_field_assignment_proto(FLOAT_FIELD_NAME, 0)],
        );

        let err = factory.create(&proto_message_builder, &context).err().unwrap();

        assert!(
            matches!(err, MessageBuilderFactoryError::IncompatibleFieldAssignment { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_factory_create_incompatible_node_and_aggregation_type_fails() {
        let factory = MessageBuilderFactoryImpl::new();
        let fds = create_test_file_descriptors();
        let node_types = vec![NodeValueType::IntList];
        let nodes = vec![Arc::new(Node::ConstantLeaf { value: NodeValue::Int32List(vec![]) })];
        let context = MessageBuilderFactoryCreateContext::new(&fds, &nodes, &node_types);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            // cannot apply VECTOR on IntList node outputs
            vec![create_field_assignment_proto_with_aggregation(
                REPEATED_INT_FIELD_NAME,
                0,
                AggregationType::VECTOR,
            )],
        );

        let err = factory.create(&proto_message_builder, &context).err().unwrap();

        assert!(
            matches!(err, MessageBuilderFactoryError::UnsupportedAggregation { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_factory_create_field_has_incompatible_aggregated_output_fails() {
        let factory = MessageBuilderFactoryImpl::new();
        let fds = create_test_file_descriptors();
        let (nodes, node_types) = create_nodes_with_size(1);
        let context = MessageBuilderFactoryCreateContext::new(&fds, &nodes, &node_types);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            // AVERAGE will produce a float output, cannot assign float to int field
            vec![create_field_assignment_proto_with_aggregation(
                INT_FIELD_NAME,
                0,
                AggregationType::AVG,
            )],
        );

        let err = factory.create(&proto_message_builder, &context).err().unwrap();

        assert!(
            matches!(err, MessageBuilderFactoryError::IncompatibleFieldAssignment { .. }),
            "Unexpected error: {}",
            err
        );
    }
}
