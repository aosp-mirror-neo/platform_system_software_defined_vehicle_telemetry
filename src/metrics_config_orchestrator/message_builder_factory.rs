use protobuf::reflect::{FileDescriptor, MessageDescriptor};
use sdv_ast::{Node, NodeValueType, NodeValueTypeExtension};
use sdvtelemetry_proto_rust::metrics_configuration::ProtoMessageBuilder;
use std::{collections::HashMap, sync::Arc};
use thiserror::Error;

use crate::{
    collection_extension::HashMapExtension,
    metrics_config_orchestrator::message_builder::{MessageBuilder, MessageBuilderImpl},
};

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
    #[error("Field assignment for {field_name} is an expression node at invalid index {i}")]
    InvalidNodeIndex { field_name: String, i: u32 },
    #[error("Field {field_name} of message {message_type} has unsupported type")]
    UnsupportedFieldType { field_name: String, message_type: String },
    #[error(
        "Field {field_name} of message {message_type} has incompatible value assignment: {msg}"
    )]
    IncompatibleFieldAssignment { field_name: String, message_type: String, msg: String },
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
        Self::map_field_assignments_to_node(
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
        // maps field_name to Option<&Arc<Node>>
        let map = Self::map_field_assignments_to_node(
            proto_message_builder,
            &message_descriptor,
            Some(context.nodes),
            context.node_types,
        )?;

        // convert HashMap<Key, Option<&Value>> into HashMap<Key, Value>
        let field_assignments = map
            .into_iter()
            .map(|(field_name, node)| {
                (
                    field_name,
                    node.expect(
                        "All indices should be valid and all expression nodes should exist",
                    )
                    .clone(),
                )
            })
            .collect();
        Ok(Box::new(MessageBuilderImpl::new(message_descriptor.clone(), field_assignments)))
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
    fn map_field_assignments_to_node<'a>(
        proto_message_builder: &'a ProtoMessageBuilder,
        message_descriptor: &'a MessageDescriptor,
        nodes: Option<&'a [Arc<Node>]>,
        node_types: &'a [NodeValueType],
    ) -> Result<HashMap<String, Option<&'a Arc<Node>>>, MessageBuilderFactoryError> {
        let get_msg_type = || proto_message_builder.message_type.clone();

        // maps field_name to Option<Arc<Node>>, the node evaluation output is the field value
        let mut field_to_node_map = HashMap::new();
        for field_assignment in &proto_message_builder.field_assignments {
            let get_field_name = || field_assignment.field_name.clone();

            // validate the field value references a valid node index
            let node_type = node_types.get(field_assignment.expression_node_index as usize).ok_or(
                MessageBuilderFactoryError::InvalidNodeIndex {
                    field_name: get_field_name(),
                    i: field_assignment.expression_node_index,
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

            // validate this field's type is consistent with node evaluation output's type
            if field_type != *node_type {
                return Err(MessageBuilderFactoryError::IncompatibleFieldAssignment {
                    field_name: get_field_name(),
                    message_type: get_msg_type(),
                    msg: format!(
                        "Field expects type {:?} but expression node's output type is {:?}",
                        field_type, node_type
                    ),
                });
            }

            // validate there is only 1 assignment for each field
            field_to_node_map
                .try_insert_value(
                    get_field_name(),
                    Self::get_node(nodes, field_assignment.expression_node_index as usize),
                ) // `try_insert_value` returns error if key already exists
                .map_err(|_| MessageBuilderFactoryError::DuplicateFieldAssignment {
                    field_name: get_field_name(),
                    message_type: get_msg_type(),
                })?;
        }
        Ok(field_to_node_map)
    }
}

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
mod tests {
    use super::*;
    use mock::get_test_file_descriptors;
    use sdv_ast::NodeValue;
    use sdvtelemetry_proto_rust::metrics_configuration::proto_message_builder::FieldAssignment;

    // Package and message name are in to src/test_utils/test_message.proto
    const TEST_MESSAGE_FULL_NAME: &str = ".android.sdv.telemetry.test.TestMessage";
    const INT_FIELD_NAME: &str = "int_field";
    const FLOAT_FIELD_NAME: &str = "float_field";
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
        let mut field_assignment = FieldAssignment::new();
        field_assignment.field_name = field_name.to_string();
        field_assignment.expression_node_index = node_index;
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
}
