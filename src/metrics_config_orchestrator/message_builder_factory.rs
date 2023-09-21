use protobuf::reflect::{FileDescriptor, MessageDescriptor};
use sdv_ast::Node;
use sdvtelemetry_proto_rust::metrics_configuration::ProtoMessageBuilder;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
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
        context: &MessageBuilderFactoryContext,
    ) -> Result<(), MessageBuilderFactoryError>;

    fn create(
        &self,
        proto_message_builder: &ProtoMessageBuilder,
        context: &MessageBuilderFactoryContext,
    ) -> Result<Box<dyn MessageBuilder>, MessageBuilderFactoryError>;
}

pub(crate) struct MessageBuilderFactoryContext<'a> {
    pub file_descriptors: &'a [FileDescriptor],
    pub nodes: &'a [Arc<Node>],
}

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
}

impl<'a> MessageBuilderFactoryContext<'a> {
    /// Constructor
    pub(crate) fn new(file_descriptors: &'a [FileDescriptor], nodes: &'a [Arc<Node>]) -> Self {
        MessageBuilderFactoryContext { file_descriptors, nodes }
    }

    fn get_message_descriptor(
        &self,
        message_type: &str,
    ) -> Result<MessageDescriptor, MessageBuilderFactoryError> {
        // iterate through all file_descriptors, find MessageDescriptor for the given message_type
        // message_by_full_name() method requires the name to start with dot, it returns a clone
        // of the MessageDescriptor, so we can return it as-is without cloning again
        self.file_descriptors.iter().find_map(|fd| fd.message_by_full_name(message_type)).ok_or(
            MessageBuilderFactoryError::MessageNotFound { message_type: message_type.to_string() },
        )
    }
}

/// Concrete implementation of the factory trait.
pub(crate) struct MessageBuilderFactoryImpl {}

impl MessageBuilderFactoryImpl {
    pub fn new() -> Self {
        MessageBuilderFactoryImpl {}
    }
}

impl MessageBuilderFactory for MessageBuilderFactoryImpl {
    // TODO: b/299950214 - verify that node output is compatible with field type.
    // As a result, validate() will ensure the dynamic message doesn't contain
    // unsupported field types such as map, string, bytes fields.
    fn validate(
        &self,
        proto_message_builder: &ProtoMessageBuilder,
        context: &MessageBuilderFactoryContext,
    ) -> Result<(), MessageBuilderFactoryError> {
        let message_descriptor =
            context.get_message_descriptor(&proto_message_builder.message_type)?;

        let get_msg_type = || proto_message_builder.message_type.clone();
        let mut field_names = HashSet::new();
        for field_assignment in &proto_message_builder.field_assignments {
            let get_field_name = || field_assignment.field_name.clone();
            // validate there is only 1 assignment for each field
            if !field_names.insert(get_field_name()) {
                return Err(MessageBuilderFactoryError::DuplicateFieldAssignment {
                    field_name: get_field_name(),
                    message_type: get_msg_type(),
                });
            }
            // validate the field value is a valid node
            if field_assignment.expression_node_index as usize >= context.nodes.len() {
                return Err(MessageBuilderFactoryError::InvalidNodeIndex {
                    field_name: get_field_name(),
                    i: field_assignment.expression_node_index,
                });
            }
            // validate this field exists in the given message
            if message_descriptor.field_by_name(&field_assignment.field_name).is_none() {
                return Err(MessageBuilderFactoryError::FieldNotFound {
                    field_name: get_field_name(),
                    message_type: get_msg_type(),
                });
            }
        }
        Ok(())
    }

    fn create(
        &self,
        proto_message_builder: &ProtoMessageBuilder,
        context: &MessageBuilderFactoryContext,
    ) -> Result<Box<dyn MessageBuilder>, MessageBuilderFactoryError> {
        let message_descriptor =
            context.get_message_descriptor(&proto_message_builder.message_type)?;
        // maps field_name to Arc<Node>, the node evaluation output is the field value
        let field_assignments = proto_message_builder.field_assignments.iter().try_fold(
            HashMap::new(),
            |mut map, field_assignment| {
                let get_field_name = || field_assignment.field_name.clone();
                map.try_insert_value(
                    get_field_name(),
                    context
                        .nodes
                        .get(field_assignment.expression_node_index as usize)
                        .ok_or(MessageBuilderFactoryError::InvalidNodeIndex {
                            field_name: get_field_name(),
                            i: field_assignment.expression_node_index,
                        })?
                        .clone(),
                ) // `try_insert_value` returns error if key already exists
                .map_err(|_| {
                    MessageBuilderFactoryError::DuplicateFieldAssignment {
                        field_name: get_field_name(),
                        message_type: proto_message_builder.message_type.clone(),
                    }
                })?;
                Ok(map)
            },
        )?;
        Ok(Box::new(MessageBuilderImpl::new(message_descriptor.clone(), field_assignments)))
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

    fn create_test_file_descriptors() -> Vec<FileDescriptor> {
        get_test_file_descriptors().to_vec()
    }

    fn create_nodes_with_size(n: usize) -> Vec<Arc<Node>> {
        let node = Arc::new(Node::ConstantLeaf { value: NodeValue::None });
        vec![node; n]
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
        let nodes = create_nodes_with_size(1);
        let context = MessageBuilderFactoryContext::new(&fds, &nodes);
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
        let nodes = create_nodes_with_size(2);
        let context = MessageBuilderFactoryContext::new(&fds, &nodes);
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
        // create 10 expression nodes and put it in context
        let fds = create_test_file_descriptors();
        let nodes = create_nodes_with_size(10);
        let context = MessageBuilderFactoryContext::new(&fds, &nodes);
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
        let nodes = create_nodes_with_size(2);
        let context = MessageBuilderFactoryContext::new(&fds, &nodes);
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
        let nodes = create_nodes_with_size(2);
        let context = MessageBuilderFactoryContext::new(&fds, &nodes);
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
    fn test_factory_create_ok() {
        let factory = MessageBuilderFactoryImpl::new();
        let fds = create_test_file_descriptors();
        let nodes = create_nodes_with_size(1);
        let context = MessageBuilderFactoryContext::new(&fds, &nodes);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            vec![create_field_assignment_proto(INT_FIELD_NAME, 0)],
        );

        factory.create(&proto_message_builder, &context).unwrap();
    }
}
