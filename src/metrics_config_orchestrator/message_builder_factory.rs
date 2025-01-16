// Copyright 2023 Google LLC

use protobuf::reflect::{FieldDescriptor, FileDescriptor, MessageDescriptor};
use sdv_telemetry_proto_rust::metrics_configuration::{
    proto_message_builder::field_assignment::Aggregated_field_value as AggregatedFieldValueProto,
    ProtoMessageBuilder,
};
use sdv_telemetry_utils::collection_extension::HashMapExtension;
use sdv_telemetry_utils::descriptor_utils;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

use crate::expr::{node::Node, node_value_type::NodeValueType};
use crate::metrics_config_orchestrator::message_builder::value_aggregator_factory::{
    ValueAggregatorFactory, ValueAggregatorFactoryCreateContext, ValueAggregatorFactoryError,
    ValueAggregatorFactoryImpl, ValueAggregatorFactoryValidateContext,
};
use crate::metrics_config_orchestrator::message_builder::{MessageBuilder, MessageBuilderImpl};

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
        owner_name: String,
        proto_message_builder: &ProtoMessageBuilder,
        context: &MessageBuilderFactoryCreateContext,
    ) -> Result<Box<dyn MessageBuilder>, MessageBuilderFactoryError>;
}

pub(crate) struct MessageBuilderFactoryCreateContext<'a> {
    pub file_descriptors: &'a [FileDescriptor],
    pub nodes: &'a [Arc<Node>],
    pub node_types: &'a [Option<NodeValueType>],
}

pub(crate) struct MessageBuilderFactoryValidateContext<'a> {
    pub file_descriptors: &'a [FileDescriptor],
    pub node_types: &'a [Option<NodeValueType>],
}

#[derive(Error, Debug)]
pub(crate) enum MessageBuilderFactoryError {
    #[error("Message {message_type} is not found in the provided file descriptor protos")]
    MessageNotFound { message_type: String },
    #[error("Field {field_name} not found in message {message_type}")]
    FieldNotFound { field_name: String, message_type: String },
    #[error("Multiple assignments found for field {field_name} in message {message_type}")]
    DuplicateFieldAssignment { field_name: String, message_type: String },
    #[error("Field {field_name} of message {message_type} did not specify aggregation type")]
    AggregatedFieldValueNotSet { field_name: String, message_type: String },
    #[error("Field {field_name} of message {message_type} has an invalid assignment: {e}")]
    InvalidAssignment {
        field_name: String,
        message_type: String,
        e: Box<ValueAggregatorFactoryError>,
    },
}

impl<'a> MessageBuilderFactoryCreateContext<'a> {
    /// Constructor for the context used by factory's create() method
    pub(crate) fn new(
        file_descriptors: &'a [FileDescriptor],
        nodes: &'a [Arc<Node>],
        node_types: &'a [Option<NodeValueType>],
    ) -> Self {
        Self { file_descriptors, nodes, node_types }
    }
}

impl<'a> MessageBuilderFactoryValidateContext<'a> {
    /// Constructor for the context used by factory's validate() method
    pub(crate) fn new(
        file_descriptors: &'a [FileDescriptor],
        node_types: &'a [Option<NodeValueType>],
    ) -> Self {
        Self { file_descriptors, node_types }
    }
}

/// Concrete implementation of the factory trait.
pub(crate) struct MessageBuilderFactoryImpl {
    pub value_aggregator_factory: Box<dyn ValueAggregatorFactory>,
}

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

        // maps field_name to an optional tuple that is required for creating field values
        let map = Self::map_field_to_descriptor(proto_message_builder, &message_descriptor)?;

        // iterate through raw assignments to validate
        map.into_iter().try_for_each(
            |(field_name, (aggregated_field_value, field_descriptor))| {
                let context = ValueAggregatorFactoryValidateContext::new(
                    &field_descriptor,
                    context.node_types,
                );
                self.value_aggregator_factory.validate(aggregated_field_value, &context).map_err(
                    |e| MessageBuilderFactoryError::InvalidAssignment {
                        field_name: field_name.clone(),
                        message_type: proto_message_builder.message_type.clone(),
                        e: e.into(),
                    },
                )
            },
        )?;
        Ok(())
    }

    fn create(
        &self,
        owner_name: String,
        proto_message_builder: &ProtoMessageBuilder,
        context: &MessageBuilderFactoryCreateContext,
    ) -> Result<Box<dyn MessageBuilder>, MessageBuilderFactoryError> {
        let message_descriptor = Self::get_message_descriptor(
            context.file_descriptors,
            &proto_message_builder.message_type,
        )?;
        // maps field_name to the aggregated field value proto and the field descriptor
        let map = Self::map_field_to_descriptor(proto_message_builder, &message_descriptor)?;

        // iterate through raw assignments to build concrete ValueAggregators
        let mut value_providers = Vec::new();
        let field_assignments = map
            .into_iter()
            .map(|(field_name, (aggregated_field_value_proto, field_descriptor))| {
                let context = ValueAggregatorFactoryCreateContext::new(
                    &field_descriptor,
                    context.node_types,
                    context.nodes,
                );
                let (aggregator, node) = self
                    .value_aggregator_factory
                    .create(aggregated_field_value_proto, &context)
                    .map_err(|e| MessageBuilderFactoryError::InvalidAssignment {
                        field_name: field_name.clone(),
                        message_type: proto_message_builder.message_type.clone(),
                        e: e.into(),
                    })?;
                if let Some(node) = node {
                    value_providers.push((field_name.clone(), node));
                }
                Ok((field_name, aggregator))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Box::new(MessageBuilderImpl::new(
            owner_name,
            message_descriptor,
            field_assignments,
            value_providers,
        )))
    }
}

impl MessageBuilderFactoryImpl {
    pub(crate) fn new() -> Self {
        Self { value_aggregator_factory: Box::new(ValueAggregatorFactoryImpl {}) }
    }

    fn get_message_descriptor(
        file_descriptors: &[FileDescriptor],
        message_type: &str,
    ) -> Result<MessageDescriptor, MessageBuilderFactoryError> {
        descriptor_utils::find_message_by_full_name(file_descriptors, message_type).ok_or(
            MessageBuilderFactoryError::MessageNotFound { message_type: message_type.to_string() },
        )
    }

    /// Map ProtoMessageBuilder's field assignment to the field descriptor.
    fn map_field_to_descriptor<'a>(
        proto_message_builder: &'a ProtoMessageBuilder,
        message_descriptor: &'a MessageDescriptor,
    ) -> Result<
        HashMap<String, (&'a AggregatedFieldValueProto, FieldDescriptor)>,
        MessageBuilderFactoryError,
    > {
        let get_msg_type = || proto_message_builder.message_type.clone();

        let mut map = HashMap::new();
        for field_assignment in &proto_message_builder.field_assignments {
            let get_field_name = || field_assignment.field_name.clone();

            // validate aggregated field value is set
            let aggregated_field_value = field_assignment.aggregated_field_value.as_ref().ok_or(
                MessageBuilderFactoryError::AggregatedFieldValueNotSet {
                    field_name: get_field_name(),
                    message_type: get_msg_type(),
                },
            )?;

            // validate this field exists in the message descriptor & get field descriptor
            let field_descriptor = message_descriptor
                .field_by_name(&field_assignment.field_name)
                .ok_or(MessageBuilderFactoryError::FieldNotFound {
                field_name: get_field_name(),
                message_type: get_msg_type(),
            })?;

            // validate there is only 1 assignment for each field
            map.try_insert_value(get_field_name(), (aggregated_field_value, field_descriptor)) // `try_insert_value` returns error if key already exists
                .map_err(|_| MessageBuilderFactoryError::DuplicateFieldAssignment {
                    field_name: get_field_name(),
                    message_type: get_msg_type(),
                })?;
        }
        Ok(map)
    }
}

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
mod tests {
    use super::*;

    use sdv_telemetry_proto_rust::metrics_configuration::proto_message_builder::{
        field_assignment::NoAggregation, FieldAssignment,
    };

    use crate::metrics_config_orchestrator::message_builder::value_aggregator_factory::mock::MockValueAggregatorFactory;
    use mock::get_test_file_descriptors;

    // Package and message name are in to src/test_utils/test_message.proto
    const INT_FIELD_NAME: &str = "int_field";
    const TEST_MESSAGE_FULL_NAME: &str = ".google.sdv.telemetry.test.TestMessage";

    fn create_message_builder_factory() -> MessageBuilderFactoryImpl {
        MessageBuilderFactoryImpl {
            value_aggregator_factory: Box::new(MockValueAggregatorFactory::new()),
        }
    }

    fn create_test_file_descriptors() -> Vec<FileDescriptor> {
        get_test_file_descriptors().to_vec()
    }

    fn create_field_assignment_proto(field_name: &str) -> FieldAssignment {
        let mut field_assignment = FieldAssignment::new();
        field_assignment.field_name = field_name.to_string();
        field_assignment.set_no_aggregation(NoAggregation::new());
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
    fn validate_ok() {
        let factory = create_message_builder_factory();
        let fds = create_test_file_descriptors();
        let context = MessageBuilderFactoryValidateContext::new(&fds, &[]);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            vec![create_field_assignment_proto(INT_FIELD_NAME)],
        );

        assert!(factory.validate(&proto_message_builder, &context).is_ok());
    }

    #[test]
    fn validate_name_without_leading_dot_ok() {
        let factory = create_message_builder_factory();
        let fds = create_test_file_descriptors();
        let context = MessageBuilderFactoryValidateContext::new(&fds, &[]);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME.strip_prefix('.').unwrap(),
            vec![create_field_assignment_proto(INT_FIELD_NAME)],
        );

        assert!(factory.validate(&proto_message_builder, &context).is_ok());
    }

    #[test]
    fn validate_no_aggregated_field_value_fails() {
        let factory = create_message_builder_factory();
        let fds = create_test_file_descriptors();
        let context = MessageBuilderFactoryValidateContext::new(&fds, &[]);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            vec![{
                let mut field_assignment = FieldAssignment::new();
                field_assignment.field_name = INT_FIELD_NAME.to_string();
                // error - didn't set aggregated_field_value
                field_assignment
            }],
        );

        let err = factory.validate(&proto_message_builder, &context).unwrap_err();

        assert!(
            matches!(err, MessageBuilderFactoryError::AggregatedFieldValueNotSet { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn validate_message_not_found_fails() {
        let factory = create_message_builder_factory();
        let fds = create_test_file_descriptors();
        let context = MessageBuilderFactoryValidateContext::new(&fds, &[]);
        let proto_message_builder = create_proto_message_builder(
            // not a valid message in test_message.proto
            ".com.example.foo.BarMessage",
            vec![create_field_assignment_proto(INT_FIELD_NAME)],
        );

        let err = factory.validate(&proto_message_builder, &context).unwrap_err();

        assert!(
            matches!(err, MessageBuilderFactoryError::MessageNotFound { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn validate_duplicate_field_assignment_fails() {
        let factory = create_message_builder_factory();
        let fds = create_test_file_descriptors();
        let context = MessageBuilderFactoryValidateContext::new(&fds, &[]);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            vec![
                // the same field has 2 assignments
                create_field_assignment_proto(INT_FIELD_NAME),
                create_field_assignment_proto(INT_FIELD_NAME),
            ],
        );

        let err = factory.validate(&proto_message_builder, &context).unwrap_err();

        assert!(
            matches!(err, MessageBuilderFactoryError::DuplicateFieldAssignment { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn validate_field_name_not_found_fails() {
        let factory = create_message_builder_factory();
        let fds = create_test_file_descriptors();
        let context = MessageBuilderFactoryValidateContext::new(&fds, &[]);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            vec![
                create_field_assignment_proto(INT_FIELD_NAME),
                // not a valid field_name in TestMessage
                create_field_assignment_proto("random_field_name"),
            ],
        );

        let err = factory.validate(&proto_message_builder, &context).unwrap_err();

        assert!(
            matches!(err, MessageBuilderFactoryError::FieldNotFound { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn validate_value_aggregator_factory_err_fails() {
        let mut value_aggregator_factory = MockValueAggregatorFactory::new();
        value_aggregator_factory.fail_validate();
        let factory = MessageBuilderFactoryImpl {
            value_aggregator_factory: Box::new(value_aggregator_factory),
        };
        let fds = create_test_file_descriptors();
        let context = MessageBuilderFactoryValidateContext::new(&fds, &[]);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            vec![create_field_assignment_proto(INT_FIELD_NAME)],
        );

        let err = factory.validate(&proto_message_builder, &context).unwrap_err();

        assert!(
            matches!(err, MessageBuilderFactoryError::InvalidAssignment { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn create_ok() {
        let factory = create_message_builder_factory();
        let fds = create_test_file_descriptors();
        let context = MessageBuilderFactoryCreateContext::new(&fds, &[], &[]);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            vec![create_field_assignment_proto(INT_FIELD_NAME)],
        );

        factory.create(String::default(), &proto_message_builder, &context).unwrap();
    }

    #[test]
    fn create_name_without_leading_dot_ok() {
        let factory = create_message_builder_factory();
        let fds = create_test_file_descriptors();
        let context = MessageBuilderFactoryCreateContext::new(&fds, &[], &[]);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME.strip_prefix('.').unwrap(),
            vec![create_field_assignment_proto(INT_FIELD_NAME)],
        );

        factory.create(String::default(), &proto_message_builder, &context).unwrap();
    }

    #[test]
    fn create_no_aggregated_field_value_fails() {
        let factory = create_message_builder_factory();
        let fds = create_test_file_descriptors();
        let context = MessageBuilderFactoryCreateContext::new(&fds, &[], &[]);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            vec![{
                let mut field_assignment = FieldAssignment::new();
                field_assignment.field_name = INT_FIELD_NAME.to_string();
                // error - `field_assignment` didn't set aggregated_field_value
                field_assignment
            }],
        );

        let err =
            factory.create(String::default(), &proto_message_builder, &context).err().unwrap();

        assert!(
            matches!(err, MessageBuilderFactoryError::AggregatedFieldValueNotSet { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn create_message_not_found_fails() {
        let factory = create_message_builder_factory();
        let fds = create_test_file_descriptors();
        let context = MessageBuilderFactoryCreateContext::new(&fds, &[], &[]);
        let proto_message_builder = create_proto_message_builder(
            // not a valid message in test_message.proto
            ".com.example.foo.BarMessage",
            vec![create_field_assignment_proto(INT_FIELD_NAME)],
        );

        let err =
            factory.create(String::default(), &proto_message_builder, &context).err().unwrap();

        assert!(
            matches!(err, MessageBuilderFactoryError::MessageNotFound { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn create_duplicate_field_assignment_fails() {
        let factory = create_message_builder_factory();
        let fds = create_test_file_descriptors();
        let context = MessageBuilderFactoryCreateContext::new(&fds, &[], &[]);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            vec![
                // the same field has 2 assignments
                create_field_assignment_proto(INT_FIELD_NAME),
                create_field_assignment_proto(INT_FIELD_NAME),
            ],
        );

        let err =
            factory.create(String::default(), &proto_message_builder, &context).err().unwrap();

        assert!(
            matches!(err, MessageBuilderFactoryError::DuplicateFieldAssignment { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn create_field_name_not_found_fails() {
        let factory = create_message_builder_factory();
        let fds = create_test_file_descriptors();
        let context = MessageBuilderFactoryCreateContext::new(&fds, &[], &[]);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            vec![
                create_field_assignment_proto(INT_FIELD_NAME),
                // not a valid field_name in TestMessage
                create_field_assignment_proto("random_field_name"),
            ],
        );

        let err =
            factory.create(String::default(), &proto_message_builder, &context).err().unwrap();

        assert!(
            matches!(err, MessageBuilderFactoryError::FieldNotFound { .. }),
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    fn create_value_aggregator_factory_err_fails() {
        let mut value_aggregator_factory = MockValueAggregatorFactory::new();
        value_aggregator_factory.fail_create();
        let factory = MessageBuilderFactoryImpl {
            value_aggregator_factory: Box::new(value_aggregator_factory),
        };
        let fds = create_test_file_descriptors();
        let context = MessageBuilderFactoryCreateContext::new(&fds, &[], &[]);
        let proto_message_builder = create_proto_message_builder(
            TEST_MESSAGE_FULL_NAME,
            vec![create_field_assignment_proto(INT_FIELD_NAME)],
        );

        let err =
            factory.create(String::default(), &proto_message_builder, &context).err().unwrap();

        assert!(
            matches!(err, MessageBuilderFactoryError::InvalidAssignment { .. }),
            "Unexpected error: {}",
            err
        );
    }
}
