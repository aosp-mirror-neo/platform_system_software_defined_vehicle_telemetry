// Copyright 2024 Google LLC

use lazy_static::lazy_static;
use parking_lot::Mutex;
use protobuf::reflect::FileDescriptor;
use sdv_telemetry_expr::{node::Node, node_value::NodeValue, node_value_type::NodeValueType};
use sdv_telemetry_proto_rust::metrics_configuration::ProtoMessageBuilder;
use sdv_telemetry_test_proto::test_message;
use std::sync::Arc;

use super::{
    MessageBuilder, MessageBuilderFactory, MessageBuilderFactoryCreateContext,
    MessageBuilderFactoryError, MessageBuilderFactoryValidateContext,
};
use crate::metrics_config_orchestrator::message_builder::mock::MockMessageBuilder;

lazy_static! {
    static ref FILE_DESCRIPTORS: Vec<FileDescriptor> =
        vec![test_message::file_descriptor().clone()];
    static ref NODES: Vec<Arc<Node>> = {
        let node = Arc::new(Node::ConstantLeaf { value: NodeValue::None });
        vec![node; 3]
    };
    static ref NODE_TYPES: Vec<Option<NodeValueType>> = {
        let node_type = Some(NodeValueType::Int);
        vec![node_type; 3]
    };
}

pub(crate) struct MockMessageBuilderFactory {
    fail_validate: bool,
    fail_create: bool,
    next_builder: Mutex<Option<Arc<MockMessageBuilder>>>,
}

impl MockMessageBuilderFactory {
    pub(crate) fn new() -> Self {
        Self { fail_validate: false, fail_create: false, next_builder: Mutex::new(None) }
    }

    /// Intercepts the mock factory's validate() call and make it return an error.
    pub(crate) fn fail_validate(&mut self) {
        self.fail_validate = true;
    }

    /// Intercepts the mock factory's create() call and make it return an error.
    pub(crate) fn fail_create(&mut self) {
        self.fail_create = true;
    }

    /// Sets the next builder to be returned by `create`.
    pub(crate) fn set_next_builder(&self, builder: Arc<MockMessageBuilder>) {
        self.next_builder.lock().replace(builder);
    }
}

impl MessageBuilderFactory for MockMessageBuilderFactory {
    fn validate(
        &self,
        _proto_message_builder: &ProtoMessageBuilder,
        _context: &MessageBuilderFactoryValidateContext,
    ) -> Result<(), MessageBuilderFactoryError> {
        if self.fail_validate {
            return Err(MessageBuilderFactoryError::MessageNotFound {
                message_type: "Simulated failure".to_string(),
            });
        }
        Ok(())
    }

    fn create(
        &self,
        _owner_name: String,
        _proto_message_builder: &ProtoMessageBuilder,
        _context: &MessageBuilderFactoryCreateContext,
    ) -> Result<Arc<dyn MessageBuilder>, MessageBuilderFactoryError> {
        if self.fail_create {
            return Err(MessageBuilderFactoryError::MessageNotFound {
                message_type: "Simulated failure".to_string(),
            });
        }
        if let Some(builder) = self.next_builder.lock().take() {
            return Ok(builder);
        }
        Ok(Arc::new(MockMessageBuilder::new()))
    }
}

/// Return the file descriptor for `src/test_utils/test_message.proto` for tests.
pub(crate) fn get_test_file_descriptors<'a>() -> &'a [FileDescriptor] {
    &FILE_DESCRIPTORS
}

/// Return 3 nodes, this is required to build the MessageBuilderFactory's create context
pub(crate) fn get_test_nodes<'a>() -> &'a [Arc<Node>] {
    &NODES
}

/// Return 3 node types, this is required to build the MessageBuilderFactory's validate context
pub(crate) fn get_test_node_types<'a>() -> &'a [Option<NodeValueType>] {
    &NODE_TYPES
}
