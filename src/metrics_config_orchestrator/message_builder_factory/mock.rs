use super::{
    MessageBuilder, MessageBuilderFactory, MessageBuilderFactoryCreateContext,
    MessageBuilderFactoryError, MessageBuilderFactoryValidateContext,
};
use crate::metrics_config_orchestrator::message_builder::mock::MockMessageBuilder;
use lazy_static::lazy_static;
use protobuf::{reflect::FileDescriptor, MessageFull};
use sdv_ast::{Node, NodeValue, NodeValueType};
use sdv_telem_test_proto::test_message::TestMessage;
use sdvtelemetry_proto_rust::metrics_configuration::ProtoMessageBuilder;
use std::sync::Arc;

lazy_static! {
    static ref FILE_DESCRIPTORS: Vec<FileDescriptor> = {
        let fd_proto = TestMessage::descriptor().file_descriptor_proto().clone();
        vec![FileDescriptor::new_dynamic(fd_proto, &[]).unwrap()]
    };
    static ref NODES: Vec<Arc<Node>> = {
        let node = Arc::new(Node::ConstantLeaf { value: NodeValue::None });
        vec![node; 3]
    };
    static ref NODE_TYPES: Vec<NodeValueType> = {
        let node_type = NodeValueType::Int;
        vec![node_type; 3]
    };
}

pub(crate) struct MockMessageBuilderFactory {
    fail_validate: bool,
    fail_create: bool,
}

impl MockMessageBuilderFactory {
    pub fn new() -> Self {
        MockMessageBuilderFactory { fail_validate: false, fail_create: false }
    }

    /// Intercepts the mock factory's validate() call and make it return an error.
    pub fn fail_validate(&mut self) {
        self.fail_validate = true;
    }

    /// Intercepts the mock factory's create() call and make it return an error.
    pub fn fail_create(&mut self) {
        self.fail_create = true;
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
        _proto_message_builder: &ProtoMessageBuilder,
        _context: &MessageBuilderFactoryCreateContext,
    ) -> Result<Box<dyn MessageBuilder>, MessageBuilderFactoryError> {
        if self.fail_create {
            return Err(MessageBuilderFactoryError::MessageNotFound {
                message_type: "Simulated failure".to_string(),
            });
        }
        Ok(Box::new(MockMessageBuilder::new()))
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
pub(crate) fn get_test_node_types<'a>() -> &'a [NodeValueType] {
    &NODE_TYPES
}
