// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use lazy_static::lazy_static;
use parking_lot::Mutex;
use sdv_telemetry_expr::{node::Node, node_value::NodeValue, node_value_type::NodeValueType};
use sdv_telemetry_proto_rust::metrics_configuration::ProtoMessageBuilder;
use sdv_telemetry_test_proto::test_message;
use sdv_telemetry_utils::descriptor_utils::FileDescriptorsAndWkts;
use std::sync::Arc;

use super::{
    MessageBuilder, MessageBuilderFactory, MessageBuilderFactoryCreateContext,
    MessageBuilderFactoryError, MessageBuilderFactoryValidateContext,
};
use crate::metrics_config_orchestrator::message_builder::mock::MockMessageBuilder;

lazy_static! {
    static ref FILE_DESCRIPTORS: FileDescriptorsAndWkts =
        FileDescriptorsAndWkts::try_from(vec![test_message::file_descriptor().proto().to_owned()])
            .unwrap();
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
pub(crate) fn get_test_file_descriptors() -> &'static FileDescriptorsAndWkts {
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
