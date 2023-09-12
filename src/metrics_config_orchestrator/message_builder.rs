// Copyright 2023 Google LLC. All Rights Reserved.

use protobuf::{reflect::MessageDescriptor, MessageDyn};
use sdv_ast::Node;
use std::collections::HashMap;
use std::sync::Arc;

#[allow(dead_code)]
pub(crate) struct MessageBuilder {
    /// Uses descriptor to create and populate a protobuf message
    pub message_descriptor: MessageDescriptor,

    /// Maps field_name to the node whose evaluation output is the field value.
    pub field_assignments: HashMap<String, Arc<Node>>,
}

#[allow(dead_code)]
impl MessageBuilder {
    pub fn new(
        message_descriptor: MessageDescriptor,
        field_assignments: HashMap<String, Arc<Node>>,
    ) -> Self {
        MessageBuilder { message_descriptor, field_assignments }
    }

    /// Builds a protobuf message dynamically.
    pub fn build(&self) -> Box<dyn MessageDyn> {
        self.message_descriptor.new_instance()
    }
}
