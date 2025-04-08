// Copyright 2025 Google LLC

//! Defines an abstraction of a dynamic message

use log::warn;
use protobuf::{reflect::MessageDescriptor, MessageDyn};
use std::fmt::Debug;
use std::sync::Arc;

use crate::{node_error::NodeEvaluationError, node_value::NodeValue};

/// A message dynamically observable through reflection.
pub trait DynamicMessage: Debug + Send + Sync {
    /// Returns descriptor of the message.
    fn get_descriptor(&self) -> &MessageDescriptor;

    /// Returns value of the field with the specified name.
    /// Returns None if the value of the field is not set.
    fn get_field_value(&self, field_name: &str) -> Result<NodeValue, NodeEvaluationError>;

    /// Creates a protobuf message out of Self.
    fn to_protobuf(&self) -> Result<Arc<dyn MessageDyn>, ProtobufConversionError>;
}

#[derive(thiserror::Error, Debug)]
#[error("Protobuf conversion failed for field {field_name}: {error}")]
pub struct ProtobufConversionError {
    pub field_name: String,
    pub error: NodeEvaluationError,
}

impl dyn DynamicMessage {
    pub fn equals(&self, other: &Self) -> bool {
        if self.get_descriptor() != other.get_descriptor() {
            return false;
        }

        for field_descriptor in self.get_descriptor().fields() {
            let Some(self_field_value) = self.maybe_get_field_value(field_descriptor.name()) else {
                return false;
            };
            let Some(other_field_value) = other.maybe_get_field_value(field_descriptor.name())
            else {
                return false;
            };

            if self_field_value != other_field_value {
                return false;
            }
        }

        true
    }

    fn maybe_get_field_value(&self, field_name: &str) -> Option<NodeValue> {
        self.get_field_value(field_name)
            .inspect_err(|err| {
                warn!(
                    "Unable to retrieve field {field_name} to compare messages of type {}: {err}",
                    self.get_descriptor()
                )
            })
            .ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::{assert_that, matchers::eq};
    use protobuf::MessageFull;
    use sdv_telemetry_test_proto::test_message::{TestMessage, TestNestedMessage};

    use crate::mock::MockDynamicMessage;

    const FIELD_1_NAME: &str = "bool_field";
    const FIELD_2_NAME: &str = "int_field";

    #[test]
    fn equals_same_message_success() {
        let mut msg1 = MockDynamicMessage::new(TestMessage::descriptor());
        msg1.set_field_value(FIELD_1_NAME.to_owned(), NodeValue::Int32(1));
        msg1.set_field_value(FIELD_2_NAME.to_owned(), NodeValue::Bool(true));

        let mut msg2 = MockDynamicMessage::new(TestMessage::descriptor());
        msg2.set_field_value(FIELD_1_NAME.to_owned(), NodeValue::Int32(1));
        msg2.set_field_value(FIELD_2_NAME.to_owned(), NodeValue::Bool(true));

        assert_that!((&msg1 as &dyn DynamicMessage).equals(&msg2), eq(true));
    }

    #[test]
    fn equals_different_messages_fails() {
        let mut msg1 = MockDynamicMessage::new(TestMessage::descriptor());
        msg1.set_field_value(FIELD_1_NAME.to_owned(), NodeValue::Int32(1));

        let mut msg2 = MockDynamicMessage::new(TestMessage::descriptor());
        msg2.set_field_value(FIELD_1_NAME.to_owned(), NodeValue::Int32(2));

        assert_that!((&msg1 as &dyn DynamicMessage).equals(&msg2), eq(false));
    }

    #[test]
    fn equals_different_descriptors_fails() {
        let mut msg1 = MockDynamicMessage::new(TestMessage::descriptor());
        msg1.set_field_value(FIELD_1_NAME.to_owned(), NodeValue::Int32(1));

        let mut msg2 = MockDynamicMessage::new(TestNestedMessage::descriptor());
        msg2.set_field_value(FIELD_1_NAME.to_owned(), NodeValue::Int32(1));

        assert_that!((&msg1 as &dyn DynamicMessage).equals(&msg2), eq(false));
    }

    #[test]
    fn equals_error_field_fails() {
        let msg1 = MockDynamicMessage::new(TestMessage::descriptor());
        msg1.set_next_error(NodeEvaluationError::ConstantValueNotSet);

        let mut msg2 = MockDynamicMessage::new(TestMessage::descriptor());
        msg2.set_field_value(FIELD_1_NAME.to_owned(), NodeValue::None);

        assert_that!((&msg1 as &dyn DynamicMessage).equals(&msg2), eq(false));
    }

    #[test]
    fn equals_handles_empty_messages() {
        let msg1 = MockDynamicMessage::new(TestMessage::descriptor());
        let msg2 = MockDynamicMessage::new(TestMessage::descriptor());

        assert_that!((&msg1 as &dyn DynamicMessage).equals(&msg2), eq(true));
    }

    #[test]
    fn equals_handles_different_node_values() {
        let mut msg1 = MockDynamicMessage::new(TestMessage::descriptor());
        msg1.set_field_value(FIELD_1_NAME.to_owned(), NodeValue::Int32(1));
        msg1.set_field_value(FIELD_2_NAME.to_owned(), NodeValue::Bool(true));

        let mut msg2 = MockDynamicMessage::new(TestMessage::descriptor());
        msg2.set_field_value(FIELD_1_NAME.to_owned(), NodeValue::Int32(1));
        msg2.set_field_value(FIELD_2_NAME.to_owned(), NodeValue::Int32(1));

        assert_that!((&msg1 as &dyn DynamicMessage).equals(&msg2), eq(false));
    }

    #[test]
    fn equals_handles_different_list_node_values() {
        let mut msg1 = MockDynamicMessage::new(TestMessage::descriptor());
        msg1.set_field_value(FIELD_1_NAME.to_owned(), NodeValue::Int32List(vec![1, 2, 3]));

        let mut msg2 = MockDynamicMessage::new(TestMessage::descriptor());
        msg2.set_field_value(FIELD_1_NAME.to_owned(), NodeValue::Int32List(vec![1, 2, 4]));

        assert_that!((&msg1 as &dyn DynamicMessage).equals(&msg2), eq(false));
    }

    #[test]
    fn equals_handles_different_message_node_values() {
        let mut msg1 = MockDynamicMessage::new(TestMessage::descriptor());
        let mut nested_msg1 = MockDynamicMessage::new(TestNestedMessage::descriptor());
        nested_msg1.set_field_value(FIELD_1_NAME.to_owned(), NodeValue::Int32(1));
        msg1.set_field_value(FIELD_1_NAME.to_owned(), NodeValue::Message(Arc::new(nested_msg1)));

        let mut msg2 = MockDynamicMessage::new(TestMessage::descriptor());
        let mut nested_msg2 = MockDynamicMessage::new(TestNestedMessage::descriptor());
        nested_msg2.set_field_value(FIELD_1_NAME.to_owned(), NodeValue::Int32(2));
        msg2.set_field_value(FIELD_1_NAME.to_owned(), NodeValue::Message(Arc::new(nested_msg2)));

        assert_that!((&msg1 as &dyn DynamicMessage).equals(&msg2), eq(false));
    }

    #[test]
    fn equals_handles_none_node_values() {
        let mut msg1 = MockDynamicMessage::new(TestMessage::descriptor());
        msg1.set_field_value(FIELD_1_NAME.to_owned(), NodeValue::None);

        let mut msg2 = MockDynamicMessage::new(TestMessage::descriptor());
        msg2.set_field_value(FIELD_1_NAME.to_owned(), NodeValue::None);

        assert_that!((&msg1 as &dyn DynamicMessage).equals(&msg2), eq(true));
    }

    #[test]
    fn equals_handles_different_none_and_some_node_values() {
        let mut msg1 = MockDynamicMessage::new(TestMessage::descriptor());
        msg1.set_field_value(FIELD_1_NAME.to_owned(), NodeValue::None);

        let mut msg2 = MockDynamicMessage::new(TestMessage::descriptor());
        msg2.set_field_value(FIELD_1_NAME.to_owned(), NodeValue::Int32(1));

        assert_that!((&msg1 as &dyn DynamicMessage).equals(&msg2), eq(false));
    }
}
