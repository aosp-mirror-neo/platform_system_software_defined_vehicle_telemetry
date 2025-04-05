// Copyright 2025 Google LLC

//! Defines an abstraction of a dynamic message

use log::warn;
use protobuf::MessageDyn;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use crate::{node_error::NodeEvaluationError, node_value::NodeValue};

/// A message dynamically observable through reflection.
pub trait DynamicMessage: Debug + Send + Sync {
    /// Returns the names of all message fields.
    /// The order of the fields is not guaranteed to be deterministic.
    fn get_all_field_names(&self) -> &[String];

    /// Returns value of the field with the specified name.
    /// Returns None if the field is not present in the message.
    fn get_field_value(&self, field_name: &str) -> Result<NodeValue, NodeEvaluationError>;

    /// Returns the name of the message type.
    fn get_type_name(&self) -> &str;

    /// Creates a protobuf message out of Self.
    // TODO(b/408041402): Remove once data collection no longer relies on protobuf.
    fn to_protobuf(&self) -> Arc<dyn MessageDyn>;
}

impl dyn DynamicMessage {
    pub fn equals(&self, other: &Self) -> bool {
        if self.get_type_name() != other.get_type_name() {
            return false;
        }

        let mut other_field_names = other.get_all_field_names().iter().collect::<HashSet<_>>();

        for field_name in self.get_all_field_names() {
            if !other_field_names.remove(field_name) {
                return false;
            }

            let Some(self_field_value) = self.maybe_get_field_value(field_name) else {
                return false;
            };
            let Some(other_field_value) = other.maybe_get_field_value(field_name) else {
                return false;
            };

            if self_field_value != other_field_value {
                return false;
            }
        }

        other_field_names.is_empty()
    }

    fn maybe_get_field_value(&self, field_name: &str) -> Option<NodeValue> {
        match self.get_field_value(field_name) {
            Ok(value) => Some(value),
            Err(NodeEvaluationError::FieldNotFound { .. }) => panic!(
                "Unexpected error: field {field_name} declared by get_all_field_names not found"
            ),
            Err(e) => {
                warn!(
                    "Unable to retrieve field {field_name} to compare messages of type {}: {e}",
                    self.get_type_name()
                );
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::{assert_that, matchers::eq};

    use crate::mock::MockDynamicMessage;

    const FIELD_1_NAME: &str = "field1";
    const FIELD_2_NAME: &str = "field2";
    const TEST_MESSAGE_NAME: &str = "TestMessage";

    #[test]
    fn equals_same_message_success() {
        let mut msg1 = MockDynamicMessage::new(TEST_MESSAGE_NAME.to_owned());
        msg1.set_field(FIELD_1_NAME.to_owned(), NodeValue::Int32(1));
        msg1.set_field(FIELD_2_NAME.to_owned(), NodeValue::Bool(true));

        let mut msg2 = MockDynamicMessage::new(TEST_MESSAGE_NAME.to_owned());
        msg2.set_field(FIELD_1_NAME.to_owned(), NodeValue::Int32(1));
        msg2.set_field(FIELD_2_NAME.to_owned(), NodeValue::Bool(true));

        assert_that!((&msg1 as &dyn DynamicMessage).equals(&msg2), eq(true));
    }

    #[test]
    fn equals_different_messages_fails() {
        let mut msg1 = MockDynamicMessage::new(TEST_MESSAGE_NAME.to_owned());
        msg1.set_field(FIELD_1_NAME.to_owned(), NodeValue::Int32(1));

        let mut msg2 = MockDynamicMessage::new(TEST_MESSAGE_NAME.to_owned());
        msg2.set_field(FIELD_1_NAME.to_owned(), NodeValue::Int32(2));

        assert_that!((&msg1 as &dyn DynamicMessage).equals(&msg2), eq(false));
    }

    #[test]
    fn equals_different_types_fails() {
        let mut msg1 = MockDynamicMessage::new(TEST_MESSAGE_NAME.to_owned());
        msg1.set_field(FIELD_1_NAME.to_owned(), NodeValue::Int32(1));

        let mut msg2 = MockDynamicMessage::new("AnotherMessage".to_owned());
        msg2.set_field(FIELD_1_NAME.to_owned(), NodeValue::Int32(1));

        assert_that!((&msg1 as &dyn DynamicMessage).equals(&msg2), eq(false));
    }

    #[test]
    fn equals_extra_field_fails() {
        let mut msg1 = MockDynamicMessage::new(TEST_MESSAGE_NAME.to_owned());
        msg1.set_field(FIELD_1_NAME.to_owned(), NodeValue::Int32(1));
        msg1.set_field(FIELD_2_NAME.to_owned(), NodeValue::Bool(true));

        let mut msg2 = MockDynamicMessage::new(TEST_MESSAGE_NAME.to_owned());
        msg2.set_field(FIELD_1_NAME.to_owned(), NodeValue::Int32(1));

        assert_that!((&msg1 as &dyn DynamicMessage).equals(&msg2), eq(false));
        assert_that!((&msg2 as &dyn DynamicMessage).equals(&msg1), eq(false));
    }

    #[test]
    fn equals_error_field_fails() {
        let mut msg1 = MockDynamicMessage::new(TEST_MESSAGE_NAME.to_owned());
        msg1.set_field(FIELD_1_NAME.to_owned(), NodeValue::Int32(1));
        msg1.set_field(FIELD_2_NAME.to_owned(), NodeValue::Bool(true));
        msg1.set_next_error(NodeEvaluationError::ConstantValueNotSet);

        let mut msg2 = MockDynamicMessage::new(TEST_MESSAGE_NAME.to_owned());
        msg2.set_field(FIELD_1_NAME.to_owned(), NodeValue::Int32(1));
        msg2.set_field(FIELD_2_NAME.to_owned(), NodeValue::Bool(true));

        assert_that!((&msg1 as &dyn DynamicMessage).equals(&msg2), eq(false));
    }

    #[test]
    fn equals_handles_empty_messages() {
        let msg1 = MockDynamicMessage::new(TEST_MESSAGE_NAME.to_owned());
        let msg2 = MockDynamicMessage::new(TEST_MESSAGE_NAME.to_owned());

        assert_that!((&msg1 as &dyn DynamicMessage).equals(&msg2), eq(true));
    }

    #[test]
    fn equals_different_field_names_fails() {
        let mut msg1 = MockDynamicMessage::new(TEST_MESSAGE_NAME.to_owned());
        msg1.set_field(FIELD_1_NAME.to_owned(), NodeValue::Int32(1));

        let mut msg2 = MockDynamicMessage::new(TEST_MESSAGE_NAME.to_owned());
        msg2.set_field(FIELD_2_NAME.to_owned(), NodeValue::Int32(1));

        assert_that!((&msg1 as &dyn DynamicMessage).equals(&msg2), eq(false));
    }

    #[test]
    fn equals_handles_different_node_values() {
        let mut msg1 = MockDynamicMessage::new(TEST_MESSAGE_NAME.to_owned());
        msg1.set_field(FIELD_1_NAME.to_owned(), NodeValue::Int32(1));
        msg1.set_field(FIELD_2_NAME.to_owned(), NodeValue::Bool(true));

        let mut msg2 = MockDynamicMessage::new(TEST_MESSAGE_NAME.to_owned());
        msg2.set_field(FIELD_1_NAME.to_owned(), NodeValue::Int32(1));
        msg2.set_field(FIELD_2_NAME.to_owned(), NodeValue::Int32(1));

        assert_that!((&msg1 as &dyn DynamicMessage).equals(&msg2), eq(false));
    }

    #[test]
    fn equals_handles_different_list_node_values() {
        let mut msg1 = MockDynamicMessage::new(TEST_MESSAGE_NAME.to_owned());
        msg1.set_field(FIELD_1_NAME.to_owned(), NodeValue::Int32List(vec![1, 2, 3]));

        let mut msg2 = MockDynamicMessage::new(TEST_MESSAGE_NAME.to_owned());
        msg2.set_field(FIELD_1_NAME.to_owned(), NodeValue::Int32List(vec![1, 2, 4]));

        assert_that!((&msg1 as &dyn DynamicMessage).equals(&msg2), eq(false));
    }

    #[test]
    fn equals_handles_different_message_node_values() {
        let mut msg1 = MockDynamicMessage::new(TEST_MESSAGE_NAME.to_owned());
        let mut nested_msg1 = MockDynamicMessage::new("NestedMessage".to_owned());
        nested_msg1.set_field(FIELD_1_NAME.to_owned(), NodeValue::Int32(1));
        msg1.set_field(FIELD_1_NAME.to_owned(), NodeValue::Message(Arc::new(nested_msg1)));

        let mut msg2 = MockDynamicMessage::new(TEST_MESSAGE_NAME.to_owned());
        let mut nested_msg2 = MockDynamicMessage::new("NestedMessage".to_owned());
        nested_msg2.set_field(FIELD_1_NAME.to_owned(), NodeValue::Int32(2));
        msg2.set_field(FIELD_1_NAME.to_owned(), NodeValue::Message(Arc::new(nested_msg2)));

        assert_that!((&msg1 as &dyn DynamicMessage).equals(&msg2), eq(false));
    }

    #[test]
    fn equals_handles_none_node_values() {
        let mut msg1 = MockDynamicMessage::new(TEST_MESSAGE_NAME.to_owned());
        msg1.set_field(FIELD_1_NAME.to_owned(), NodeValue::None);

        let mut msg2 = MockDynamicMessage::new(TEST_MESSAGE_NAME.to_owned());
        msg2.set_field(FIELD_1_NAME.to_owned(), NodeValue::None);

        assert_that!((&msg1 as &dyn DynamicMessage).equals(&msg2), eq(true));
    }

    #[test]
    fn equals_handles_different_none_and_some_node_values() {
        let mut msg1 = MockDynamicMessage::new(TEST_MESSAGE_NAME.to_owned());
        msg1.set_field(FIELD_1_NAME.to_owned(), NodeValue::None);

        let mut msg2 = MockDynamicMessage::new(TEST_MESSAGE_NAME.to_owned());
        msg2.set_field(FIELD_1_NAME.to_owned(), NodeValue::Int32(1));

        assert_that!((&msg1 as &dyn DynamicMessage).equals(&msg2), eq(false));
    }
}
