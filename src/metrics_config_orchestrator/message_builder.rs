// Copyright 2023 Google LLC. All Rights Reserved.

mod value_aggregator;
pub(super) mod value_aggregator_factory;

use protobuf::{reflect::MessageDescriptor, MessageDyn};
use std::collections::HashMap;
use std::sync::Mutex;
use thiserror::Error;

use crate::expr::{
    node_errors::NodeError,
    proto_node_value_converter::node_value_to_protobuf_reflection::{
        ReflectRepeatedMutExt, ToReflectValueBox,
    },
};
use value_aggregator::{AggregationError, ValueAggregator};

pub(crate) trait MessageBuilder: Send + Sync {
    /// Builds a protobuf message dynamically.
    fn build(&self) -> Result<Box<dyn MessageDyn>, MessageBuilderError>;
}

pub(crate) struct MessageBuilderImpl {
    /// Uses descriptor to create and populate a protobuf message
    pub message_descriptor: MessageDescriptor,

    /// Maps field_name to the node whose evaluation output is the field value.
    pub field_assignments: HashMap<String, Mutex<Box<dyn ValueAggregator>>>,
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum MessageBuilderError {
    #[error("Unable to assign value to field {field_name} because of: {e}")]
    BadAggregation { field_name: String, e: AggregationError },
    #[error("Failed to assign node value to field because of: {e}")]
    FailedToAssignValueToField {
        #[from]
        e: NodeError,
    },
}

impl MessageBuilder for MessageBuilderImpl {
    fn build(&self) -> Result<Box<dyn MessageDyn>, MessageBuilderError> {
        let mut msg = self.message_descriptor.new_instance();
        for (field_name, field_aggregator_mutex) in self.field_assignments.iter() {
            let to_builder_err =
                |e| MessageBuilderError::BadAggregation { field_name: field_name.to_string(), e };
            let field_descriptor = self
                .message_descriptor
                .field_by_name(field_name)
                .expect("All fields should have been validated to exist");
            let mut field_aggregator = field_aggregator_mutex.lock().unwrap();
            let node_value = field_aggregator.compute().map_err(to_builder_err)?;
            // if node_value is none, skip populating this field
            if node_value.is_none() {
                // TODO: b/307547570 - properly handle absent values
                continue;
            }
            if field_descriptor.is_singular() {
                field_descriptor.set_singular_field(
                    msg.as_mut(),
                    node_value.to_reflect_value_box(&field_descriptor.singular_runtime_type())?,
                );
            } else if field_descriptor.is_repeated() {
                field_descriptor.mut_repeated(msg.as_mut()).push_node_value_content(&node_value)?;
            } else {
                panic!("Validation should have ensured map fields are not allowed");
            }
        }
        Ok(msg)
    }
}

impl MessageBuilderImpl {
    pub fn new(
        message_descriptor: MessageDescriptor,
        field_assignments: HashMap<String, Mutex<Box<dyn ValueAggregator>>>,
    ) -> Self {
        MessageBuilderImpl { message_descriptor, field_assignments }
    }
}

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
mod tests {
    use lazy_static::lazy_static;
    use protobuf::{reflect::ReflectEqMode, MessageField, MessageFull};
    use sdv_telemetry_test_proto::test_message::{TestEnum, TestMessage, TestNestedMessage};

    use super::*;
    use crate::expr::node_value::NodeValue;
    use value_aggregator::mock::MockValueAggregator;

    lazy_static! {
        static ref REFLECT_EQ_MODE: ReflectEqMode = ReflectEqMode::default();
        static ref TEST_NESTED_MESSAGE: TestNestedMessage = {
            let mut msg = TestNestedMessage::new();
            msg.bool_field = true;
            msg.repeated_i32s = vec![1, 2];
            msg
        };
    }

    // For all available field names, see src/test_utils/test_message.proto
    const INT_FIELD_NAME: &str = "int_field";
    const FLOAT_FIELD_NAME: &str = "float_field";
    const ENUM_FIELD_NAME: &str = "enum_field";
    const MESSAGE_FIELD_NAME: &str = "message_field";
    const REPEATED_BOOL_FIELD_NAME: &str = "repeated_bools";
    const REPEATED_INT_FIELD_NAME: &str = "repeated_i64s";
    const REPEATED_DOUBLE_FIELD_NAME: &str = "repeated_doubles";
    const REPEATED_ENUM_FIELD_NAME: &str = "repeated_enums";
    const OPTIONAL_BOOL_FIELD_NAME: &str = "optional_bool_field";
    const OPTIONAL_INT64_FIELD_NAME: &str = "optional_int64_field";

    /// Creates a MessageBuilderImpl struct object using the descriptor and
    /// field_name + node_value pairs as field assignments.
    fn create_message_builder(
        message_descriptor: MessageDescriptor,
        field_name_and_value_pairs: Vec<(&str, NodeValue)>,
    ) -> MessageBuilderImpl {
        let field_assignments = field_name_and_value_pairs
            .into_iter()
            .map(|(field, value)| {
                // the mocked field value aggregator always return the node value
                // that was used to construct it.
                let mock_aggregator: Box<dyn ValueAggregator> =
                    Box::new(MockValueAggregator::new(value));
                (field.to_string(), Mutex::new(mock_aggregator))
            })
            .collect();
        MessageBuilderImpl { message_descriptor, field_assignments }
    }

    #[test]
    fn build_singular_fields_ok() -> Result<(), MessageBuilderError> {
        let mut expected_message = TestMessage::new();
        expected_message.int_field = 5;
        expected_message.float_field = 0.0;
        expected_message.enum_field = TestEnum::SOME.into(); // this enum is equal to int 1

        // create MessageBuilderImpl struct object, this is the subject of the test
        let builder = create_message_builder(
            TestMessage::descriptor(),
            vec![
                (INT_FIELD_NAME, NodeValue::Int32(5)),
                (FLOAT_FIELD_NAME, NodeValue::Float(0.0)),
                (ENUM_FIELD_NAME, NodeValue::Int32(1)),
            ],
        );

        // action
        let res = builder.build()?;

        // compare `Box<dyn MessageDyn>` with `MessageFull` using reflect_eq_dyn
        assert!(res.reflect_eq_dyn(&expected_message, &REFLECT_EQ_MODE));
        Ok(())
    }

    #[test]
    fn build_message_field_ok() -> Result<(), MessageBuilderError> {
        let mut expected_message = TestMessage::new();
        expected_message.int_field = 42;
        expected_message.message_field = MessageField::some(TEST_NESTED_MESSAGE.clone());

        // create MessageBuilderImpl struct object, this is the subject of the test
        let builder = create_message_builder(
            TestMessage::descriptor(),
            vec![
                (INT_FIELD_NAME, NodeValue::Int64(42)),
                (MESSAGE_FIELD_NAME, NodeValue::Message(Box::new(TEST_NESTED_MESSAGE.clone()))),
            ],
        );

        // action
        let res = builder.build()?;

        // compare `Box<dyn MessageDyn>` with `MessageFull` using reflect_eq_dyn
        assert!(res.reflect_eq_dyn(&expected_message, &REFLECT_EQ_MODE));
        Ok(())
    }

    #[test]
    fn build_repeated_primitive_fields_ok() -> Result<(), MessageBuilderError> {
        let booleans = vec![true, false, true, false];
        let integers = vec![42, 100, -1];
        let doubles = vec![0.0, 1.0];
        let enums = vec![1, 1, 0]; // SOME, SOME, NONE
        let mut expected_message = TestMessage::new();
        expected_message.repeated_bools = booleans.clone();
        expected_message.repeated_i64s = integers.clone();
        expected_message.repeated_doubles = doubles.clone();
        expected_message.repeated_enums =
            vec![TestEnum::SOME.into(), TestEnum::SOME.into(), TestEnum::NONE.into()];

        // create MessageBuilderImpl struct object, this is the subject of the test
        let builder = create_message_builder(
            TestMessage::descriptor(),
            vec![
                (REPEATED_BOOL_FIELD_NAME, NodeValue::BoolList(booleans)),
                (REPEATED_INT_FIELD_NAME, NodeValue::Int64List(integers)),
                (REPEATED_DOUBLE_FIELD_NAME, NodeValue::DoubleList(doubles)),
                (REPEATED_ENUM_FIELD_NAME, NodeValue::Int64List(enums)),
            ],
        );

        // action
        let res = builder.build()?;

        // compare `Box<dyn MessageDyn>` with `MessageFull` using reflect_eq_dyn
        assert!(res.reflect_eq_dyn(&expected_message, &REFLECT_EQ_MODE));
        Ok(())
    }

    #[test]
    fn build_handles_none_value_ok() -> Result<(), MessageBuilderError> {
        let mut expected_message = TestMessage::new();
        expected_message.int_field = 5;

        // create MessageBuilderImpl struct object, this is the subject of the test
        let builder = create_message_builder(
            TestMessage::descriptor(),
            vec![
                (INT_FIELD_NAME, NodeValue::Int32(5)),
                (FLOAT_FIELD_NAME, NodeValue::None),
                (OPTIONAL_BOOL_FIELD_NAME, NodeValue::None),
            ],
        );

        // action
        let res = builder.build()?;

        // compare `Box<dyn MessageDyn>` with `MessageFull` using reflect_eq_dyn
        assert!(res.reflect_eq_dyn(&expected_message, &REFLECT_EQ_MODE));
        Ok(())
    }

    #[test]
    fn build_zeros_are_not_encoded_in_implicitly_optional_fields() -> Result<(), MessageBuilderError>
    {
        // expect empty message because zeros are not encoded in implicitly optional fields
        // that means 0 is indistinguishable from field-not-set
        let expected_message = TestMessage::new();

        // create MessageBuilderImpl struct object, this is the subject of the test
        let builder = create_message_builder(
            TestMessage::descriptor(),
            vec![(INT_FIELD_NAME, NodeValue::Int32(0))],
        );

        // action
        let res = builder.build()?;

        // compare `Box<dyn MessageDyn>` with `MessageFull` using reflect_eq_dyn
        assert!(res.reflect_eq_dyn(&expected_message, &REFLECT_EQ_MODE));
        Ok(())
    }

    #[test]
    fn build_zeros_are_encoded_in_explicitly_optional_fields() -> Result<(), MessageBuilderError> {
        let mut expected_message = TestMessage::new();
        expected_message.optional_bool_field = Some(false);
        expected_message.optional_int64_field = Some(0);

        // create MessageBuilderImpl struct object, this is the subject of the test
        let builder = create_message_builder(
            TestMessage::descriptor(),
            vec![
                (OPTIONAL_BOOL_FIELD_NAME, NodeValue::Bool(false)),
                (OPTIONAL_INT64_FIELD_NAME, NodeValue::Int64(0)),
            ],
        );

        // action
        let res = builder.build()?;

        // compare `Box<dyn MessageDyn>` with `MessageFull` using reflect_eq_dyn
        assert!(res.reflect_eq_dyn(&expected_message, &REFLECT_EQ_MODE));
        Ok(())
    }

    #[test]
    fn build_node_value_incompatible_with_singular_field_fails() {
        let builder = create_message_builder(
            TestMessage::descriptor(),
            // field expects integer, actual node value is a float
            vec![(INT_FIELD_NAME, NodeValue::Float(0.5))],
        );

        // action
        let res = builder.build();

        assert!(matches!(
            res.err().unwrap(),
            MessageBuilderError::FailedToAssignValueToField { .. }
        ));
    }

    #[test]
    fn build_node_value_incompatible_with_repeated_field_fails() {
        let builder = create_message_builder(
            TestMessage::descriptor(),
            // field expects repeated integers, actual node value is repeated floats
            vec![(REPEATED_INT_FIELD_NAME, NodeValue::FloatList(vec![0.5]))],
        );

        // action
        let res = builder.build();

        assert!(matches!(
            res.err().unwrap(),
            MessageBuilderError::FailedToAssignValueToField { .. }
        ));
    }

    #[test]
    fn build_aggregated_field_value_compute_error_fails() {
        let mut mock_aggregated_field_value = MockValueAggregator::new(NodeValue::None);
        mock_aggregated_field_value.fail_compute();
        let field_assignments = {
            let mut map = HashMap::new();
            let boxed_value: Box<dyn ValueAggregator> = Box::new(mock_aggregated_field_value);
            map.insert(INT_FIELD_NAME.to_string(), Mutex::new(boxed_value));
            map
        };
        let builder =
            MessageBuilderImpl { message_descriptor: TestMessage::descriptor(), field_assignments };

        // action
        let res = builder.build();

        assert!(matches!(res.err().unwrap(), MessageBuilderError::BadAggregation { .. }));
    }
}
