// Copyright 2023 Google LLC. All Rights Reserved.

mod node_value_to_protobuf_reflection;
mod value_aggregator;

use protobuf::{reflect::MessageDescriptor, MessageDyn};
use std::collections::HashMap;
use std::sync::Mutex;
use thiserror::Error;

use node_value_to_protobuf_reflection::{ReflectRepeatedMutExt, ToReflectValueBox};
pub use value_aggregator::AggregationFactory;
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
    #[error("Cannot assign {value} to a repeated field")]
    RepeatedFieldWrongAssignment { value: String },
    #[error("Cannot convert {value} to a protobuf reflection of type {refl_type}")]
    ValueToProtoConversionError { value: String, refl_type: String },
}

impl MessageBuilder for MessageBuilderImpl {
    fn build(&self) -> Result<Box<dyn MessageDyn>, MessageBuilderError> {
        let mut msg = self.message_descriptor.new_instance();
        for (field_name, field_value_mutex) in self.field_assignments.iter() {
            let to_builder_err =
                |e| MessageBuilderError::BadAggregation { field_name: field_name.to_string(), e };
            let field_descriptor = self
                .message_descriptor
                .field_by_name(field_name)
                .expect("All fields should have been validated to exist");
            let mut field_value = field_value_mutex.lock().unwrap();
            if field_descriptor.is_singular() {
                let node_value = field_value.compute().map_err(to_builder_err)?;
                field_descriptor.set_singular_field(
                    msg.as_mut(),
                    node_value.to_reflect_value_box(&field_descriptor.singular_runtime_type())?,
                );
            } else if field_descriptor.is_repeated() {
                let node_value = field_value.compute().map_err(to_builder_err)?;
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
    use sdv_telem_test_proto::test_message::{TestEnum, TestMessage, TestNestedMessage};

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
    fn test_build_singular_fields_ok() -> Result<(), MessageBuilderError> {
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
    fn test_build_message_field_ok() -> Result<(), MessageBuilderError> {
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
    fn test_build_repeated_primitive_fields_ok() -> Result<(), MessageBuilderError> {
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
    fn test_build_node_value_incompatible_with_field_fails() {
        let builder = create_message_builder(
            TestMessage::descriptor(),
            vec![(INT_FIELD_NAME, NodeValue::Float(0.5))],
        );

        // action
        let res = builder.build();

        assert!(matches!(
            res.err().unwrap(),
            MessageBuilderError::ValueToProtoConversionError { .. }
        ));
    }

    #[test]
    fn test_build_aggregated_field_value_compute_error_fails() {
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
