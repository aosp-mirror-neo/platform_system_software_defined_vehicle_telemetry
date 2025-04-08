// Copyright 2025 Google LLC

mod node_value_to_protobuf_reflection;

use log::trace;
use parking_lot::Mutex;
use protobuf::{reflect::MessageDescriptor, MessageDyn};
use sdv_telemetry_expr::dynamic_message::ProtobufConversionError;
use sdv_telemetry_expr::{
    dynamic_message::DynamicMessage, node_error::NodeEvaluationError, node_value::NodeValue,
};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use crate::metrics_config_orchestrator::node_value_dynamic_message::node_value_to_protobuf_reflection::{ToReflectValueBox, ReflectRepeatedMutExt};

/// Implementation of Telemetry expression evaluation DynamicMessage trait
/// that operates on NodeValues.
#[derive(Debug)]
pub(super) struct NodeValueDynamicMessage {
    message_descriptor: MessageDescriptor,
    protobuf_message: Mutex<Option<Arc<dyn MessageDyn>>>,
    values: HashMap<String, NodeValue>,
}

impl DynamicMessage for NodeValueDynamicMessage {
    fn get_descriptor(&self) -> &MessageDescriptor {
        &self.message_descriptor
    }

    fn get_field_value(&self, field_name: &str) -> Result<NodeValue, NodeEvaluationError> {
        if self.message_descriptor.field_by_name(field_name).is_none() {
            return Err(NodeEvaluationError::FieldNotFound {
                field_name: field_name.to_owned(),
                message_name: self.message_descriptor.name().to_owned(),
            });
        }

        let value = self
            .values
            .get(field_name)
            .map_or_else(|| NodeValue::None, |node_value| node_value.clone());
        Ok(value)
    }

    fn to_protobuf(&self) -> Result<Arc<dyn MessageDyn>, ProtobufConversionError> {
        let mut protobuf_message_lock = self.protobuf_message.lock();
        if let Some(protobuf_message) = protobuf_message_lock.as_ref() {
            Ok(Arc::clone(protobuf_message))
        } else {
            let protobuf_message = self.to_protobuf_impl()?;
            protobuf_message_lock.replace(Arc::clone(&protobuf_message));
            Ok(protobuf_message)
        }
    }
}

impl NodeValueDynamicMessage {
    pub(super) fn new(
        message_descriptor: MessageDescriptor,
        values: HashMap<String, NodeValue>,
    ) -> Self {
        for field_name in values.keys() {
            assert!(
                message_descriptor.field_by_name(field_name).is_some(),
                "Field {field_name} not found in message descriptor {message_descriptor}"
            );
        }

        Self { message_descriptor, protobuf_message: None.into(), values }
    }

    fn to_protobuf_impl(&self) -> Result<Arc<dyn MessageDyn>, ProtobufConversionError> {
        trace!(
            "Converting {} with {} fields to protobuf",
            self.message_descriptor,
            self.values.len()
        );

        let mut msg = self.message_descriptor.new_instance();

        for (field_name, value) in &self.values {
            if value.is_none() {
                continue;
            }

            let field_descriptor = self
                .message_descriptor
                .field_by_name(field_name)
                .expect("Fields in the map should have a matching descriptor");
            if field_descriptor.is_singular() {
                let converted_value = value
                    .to_reflect_value_box(&field_descriptor.singular_runtime_type())
                    .map_err(|error| ProtobufConversionError {
                        field_name: field_name.to_owned(),
                        error,
                    })?;
                field_descriptor.set_singular_field(msg.as_mut(), converted_value);
            } else if field_descriptor.is_repeated() {
                field_descriptor
                    .mut_repeated(msg.as_mut())
                    .push_node_value_content(value)
                    .map_err(|error| ProtobufConversionError {
                        field_name: field_name.to_owned(),
                        error,
                    })?;
            } else {
                panic!("Validation should have ensured map fields are not allowed");
            }
        }

        Ok(msg.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::prelude::*;
    use lazy_static::lazy_static;
    use protobuf::well_known_types::any::Any;
    use protobuf::Enum;
    use protobuf::{reflect::ReflectEqMode, MessageField, MessageFull};
    use sdv_telemetry_expr::node_value::NodeValue;
    use sdv_telemetry_test_proto::test_message::{TestEnum, TestMessage, TestNestedMessage};
    use sdv_telemetry_test_utils::msg_dyn_eq;
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::metrics_config_orchestrator::protobuf_dynamic_message::ProtobufDynamicMessage;

    lazy_static! {
        static ref REFLECT_EQ_MODE: ReflectEqMode = ReflectEqMode::default();
        static ref TEST_NESTED_MESSAGE: TestNestedMessage = {
            let mut msg = TestNestedMessage::new();
            msg.bool_field = true;
            msg.repeated_i32s = vec![1, 2];
            msg
        };
        static ref TEST_NESTED_MESSAGE_DYN: Arc<dyn DynamicMessage> =
            Arc::new(ProtobufDynamicMessage::new(Arc::new(TEST_NESTED_MESSAGE.clone())));
    }

    // Field names from test_message.proto
    const INT_FIELD_NAME: &str = "int_field";
    const FLOAT_FIELD_NAME: &str = "float_field";
    const STRING_FIELD_NAME: &str = "string_field";
    const BYTES_FIELD_NAME: &str = "bytes_field";
    const ENUM_FIELD_NAME: &str = "enum_field";
    const MESSAGE_FIELD_NAME: &str = "message_field";
    const ANY_FIELD_NAME: &str = "any_field";
    const REPEATED_BOOL_FIELD_NAME: &str = "repeated_bools";
    const REPEATED_INT_FIELD_NAME: &str = "repeated_i64s";
    const REPEATED_DOUBLE_FIELD_NAME: &str = "repeated_doubles";
    const REPEATED_ENUM_FIELD_NAME: &str = "repeated_enums";
    const REPEATED_MESSAGE_FIELD_NAME: &str = "repeated_messages";
    const OPTIONAL_BOOL_FIELD_NAME: &str = "optional_bool_field";
    const OPTIONAL_INT64_FIELD_NAME: &str = "optional_int64_field";
    const NON_EXISTENT_FIELD: &str = "non_existent_field";

    fn create_message(values: HashMap<String, NodeValue>) -> NodeValueDynamicMessage {
        NodeValueDynamicMessage::new(TestMessage::descriptor(), values)
    }

    #[test]
    fn new_success() {
        let values = HashMap::from([(INT_FIELD_NAME.to_owned(), NodeValue::Int32(1))]);
        let _msg = create_message(values);
        // Implicitly tested: doesn't panic
    }

    #[test]
    #[should_panic(expected = "Field non_existent_field not found in message descriptor")]
    fn new_panic_on_invalid_field() {
        let values = HashMap::from([(NON_EXISTENT_FIELD.to_owned(), NodeValue::Int32(1))]);
        create_message(values);
    }

    #[test]
    fn get_descriptor_success() {
        let msg = create_message(HashMap::new());
        assert_that!(msg.get_descriptor(), eq(&TestMessage::descriptor()));
    }

    #[test]
    fn get_field_value_present_success() {
        let values = HashMap::from([
            (INT_FIELD_NAME.to_owned(), NodeValue::Int32(42)),
            (FLOAT_FIELD_NAME.to_owned(), NodeValue::Float(std::f32::consts::PI)),
            (STRING_FIELD_NAME.to_owned(), NodeValue::String("hello".to_owned())),
            (
                MESSAGE_FIELD_NAME.to_owned(),
                NodeValue::Message(Arc::clone(&TEST_NESTED_MESSAGE_DYN)),
            ),
            (REPEATED_INT_FIELD_NAME.to_owned(), NodeValue::Int64List(vec![1, 2, 3])),
        ]);
        let msg = create_message(values);

        assert_that!(msg.get_field_value(INT_FIELD_NAME), ok(eq(&NodeValue::Int32(42))));
        assert_that!(
            msg.get_field_value(FLOAT_FIELD_NAME),
            ok(eq(&NodeValue::Float(std::f32::consts::PI)))
        );
        assert_that!(
            msg.get_field_value(STRING_FIELD_NAME),
            ok(eq(&NodeValue::String("hello".to_owned())))
        );
        assert_that!(
            msg.get_field_value(MESSAGE_FIELD_NAME),
            ok(eq(&NodeValue::Message(Arc::clone(&TEST_NESTED_MESSAGE_DYN))))
        );
        assert_that!(
            msg.get_field_value(REPEATED_INT_FIELD_NAME),
            ok(eq(&NodeValue::Int64List(vec![1, 2, 3])))
        );
    }

    #[test]
    fn get_field_value_absent_returns_none() {
        // Create message without int_field
        let values = HashMap::from([(FLOAT_FIELD_NAME.to_owned(), NodeValue::Float(1.0))]);
        let msg = create_message(values);

        // Field exists in descriptor but not in values map
        assert_that!(msg.get_field_value(INT_FIELD_NAME), ok(eq(&NodeValue::None)));
    }

    #[test]
    fn get_field_value_non_existent_field_returns_error() {
        let msg = create_message(HashMap::new());

        assert_that!(
            msg.get_field_value(NON_EXISTENT_FIELD),
            err(pat!(NodeEvaluationError::FieldNotFound {
                field_name: eq(NON_EXISTENT_FIELD),
                message_name: eq(TestMessage::descriptor().name()),
            }))
        );
    }

    #[test]
    fn to_protobuf_empty_success() {
        let msg = create_message(HashMap::new());
        let expected_proto = TestMessage::new();

        let proto_result = msg.to_protobuf();

        assert_that!(&proto_result, &ok(msg_dyn_eq(&expected_proto, &REFLECT_EQ_MODE)));
    }

    #[test]
    fn to_protobuf_singular_fields_success() {
        let values = HashMap::from([
            (INT_FIELD_NAME.to_owned(), NodeValue::Int32(5)),
            (FLOAT_FIELD_NAME.to_owned(), NodeValue::Float(0.5)),
            (ENUM_FIELD_NAME.to_owned(), NodeValue::Int32(TestEnum::SOME.value())),
            (STRING_FIELD_NAME.to_owned(), NodeValue::String("test".to_owned())),
            (BYTES_FIELD_NAME.to_owned(), NodeValue::ByteList(vec![1, 2, 3])),
            (
                MESSAGE_FIELD_NAME.to_owned(),
                NodeValue::Message(Arc::clone(&TEST_NESTED_MESSAGE_DYN)),
            ),
            // Any field requires a message that can be packed
            (ANY_FIELD_NAME.to_owned(), NodeValue::Message(Arc::clone(&TEST_NESTED_MESSAGE_DYN))),
            (OPTIONAL_BOOL_FIELD_NAME.to_owned(), NodeValue::Bool(true)),
            (OPTIONAL_INT64_FIELD_NAME.to_owned(), NodeValue::Int64(0)), // Zero in optional is preserved
        ]);
        let msg = create_message(values);

        let mut expected_proto = TestMessage::new();
        expected_proto.int_field = 5;
        expected_proto.float_field = 0.5;
        expected_proto.enum_field = TestEnum::SOME.into();
        expected_proto.string_field = "test".to_owned();
        expected_proto.bytes_field = vec![1, 2, 3];
        expected_proto.message_field = MessageField::some(TEST_NESTED_MESSAGE.clone());
        expected_proto.any_field = MessageField::some(Any::pack(&*TEST_NESTED_MESSAGE).unwrap());
        expected_proto.optional_bool_field = Some(true);
        expected_proto.optional_int64_field = Some(0);

        let proto_result = msg.to_protobuf();

        assert_that!(&proto_result, &ok(msg_dyn_eq(&expected_proto, &REFLECT_EQ_MODE)));
    }

    #[test]
    fn to_protobuf_repeated_fields_success() {
        let bools = vec![true, false];
        let ints = vec![10i64, -20];
        let doubles = vec![1.1, 2.2];
        let enums = vec![TestEnum::SOME.value(), TestEnum::NONE.value()]; // Enums as Int32 list
        let messages = vec![Arc::clone(&TEST_NESTED_MESSAGE_DYN); 2];

        let values = HashMap::from([
            (REPEATED_BOOL_FIELD_NAME.to_owned(), NodeValue::BoolList(bools.clone())),
            (REPEATED_INT_FIELD_NAME.to_owned(), NodeValue::Int64List(ints.clone())),
            (REPEATED_DOUBLE_FIELD_NAME.to_owned(), NodeValue::DoubleList(doubles.clone())),
            (REPEATED_ENUM_FIELD_NAME.to_owned(), NodeValue::Int32List(enums.clone())),
            (REPEATED_MESSAGE_FIELD_NAME.to_owned(), NodeValue::MessageList(messages.clone())),
        ]);
        let msg = create_message(values);

        let mut expected_proto = TestMessage::new();
        expected_proto.repeated_bools = bools;
        expected_proto.repeated_i64s = ints;
        expected_proto.repeated_doubles = doubles;
        expected_proto.repeated_enums = vec![TestEnum::SOME.into(), TestEnum::NONE.into()];
        expected_proto.repeated_messages = vec![TEST_NESTED_MESSAGE.clone(); 2];

        let proto_result = msg.to_protobuf();

        assert_that!(&proto_result, &ok(msg_dyn_eq(&expected_proto, &REFLECT_EQ_MODE)));
    }

    #[test]
    fn to_protobuf_missing_fields_use_defaults() {
        // Provide only float_field
        let values = HashMap::from([(FLOAT_FIELD_NAME.to_owned(), NodeValue::Float(1.23))]);
        let msg = create_message(values);

        let mut expected_proto = TestMessage::new(); // All other fields should be default
        expected_proto.float_field = 1.23;

        let proto_result = msg.to_protobuf();

        assert_that!(&proto_result, &ok(msg_dyn_eq(&expected_proto, &REFLECT_EQ_MODE)));
    }

    #[test]
    fn to_protobuf_none_value_is_skipped_for_implicit_optional() {
        // Provide None for int_field (implicitly optional in proto3)
        let values = HashMap::from([(INT_FIELD_NAME.to_owned(), NodeValue::None)]);
        let msg = create_message(values);

        // Expect an empty message, as None shouldn't set the field
        let expected_proto = TestMessage::new();

        let proto_result = msg.to_protobuf();

        assert_that!(&proto_result, &ok(msg_dyn_eq(&expected_proto, &REFLECT_EQ_MODE)));
    }

    #[test]
    fn to_protobuf_none_value_is_skipped_for_explicit_optional() {
        // Provide None for optional_int64_field (explicitly optional)
        let values = HashMap::from([(OPTIONAL_INT64_FIELD_NAME.to_owned(), NodeValue::None)]);
        let msg = create_message(values);

        // Expect an empty message, as None shouldn't set the field
        let expected_proto = TestMessage::new();

        let proto_result = msg.to_protobuf();

        assert_that!(&proto_result, &ok(msg_dyn_eq(&expected_proto, &REFLECT_EQ_MODE)));
    }

    #[test]
    fn to_protobuf_type_mismatch_singular_fails() {
        // Assign Bool to Int field
        let values = HashMap::from([(INT_FIELD_NAME.to_owned(), NodeValue::Bool(true))]);
        let msg = create_message(values);

        let proto_result = msg.to_protobuf();

        assert_that!(
            proto_result,
            err(pat!(ProtobufConversionError {
                field_name: eq(INT_FIELD_NAME),
                error: pat!(NodeEvaluationError::ValueToProtoConversionError { .. }),
            }))
        );
    }

    #[test]
    fn to_protobuf_type_mismatch_repeated_fails() {
        // Assign IntList to BoolList field
        let values =
            HashMap::from([(REPEATED_BOOL_FIELD_NAME.to_owned(), NodeValue::Int64List(vec![1]))]);
        let msg = create_message(values);

        let proto_result = msg.to_protobuf();

        assert_that!(
            proto_result,
            err(pat!(ProtobufConversionError {
                field_name: eq(REPEATED_BOOL_FIELD_NAME),
                error: pat!(NodeEvaluationError::ValueToProtoConversionError { .. }),
            }))
        );
    }

    #[test]
    fn to_protobuf_type_mismatch_message_fails() {
        // Assign Int to Message field
        let values = HashMap::from([(MESSAGE_FIELD_NAME.to_owned(), NodeValue::Int32(1))]);
        let msg = create_message(values);

        let proto_result = msg.to_protobuf();

        assert_that!(
            proto_result,
            err(pat!(ProtobufConversionError {
                field_name: eq(MESSAGE_FIELD_NAME),
                error: pat!(NodeEvaluationError::ValueToProtoConversionError { .. }),
            }))
        );
    }

    #[test]
    fn to_protobuf_caches_result() {
        let values = HashMap::from([(INT_FIELD_NAME.to_owned(), NodeValue::Int32(10))]);
        let msg = create_message(values);

        let proto_result1 = msg.to_protobuf().unwrap();
        let proto_result2 = msg.to_protobuf().unwrap();

        // Check if the Arc pointers are the same, indicating caching
        assert!(Arc::ptr_eq(&proto_result1, &proto_result2));

        // Also check the internal lock state
        assert!(msg.protobuf_message.lock().is_some());
    }
}
