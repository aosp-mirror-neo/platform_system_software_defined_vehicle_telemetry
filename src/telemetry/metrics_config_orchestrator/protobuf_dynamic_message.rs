// Copyright 2025 Google LLC

use protobuf::reflect::{MessageDescriptor, ReflectFieldRef, RuntimeType as ProtobufRuntimeType};
use protobuf::MessageDyn;
use sdv_telemetry_expr::dynamic_message::DynamicMessage;
use sdv_telemetry_expr::node_error::NodeEvaluationError;
use sdv_telemetry_expr::node_value::NodeValue;
use std::sync::{Arc, LazyLock};

mod to_node_value;

use to_node_value::ToNodeValue;

/// Wrapper for protobuf MessageDyn that implements Telemetry expression evaluation
/// DynamicMessage trait.
pub(super) struct ProtobufDynamicMessage {
    field_names: LazyLock<Vec<String>, Box<dyn FnOnce() -> Vec<String> + Send + Sync>>,
    message: Arc<dyn MessageDyn>,
    message_descriptor: MessageDescriptor,
}

impl std::fmt::Debug for ProtobufDynamicMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.message.fmt(f)
    }
}

impl DynamicMessage for ProtobufDynamicMessage {
    fn get_all_field_names(&self) -> &[String] {
        &self.field_names
    }

    fn get_field_value(&self, field_name: &str) -> Result<NodeValue, NodeEvaluationError> {
        let field_descriptor =
            self.message_descriptor.field_by_name(field_name).ok_or_else(|| {
                NodeEvaluationError::FieldNotFound {
                    field_name: field_name.to_owned(),
                    message_name: self.message_descriptor.name().to_owned(),
                }
            })?;

        match field_descriptor.get_reflect(self.message.as_ref()) {
            // `optional` or singular protobuf field
            ReflectFieldRef::Optional(singular_field) => singular_field.value().map_or_else(
                move || {
                    // "None" has different meanings based on whether a field is explicitly marked
                    // as optional
                    let field_full_name = field_descriptor.full_name();
                    // proto3 optional fields will have a "synthetic oneof descriptor"
                    let proto3_optional = self.message_descriptor.all_oneofs().any(|oneof| {
                        oneof.fields().any(|field| field.full_name() == field_full_name)
                    });
                    // proto3 optional fields track field presence, so "None" means field not set
                    if proto3_optional {
                        Ok(NodeValue::None)
                    } else {
                        // otherwise rust protobuf encodes default value as "None", so the field
                        // value should actually be the default (either some sort of zero or None)
                        Self::get_default_node_value_from_type(
                            &field_descriptor.singular_runtime_type(),
                        )
                    }
                },
                |field_value| field_value.to_node_value(),
            ),
            // `repeated` protobuf field
            ReflectFieldRef::Repeated(repeated_field) => repeated_field.to_node_value(),
            // map field
            ReflectFieldRef::Map(_) => Err(NodeEvaluationError::UnsupportedSingularFieldType),
        }
    }

    fn get_type_name(&self) -> &str {
        self.message_descriptor.full_name()
    }

    fn to_protobuf(&self) -> Arc<dyn MessageDyn> {
        Arc::clone(&self.message)
    }
}

impl ProtobufDynamicMessage {
    pub(super) fn new(message: Arc<dyn MessageDyn>) -> Self {
        let message_descriptor = message.descriptor_dyn();
        let message_descriptor_clone = message_descriptor.clone();
        Self {
            field_names: LazyLock::new(Box::new(move || {
                message_descriptor_clone.fields().map(|field| field.name().to_owned()).collect()
            })),
            message,
            message_descriptor,
        }
    }

    fn get_default_node_value_from_type(
        runtime_type: &ProtobufRuntimeType,
    ) -> Result<NodeValue, NodeEvaluationError> {
        Ok(match runtime_type {
            ProtobufRuntimeType::I32 => NodeValue::Int32(0),
            ProtobufRuntimeType::I64 => NodeValue::Int64(0),
            ProtobufRuntimeType::U32 => NodeValue::Int64(0),
            ProtobufRuntimeType::U64 => NodeValue::Int64(0),
            ProtobufRuntimeType::F32 => NodeValue::Float(0.0),
            ProtobufRuntimeType::F64 => NodeValue::Double(0.0),
            ProtobufRuntimeType::Bool => NodeValue::Bool(false),
            ProtobufRuntimeType::String => NodeValue::String(String::default()),
            ProtobufRuntimeType::Enum(_) => NodeValue::Int32(0),
            ProtobufRuntimeType::Message(_) => NodeValue::None,
            ProtobufRuntimeType::VecU8 => NodeValue::ByteList(vec![]),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::assert_that;
    use googletest::matchers::eq;
    use sdv_telemetry_test_proto::test_message::TestMessage;
    use sdv_telemetry_test_utils::TEST_MESSAGE;

    #[test]
    fn get_field_value_optional_fields_success() {
        let msg = TestMessage {
            optional_bool_field: Some(true),
            optional_int64_field: Some(42),
            ..Default::default()
        };
        let dynamic_message = ProtobufDynamicMessage::new(Arc::new(msg));

        assert_that!(
            dynamic_message.get_field_value("optional_bool_field").unwrap(),
            eq(&NodeValue::Bool(true)),
        );
        assert_that!(
            dynamic_message.get_field_value("optional_int64_field").unwrap(),
            eq(&NodeValue::Int64(42)),
        );
    }

    #[test]
    fn get_field_value_optional_fields_not_set_success() {
        let dynamic_message = ProtobufDynamicMessage::new(Arc::new(TestMessage::new()));

        assert_that!(
            dynamic_message.get_field_value("optional_bool_field").unwrap(),
            eq(&NodeValue::None),
        );
        assert_that!(
            dynamic_message.get_field_value("optional_int64_field").unwrap(),
            eq(&NodeValue::None),
        );
    }

    #[test]
    fn get_field_value_field_not_set_uses_default_values() {
        let dynamic_message = ProtobufDynamicMessage::new(Arc::new(TestMessage::new()));

        assert_that!(
            dynamic_message.get_field_value("int_field").unwrap(),
            eq(&NodeValue::Int32(0)),
        );
        assert_that!(
            dynamic_message.get_field_value("float_field").unwrap(),
            eq(&NodeValue::Float(0.0)),
        );
        assert_that!(
            dynamic_message.get_field_value("enum_field").unwrap(),
            eq(&NodeValue::Int32(0)),
        );
        assert_that!(
            dynamic_message.get_field_value("string_field").unwrap(),
            eq(&NodeValue::String(String::default())),
        );
        assert_that!(
            dynamic_message.get_field_value("bytes_field").unwrap(),
            eq(&NodeValue::ByteList(vec![])),
        );
    }

    #[test]
    fn get_field_value_message_field_not_set_returns_none() {
        let dynamic_message = ProtobufDynamicMessage::new(Arc::new(TestMessage::new()));

        assert_that!(
            dynamic_message.get_field_value("message_field").unwrap(),
            eq(&NodeValue::None),
        );
    }

    #[test]
    fn get_field_value_repeated_field_not_set_returns_empty_list() {
        let dynamic_message = ProtobufDynamicMessage::new(Arc::new(TestMessage::new()));

        assert_that!(
            dynamic_message.get_field_value("repeated_bools").unwrap(),
            eq(&NodeValue::BoolList(vec![])),
        );
    }

    #[test]
    fn get_field_value_int_field_ok() {
        let dynamic_message = ProtobufDynamicMessage::new(Arc::new(TEST_MESSAGE.clone()));

        assert_that!(
            dynamic_message.get_field_value("int_field").unwrap(),
            eq(&NodeValue::Int32(80)),
        );
    }

    #[test]
    fn get_field_value_float_field_ok() {
        let dynamic_message = ProtobufDynamicMessage::new(Arc::new(TEST_MESSAGE.clone()));

        assert_that!(
            dynamic_message.get_field_value("float_field").unwrap(),
            eq(&NodeValue::Float(100.0)),
        );
    }

    #[test]
    fn get_field_value_enum_field_ok() {
        let dynamic_message = ProtobufDynamicMessage::new(Arc::new(TEST_MESSAGE.clone()));

        assert_that!(
            dynamic_message.get_field_value("enum_field").unwrap(),
            eq(&NodeValue::Int32(1)),
        );
    }

    #[test]
    fn get_field_value_message_field_ok() {
        let dynamic_message = ProtobufDynamicMessage::new(Arc::new(TEST_MESSAGE.clone()));
        let expected_message = NodeValue::Message(Arc::new(ProtobufDynamicMessage::new(Arc::new(
            TEST_MESSAGE.message_field.clone().unwrap(),
        ))));

        assert_that!(
            dynamic_message.get_field_value("message_field").unwrap(),
            eq(&expected_message)
        );
    }

    #[test]
    fn get_field_value_string_field_ok() {
        let dynamic_message = ProtobufDynamicMessage::new(Arc::new(TEST_MESSAGE.clone()));

        assert_that!(
            dynamic_message.get_field_value("string_field").unwrap(),
            eq(&NodeValue::String("Hello world".to_owned())),
        );
    }

    #[test]
    fn get_field_value_bytes_field_ok() {
        let dynamic_message = ProtobufDynamicMessage::new(Arc::new(TEST_MESSAGE.clone()));

        assert_that!(
            dynamic_message.get_field_value("bytes_field").unwrap(),
            eq(&NodeValue::ByteList("Hello world".as_bytes().to_owned())),
        );
    }

    #[test]
    fn get_field_value_repeated_bool_field_ok() {
        let dynamic_message = ProtobufDynamicMessage::new(Arc::new(TEST_MESSAGE.clone()));
        let repeated_bools = NodeValue::BoolList(TEST_MESSAGE.repeated_bools.clone());

        assert_that!(
            dynamic_message.get_field_value("repeated_bools").unwrap(),
            eq(&repeated_bools),
        );
    }

    #[test]
    fn get_field_value_repeated_int_field_ok() {
        let dynamic_message = ProtobufDynamicMessage::new(Arc::new(TEST_MESSAGE.clone()));
        let repeated_i64s = NodeValue::Int64List(TEST_MESSAGE.repeated_i64s.clone());

        assert_that!(dynamic_message.get_field_value("repeated_i64s").unwrap(), eq(&repeated_i64s),);
    }

    #[test]
    fn get_field_value_repeated_double_field_ok() {
        let dynamic_message = ProtobufDynamicMessage::new(Arc::new(TEST_MESSAGE.clone()));
        let repeated_doubles = NodeValue::DoubleList(TEST_MESSAGE.repeated_doubles.clone());

        assert_that!(
            dynamic_message.get_field_value("repeated_doubles").unwrap(),
            eq(&repeated_doubles),
        );
    }

    #[test]
    fn get_field_value_repeated_enum_field_ok() {
        let dynamic_message = ProtobufDynamicMessage::new(Arc::new(TEST_MESSAGE.clone()));
        let repeated_enums = NodeValue::Int32List(
            TEST_MESSAGE.repeated_enums.iter().map(|x| x.value()).collect::<Vec<_>>(),
        );

        assert_that!(
            dynamic_message.get_field_value("repeated_enums").unwrap(),
            eq(&repeated_enums)
        );
    }

    #[test]
    fn get_field_value_repeated_message_field_ok() {
        let dynamic_message = ProtobufDynamicMessage::new(Arc::new(TEST_MESSAGE.clone()));
        let repeated_msgs = NodeValue::MessageList(
            TEST_MESSAGE
                .repeated_messages
                .iter()
                .map(|x| {
                    Arc::new(ProtobufDynamicMessage::new(Arc::new(x.clone())))
                        as Arc<dyn DynamicMessage>
                })
                .collect::<Vec<_>>(),
        );

        assert_that!(
            dynamic_message.get_field_value("repeated_messages").unwrap(),
            eq(&repeated_msgs)
        );
    }
}
