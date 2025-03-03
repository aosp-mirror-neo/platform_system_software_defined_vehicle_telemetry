// Copyright 2023 Google LLC

//! Helper functions for converting NodeValue into protobuf reflection types.

use protobuf::reflect::{ReflectRepeatedMut, ReflectValueBox, RuntimeType as ProtobufRuntimeType};
use protobuf::well_known_types::any::Any;
use protobuf::{MessageDyn, MessageFull};
use std::sync::Arc;

use crate::{node_error::NodeEvaluationError, node_value::NodeValue};

/// Implementing type will be able to convert self into a ReflectValueBox.
pub trait ToReflectValueBox {
    /// This method will attempt to convert self into a ReflectValueBox of the
    /// specified protobuf type, but if self is not compatible to the specified type,
    /// an error will be returned.
    fn to_reflect_value_box(
        &self,
        protobuf_type: &ProtobufRuntimeType,
    ) -> Result<ReflectValueBox, NodeEvaluationError>;
}

/// Extensions for ReflectRepeatedMut.
pub trait ReflectRepeatedMutExt {
    /// If NodeValue holds a list, it will push all the contents of the list into self.
    fn push_node_value_content(
        &mut self,
        node_value: &NodeValue,
    ) -> Result<(), NodeEvaluationError>;

    /// Convert every vector element into a ReflectValueBox and pushes it to self.
    fn push_vec_content<T>(&mut self, list: &[T]) -> Result<(), NodeEvaluationError>
    where
        T: ToReflectValueBox;
}

impl ReflectRepeatedMutExt for ReflectRepeatedMut<'_> {
    fn push_node_value_content(
        &mut self,
        node_value: &NodeValue,
    ) -> Result<(), NodeEvaluationError> {
        match node_value {
            NodeValue::BoolList(v) => self.push_vec_content(v),
            NodeValue::Int32List(v) => self.push_vec_content(v),
            NodeValue::Int64List(v) => self.push_vec_content(v),
            NodeValue::FloatList(v) => self.push_vec_content(v),
            NodeValue::DoubleList(v) => self.push_vec_content(v),
            NodeValue::MessageList(v) => self.push_vec_content(v),
            _ => Err(NodeEvaluationError::RepeatedFieldWrongAssignment {
                value: format!("{:?}", node_value),
            }),
        }
    }

    fn push_vec_content<T>(&mut self, list: &[T]) -> Result<(), NodeEvaluationError>
    where
        T: ToReflectValueBox,
    {
        for value in list.iter() {
            self.push(value.to_reflect_value_box(&self.element_type())?);
        }
        Ok(())
    }
}

/// A NodeValue can be converted into an optional/singular protobuf reflection value. For NodeValues
/// holding a list, use `push_node_value_content()` in `ReflectRepeatedMutExt` trait.
impl ToReflectValueBox for NodeValue {
    fn to_reflect_value_box(
        &self,
        protobuf_type: &ProtobufRuntimeType,
    ) -> Result<ReflectValueBox, NodeEvaluationError> {
        match self {
            Self::None
            | Self::BoolList(_)
            | Self::Int32List(_)
            | Self::Int64List(_)
            | Self::FloatList(_)
            | Self::DoubleList(_)
            | Self::MessageList(_) => Err(NodeEvaluationError::ValueToProtoConversionError {
                value: format!("{self:?}"),
                refl_type: format!("{protobuf_type:?}"),
            }),

            Self::Bool(value) => value.to_reflect_value_box(protobuf_type),
            Self::Int32(value) => value.to_reflect_value_box(protobuf_type),
            Self::Int64(value) => value.to_reflect_value_box(protobuf_type),
            Self::Float(value) => value.to_reflect_value_box(protobuf_type),
            Self::Double(value) => value.to_reflect_value_box(protobuf_type),
            Self::Message(value) => value.to_reflect_value_box(protobuf_type),
            Self::String(value) => value.as_str().to_reflect_value_box(protobuf_type),
            Self::ByteList(value) => value.as_slice().to_reflect_value_box(protobuf_type),
        }
    }
}

/// A boolean can be converted into a protobuf reflection value.
impl ToReflectValueBox for bool {
    fn to_reflect_value_box(
        &self,
        protobuf_type: &ProtobufRuntimeType,
    ) -> Result<ReflectValueBox, NodeEvaluationError> {
        if matches!(protobuf_type, ProtobufRuntimeType::Bool) {
            return Ok(ReflectValueBox::Bool(*self));
        }
        Err(NodeEvaluationError::ValueToProtoConversionError {
            value: format!("{:?}", self),
            refl_type: format!("{:?}", protobuf_type),
        })
    }
}

/// An i32 can be converted into a protobuf reflection value.
impl ToReflectValueBox for i32 {
    fn to_reflect_value_box(
        &self,
        protobuf_type: &ProtobufRuntimeType,
    ) -> Result<ReflectValueBox, NodeEvaluationError> {
        match protobuf_type {
            // Technically, the match arms for I32, F64, and Enum aren't necessary, because
            // `i64::to_reflect_value_box` handles them as well. However, we list them here as they
            // are loss-less only for `i32`, whereas they are lossy for `i64`.
            ProtobufRuntimeType::I32 => Ok(ReflectValueBox::I32(*self)),
            ProtobufRuntimeType::F64 => Ok(ReflectValueBox::F64(f64::from(*self))),
            ProtobufRuntimeType::Enum(enum_descriptor) => {
                Ok(ReflectValueBox::Enum(enum_descriptor.clone(), *self))
            }
            _ => i64::from(*self).to_reflect_value_box(protobuf_type),
        }
    }
}

/// An i64 can be converted into a protobuf reflection value.
impl ToReflectValueBox for i64 {
    fn to_reflect_value_box(
        &self,
        protobuf_type: &ProtobufRuntimeType,
    ) -> Result<ReflectValueBox, NodeEvaluationError> {
        let err_provider = |_| {
            Err(NodeEvaluationError::ValueToProtoConversionError {
                value: format!("{:?}", self),
                refl_type: format!("{:?}", protobuf_type),
            })
        };
        match protobuf_type {
            ProtobufRuntimeType::I32 => {
                i32::try_from(*self).map_or_else(err_provider, |v| Ok(ReflectValueBox::I32(v)))
            }
            ProtobufRuntimeType::I64 => Ok(ReflectValueBox::I64(*self)),
            ProtobufRuntimeType::U32 => {
                u32::try_from(*self).map_or_else(err_provider, |v| Ok(ReflectValueBox::U32(v)))
            }
            ProtobufRuntimeType::U64 => {
                u64::try_from(*self).map_or_else(err_provider, |v| Ok(ReflectValueBox::U64(v)))
            }
            ProtobufRuntimeType::F32 => Ok(ReflectValueBox::F32(*self as f32)),
            ProtobufRuntimeType::F64 => Ok(ReflectValueBox::F64(*self as f64)),
            ProtobufRuntimeType::Enum(enum_descriptor) => i32::try_from(*self)
                .map_or_else(err_provider, |v| {
                    Ok(ReflectValueBox::Enum(enum_descriptor.clone(), v))
                }),
            ProtobufRuntimeType::Bool
            | ProtobufRuntimeType::Message(_)
            | ProtobufRuntimeType::String
            | ProtobufRuntimeType::VecU8 => Err(NodeEvaluationError::ValueToProtoConversionError {
                value: format!("{:?}", self),
                refl_type: format!("{:?}", protobuf_type),
            }),
        }
    }
}

/// A f32 can be converted into a protobuf reflection value.
impl ToReflectValueBox for f32 {
    fn to_reflect_value_box(
        &self,
        protobuf_type: &ProtobufRuntimeType,
    ) -> Result<ReflectValueBox, NodeEvaluationError> {
        match protobuf_type {
            // Technically, the match arm for F32 isn't necessary, because
            // `f64::to_reflect_value_box` handles it as well. However, we list it here as it is
            // loss-less only for `f32`, whereas it is lossy for `f64`.
            ProtobufRuntimeType::F32 => Ok(ReflectValueBox::F32(*self)),
            _ => f64::from(*self).to_reflect_value_box(protobuf_type),
        }
    }
}

/// A f64 can be converted into a protobuf reflection value.
impl ToReflectValueBox for f64 {
    fn to_reflect_value_box(
        &self,
        protobuf_type: &ProtobufRuntimeType,
    ) -> Result<ReflectValueBox, NodeEvaluationError> {
        match protobuf_type {
            ProtobufRuntimeType::F32 => Ok(ReflectValueBox::F32(*self as f32)),
            ProtobufRuntimeType::F64 => Ok(ReflectValueBox::F64(*self)),
            _ => Err(NodeEvaluationError::ValueToProtoConversionError {
                value: format!("{:?}", self),
                refl_type: format!("{:?}", protobuf_type),
            }),
        }
    }
}

// A Arc<dyn MessageDyn> can be converted into a protobuf reflection value.
impl ToReflectValueBox for Arc<dyn MessageDyn> {
    fn to_reflect_value_box(
        &self,
        protobuf_type: &ProtobufRuntimeType,
    ) -> Result<ReflectValueBox, NodeEvaluationError> {
        match protobuf_type {
            ProtobufRuntimeType::Message(descriptor) if descriptor == &Any::descriptor() => {
                Ok(ReflectValueBox::Message(Box::new(Any::pack_dyn(self.as_ref()).unwrap())))
            }
            ProtobufRuntimeType::Message(descriptor) if descriptor == &self.descriptor_dyn() => {
                Ok(ReflectValueBox::Message(self.as_ref().clone_box()))
            }
            _ => Err(NodeEvaluationError::ValueToProtoConversionError {
                value: format!("{:?}", self),
                refl_type: format!("{:?}", protobuf_type),
            }),
        }
    }
}

/// A String can be converted into a protobuf reflection value.
impl ToReflectValueBox for &str {
    fn to_reflect_value_box(
        &self,
        protobuf_type: &ProtobufRuntimeType,
    ) -> Result<ReflectValueBox, NodeEvaluationError> {
        match protobuf_type {
            ProtobufRuntimeType::String => Ok(ReflectValueBox::String(self.to_string())),
            _ => Err(NodeEvaluationError::ValueToProtoConversionError {
                value: format!("{:?}", self),
                refl_type: format!("{:?}", protobuf_type),
            }),
        }
    }
}

/// A Vec<u8> can be converted into a protobuf reflection value.
impl ToReflectValueBox for &[u8] {
    fn to_reflect_value_box(
        &self,
        protobuf_type: &ProtobufRuntimeType,
    ) -> Result<ReflectValueBox, NodeEvaluationError> {
        match protobuf_type {
            ProtobufRuntimeType::VecU8 => Ok(ReflectValueBox::Bytes(self.to_vec())),
            _ => Err(NodeEvaluationError::ValueToProtoConversionError {
                value: format!("{:?}", self),
                refl_type: format!("{:?}", protobuf_type),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::assert_that;
    use googletest::matchers::{eq, err, matches_pattern, ok};
    use protobuf::well_known_types::{any::Any, empty::Empty, wrappers::Int32Value};
    use protobuf::{Enum, EnumFull, Message};
    use sdv_telemetry_test_proto::test_message::{TestEnum, TestMessage};
    use sdv_telemetry_test_utils::parameterized_test;

    fn create_int32_value() -> Int32Value {
        Int32Value { value: 42, ..Default::default() }
    }

    fn create_int32_value_arc() -> Arc<dyn MessageDyn> {
        Arc::new(create_int32_value())
    }

    parameterized_test!(
        #[test]
        to_reflect_value_box,

        |input: &dyn ToReflectValueBox, runtime_type, expected| {
            let result = input.to_reflect_value_box(&runtime_type);

            assert_that!(result, expected);
        },

        bool_to_bool: (&true, ProtobufRuntimeType::Bool, ok(eq(&ReflectValueBox::Bool(true)))),
        bool_to_i32: (&true, ProtobufRuntimeType::I32, err(matches_pattern!(NodeEvaluationError::ValueToProtoConversionError { .. }))),

        bool_node_value_to_bool: (&NodeValue::Bool(true), ProtobufRuntimeType::Bool, ok(eq(&ReflectValueBox::Bool(true)))),

        i32_to_i32: (&42, ProtobufRuntimeType::I32, ok(eq(&ReflectValueBox::I32(42)))),
        i32_to_bool: (&42, ProtobufRuntimeType::Bool, err(matches_pattern!(NodeEvaluationError::ValueToProtoConversionError { .. }))),

        i32_node_value_to_i32: (&NodeValue::Int32(42), ProtobufRuntimeType::I32, ok(eq(&ReflectValueBox::I32(42)))),

        i64_to_i64: (&42, ProtobufRuntimeType::I64, ok(eq(&ReflectValueBox::I64(42)))),
        i64_to_bool: (&42, ProtobufRuntimeType::Bool, err(matches_pattern!(NodeEvaluationError::ValueToProtoConversionError { .. }))),

        i64_node_value_to_i64: (&NodeValue::Int64(42), ProtobufRuntimeType::I64, ok(eq(&ReflectValueBox::I64(42)))),

        f32_to_f32: (&42.0, ProtobufRuntimeType::F32, ok(eq(&ReflectValueBox::F32(42.0)))),
        f32_to_bool: (&42.0, ProtobufRuntimeType::Bool, err(matches_pattern!(NodeEvaluationError::ValueToProtoConversionError { .. }))),

        f32_node_value_to_f32: (&NodeValue::Float(42.0), ProtobufRuntimeType::F32, ok(eq(&ReflectValueBox::F32(42.0)))),

        f64_to_f64: (&42.0, ProtobufRuntimeType::F64, ok(eq(&ReflectValueBox::F64(42.0)))),
        f64_to_bool: (&42.0, ProtobufRuntimeType::Bool, err(matches_pattern!(NodeEvaluationError::ValueToProtoConversionError { .. }))),

        f64_node_value_to_f64: (&NodeValue::Double(42.0), ProtobufRuntimeType::F64, ok(eq(&ReflectValueBox::F64(42.0)))),

        msg_to_msg: (&create_int32_value_arc(), ProtobufRuntimeType::Message(Int32Value::descriptor()), ok(eq(&ReflectValueBox::Message(Box::new(create_int32_value()))))),
        msg_to_any: (&create_int32_value_arc(), ProtobufRuntimeType::Message(Any::descriptor()), ok(eq(&ReflectValueBox::Message(Box::new(Any::pack_dyn(&create_int32_value()).unwrap()))))),
        msg_to_i32: (&create_int32_value_arc(), ProtobufRuntimeType::I32, err(matches_pattern!(NodeEvaluationError::ValueToProtoConversionError { .. }))),

        msg_node_value_to_msg: (&NodeValue::Message(create_int32_value_arc()), ProtobufRuntimeType::Message(Int32Value::descriptor()), ok(eq(&ReflectValueBox::Message(Box::new(create_int32_value()))))),
        msg_node_value_to_any: (&NodeValue::Message(create_int32_value_arc()), ProtobufRuntimeType::Message(Any::descriptor()), ok(eq(&ReflectValueBox::Message(Box::new(Any::pack_dyn(&create_int32_value()).unwrap()))))),

        string_to_string: (&"kukkula", ProtobufRuntimeType::String, ok(eq(&ReflectValueBox::String("kukkula".to_owned())))),
        string_to_i32: (&"kukkula", ProtobufRuntimeType::I32, err(matches_pattern!(NodeEvaluationError::ValueToProtoConversionError { .. }))),

        string_node_value_to_string: (&NodeValue::String("kukkula".to_owned()), ProtobufRuntimeType::String, ok(eq(&ReflectValueBox::String("kukkula".to_owned())))),

        vec_to_vec: (&[1, 2, 3].as_slice(), ProtobufRuntimeType::VecU8, ok(eq(&ReflectValueBox::Bytes(vec![1, 2, 3])))),
        vec_to_i32: (&[1, 2, 3].as_slice(), ProtobufRuntimeType::I32, err(matches_pattern!(NodeEvaluationError::ValueToProtoConversionError { .. }))),
    );

    parameterized_test!(
        #[test]
        push_node_value_content,

        |field_name: &str, node_value: &NodeValue, expected| {
            let message_descriptor = TestMessage::descriptor();
            let mut message = message_descriptor.new_instance();

            let field_descriptor = message_descriptor.field_by_name(field_name).unwrap();
            assert!(field_descriptor.is_repeated());
            field_descriptor
                .mut_repeated(message.as_mut())
                .push_node_value_content(node_value)
                .unwrap();

            assert_eq!(
                field_descriptor.get_repeated(message.as_ref()),
                expected
            );
        },

        bool_list_to_bool_list: ("repeated_bools", &NodeValue::BoolList(vec![true, false]), vec![ReflectValueBox::Bool(true), ReflectValueBox::Bool(false)]),

        int32_list_to_int32_list: ("repeated_i32s", &NodeValue::Int32List(vec![1, 2]), vec![ReflectValueBox::I32(1), ReflectValueBox::I32(2)]),
        int32_list_to_int64_list: ("repeated_i64s", &NodeValue::Int32List(vec![1, 2]), vec![ReflectValueBox::I64(1), ReflectValueBox::I64(2)]),
        int32_list_to_float_list: ("repeated_floats", &NodeValue::Int32List(vec![1, 2]), vec![ReflectValueBox::F32(1.0), ReflectValueBox::F32(2.0)]),
        int32_list_to_double_list: ("repeated_doubles", &NodeValue::Int32List(vec![1, 2]), vec![ReflectValueBox::F64(1.0), ReflectValueBox::F64(2.0)]),
        int32_list_to_enum_list: ("repeated_enums", &NodeValue::Int32List(vec![TestEnum::NONE.value(), TestEnum::SOME.value()]), vec![ReflectValueBox::Enum(TestEnum::enum_descriptor(), TestEnum::NONE.value()), ReflectValueBox::Enum(TestEnum::enum_descriptor(), TestEnum::SOME.value())]),

        int64_list_to_int32_list: ("repeated_i32s", &NodeValue::Int64List(vec![1, 2]), vec![ReflectValueBox::I32(1), ReflectValueBox::I32(2)]),
        int64_list_to_int64_list: ("repeated_i64s", &NodeValue::Int64List(vec![1, 2]), vec![ReflectValueBox::I64(1), ReflectValueBox::I64(2)]),
        int64_list_to_float_list: ("repeated_floats", &NodeValue::Int64List(vec![1, 2]), vec![ReflectValueBox::F32(1.0), ReflectValueBox::F32(2.0)]),
        int64_list_to_double_list: ("repeated_doubles", &NodeValue::Int64List(vec![1, 2]), vec![ReflectValueBox::F64(1.0), ReflectValueBox::F64(2.0)]),
        int64_list_to_enum_list: ("repeated_enums", &NodeValue::Int64List(vec![TestEnum::NONE.value().into(), TestEnum::SOME.value().into()]), vec![ReflectValueBox::Enum(TestEnum::enum_descriptor(), TestEnum::NONE.value()), ReflectValueBox::Enum(TestEnum::enum_descriptor(), TestEnum::SOME.value())]),

        float_list_to_float_list: ("repeated_floats", &NodeValue::FloatList(vec![1.0, 2.0]), vec![ReflectValueBox::F32(1.0), ReflectValueBox::F32(2.0)]),
        float_list_to_double_list: ("repeated_doubles", &NodeValue::FloatList(vec![1.0, 2.0]), vec![ReflectValueBox::F64(1.0), ReflectValueBox::F64(2.0)]),

        double_list_to_float_list: ("repeated_floats", &NodeValue::DoubleList(vec![1.0, 2.0]), vec![ReflectValueBox::F32(1.0), ReflectValueBox::F32(2.0)]),
        double_list_to_double_list: ("repeated_doubles", &NodeValue::DoubleList(vec![1.0, 2.0]), vec![ReflectValueBox::F64(1.0), ReflectValueBox::F64(2.0)]),

        message_list_to_message_list: ("repeated_empty", &NodeValue::MessageList(vec![Arc::new(Empty::default())]), vec![ReflectValueBox::Message(Empty::descriptor().new_instance())]),
        message_list_to_any_list: ("repeated_any", &NodeValue::MessageList(vec![Arc::new(Empty::default())]), vec![ReflectValueBox::Message(Box::new(Any::pack(Empty::default_instance()).unwrap()))]),
    );
}
