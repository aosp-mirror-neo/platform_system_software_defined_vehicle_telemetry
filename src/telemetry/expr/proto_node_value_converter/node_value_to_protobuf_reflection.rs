// Copyright 2023 Google LLC

//! Helper functions for converting NodeValue into protobuf reflection types.

use protobuf::reflect::{ReflectRepeatedMut, ReflectValueBox, RuntimeType as ProtobufRuntimeType};
use protobuf::well_known_types::any::Any;
use protobuf::{MessageDyn, MessageFull};
use std::sync::Arc;

use crate::expr::{node_error::NodeEvaluationError, node_value::NodeValue};

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

/// A NodeValue can be converted into an optional/singular protobuf reflection value.
/// For NodeValues holding a list, use `push_node_value_content()` in
///  `ReflectRepeatedMutExt` trait.
impl ToReflectValueBox for NodeValue {
    fn to_reflect_value_box(
        &self,
        protobuf_type: &ProtobufRuntimeType,
    ) -> Result<ReflectValueBox, NodeEvaluationError> {
        let err_provider = || NodeEvaluationError::ValueToProtoConversionError {
            value: format!("{:?}", self),
            refl_type: format!("{:?}", protobuf_type),
        };
        match protobuf_type {
            ProtobufRuntimeType::I32
            | ProtobufRuntimeType::I64
            | ProtobufRuntimeType::U32
            | ProtobufRuntimeType::U64
            | ProtobufRuntimeType::Enum(_) => {
                self.get_int().ok_or_else(err_provider)?.to_reflect_value_box(protobuf_type)
            }
            ProtobufRuntimeType::F32 | ProtobufRuntimeType::F64 => {
                self.get_double().ok_or_else(err_provider)?.to_reflect_value_box(protobuf_type)
            }
            ProtobufRuntimeType::Bool => {
                self.get_bool().ok_or_else(err_provider)?.to_reflect_value_box(protobuf_type)
            }
            ProtobufRuntimeType::Message(_) => {
                self.get_message().ok_or_else(err_provider)?.to_reflect_value_box(protobuf_type)
            }
            ProtobufRuntimeType::String => {
                self.get_string().ok_or_else(err_provider)?.to_reflect_value_box(protobuf_type)
            }
            ProtobufRuntimeType::VecU8 => {
                self.get_bytes().ok_or_else(err_provider)?.to_reflect_value_box(protobuf_type)
            }
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
            ProtobufRuntimeType::I32 => Ok(ReflectValueBox::I32(*self)),
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
            ProtobufRuntimeType::Enum(enum_descriptor) => i32::try_from(*self)
                .map_or_else(err_provider, |v| {
                    Ok(ReflectValueBox::Enum(enum_descriptor.clone(), v))
                }),
            _ => Err(NodeEvaluationError::ValueToProtoConversionError {
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
    use protobuf::well_known_types::{any::Any, wrappers::Int32Value};
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

        string_to_string: (&"kukkula", ProtobufRuntimeType::String, ok(eq(&ReflectValueBox::String("kukkula".to_owned())))),
        string_to_i32: (&"kukkula", ProtobufRuntimeType::I32, err(matches_pattern!(NodeEvaluationError::ValueToProtoConversionError { .. }))),

        vec_to_vec: (&[1, 2, 3].as_slice(), ProtobufRuntimeType::VecU8, ok(eq(&ReflectValueBox::Bytes(vec![1, 2, 3])))),
        vec_to_i32: (&[1, 2, 3].as_slice(), ProtobufRuntimeType::I32, err(matches_pattern!(NodeEvaluationError::ValueToProtoConversionError { .. }))),

        msg_to_msg: (&create_int32_value_arc(), ProtobufRuntimeType::Message(Int32Value::descriptor()), ok(eq(&ReflectValueBox::Message(Box::new(create_int32_value()))))),
        msg_to_any: (&create_int32_value_arc(), ProtobufRuntimeType::Message(Any::descriptor()), ok(eq(&ReflectValueBox::Message(Box::new(Any::pack_dyn(&create_int32_value()).unwrap()))))),
        msg_to_i32: (&create_int32_value_arc(), ProtobufRuntimeType::I32, err(matches_pattern!(NodeEvaluationError::ValueToProtoConversionError { .. }))),
    );
}
