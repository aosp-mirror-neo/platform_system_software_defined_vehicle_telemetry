// Copyright 2023 Google LLC. All Rights Reserved.

//! Helper functions for converting NodeValue into protobuf reflection types.

use protobuf::reflect::{ReflectRepeatedMut, ReflectValueBox, RuntimeType as ProtobufRuntimeType};
use protobuf::well_known_types::any::Any;
use protobuf::{MessageDyn, MessageFull};

use crate::expr::{node_errors::NodeError, node_value::NodeValue};

/// Implementing type will be able to convert self into a ReflectValueBox.
pub trait ToReflectValueBox {
    /// This method will attempt to convert self into a ReflectValueBox of the
    /// specified protobuf type, but if self is not compatible to the specified type,
    /// an error will be returned.
    fn to_reflect_value_box(
        &self,
        protobuf_type: &ProtobufRuntimeType,
    ) -> Result<ReflectValueBox, NodeError>;
}

/// Extensions for ReflectRepeatedMut.
pub trait ReflectRepeatedMutExt {
    /// If NodeValue holds a list, it will push all the contents of the list into self.
    fn push_node_value_content(&mut self, node_value: &NodeValue) -> Result<(), NodeError>;

    /// Convert every vector element into a ReflectValueBox and pushes it to self.
    fn push_vec_content<T>(&mut self, list: &[T]) -> Result<(), NodeError>
    where
        T: ToReflectValueBox;
}

impl ReflectRepeatedMutExt for ReflectRepeatedMut<'_> {
    fn push_node_value_content(&mut self, node_value: &NodeValue) -> Result<(), NodeError> {
        match node_value {
            NodeValue::BoolList(v) => self.push_vec_content(v),
            NodeValue::Int32List(v) => self.push_vec_content(v),
            NodeValue::Int64List(v) => self.push_vec_content(v),
            NodeValue::FloatList(v) => self.push_vec_content(v),
            NodeValue::DoubleList(v) => self.push_vec_content(v),
            NodeValue::MessageList(v) => self.push_vec_content(v),
            _ => {
                Err(NodeError::RepeatedFieldWrongAssignment { value: format!("{:?}", node_value) })
            }
        }
    }

    fn push_vec_content<T>(&mut self, list: &[T]) -> Result<(), NodeError>
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
    ) -> Result<ReflectValueBox, NodeError> {
        let err_provider = || NodeError::ValueToProtoConversionError {
            value: format!("{:?}", self),
            refl_type: format!("{:?}", protobuf_type),
        };
        match protobuf_type {
            ProtobufRuntimeType::I32
            | ProtobufRuntimeType::I64
            | ProtobufRuntimeType::U32
            | ProtobufRuntimeType::U64
            | ProtobufRuntimeType::Enum(_) => {
                self.get_int(&err_provider)?.to_reflect_value_box(protobuf_type)
            }
            ProtobufRuntimeType::F32 | ProtobufRuntimeType::F64 => {
                self.get_double(&err_provider)?.to_reflect_value_box(protobuf_type)
            }
            ProtobufRuntimeType::Bool => {
                self.get_bool(&err_provider)?.to_reflect_value_box(protobuf_type)
            }
            ProtobufRuntimeType::Message(descriptor) => {
                let msg = self.get_message(err_provider)?;
                let msg_descriptor = msg.descriptor_dyn();
                if &msg_descriptor != descriptor {
                    if descriptor == &Any::descriptor() {
                        Ok(ReflectValueBox::Message(Box::new(Any::pack_dyn(msg).unwrap())))
                    } else {
                        Err(err_provider())
                    }
                } else {
                    Ok(ReflectValueBox::Message(msg.clone_box()))
                }
            }
            _ => Err(err_provider()),
        }
    }
}

/// A boolean can be converted into a protobuf reflection value.
impl ToReflectValueBox for bool {
    fn to_reflect_value_box(
        &self,
        protobuf_type: &ProtobufRuntimeType,
    ) -> Result<ReflectValueBox, NodeError> {
        if matches!(protobuf_type, ProtobufRuntimeType::Bool) {
            return Ok(ReflectValueBox::Bool(*self));
        }
        Err(NodeError::ValueToProtoConversionError {
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
    ) -> Result<ReflectValueBox, NodeError> {
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
    ) -> Result<ReflectValueBox, NodeError> {
        let err_provider = |_| {
            Err(NodeError::ValueToProtoConversionError {
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
            _ => Err(NodeError::ValueToProtoConversionError {
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
    ) -> Result<ReflectValueBox, NodeError> {
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
    ) -> Result<ReflectValueBox, NodeError> {
        match protobuf_type {
            ProtobufRuntimeType::F32 => Ok(ReflectValueBox::F32(*self as f32)),
            ProtobufRuntimeType::F64 => Ok(ReflectValueBox::F64(*self)),
            _ => Err(NodeError::ValueToProtoConversionError {
                value: format!("{:?}", self),
                refl_type: format!("{:?}", protobuf_type),
            }),
        }
    }
}

// A Box<dyn MessageDyn> can be converted into a protobuf reflection value.
impl ToReflectValueBox for Box<dyn MessageDyn> {
    fn to_reflect_value_box(
        &self,
        protobuf_type: &ProtobufRuntimeType,
    ) -> Result<ReflectValueBox, NodeError> {
        match protobuf_type {
            ProtobufRuntimeType::Message(_) => Ok(ReflectValueBox::Message(self.clone())),
            _ => Err(NodeError::ValueToProtoConversionError {
                value: format!("{:?}", self),
                refl_type: format!("{:?}", protobuf_type),
            }),
        }
    }
}
