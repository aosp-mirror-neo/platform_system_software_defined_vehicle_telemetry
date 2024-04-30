// Copyright 2023 Google LLC. All Rights Reserved.

//! Helper functions for converting protobuf reflection types into NodeValue.

use protobuf::{
    reflect::{
        FieldDescriptor, MessageDescriptor, ReflectFieldRef, ReflectRepeatedRef, ReflectValueRef,
        RuntimeType as ProtobufRuntimeType,
    },
    MessageDyn,
};

use crate::expr::{node_errors::NodeError, node_value::NodeValue};

pub trait MessageDescriptorExt {
    fn get_field_value(
        &self,
        field_descriptor: &FieldDescriptor,
        msg: &dyn MessageDyn,
    ) -> Result<NodeValue, NodeError>;
}

/// Implementing type will be able to convert itself into a NodeValue.
trait ToNodeValue {
    fn to_node_value(&self) -> Result<NodeValue, NodeError>;
}

/// Extensions for ReflectRepeatedRef.
trait ReflectRepeatedRefExt {
    /// Converts a repeated field reflection into a vector.
    /// For example, a `repeated int32` field will become `Vec<i32>`, or error if
    /// the `transform_func` returned an error.
    fn to_vec<T>(
        &self,
        transform_func: impl FnMut(ReflectValueRef) -> Result<T, NodeError>,
    ) -> Result<Vec<T>, NodeError>;
}

impl MessageDescriptorExt for MessageDescriptor {
    fn get_field_value(
        &self,
        field_descriptor: &FieldDescriptor,
        msg: &dyn MessageDyn,
    ) -> Result<NodeValue, NodeError> {
        match field_descriptor.get_reflect(msg) {
            // `optional` or singular protobuf field
            ReflectFieldRef::Optional(singular_field) => match singular_field.value() {
                Some(field_value) => field_value.to_node_value(),
                // "None" has different meanings based on whether a field is explicitly
                // marked as optional
                None => {
                    let field_full_name = field_descriptor.full_name();
                    // proto3 optional fields will have a "synthetic oneof descriptor"
                    let proto3_optional = self.all_oneofs().any(|oneof| {
                        oneof.fields().any(|field| field.full_name() == field_full_name)
                    });
                    // proto3 optional fields track field presence, so "None" means field not set
                    if proto3_optional {
                        Ok(NodeValue::None)
                    } else {
                        // otherwise rust protobuf encodes default value as "None", so the field
                        // value should actually be the default (either some sort of zero or None)
                        get_default_node_value_from_type(&field_descriptor.singular_runtime_type())
                    }
                }
            },
            // `repeated` protobuf field
            ReflectFieldRef::Repeated(repeated_field) => repeated_field.to_node_value(),
            // map field
            _ => Err(NodeError::UnsupportedSingularFieldType),
        }
    }
}

/// An optional protobuf field value can be converted into a NodeValue.
impl ToNodeValue for ReflectValueRef<'_> {
    /// Converts a protobuf reflection on a singular field into a NodeValue.
    fn to_node_value(&self) -> Result<NodeValue, NodeError> {
        Ok(match &self {
            ReflectValueRef::U32(v) => NodeValue::Int64(i64::from(*v)),
            ReflectValueRef::U64(v) => NodeValue::Int64(*v as i64),
            ReflectValueRef::I32(v) => NodeValue::Int32(*v),
            ReflectValueRef::I64(v) => NodeValue::Int64(*v),
            ReflectValueRef::F32(v) => NodeValue::Float(*v),
            ReflectValueRef::F64(v) => NodeValue::Double(*v),
            ReflectValueRef::Bool(v) => NodeValue::Bool(*v),
            ReflectValueRef::String(v) => NodeValue::String((*v).to_owned()),
            ReflectValueRef::Enum(_, v) => NodeValue::Int32(*v),
            ReflectValueRef::Message(v) => NodeValue::Message(v.clone_box()),
            _ => return Err(NodeError::UnsupportedSingularFieldType),
        })
    }
}

/// A repeated protobuf field can be converted into a NodeValue.
impl ToNodeValue for ReflectRepeatedRef<'_> {
    fn to_node_value(&self) -> Result<NodeValue, NodeError> {
        // depending on the field type, create the correct list type
        // `repeated int32` = NodeValue::Int32List etc.
        Ok(match self.element_type() {
            ProtobufRuntimeType::I32 => {
                NodeValue::Int32List(self.to_vec(
                    // closure that unwraps an i32 value from a value reflection
                    |val: ReflectValueRef<'_>| {
                        val.to_i32().ok_or(NodeError::TypeMismatch {
                            msg: format!("Repeated field {:?} type expected to be i32", self),
                        })
                    },
                )?)
            }
            ProtobufRuntimeType::I64 => {
                NodeValue::Int64List(self.to_vec(
                    // unwraps an i64 value from a value reflection or error
                    |val: ReflectValueRef<'_>| {
                        val.to_i64().ok_or(NodeError::TypeMismatch {
                            msg: format!("Repeated field {:?} type expected to be i64", self),
                        })
                    },
                )?)
            }
            ProtobufRuntimeType::U32 => {
                NodeValue::Int64List(self.to_vec(|val: ReflectValueRef<'_>| {
                    // map Option<u32> into Option<i64> and convert it to a Result
                    val.to_u32().map(i64::from).ok_or(NodeError::TypeMismatch {
                        msg: format!("Repeated field {:?} type expected to be u32", self),
                    })
                })?)
            }
            ProtobufRuntimeType::U64 => {
                NodeValue::Int64List(self.to_vec(|val: ReflectValueRef<'_>| {
                    // map Option<u64> into Option<i64> and convert it to a Result
                    val.to_u64().map(|x| x as i64).ok_or(NodeError::TypeMismatch {
                        msg: format!("Repeated field {:?} type expected to be u64", self),
                    })
                })?)
            }
            ProtobufRuntimeType::F32 => {
                NodeValue::FloatList(self.to_vec(
                    // closure that unwraps f32 value from a single value reflection
                    |val: ReflectValueRef<'_>| {
                        val.to_f32().ok_or(NodeError::TypeMismatch {
                            msg: format!("Repeated field {:?} type expected to be f32", self),
                        })
                    },
                )?)
            }
            ProtobufRuntimeType::F64 => {
                NodeValue::DoubleList(self.to_vec(
                    // unwraps a f64 value from a value reflection or error
                    |val: ReflectValueRef<'_>| {
                        val.to_f64().ok_or(NodeError::TypeMismatch {
                            msg: format!("Repeated field {:?} type expected to be f64", self),
                        })
                    },
                )?)
            }
            ProtobufRuntimeType::Bool => {
                NodeValue::BoolList(self.to_vec(
                    // unwraps a bool value from a value reflection or error
                    |val: ReflectValueRef<'_>| {
                        val.to_bool().ok_or(NodeError::TypeMismatch {
                            msg: format!("Repeated field {:?} type expected to be bool", self),
                        })
                    },
                )?)
            }
            ProtobufRuntimeType::Enum(_) => NodeValue::Int32List(self.to_vec(
                // unwraps an enum as i32 from a value reflection or error
                |val: ReflectValueRef<'_>| {
                    val.to_enum_value().ok_or(NodeError::TypeMismatch {
                        msg: format!("Repeated field {:?} type expected to be enum", self),
                    })
                },
            )?),
            ProtobufRuntimeType::Message(_) => NodeValue::MessageList(self.to_vec(
                // unwraps a protobuf message from a value reflection or error
                |val: ReflectValueRef<'_>| {
                    Ok(val
                        .to_message()
                        .ok_or(NodeError::TypeMismatch {
                            msg: format!(
                                "Repeated field {:?} type expected to be protobuf message",
                                self
                            ),
                        })?
                        .clone_box())
                },
            )?),
            _ => {
                return Err(NodeError::UnsupportedRepeatedFieldType);
            }
        })
    }
}

impl ReflectRepeatedRefExt for ReflectRepeatedRef<'_> {
    fn to_vec<T>(
        &self,
        transform_func: impl FnMut(ReflectValueRef) -> Result<T, NodeError>,
    ) -> Result<Vec<T>, NodeError> {
        // apply `transform_func` to each element in the repeated_field
        self.into_iter().map(transform_func).collect()
    }
}

fn get_default_node_value_from_type(
    runtime_type: &ProtobufRuntimeType,
) -> Result<NodeValue, NodeError> {
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
        _ => {
            return Err(NodeError::UnsupportedSingularFieldType);
        }
    })
}
