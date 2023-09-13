// Copyright 2023 Google LLC. All Rights Reserved.

//! Helper functions used for converting between protobuf reflection value and
//! node value.

use crate::{NodeError, NodeValue};

use protobuf::reflect::{
    ReflectFieldRef, ReflectRepeatedRef, ReflectValueBox, ReflectValueRef,
    RuntimeType as ProtobufRuntimeType,
};

/// Protobuf reflection to NodeValue.
pub trait ProtobufReflectionExt {
    fn to_node_value(&self) -> Result<NodeValue, NodeError>;
}

/// NodeValue to protobuf reflection.
pub trait NodeValueToReflExt {
    /// Convert the NodeValue into a ReflectValueBox using the given type.
    /// For example, if the value_type is Bool, it will attempt to get a boolean
    /// from NodeValue (self) and put that boolean into a ReflectValueBox.
    /// Returns an error if the `value_type` does not match with the NodeValue type.
    fn to_reflect_value_box(
        &self,
        value_type: &ProtobufRuntimeType,
    ) -> Result<ReflectValueBox, NodeError>;
}

trait RepeatedFieldReflectionExt {
    /// Converts a repeated field reflection into a vector.
    /// For example, a `repeated int32` field will become `Vec<i32>`, or error if
    /// the `transform_func` returned an error.
    fn to_vec<T>(
        &self,
        transform_func: impl Fn(ReflectValueRef) -> Result<T, NodeError>,
    ) -> Result<Vec<T>, NodeError>;
}

impl ProtobufReflectionExt for ReflectFieldRef<'_> {
    fn to_node_value(&self) -> Result<NodeValue, NodeError> {
        match &self {
            // `optional` or singular protobuf field
            ReflectFieldRef::Optional(singular_field) => match singular_field.value() {
                Some(field_value) => field_value.to_node_value(),
                None => Ok(NodeValue::None),
            },
            // `repeated` protobuf field
            ReflectFieldRef::Repeated(repeated_field) => repeated_field.to_node_value(),
            // map field
            _ => Err(NodeError::UnsupportedSingularFieldType),
        }
    }
}

impl ProtobufReflectionExt for ReflectValueRef<'_> {
    /// Converts a protobuf reflection on a singular field into a NodeValue.
    fn to_node_value(&self) -> Result<NodeValue, NodeError> {
        Ok(match &self {
            ReflectValueRef::U32(v) => NodeValue::Int64(*v as i64),
            ReflectValueRef::U64(v) => NodeValue::Int64(*v as i64),
            ReflectValueRef::I32(v) => NodeValue::Int32(*v),
            ReflectValueRef::I64(v) => NodeValue::Int64(*v),
            ReflectValueRef::F32(v) => NodeValue::Float(*v),
            ReflectValueRef::F64(v) => NodeValue::Double(*v),
            ReflectValueRef::Bool(v) => NodeValue::Bool(*v),
            ReflectValueRef::Enum(_, v) => NodeValue::Int32(*v),
            ReflectValueRef::Message(v) => NodeValue::Message(v.clone_box()),
            _ => return Err(NodeError::UnsupportedSingularFieldType),
        })
    }
}

impl ProtobufReflectionExt for ReflectRepeatedRef<'_> {
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
                    val.to_u32().map(|x| x as i64).ok_or(NodeError::TypeMismatch {
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
            _ => {
                return Err(NodeError::UnsupportedRepeatedFieldType);
            }
        })
    }
}

impl RepeatedFieldReflectionExt for ReflectRepeatedRef<'_> {
    fn to_vec<T>(
        &self,
        transform_func: impl Fn(ReflectValueRef) -> Result<T, NodeError>,
    ) -> Result<Vec<T>, NodeError> {
        // apply `transform_func` to each element in the repeated_field
        self.into_iter().map(transform_func).collect()
    }
}

impl NodeValueToReflExt for NodeValue {
    fn to_reflect_value_box(
        &self,
        value_type: &ProtobufRuntimeType,
    ) -> Result<ReflectValueBox, NodeError> {
        let err_provider = || NodeError::TypeMismatch {
            msg: format!(
                "{:?} cannot be converted into ReflectValueBox of type {:?}",
                self, value_type
            ),
        };
        Ok(match value_type {
            ProtobufRuntimeType::I32 => ReflectValueBox::I32(self.get_int(&err_provider)? as i32),
            ProtobufRuntimeType::I64 => ReflectValueBox::I64(self.get_int(&err_provider)?),
            ProtobufRuntimeType::U32 => ReflectValueBox::U32(self.get_int(&err_provider)? as u32),
            ProtobufRuntimeType::U64 => ReflectValueBox::U64(self.get_int(&err_provider)? as u64),
            ProtobufRuntimeType::F32 => {
                ReflectValueBox::F32(self.get_double(&err_provider)? as f32)
            }
            ProtobufRuntimeType::F64 => ReflectValueBox::F64(self.get_double(&err_provider)?),
            ProtobufRuntimeType::Bool => ReflectValueBox::Bool(self.get_bool(&err_provider)?),
            ProtobufRuntimeType::Enum(enum_descriptor) => {
                ReflectValueBox::Enum(enum_descriptor.clone(), self.get_int(&err_provider)? as i32)
            }
            ProtobufRuntimeType::Message(_) => {
                ReflectValueBox::Message(self.get_message(err_provider)?.clone_box())
            }
            _ => return Err(NodeError::UnsupportedSingularFieldType),
        })
    }
}
