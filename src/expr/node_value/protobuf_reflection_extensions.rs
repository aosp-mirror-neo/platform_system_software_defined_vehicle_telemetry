// Copyright 2023 Google LLC. All Rights Reserved.

//! Helper functions used for converting between protobuf reflection value and
//! node value.

use protobuf::reflect::{
    ReflectFieldRef, ReflectRepeatedMut, ReflectRepeatedRef, ReflectValueBox, ReflectValueRef,
    RuntimeType as ProtobufRuntimeType,
};

use crate::expr::{node_errors::NodeError, node_value::NodeValue};

/// Implementing type will be able to convert itself into a NodeValue.
pub trait ToNodeValue {
    fn to_node_value(&self) -> Result<NodeValue, NodeError>;
}

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

/// An optional protobuf field can be converted into a NodeValue.
impl ToNodeValue for ReflectFieldRef<'_> {
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

/// An optional protobuf field value can be converted into a NodeValue.
impl ToNodeValue for ReflectValueRef<'_> {
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

impl ReflectRepeatedRefExt for ReflectRepeatedRef<'_> {
    fn to_vec<T>(
        &self,
        transform_func: impl FnMut(ReflectValueRef) -> Result<T, NodeError>,
    ) -> Result<Vec<T>, NodeError> {
        // apply `transform_func` to each element in the repeated_field
        self.into_iter().map(transform_func).collect()
    }
}

impl ReflectRepeatedMutExt for ReflectRepeatedMut<'_> {
    fn push_node_value_content(&mut self, node_value: &NodeValue) -> Result<(), NodeError> {
        match node_value {
            NodeValue::BoolList(v) => self.push_vec_content(v),
            NodeValue::Int32List(v) => self.push_vec_content(v),
            NodeValue::Int64List(v) => self.push_vec_content(v),
            NodeValue::FloatList(v) => self.push_vec_content(v),
            NodeValue::DoubleList(v) => self.push_vec_content(v),
            _ => Err(NodeError::TypeMismatch {
                msg: format!(
                    "NodeValue {:?} is not a list type, so it's not compatible with {:?}",
                    node_value, self
                ),
            }),
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
        let err_provider = || NodeError::TypeMismatch {
            msg: format!(
                "{:?} cannot be converted into ReflectValueBox of type {:?}",
                self, protobuf_type
            ),
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
            ProtobufRuntimeType::Message(_) => {
                Ok(ReflectValueBox::Message(self.get_message(err_provider)?.clone_box()))
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
        Err(NodeError::TypeMismatch {
            msg: format!(
                "Cannot convert {} into a ReflectValueBox of type {:?}",
                self, protobuf_type
            ),
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
            _ => (*self as i64).to_reflect_value_box(protobuf_type),
        }
    }
}

/// An i64 can be converted into a protobuf reflection value.
impl ToReflectValueBox for i64 {
    fn to_reflect_value_box(
        &self,
        protobuf_type: &ProtobufRuntimeType,
    ) -> Result<ReflectValueBox, NodeError> {
        let to_node_error = |e| {
            Err(NodeError::TypeMismatch {
                msg: format!(
                    "Cannot convert {} into a ReflectValueBox of type {:?} because of {:?}",
                    self, protobuf_type, e
                ),
            })
        };
        match protobuf_type {
            ProtobufRuntimeType::I32 => {
                i32::try_from(*self).map_or_else(to_node_error, |v| Ok(ReflectValueBox::I32(v)))
            }
            ProtobufRuntimeType::I64 => Ok(ReflectValueBox::I64(*self)),
            ProtobufRuntimeType::U32 => {
                u32::try_from(*self).map_or_else(to_node_error, |v| Ok(ReflectValueBox::U32(v)))
            }
            ProtobufRuntimeType::U64 => {
                u64::try_from(*self).map_or_else(to_node_error, |v| Ok(ReflectValueBox::U64(v)))
            }
            ProtobufRuntimeType::Enum(enum_descriptor) => i32::try_from(*self)
                .map_or_else(to_node_error, |v| {
                    Ok(ReflectValueBox::Enum(enum_descriptor.clone(), v))
                }),
            _ => Err(NodeError::TypeMismatch {
                msg: format!(
                    "Cannot convert {} into a ReflectValueBox of type {:?}",
                    self, protobuf_type,
                ),
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
            _ => (*self as f64).to_reflect_value_box(protobuf_type),
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
            _ => Err(NodeError::TypeMismatch {
                msg: format!(
                    "Cannot convert {} into a ReflectValueBox of type {:?}",
                    self, protobuf_type
                ),
            }),
        }
    }
}
