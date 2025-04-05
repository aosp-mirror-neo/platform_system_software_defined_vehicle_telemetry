// Copyright 2025 Google LLC

use protobuf::reflect::{ReflectRepeatedRef, ReflectValueRef, RuntimeType as ProtobufRuntimeType};
use sdv_telemetry_expr::dynamic_message::DynamicMessage;
use sdv_telemetry_expr::node_error::NodeEvaluationError;
use sdv_telemetry_expr::node_value::NodeValue;
use std::sync::Arc;

use crate::metrics_config_orchestrator::protobuf_dynamic_message::ProtobufDynamicMessage;

/// Implementing type will be able to convert itself into a NodeValue.
pub(super) trait ToNodeValue {
    fn to_node_value(&self) -> Result<NodeValue, NodeEvaluationError>;
}

/// Extensions for ReflectRepeatedRef.
trait ReflectRepeatedRefExt {
    /// Converts a repeated field reflection into a vector.
    /// For example, a `repeated int32` field will become `Vec<i32>`, or error if
    /// the `transform_func` returned an error.
    fn to_vec<T>(
        &self,
        transform_func: impl FnMut(ReflectValueRef) -> Result<T, NodeEvaluationError>,
    ) -> Result<Vec<T>, NodeEvaluationError>;
}

/// An optional protobuf field value can be converted into a NodeValue.
impl ToNodeValue for ReflectValueRef<'_> {
    /// Converts a protobuf reflection on a singular field into a NodeValue.
    fn to_node_value(&self) -> Result<NodeValue, NodeEvaluationError> {
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
            ReflectValueRef::Message(v) => {
                NodeValue::Message(Arc::new(ProtobufDynamicMessage::new((*v).clone_box().into())))
            }
            ReflectValueRef::Bytes(v) => NodeValue::ByteList((*v).to_vec()),
        })
    }
}

/// A repeated protobuf field can be converted into a NodeValue.
impl ToNodeValue for ReflectRepeatedRef<'_> {
    fn to_node_value(&self) -> Result<NodeValue, NodeEvaluationError> {
        // depending on the field type, create the correct list type
        // `repeated int32` = NodeValue::Int32List etc.
        Ok(match self.element_type() {
            ProtobufRuntimeType::I32 => {
                NodeValue::Int32List(self.to_vec(
                    // closure that unwraps an i32 value from a value reflection
                    |val: ReflectValueRef<'_>| {
                        val.to_i32().ok_or(NodeEvaluationError::TypeMismatch {
                            msg: format!("Repeated field {:?} type expected to be i32", self),
                        })
                    },
                )?)
            }
            ProtobufRuntimeType::I64 => {
                NodeValue::Int64List(self.to_vec(
                    // unwraps an i64 value from a value reflection or error
                    |val: ReflectValueRef<'_>| {
                        val.to_i64().ok_or(NodeEvaluationError::TypeMismatch {
                            msg: format!("Repeated field {:?} type expected to be i64", self),
                        })
                    },
                )?)
            }
            ProtobufRuntimeType::U32 => {
                NodeValue::Int64List(self.to_vec(|val: ReflectValueRef<'_>| {
                    // map Option<u32> into Option<i64> and convert it to a Result
                    val.to_u32().map(i64::from).ok_or(NodeEvaluationError::TypeMismatch {
                        msg: format!("Repeated field {:?} type expected to be u32", self),
                    })
                })?)
            }
            ProtobufRuntimeType::U64 => {
                NodeValue::Int64List(self.to_vec(|val: ReflectValueRef<'_>| {
                    // map Option<u64> into Option<i64> and convert it to a Result
                    val.to_u64().map(|x| x as i64).ok_or(NodeEvaluationError::TypeMismatch {
                        msg: format!("Repeated field {:?} type expected to be u64", self),
                    })
                })?)
            }
            ProtobufRuntimeType::F32 => {
                NodeValue::FloatList(self.to_vec(
                    // closure that unwraps f32 value from a single value reflection
                    |val: ReflectValueRef<'_>| {
                        val.to_f32().ok_or(NodeEvaluationError::TypeMismatch {
                            msg: format!("Repeated field {:?} type expected to be f32", self),
                        })
                    },
                )?)
            }
            ProtobufRuntimeType::F64 => {
                NodeValue::DoubleList(self.to_vec(
                    // unwraps a f64 value from a value reflection or error
                    |val: ReflectValueRef<'_>| {
                        val.to_f64().ok_or(NodeEvaluationError::TypeMismatch {
                            msg: format!("Repeated field {:?} type expected to be f64", self),
                        })
                    },
                )?)
            }
            ProtobufRuntimeType::Bool => {
                NodeValue::BoolList(self.to_vec(
                    // unwraps a bool value from a value reflection or error
                    |val: ReflectValueRef<'_>| {
                        val.to_bool().ok_or(NodeEvaluationError::TypeMismatch {
                            msg: format!("Repeated field {:?} type expected to be bool", self),
                        })
                    },
                )?)
            }
            ProtobufRuntimeType::Enum(_) => NodeValue::Int32List(self.to_vec(
                // unwraps an enum as i32 from a value reflection or error
                |val: ReflectValueRef<'_>| {
                    val.to_enum_value().ok_or(NodeEvaluationError::TypeMismatch {
                        msg: format!("Repeated field {:?} type expected to be enum", self),
                    })
                },
            )?),
            ProtobufRuntimeType::Message(_) => NodeValue::MessageList(self.to_vec(
                // unwraps a protobuf message from a value reflection or error
                |val: ReflectValueRef<'_>| {
                    Ok(Arc::new(ProtobufDynamicMessage::new(
                        val.to_message()
                            .ok_or(NodeEvaluationError::TypeMismatch {
                                msg: format!(
                                    "Repeated field {:?} type expected to be protobuf message",
                                    self
                                ),
                            })?
                            .clone_box()
                            .into(),
                    )) as Arc<dyn DynamicMessage>)
                },
            )?),
            _ => {
                return Err(NodeEvaluationError::UnsupportedRepeatedFieldType);
            }
        })
    }
}

impl ReflectRepeatedRefExt for ReflectRepeatedRef<'_> {
    fn to_vec<T>(
        &self,
        transform_func: impl FnMut(ReflectValueRef) -> Result<T, NodeEvaluationError>,
    ) -> Result<Vec<T>, NodeEvaluationError> {
        // apply `transform_func` to each element in the repeated_field
        self.into_iter().map(transform_func).collect()
    }
}
