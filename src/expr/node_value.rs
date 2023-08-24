// Copyright 2023 Google LLC. All Rights Reserved.

//! Wrapper around a generic value.
//! Should be able to hold all the values in a ConstantLeafNode protobuf
//! message, as well as all the values that a FieldLeafNode can select.

use protobuf::reflect::{
    ReflectFieldRef, ReflectRepeatedRef, ReflectValueRef, RuntimeType as ProtobufRuntimeType,
};
use sdvtelemetry_proto_rust::expression::{
    combination_node::{
        ArithmeticOperator, LogicalOperator, Operator as OperatorProto, RelationalOperator,
    },
    constant_leaf_node::Node_value as NodeValueProto,
    ConstantLeafNode as ConstantLeafNodeProto,
};

const EPSILON: f32 = 1e-6;

/// Holds a generic value.
/// TODO(b/288481877): Add list value because FieldLeafNode can extract values
/// from repeated fields.
#[derive(Clone, Debug)]
pub enum NodeValue {
    /// No value.
    None,

    /// A boolean node value.
    Bool(bool),

    /// An int32 node value.
    Int32(i32),

    /// An int64 node value.
    Int64(i64),

    /// A float node value.
    Float(f32),

    /// A double node value.
    Double(f64),

    /// A protobuf message value.
    Message(Box<dyn protobuf::MessageDyn>),

    /// Repeated boolean values.
    BoolList(Vec<bool>),

    /// Repeated int32 values.
    Int32List(Vec<i32>),

    /// Repeated int64 values.
    Int64List(Vec<i64>),

    /// Repeated float values.
    FloatList(Vec<f32>),

    /// Repeated double values.
    DoubleList(Vec<f64>),
}

/// Check if two floating point values are approximately equal.
fn approximately_equal(v1: f64, v2: f64) -> bool {
    (v1 - v2).abs() < EPSILON.into()
}

/// The PartialEq trait allows comparing to NodeValue's accurately, including
/// for floating point values.
impl PartialEq for NodeValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (NodeValue::None, NodeValue::None) => true,
            (NodeValue::Bool(v1), NodeValue::Bool(v2)) => *v1 == *v2,
            (NodeValue::Int32(v1), NodeValue::Int32(v2)) => *v1 == *v2,
            (NodeValue::Int64(v1), NodeValue::Int64(v2)) => *v1 == *v2,
            (NodeValue::Float(v1), NodeValue::Float(v2)) => {
                approximately_equal(*v1 as f64, *v2 as f64)
            }
            (NodeValue::Double(v1), NodeValue::Double(v2)) => approximately_equal(*v1, *v2),
            (NodeValue::BoolList(v1), NodeValue::BoolList(v2)) => *v1 == *v2,
            (NodeValue::Int32List(v1), NodeValue::Int32List(v2)) => *v1 == *v2,
            (NodeValue::Int64List(v1), NodeValue::Int64List(v2)) => *v1 == *v2,
            (NodeValue::FloatList(v1), NodeValue::FloatList(v2)) => {
                v1.len() == v2.len()
                    && v1.iter().zip(v2).all(|(a, b)| approximately_equal(*a as f64, *b as f64))
            }
            (NodeValue::DoubleList(v1), NodeValue::DoubleList(v2)) => {
                v1.len() == v2.len() && v1.iter().zip(v2).all(|(a, b)| approximately_equal(*a, *b))
            }
            _ => false,
        }
    }
}

impl NodeValue {
    /// Converts a ConstantLeafNode into a NodeValue.
    pub fn from_constant_leaf_node(
        constant_leaf_node: &ConstantLeafNodeProto,
    ) -> Result<NodeValue, String> {
        Ok(match &constant_leaf_node.node_value {
            Some(NodeValueProto::BoolValue(val)) => NodeValue::Bool(*val),
            Some(NodeValueProto::Int32Value(val)) => NodeValue::Int32(*val),
            Some(NodeValueProto::Int64Value(val)) => NodeValue::Int64(*val),
            Some(NodeValueProto::FloatValue(val)) => NodeValue::Float(*val),
            Some(NodeValueProto::DoubleValue(val)) => NodeValue::Double(*val),
            _ => return Err(format!("Node {:?} did not specify a node_value", constant_leaf_node)),
        })
    }

    /// Performs operations using the operator.
    /// This call will short-circuit and skip evaluating the right branch if the
    /// left operand uniquely determines the result.
    ///
    /// # Arguments:
    ///
    /// * `&self` - The left operand.
    /// * `op` - The operator.
    ///          If unary operator, then operates on self.
    ///          If binary operator, then [self] <operator> [right].
    /// * `get_right_operand` - The function that provides the right operand when called.
    pub fn operate(
        &self,
        op: &OperatorProto,
        get_right_operand: impl FnOnce() -> Result<NodeValue, String>,
    ) -> Result<NodeValue, String> {
        match op {
            OperatorProto::ArithmeticOperator(op) => self.handle_arithmetic(
                &op.enum_value().map_err(|i| {
                    format!("Arithmetic operator with enum value {} not recognized", i)
                })?,
                &get_right_operand()?,
            ),
            OperatorProto::LogicalOperator(op) => self.handle_logical(
                &op.enum_value().map_err(|i| {
                    format!("Logical operator with enum value {} not recognized", i)
                })?,
                get_right_operand,
            ),
            OperatorProto::RelationalOperator(op) => self.handle_relational(
                &op.enum_value().map_err(|i| {
                    format!("Relational operator with enum value {} not recognized", i)
                })?,
                &get_right_operand()?,
            ),
            _ => Err(format!("Operator {:?} not recognized.", op)),
        }
    }

    /// Gets a NodeValue from a protobuf message and its fields.
    pub fn extract_value(
        msg: Box<dyn protobuf::MessageDyn>,
        fields: &[String],
    ) -> Result<NodeValue, String> {
        // if no field names, return the message itself as the NodeValue
        if fields.is_empty() {
            return Ok(NodeValue::Message(msg));
        }
        // recursively gets the field value from field names
        Self::extract_field_value(msg.as_ref(), fields)
    }

    // private functions and methods below

    /// Reads a field value from a protobuf message using the reflection.
    fn extract_field_value(
        msg: &dyn protobuf::MessageDyn,
        fields: &[String],
    ) -> Result<NodeValue, String> {
        let msg_descriptor = msg.descriptor_dyn();
        let field_name = fields
            .get(0)
            .ok_or("Getting field value requires at least one field name".to_string())?;
        let field_descriptor = msg_descriptor.field_by_name(field_name).ok_or(format!(
            "No field descriptor exists for field {} in message {}. Typo in field name?",
            field_name,
            msg_descriptor.name()
        ))?;
        let field_value = field_descriptor.get_reflect(msg);

        // Base case for recursion: one field is provided, simply read the value
        if fields.len() == 1 {
            return match &field_value {
                // `optional` or singular protobuf field
                ReflectFieldRef::Optional(singular_field) => match singular_field.value() {
                    Some(field_value) => Self::from_singular_field_value(field_value),
                    None => Ok(NodeValue::None),
                },
                // `repeated` protobuf field
                ReflectFieldRef::Repeated(repeated_field) => {
                    // convert reflection value into a NodeValue and return
                    Self::from_repeated_field(repeated_field)
                }
                _ => Err("Map field is not supported".to_string()),
            };
        }

        // If fields length > 1, get nested field values iff the value is a single protobuf message,
        // otherwise return error
        if let ReflectFieldRef::Optional(singular_field) = field_value {
            match singular_field.value() {
                Some(nested_value) => {
                    if let ReflectValueRef::Message(msg) = nested_value {
                        // recurse to read from nested messages
                        Self::extract_field_value(&(*msg), &fields[1..])
                    } else {
                        Err(format!(
                            "Get nested field value failed: a non-terminal field {} \
                            is not a protobuf message",
                            field_name
                        ))
                    }
                }
                None => Ok(NodeValue::None),
            }
        } else {
            Err(format!(
                "Get nested field value failed: a non-terminal field {} \
                        is either repeated or a map",
                field_name
            ))
        }
    }

    /// Handle logical operations.
    fn handle_logical(
        &self,
        op: &LogicalOperator,
        get_right_operand: impl FnOnce() -> Result<NodeValue, String>,
    ) -> Result<NodeValue, String> {
        let get_err_msg = || "Logical operator requires boolean operands".to_string();
        // right operand is checked after get_right_operand() is called
        if !self.is_bool() {
            return Err(get_err_msg());
        }
        Ok(NodeValue::Bool(match op {
            LogicalOperator::AND => {
                if !(self.get_bool()?) {
                    return Ok(self.clone());
                }
                self.get_bool()? && get_right_operand()?.get_bool_with_msg(get_err_msg())?
            }
            LogicalOperator::OR => {
                if self.get_bool()? {
                    return Ok(self.clone());
                }
                self.get_bool()? || get_right_operand()?.get_bool_with_msg(get_err_msg())?
            }
            LogicalOperator::XOR => {
                self.get_bool()? != get_right_operand()?.get_bool_with_msg(get_err_msg())?
            }
            LogicalOperator::NOT => !self.get_bool()?,
        }))
    }

    /// Handle arithmetic operations.
    fn handle_arithmetic(
        &self,
        op: &ArithmeticOperator,
        other: &NodeValue,
    ) -> Result<NodeValue, String> {
        // assert that both operands are numeric types
        if !self.is_numeric() || !other.is_numeric() {
            return Err("Arithmetic operators must have numeric operands".to_string());
        }
        match op {
            ArithmeticOperator::ADD => self.apply_numeric(
                other,
                |a, b| NodeValue::Int64(a + b),
                |a, b| NodeValue::Double(a + b),
            ),
            ArithmeticOperator::SUBTRACT => self.apply_numeric(
                other,
                |a, b| NodeValue::Int64(a - b),
                |a, b| NodeValue::Double(a - b),
            ),
            ArithmeticOperator::MULTIPLY => self.apply_numeric(
                other,
                |a, b| NodeValue::Int64(a * b),
                |a, b| NodeValue::Double(a * b),
            ),
            ArithmeticOperator::DIVIDE => {
                if other.is_zero()? {
                    return Err("Cannot divide by zero".to_string());
                }
                Ok(NodeValue::Double(self.get_double()? / other.get_double()?))
            }
            ArithmeticOperator::MODULUS => {
                if !self.is_int() || !other.is_int() {
                    return Err("Modulus operator requires integer operands".to_string());
                }
                if other.is_zero()? {
                    return Err("Cannot perform mod 0. Divide by 0 is undefined.".to_string());
                }
                Ok(NodeValue::Int64(self.get_int()? % other.get_int()?))
            }
            ArithmeticOperator::POWER => self.apply_numeric(
                other,
                |a, b| NodeValue::Int64(a.pow(b as u32)),
                |a, b| NodeValue::Double(a.powf(b)),
            ),
        }
    }

    /// Handle relational operations.
    fn handle_relational(
        &self,
        op: &RelationalOperator,
        other: &NodeValue,
    ) -> Result<NodeValue, String> {
        Ok(NodeValue::Bool(match op {
            RelationalOperator::EQ => {
                if !self.is_numeric() || !other.is_numeric() {
                    return Ok(NodeValue::Bool(self == other));
                }
                self.apply_numeric(other, |a, b| a == b, approximately_equal)
            }
            RelationalOperator::NOT_EQ => {
                if !self.is_numeric() || !other.is_numeric() {
                    return Ok(NodeValue::Bool(self != other));
                }
                self.apply_numeric(other, |a, b| a != b, |a, b| !approximately_equal(a, b))
            }
            RelationalOperator::GT => self.apply_numeric(other, |a, b| a > b, |a, b| a > b),
            RelationalOperator::GT_OR_EQ => {
                self.apply_numeric(other, |a, b| a >= b, |a, b| a >= b || approximately_equal(a, b))
            }
            RelationalOperator::LT => self.apply_numeric(other, |a, b| a < b, |a, b| a < b),
            RelationalOperator::LT_OR_EQ => {
                self.apply_numeric(other, |a, b| a <= b, |a, b| a <= b || approximately_equal(a, b))
            }
            RelationalOperator::CONTAINS => self.contains(other),
            RelationalOperator::DOES_NOT_CONTAIN => {
                // negate the boolean in a Result<bool, String>
                self.contains(other).map(|x| !x)
            }
            RelationalOperator::ALL_EQ => self.all_equals(other),
        }?))
    }

    /// Converts a protobuf reflection on a singular field into a NodeValue.
    fn from_singular_field_value(field_value: ReflectValueRef<'_>) -> Result<NodeValue, String> {
        Ok(match field_value {
            ReflectValueRef::U32(v) => NodeValue::Int64(v as i64),
            ReflectValueRef::U64(v) => NodeValue::Int64(v as i64),
            ReflectValueRef::I32(v) => NodeValue::Int32(v),
            ReflectValueRef::I64(v) => NodeValue::Int64(v),
            ReflectValueRef::F32(v) => NodeValue::Float(v),
            ReflectValueRef::F64(v) => NodeValue::Double(v),
            ReflectValueRef::Bool(v) => NodeValue::Bool(v),
            ReflectValueRef::Enum(_, v) => NodeValue::Int32(v),
            ReflectValueRef::Message(v) => NodeValue::Message(v.clone_box()),
            _ => {
                return Err("The following types are not supported in an optional field: \
                            bytes, string, or protobuf message"
                    .to_string())
            }
        })
    }

    /// Converts a protobuf reflection on a repeated field into a NodeValue.
    fn from_repeated_field(repeated_field: &ReflectRepeatedRef<'_>) -> Result<NodeValue, String> {
        // depending on the field type, create the correct list type
        // `repeated int32` = NodeValue::Int32List etc.
        Ok(match repeated_field.element_type() {
            ProtobufRuntimeType::I32 => {
                NodeValue::Int32List(Self::refl_to_vec(
                    repeated_field,
                    // closure that unwraps an i32 value from a value reflection
                    |val: ReflectValueRef<'_>| {
                        val.to_i32().ok_or(format!(
                            "Repeated field {:?} type expected to be i32",
                            repeated_field
                        ))
                    },
                )?)
            }
            ProtobufRuntimeType::I64 => {
                NodeValue::Int64List(Self::refl_to_vec(
                    repeated_field,
                    // unwraps an i64 value from a value reflection or error
                    |val: ReflectValueRef<'_>| {
                        val.to_i64().ok_or(format!(
                            "Repeated field {:?} type expected to be i64",
                            repeated_field
                        ))
                    },
                )?)
            }
            ProtobufRuntimeType::U32 => {
                NodeValue::Int64List(Self::refl_to_vec(
                    repeated_field,
                    |val: ReflectValueRef<'_>| {
                        // map Option<u32> into Option<i64> and convert it to a Result
                        val.to_u32().map(|x| x as i64).ok_or(format!(
                            "Repeated field {:?} type expected to be u32",
                            repeated_field
                        ))
                    },
                )?)
            }
            ProtobufRuntimeType::U64 => {
                NodeValue::Int64List(Self::refl_to_vec(
                    repeated_field,
                    |val: ReflectValueRef<'_>| {
                        // map Option<u64> into Option<i64> and convert it to a Result
                        val.to_u64().map(|x| x as i64).ok_or(format!(
                            "Repeated field {:?} type expected to be u64",
                            repeated_field
                        ))
                    },
                )?)
            }
            ProtobufRuntimeType::F32 => {
                NodeValue::FloatList(Self::refl_to_vec(
                    repeated_field,
                    // closure that unwraps f32 value from a single value reflection
                    |val: ReflectValueRef<'_>| {
                        val.to_f32().ok_or(format!(
                            "Repeated field {:?} type expected to be f32",
                            repeated_field
                        ))
                    },
                )?)
            }
            ProtobufRuntimeType::F64 => {
                NodeValue::DoubleList(Self::refl_to_vec(
                    repeated_field,
                    // unwraps a f64 value from a value reflection or error
                    |val: ReflectValueRef<'_>| {
                        val.to_f64().ok_or(format!(
                            "Repeated field {:?} type expected to be f64",
                            repeated_field
                        ))
                    },
                )?)
            }
            ProtobufRuntimeType::Bool => {
                NodeValue::BoolList(Self::refl_to_vec(
                    repeated_field,
                    // unwraps a bool value from a value reflection or error
                    |val: ReflectValueRef<'_>| {
                        val.to_bool().ok_or(format!(
                            "Repeated field {:?} type expected to be bool",
                            repeated_field
                        ))
                    },
                )?)
            }
            ProtobufRuntimeType::Enum(_) => NodeValue::Int32List(Self::refl_to_vec(
                repeated_field,
                // unwraps an enum as i32 from a value reflection or error
                |val: ReflectValueRef<'_>| {
                    val.to_enum_value().ok_or(format!(
                        "Repeated field {:?} type expected to be enum",
                        repeated_field
                    ))
                },
            )?),
            _ => {
                return Err("Getting repeated strings, messages, and bytes \
                            are not supported"
                    .to_string());
            }
        })
    }

    /// Converts a protobuf reflection of a repeated field into a vector.
    /// For example, a `repeated int32` field will become `Vec<i32>`, or error if
    /// the `transform_func` returned an error.
    fn refl_to_vec<T>(
        repeated_field: &ReflectRepeatedRef,
        transform_func: impl Fn(ReflectValueRef) -> Result<T, String>,
    ) -> Result<Vec<T>, String> {
        // apply `transform_func` to each element in the repeated_field
        repeated_field.into_iter().map(transform_func).collect()
    }

    /// Check if list contains some element.
    /// Returns error if `self` is not a list value or `element` is not a compatible type.
    fn contains(&self, element: &NodeValue) -> Result<bool, String> {
        let err_msg = format!(
            "contains() called on incompatible types, left operand = {:?}, \
                    right operand = {:?}. Ensure left operand is a list and \
                    right operand is a scalar of compatible type to the list value",
            self, element
        );
        Ok(match self {
            NodeValue::BoolList(v) => v.contains(&element.get_bool_with_msg(err_msg)?),
            NodeValue::Int32List(v) => {
                let target_value = element.get_int_with_msg(err_msg)? as i32;
                v.contains(&target_value)
            }
            NodeValue::Int64List(v) => v.contains(&element.get_int_with_msg(err_msg)?),
            NodeValue::FloatList(v) => {
                let target_value = element.get_double_with_msg(err_msg)?;
                v.iter().any(|x| approximately_equal(*x as f64, target_value))
            }
            NodeValue::DoubleList(v) => {
                let target_value = element.get_double_with_msg(err_msg)?;
                v.iter().any(|x| approximately_equal(*x, target_value))
            }
            _ => return Err(err_msg),
        })
    }

    /// Check if all list items are equal to some element.
    /// Returns errors if `self` is not a list value or `element` is not a compatible type.
    fn all_equals(&self, element: &NodeValue) -> Result<bool, String> {
        let err_msg = format!(
            "all_equals() called on incompatible types, left operand = {:?}, \
                    right operand = {:?}. Ensure left operand is a list and \
                    right operand is a scalar of compatible type to the list value",
            self, element
        );
        Ok(match self {
            NodeValue::BoolList(v) => {
                let target_value = element.get_bool_with_msg(err_msg)?;
                Self::all_equal_to(v, target_value)
            }
            NodeValue::Int32List(v) => {
                let target_value = element.get_int_with_msg(err_msg)? as i32;
                Self::all_equal_to(v, target_value)
            }
            NodeValue::Int64List(v) => {
                let target_value = element.get_int_with_msg(err_msg)?;
                Self::all_equal_to(v, target_value)
            }
            NodeValue::FloatList(v) => {
                let target_value = element.get_double_with_msg(err_msg)?;
                v.iter().all(|x| approximately_equal(*x as f64, target_value))
            }
            NodeValue::DoubleList(v) => {
                let target_value = element.get_double_with_msg(err_msg)?;
                v.iter().all(|x| approximately_equal(*x, target_value))
            }
            _ => return Err(err_msg),
        })
    }

    /// Check if all vector elements are equal to target_value.
    fn all_equal_to<T: PartialEq>(v: &[T], target_value: T) -> bool {
        v.iter().all(|x| *x == target_value)
    }

    /// Check if node holds a boolean value.
    fn is_bool(&self) -> bool {
        matches!(self, NodeValue::Bool(_))
    }

    /// Returns the bool value or error if NodeValue is not a boolean.
    fn get_bool(&self) -> Result<bool, String> {
        self.get_bool_with_msg(format!("get_bool() invoked on non-boolean NodeValue: {:?}", self))
    }

    /// Returns the bool value or a custom error message if NodeValue is not a bool.
    fn get_bool_with_msg(&self, err_msg: String) -> Result<bool, String> {
        if let NodeValue::Bool(val) = self {
            return Ok(*val);
        }
        Err(err_msg)
    }

    /// Check if node holds a numeric value.
    fn is_numeric(&self) -> bool {
        matches!(
            self,
            NodeValue::Int32(_) | NodeValue::Int64(_) | NodeValue::Float(_) | NodeValue::Double(_)
        )
    }

    /// Check if node holds an integer value.
    fn is_int(&self) -> bool {
        matches!(self, NodeValue::Int32(_) | NodeValue::Int64(_))
    }

    /// Returns the int value or error if NodeValue is not an int.
    fn get_int(&self) -> Result<i64, String> {
        self.get_int_with_msg(format!("get_int() invoked on non-int NodeValue: {:?}", self))
    }

    /// Returns the int value or a custom error message if NodeValue is not an int.
    fn get_int_with_msg(&self, err_msg: String) -> Result<i64, String> {
        Ok(match self {
            NodeValue::Int32(val) => *val as i64,
            NodeValue::Int64(val) => *val,
            _ => return Err(err_msg),
        })
    }

    /// Returns the double value or error if NodeValue is not numeric.
    fn get_double(&self) -> Result<f64, String> {
        self.get_double_with_msg(format!(
            "get_double() invoked on non-numeric NodeValue: {:?}",
            self
        ))
    }

    /// Returns the double value or a custom error message if NodeValue is not numeric.
    fn get_double_with_msg(&self, err_msg: String) -> Result<f64, String> {
        Ok(match self {
            NodeValue::Int32(val) => *val as f64,
            NodeValue::Int64(val) => *val as f64,
            NodeValue::Float(val) => *val as f64,
            NodeValue::Double(val) => *val,
            _ => return Err(err_msg),
        })
    }

    /// Checks whether the node value is zero.
    fn is_zero(&self) -> Result<bool, String> {
        Ok(match self {
            NodeValue::Int32(val) => *val == 0,
            NodeValue::Int64(val) => *val == 0,
            NodeValue::Float(val) => (*val).abs() < EPSILON,
            NodeValue::Double(val) => (*val).abs() < EPSILON.into(),
            _ => return Err(format!("is_zero() invoked on non-numeric NodeValue: {:?}", self)),
        })
    }

    /// Applies the provided function on two numeric NodeValues based on
    /// the numeric type (integer or floating point).
    /// Returns an error if the NodeValues are not numeric types.
    fn apply_numeric<IF, FF, R>(
        &self,
        other: &NodeValue,
        int_func: IF,
        float_func: FF,
    ) -> Result<R, String>
    where
        IF: FnOnce(i64, i64) -> R,
        FF: FnOnce(f64, f64) -> R,
    {
        if self.is_int() && other.is_int() {
            Ok(int_func(self.get_int()?, other.get_int()?))
        } else if self.is_numeric() && other.is_numeric() {
            Ok(float_func(self.get_double()?, other.get_double()?))
        } else {
            Err("Operator requires numeric operands but incompatible operands were provided"
                .to_string())
        }
    }
}

#[cfg(test)]
mod tests {

    use super::NodeValue;
    use lazy_static::lazy_static;
    use protobuf::Message;
    use sdv_telem_expr_test_proto::test_message::{TestEnum, TestMessage, TestNestedMessage};
    use sdvtelemetry_proto_rust::expression::{
        combination_node::{
            ArithmeticOperator, LogicalOperator, Operator as OperatorProto, RelationalOperator,
        },
        ConstantLeafNode,
    };
    use std::sync::Mutex;

    // Static variables within the tests module scope that are reused across tests.
    lazy_static! {
        static ref OP_AND: OperatorProto =
            OperatorProto::LogicalOperator(LogicalOperator::AND.into());
        static ref OP_OR: OperatorProto =
            OperatorProto::LogicalOperator(LogicalOperator::OR.into());
        static ref OP_XOR: OperatorProto =
            OperatorProto::LogicalOperator(LogicalOperator::XOR.into());
        static ref OP_NOT: OperatorProto =
            OperatorProto::LogicalOperator(LogicalOperator::NOT.into());
        static ref OP_ADD: OperatorProto =
            OperatorProto::ArithmeticOperator(ArithmeticOperator::ADD.into());
        static ref OP_SUBTRACT: OperatorProto =
            OperatorProto::ArithmeticOperator(ArithmeticOperator::SUBTRACT.into());
        static ref OP_MULTIPLY: OperatorProto =
            OperatorProto::ArithmeticOperator(ArithmeticOperator::MULTIPLY.into());
        static ref OP_DIVIDE: OperatorProto =
            OperatorProto::ArithmeticOperator(ArithmeticOperator::DIVIDE.into());
        static ref OP_MODULUS: OperatorProto =
            OperatorProto::ArithmeticOperator(ArithmeticOperator::MODULUS.into());
        static ref OP_POWER: OperatorProto =
            OperatorProto::ArithmeticOperator(ArithmeticOperator::POWER.into());
        static ref OP_EQ: OperatorProto =
            OperatorProto::RelationalOperator(RelationalOperator::EQ.into());
        static ref OP_NOT_EQ: OperatorProto =
            OperatorProto::RelationalOperator(RelationalOperator::NOT_EQ.into());
        static ref OP_GT: OperatorProto =
            OperatorProto::RelationalOperator(RelationalOperator::GT.into());
        static ref OP_GT_OR_EQ: OperatorProto =
            OperatorProto::RelationalOperator(RelationalOperator::GT_OR_EQ.into());
        static ref OP_LT: OperatorProto =
            OperatorProto::RelationalOperator(RelationalOperator::LT.into());
        static ref OP_LT_OR_EQ: OperatorProto =
            OperatorProto::RelationalOperator(RelationalOperator::LT_OR_EQ.into());
        static ref OP_CONTAINS: OperatorProto =
            OperatorProto::RelationalOperator(RelationalOperator::CONTAINS.into());
        static ref OP_DOES_NOT_CONTAIN: OperatorProto =
            OperatorProto::RelationalOperator(RelationalOperator::DOES_NOT_CONTAIN.into());
        static ref OP_ALL_EQ: OperatorProto =
            OperatorProto::RelationalOperator(RelationalOperator::ALL_EQ.into());
        static ref TEST_MESSAGE: TestMessage = {
            let mut test_message = TestMessage::new();
            // initialize scalar (optional) fields
            test_message.int_field = 80;
            test_message.float_field = 100.0;
            test_message.string_field = "Hello world".to_string();
            test_message.enum_field = TestEnum::SOME.into();

            // initialize nested field
            let mut test_nested_message = TestNestedMessage::new();
            test_nested_message.bool_field = true;
            test_nested_message.repeated_i32s = vec![1, 2];
            test_message.message_field = protobuf::MessageField::some(test_nested_message.clone());

            // initialize repeated fields
            test_message.repeated_bools = vec![true, true, false, true];
            test_message.repeated_i64s = vec![3, 4, 5];
            test_message.repeated_doubles = vec![
                std::f64::consts::PI, std::f64::consts::TAU, std::f64::consts::E];
            test_message.repeated_enums = vec![
                TestEnum::SOME.into(), TestEnum::SOME.into(), TestEnum::NONE.into()];
            test_message.repeated_messages = vec![
                test_nested_message.clone(), test_nested_message];

            test_message
        };
        static ref EMPTY_TEST_MESSAGE: TestMessage = TestMessage::new();
    }

    fn provide(value: &NodeValue) -> Result<NodeValue, String> {
        Ok(value.clone())
    }

    #[test]
    fn test_from_constant_leaf_node() -> Result<(), String> {
        // setup
        let mut constant_leaf_node_bool = ConstantLeafNode::new();
        constant_leaf_node_bool.set_bool_value(true);

        let mut constant_leaf_node_i32 = ConstantLeafNode::new();
        constant_leaf_node_i32.set_int32_value(32);

        let mut constant_leaf_node_i64 = ConstantLeafNode::new();
        constant_leaf_node_i64.set_int64_value(64);

        let mut constant_leaf_node_float = ConstantLeafNode::new();
        constant_leaf_node_float.set_float_value(1.5);

        let mut constant_leaf_node_double = ConstantLeafNode::new();
        constant_leaf_node_double.set_double_value(1.5);

        // assert
        assert_eq!(
            NodeValue::from_constant_leaf_node(&constant_leaf_node_bool)?,
            NodeValue::Bool(true)
        );
        assert_eq!(
            NodeValue::from_constant_leaf_node(&constant_leaf_node_i32)?,
            NodeValue::Int32(32)
        );
        assert_eq!(
            NodeValue::from_constant_leaf_node(&constant_leaf_node_i64)?,
            NodeValue::Int64(64)
        );
        assert_eq!(
            NodeValue::from_constant_leaf_node(&constant_leaf_node_float)?,
            NodeValue::Float(1.5)
        );
        assert_eq!(
            NodeValue::from_constant_leaf_node(&constant_leaf_node_double)?,
            NodeValue::Double(1.5)
        );
        Ok(())
    }

    #[test]
    fn test_extract_value_no_field_names_ok() -> Result<(), String> {
        // setup
        let msg = TEST_MESSAGE.clone();
        let expected_msg_bytes =
            msg.write_to_bytes().map_err(|_| "Failed to serialize TestMessage".to_string())?;

        // action
        let node_value = NodeValue::extract_value(Box::new(msg), &[])?;

        // assert
        if let NodeValue::Message(node_msg) = node_value {
            let bytes = node_msg
                .write_to_bytes_dyn()
                .map_err(|_| "Failed to serialize NodeValue's protobuf message".to_string())?;
            assert_eq!(bytes, expected_msg_bytes);
        } else {
            return Err(format!("Expect Message variant of NodeValue, got {:?}", node_value));
        }
        Ok(())
    }

    #[test]
    fn test_extract_value_field_not_set_ok() {
        let field_names = ["int_field".to_string()];

        assert_eq!(
            NodeValue::extract_value(Box::new(EMPTY_TEST_MESSAGE.clone()), &field_names),
            Ok(NodeValue::None)
        );
    }

    #[test]
    fn test_extract_value_int_field_ok() {
        let field_names = ["int_field".to_string()];

        assert_eq!(
            NodeValue::extract_value(Box::new(TEST_MESSAGE.clone()), &field_names),
            Ok(NodeValue::Int32(80))
        );
    }

    #[test]
    fn test_extract_value_float_field_ok() {
        let field_names = ["float_field".to_string()];

        assert_eq!(
            NodeValue::extract_value(Box::new(TEST_MESSAGE.clone()), &field_names),
            Ok(NodeValue::Float(100.0))
        );
    }

    #[test]
    fn test_extract_value_enum_field_ok() {
        let field_names = ["enum_field".to_string()];

        assert_eq!(
            NodeValue::extract_value(Box::new(TEST_MESSAGE.clone()), &field_names),
            Ok(NodeValue::Int32(1))
        );
    }

    #[test]
    fn test_extract_field_value_message_field_ok() -> Result<(), String> {
        let msg = TEST_MESSAGE.clone();
        let field_names = ["message_field".to_string()];
        let expected_msg_bytes = msg
            .message_field
            .write_to_bytes()
            .map_err(|_| "Failed to serialize TestNestedMessage".to_string())?;

        // action
        let node_value = NodeValue::extract_value(Box::new(msg), &field_names)?;

        // assert
        if let NodeValue::Message(node_msg) = node_value {
            let bytes = node_msg
                .write_to_bytes_dyn()
                .map_err(|_| "Failed to serialize NodeValue's protobuf message".to_string())?;
            assert_eq!(bytes, expected_msg_bytes);
        } else {
            return Err(format!("Expect Message variant of NodeValue, got {:?}", node_value));
        }
        Ok(())
    }

    #[test]
    fn test_extract_value_nested_field_not_set_ok() {
        let field_names = ["message_field".to_string(), "int_field".to_string()];

        assert_eq!(
            NodeValue::extract_value(Box::new(EMPTY_TEST_MESSAGE.clone()), &field_names),
            Ok(NodeValue::None)
        );
    }

    #[test]
    fn test_extract_value_nested_singular_field_ok() {
        let field_names = ["message_field".to_string(), "bool_field".to_string()];

        assert_eq!(
            NodeValue::extract_value(Box::new(TEST_MESSAGE.clone()), &field_names),
            Ok(NodeValue::Bool(true))
        );
    }

    #[test]
    fn test_extract_value_repeated_bool_field_ok() {
        let msg = TEST_MESSAGE.clone();
        let field_names = ["repeated_bools".to_string()];
        let repeated_bools = msg.repeated_bools.clone();

        assert_eq!(
            NodeValue::extract_value(Box::new(msg), &field_names),
            Ok(NodeValue::BoolList(repeated_bools))
        );
    }

    #[test]
    fn test_extract_value_repeated_int_field_ok() {
        let msg = TEST_MESSAGE.clone();
        let field_names = ["repeated_i64s".to_string()];
        let repeated_i64s = msg.repeated_i64s.clone();

        assert_eq!(
            NodeValue::extract_value(Box::new(msg), &field_names),
            Ok(NodeValue::Int64List(repeated_i64s))
        );
    }

    #[test]
    fn test_extract_value_repeated_double_field_ok() {
        let msg = TEST_MESSAGE.clone();
        let field_names = ["repeated_doubles".to_string()];
        let repeated_doubles = msg.repeated_doubles.clone();

        assert_eq!(
            NodeValue::extract_value(Box::new(msg), &field_names),
            Ok(NodeValue::DoubleList(repeated_doubles))
        );
    }

    #[test]
    fn test_extract_value_repeated_enum_field_ok() {
        let msg = TEST_MESSAGE.clone();
        let field_names = ["repeated_enums".to_string()];
        let repeated_i32s = msg.repeated_enums.iter().map(|x| x.value()).collect();

        assert_eq!(
            NodeValue::extract_value(Box::new(msg), &field_names),
            Ok(NodeValue::Int32List(repeated_i32s))
        );
    }

    #[test]
    fn test_extract_value_nested_repeated_field_ok() {
        let msg = TEST_MESSAGE.clone();
        let field_names = ["message_field".to_string(), "repeated_i32s".to_string()];
        let repeated_i32s = msg.message_field.repeated_i32s.clone();

        assert_eq!(
            NodeValue::extract_value(Box::new(msg), &field_names),
            Ok(NodeValue::Int32List(repeated_i32s))
        );
    }

    #[test]
    fn test_extract_value_string_field_should_err() {
        let field_names = ["string_field".to_string()];

        assert!(NodeValue::extract_value(Box::new(TEST_MESSAGE.clone()), &field_names).is_err());
    }

    #[test]
    fn test_extract_value_repeated_message_field_should_err() {
        let field_names = ["repeated_messages".to_string()];

        assert!(NodeValue::extract_value(Box::new(TEST_MESSAGE.clone()), &field_names).is_err());
    }

    #[test]
    fn test_extract_value_repeated_message_as_intermediate_field_name_should_err() {
        let field_names = ["repeated_messages".to_string(), "bool_field".to_string()];

        assert!(NodeValue::extract_value(Box::new(TEST_MESSAGE.clone()), &field_names).is_err());
    }

    #[test]
    fn test_extract_value_nonexistent_field_should_err() {
        let field_names = ["foo".to_string()];

        assert!(NodeValue::extract_value(Box::new(TEST_MESSAGE.clone()), &field_names).is_err());
    }

    #[test]
    fn test_operate_logical_requires_boolean_types() {
        let true_val = NodeValue::Bool(true);
        let int_val = NodeValue::Int32(1);

        assert!(true_val.operate(&OP_AND, || provide(&int_val)).is_err());
        assert!(int_val.operate(&OP_AND, || provide(&true_val)).is_err());
    }

    #[test]
    fn test_operate_logical_and() {
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        // true && true = true
        assert_eq!(true_val.operate(&OP_AND, || provide(&true_val)), Ok(NodeValue::Bool(true)));
        // true && false = false
        assert_eq!(true_val.operate(&OP_AND, || provide(&false_val)), Ok(NodeValue::Bool(false)));
        // false && false = false
        assert_eq!(false_val.operate(&OP_AND, || provide(&false_val)), Ok(NodeValue::Bool(false)));
    }

    #[test]
    fn test_operate_logical_and_short_circuits() {
        let false_val = NodeValue::Bool(false);
        let num_calls = Mutex::new(0);
        let right_operand_provider = || {
            *num_calls.lock().unwrap() += 1;
            provide(&false_val)
        };

        let res = false_val.operate(&OP_AND, right_operand_provider);

        // false && anything = false
        assert_eq!(res, Ok(NodeValue::Bool(false)));
        // should not compute right operand because of short circuiting
        assert_eq!(*num_calls.lock().unwrap(), 0);
    }

    #[test]
    fn test_operate_logical_and_short_circuiting_attempted() {
        let true_val = NodeValue::Bool(true);
        let num_calls = Mutex::new(0);
        let right_operand_provider = || {
            *num_calls.lock().unwrap() += 1;
            provide(&true_val)
        };

        // true && [right_operand] = [right_operand]
        let res = true_val.operate(&OP_AND, right_operand_provider);

        assert_eq!(res, Ok(NodeValue::Bool(true)));
        // should compute right operand because left operand does not uniquely determine result
        assert_eq!(*num_calls.lock().unwrap(), 1);
    }

    #[test]
    fn test_operate_logical_or() {
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        // true || true = true
        assert_eq!(true_val.operate(&OP_OR, || provide(&true_val)), Ok(NodeValue::Bool(true)));
        // true || false = true
        assert_eq!(true_val.operate(&OP_OR, || provide(&false_val)), Ok(NodeValue::Bool(true)));
        // false || false = false
        assert_eq!(false_val.operate(&OP_OR, || provide(&false_val)), Ok(NodeValue::Bool(false)));
    }

    #[test]
    fn test_operate_logical_or_short_circuits() {
        let true_val = NodeValue::Bool(true);
        let num_calls = Mutex::new(0);
        let right_operand_provider = || {
            *num_calls.lock().unwrap() += 1;
            provide(&true_val)
        };

        let res = true_val.operate(&OP_OR, right_operand_provider);

        // true || anything = true
        assert_eq!(res, Ok(NodeValue::Bool(true)));
        // should not compute right operand because of short circuiting
        assert_eq!(*num_calls.lock().unwrap(), 0);
    }

    #[test]
    fn test_operate_logical_or_short_circuiting_attempted() {
        let false_val = NodeValue::Bool(false);
        let num_calls = Mutex::new(0);
        let right_operand_provider = || {
            *num_calls.lock().unwrap() += 1;
            provide(&false_val)
        };

        // false || [right_operand] = [right_operand]
        let res = false_val.operate(&OP_OR, right_operand_provider);

        assert_eq!(res, Ok(NodeValue::Bool(false)));
        // should compute right operand because left operand does not uniquely determine result
        assert_eq!(*num_calls.lock().unwrap(), 1);
    }

    #[test]
    fn test_operate_logical_xor() {
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        // true XOR true = false
        assert_eq!(true_val.operate(&OP_XOR, || provide(&true_val)), Ok(NodeValue::Bool(false)));
        // true XOR false = true
        assert_eq!(true_val.operate(&OP_XOR, || provide(&false_val)), Ok(NodeValue::Bool(true)));
        // false XOR false = false
        assert_eq!(false_val.operate(&OP_XOR, || provide(&false_val)), Ok(NodeValue::Bool(false)));
    }

    #[test]
    fn test_operate_logical_not() {
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        // !true = false
        assert_eq!(true_val.operate(&OP_NOT, || Ok(NodeValue::None)), Ok(NodeValue::Bool(false)));
        // !false = true
        assert_eq!(false_val.operate(&OP_NOT, || Ok(NodeValue::None)), Ok(NodeValue::Bool(true)));
    }

    #[test]
    fn test_operate_arithmetic_requires_numeric_types() {
        let true_val = NodeValue::Bool(true);
        let int_val = NodeValue::Int32(1);

        assert!(true_val.operate(&OP_ADD, || provide(&int_val)).is_err());
        assert!(int_val.operate(&OP_ADD, || provide(&true_val)).is_err());
    }

    #[test]
    fn test_operate_arithmetic_add_integers() {
        let five = NodeValue::Int32(5);
        let twenty = NodeValue::Int32(20);

        // 5 + 20 = 25
        assert_eq!(five.operate(&OP_ADD, || provide(&twenty)), Ok(NodeValue::Int64(25)));
    }

    #[test]
    fn test_operate_arithmetic_add_floats() {
        let five = NodeValue::Float(5.2);
        let twenty = NodeValue::Double(20.1);

        // 5.2 + 20.1 = 25.3
        assert_eq!(five.operate(&OP_ADD, || provide(&twenty)), Ok(NodeValue::Double(25.3)));
    }

    #[test]
    fn test_operate_arithmetic_subtract_integers() {
        let five = NodeValue::Int32(5);
        let twenty = NodeValue::Int32(20);

        // 5 - 20 = -15
        assert_eq!(five.operate(&OP_SUBTRACT, || provide(&twenty)), Ok(NodeValue::Int64(-15)));
    }

    #[test]
    fn test_operate_arithmetic_subtract_floats() {
        let five = NodeValue::Float(5.2);
        let twenty = NodeValue::Double(20.1);

        // 5.2 - 20.1 = -14.9
        assert_eq!(five.operate(&OP_SUBTRACT, || provide(&twenty)), Ok(NodeValue::Double(-14.9)));
    }

    #[test]
    fn test_operate_arithmetic_multiply() {
        let five = NodeValue::Int32(5);
        let point_five = NodeValue::Float(0.5);

        // 5 * 0.5 = 2.5
        assert_eq!(five.operate(&OP_MULTIPLY, || provide(&point_five)), Ok(NodeValue::Double(2.5)));
    }

    #[test]
    fn test_operate_arithmetic_divide_by_zero() {
        let five = NodeValue::Int32(5);
        let zero = NodeValue::Int32(0);

        assert!(five.operate(&OP_DIVIDE, || provide(&zero)).is_err());
    }

    #[test]
    fn test_operate_arithmetic_divide() {
        let five = NodeValue::Int32(5);
        let two = NodeValue::Int32(2);

        // 5 / 2 = 2.5
        assert_eq!(five.operate(&OP_DIVIDE, || provide(&two)), Ok(NodeValue::Double(2.5)));
    }

    #[test]
    fn test_operate_arithmetic_modulus_by_zero() {
        let five = NodeValue::Int32(5);
        let zero = NodeValue::Int32(0);

        assert!(five.operate(&OP_MODULUS, || provide(&zero)).is_err());
    }

    #[test]
    fn test_operate_arithmetic_modulus() {
        let five = NodeValue::Int32(5);
        let two = NodeValue::Int32(2);

        // 5 % 2 = 1
        assert_eq!(five.operate(&OP_MODULUS, || provide(&two)), Ok(NodeValue::Int64(1)));
    }

    #[test]
    fn test_operate_arithmetic_modulus_no_floating_points() {
        let five = NodeValue::Float(5.0);
        let two = NodeValue::Int32(2);

        // both operands must be integers
        assert!(five.operate(&OP_MODULUS, || provide(&two)).is_err());
    }

    #[test]
    fn test_operate_arithmetic_power_integers() {
        let two = NodeValue::Int32(2);

        // 2^2 = 4
        assert_eq!(two.operate(&OP_POWER, || provide(&two)), Ok(NodeValue::Int64(4)));
    }

    #[test]
    fn test_operate_arithmetic_power_floats() {
        let val = 2.5_f64;
        let expected = val.powf(val);
        let two_point_five = NodeValue::Double(val);

        assert_eq!(
            two_point_five.operate(&OP_POWER, || provide(&two_point_five)),
            Ok(NodeValue::Double(expected))
        );
    }

    #[test]
    fn test_operate_relational_eq() {
        let two = NodeValue::Int32(2);
        let two_i64 = NodeValue::Int64(2);
        let two_f32 = NodeValue::Float(2.0);
        let two_f64 = NodeValue::Double(2.0);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(two.operate(&OP_EQ, || provide(&two)), Ok(NodeValue::Bool(true)));
        assert_eq!(two.operate(&OP_EQ, || provide(&two_i64)), Ok(NodeValue::Bool(true)));
        assert_eq!(two.operate(&OP_EQ, || provide(&two_f32)), Ok(NodeValue::Bool(true)));
        assert_eq!(two.operate(&OP_EQ, || provide(&two_f64)), Ok(NodeValue::Bool(true)));
        assert_eq!(two.operate(&OP_EQ, || provide(&five)), Ok(NodeValue::Bool(false)));
        assert_eq!(two.operate(&OP_EQ, || provide(&true_val)), Ok(NodeValue::Bool(false)));
    }

    #[test]
    fn test_operate_relational_not_eq() {
        let two = NodeValue::Int32(2);
        let two_f32 = NodeValue::Float(2.0);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(two.operate(&OP_NOT_EQ, || provide(&two)), Ok(NodeValue::Bool(false)));
        assert_eq!(two.operate(&OP_NOT_EQ, || provide(&two_f32)), Ok(NodeValue::Bool(false)));
        assert_eq!(two.operate(&OP_NOT_EQ, || provide(&five)), Ok(NodeValue::Bool(true)));
        assert_eq!(two.operate(&OP_NOT_EQ, || provide(&true_val)), Ok(NodeValue::Bool(true)));
    }

    #[test]
    fn test_operate_relational_gt() {
        let two = NodeValue::Int32(2);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(two.operate(&OP_GT, || provide(&two)), Ok(NodeValue::Bool(false)));
        assert_eq!(two.operate(&OP_GT, || provide(&five)), Ok(NodeValue::Bool(false)));
        assert_eq!(five.operate(&OP_GT, || provide(&two)), Ok(NodeValue::Bool(true)));
        assert!(five.operate(&OP_GT, || provide(&true_val)).is_err());
    }

    #[test]
    fn test_operate_relational_gt_or_eq() {
        let two = NodeValue::Int32(2);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(two.operate(&OP_GT_OR_EQ, || provide(&two)), Ok(NodeValue::Bool(true)));
        assert_eq!(two.operate(&OP_GT_OR_EQ, || provide(&five)), Ok(NodeValue::Bool(false)));
        assert_eq!(five.operate(&OP_GT_OR_EQ, || provide(&two)), Ok(NodeValue::Bool(true)));
        assert!(five.operate(&OP_GT_OR_EQ, || provide(&true_val)).is_err());
    }

    #[test]
    fn test_operate_relational_lt() {
        let two = NodeValue::Int32(2);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(two.operate(&OP_LT, || provide(&two)), Ok(NodeValue::Bool(false)));
        assert_eq!(two.operate(&OP_LT, || provide(&five)), Ok(NodeValue::Bool(true)));
        assert_eq!(five.operate(&OP_LT, || provide(&two)), Ok(NodeValue::Bool(false)));
        assert!(five.operate(&OP_LT, || provide(&true_val)).is_err());
    }

    #[test]
    fn test_operate_relational_lt_or_eq() {
        let two = NodeValue::Int32(2);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(two.operate(&OP_LT_OR_EQ, || provide(&two)), Ok(NodeValue::Bool(true)));
        assert_eq!(two.operate(&OP_LT_OR_EQ, || provide(&five)), Ok(NodeValue::Bool(true)));
        assert_eq!(five.operate(&OP_LT_OR_EQ, || provide(&two)), Ok(NodeValue::Bool(false)));
        assert!(five.operate(&OP_LT_OR_EQ, || provide(&true_val)).is_err());
    }

    #[test]
    fn test_operate_relational_contains_errs_if_left_operand_not_a_list() {
        let two = NodeValue::Int32(2);

        assert!(two.operate(&OP_CONTAINS, || provide(&two)).is_err());
    }

    #[test]
    fn test_operate_relational_contains_errs_if_operands_have_incompatible_types() {
        let bool_list = NodeValue::BoolList(vec![true, true, false]);
        let five = NodeValue::Float(5.5);

        assert!(bool_list.operate(&OP_CONTAINS, || provide(&five)).is_err());
    }

    #[test]
    fn test_operate_relational_contains_bool_type_ok() {
        let bool_list = NodeValue::BoolList(vec![true, true, true]);
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        assert_eq!(
            bool_list.operate(&OP_CONTAINS, || provide(&true_val)),
            Ok(NodeValue::Bool(true))
        );
        assert_eq!(
            bool_list.operate(&OP_CONTAINS, || provide(&false_val)),
            Ok(NodeValue::Bool(false))
        );
    }

    #[test]
    fn test_operate_relational_contains_int_type_ok() {
        let int_list = NodeValue::Int32List(vec![3, 30, 300]);
        let zero = NodeValue::Int32(0);
        let three = NodeValue::Int32(3);
        let thirty = NodeValue::Int64(30);
        // not int
        let three_hundred = NodeValue::Double(300.0);

        assert_eq!(int_list.operate(&OP_CONTAINS, || provide(&zero)), Ok(NodeValue::Bool(false)));
        assert_eq!(int_list.operate(&OP_CONTAINS, || provide(&three)), Ok(NodeValue::Bool(true)));
        assert_eq!(int_list.operate(&OP_CONTAINS, || provide(&thirty)), Ok(NodeValue::Bool(true)));
        assert!(int_list.operate(&OP_CONTAINS, || provide(&three_hundred)).is_err());
    }

    #[test]
    fn test_operate_relational_contains_float_type_ok() {
        let float_list = NodeValue::FloatList(vec![0.5, std::f32::consts::PI, 10.0]);
        let point_five = NodeValue::Float(0.5);
        // f64 PI, not f32 PI
        let pi = NodeValue::Double(std::f64::consts::PI);
        // not float but is a numeric type
        let ten = NodeValue::Int32(10);
        let thirty = NodeValue::Float(30.0);

        assert_eq!(
            float_list.operate(&OP_CONTAINS, || provide(&point_five)),
            Ok(NodeValue::Bool(true))
        );
        assert_eq!(float_list.operate(&OP_CONTAINS, || provide(&pi)), Ok(NodeValue::Bool(true)));
        assert_eq!(float_list.operate(&OP_CONTAINS, || provide(&ten)), Ok(NodeValue::Bool(true)));
        assert_eq!(
            float_list.operate(&OP_CONTAINS, || provide(&thirty)),
            Ok(NodeValue::Bool(false))
        );
    }

    #[test]
    fn test_operate_relational_does_not_contain_errs_if_left_operand_not_a_list() {
        let two = NodeValue::Int32(2);

        assert!(two.operate(&OP_DOES_NOT_CONTAIN, || provide(&two)).is_err());
    }

    #[test]
    fn test_operate_relational_does_not_contain_errs_if_operands_have_incompatible_types() {
        let bool_list = NodeValue::BoolList(vec![true, true, false]);
        let five = NodeValue::Float(5.5);

        assert!(bool_list.operate(&OP_DOES_NOT_CONTAIN, || provide(&five)).is_err());
    }

    #[test]
    fn test_operate_relational_does_not_contain_bool_type_ok() {
        let bool_list = NodeValue::BoolList(vec![true, true, true]);
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        assert_eq!(
            bool_list.operate(&OP_DOES_NOT_CONTAIN, || provide(&true_val)),
            Ok(NodeValue::Bool(false))
        );
        assert_eq!(
            bool_list.operate(&OP_DOES_NOT_CONTAIN, || provide(&false_val)),
            Ok(NodeValue::Bool(true))
        );
    }

    #[test]
    fn test_operate_relational_does_not_contain_int_type_ok() {
        let int_list = NodeValue::Int32List(vec![3, 30, 300]);
        let zero = NodeValue::Int32(0);
        let three = NodeValue::Int32(3);
        let thirty = NodeValue::Int64(30);
        // not int
        let three_hundred = NodeValue::Double(300.0);

        assert_eq!(
            int_list.operate(&OP_DOES_NOT_CONTAIN, || provide(&zero)),
            Ok(NodeValue::Bool(true))
        );
        assert_eq!(
            int_list.operate(&OP_DOES_NOT_CONTAIN, || provide(&three)),
            Ok(NodeValue::Bool(false))
        );
        assert_eq!(
            int_list.operate(&OP_DOES_NOT_CONTAIN, || provide(&thirty)),
            Ok(NodeValue::Bool(false))
        );
        assert!(int_list.operate(&OP_DOES_NOT_CONTAIN, || provide(&three_hundred)).is_err());
    }

    #[test]
    fn test_operate_relational_does_not_contain_float_type_ok() {
        let float_list = NodeValue::FloatList(vec![0.5, std::f32::consts::PI, 10.0]);
        let point_five = NodeValue::Float(0.5);
        // f64 PI, not f32 PI
        let pi = NodeValue::Double(std::f64::consts::PI);
        // not float but is a numeric type
        let ten = NodeValue::Int32(10);
        let thirty = NodeValue::Float(30.0);

        assert_eq!(
            float_list.operate(&OP_DOES_NOT_CONTAIN, || provide(&point_five)),
            Ok(NodeValue::Bool(false))
        );
        assert_eq!(
            float_list.operate(&OP_DOES_NOT_CONTAIN, || provide(&pi)),
            Ok(NodeValue::Bool(false))
        );
        assert_eq!(
            float_list.operate(&OP_DOES_NOT_CONTAIN, || provide(&ten)),
            Ok(NodeValue::Bool(false))
        );
        assert_eq!(
            float_list.operate(&OP_DOES_NOT_CONTAIN, || provide(&thirty)),
            Ok(NodeValue::Bool(true))
        );
    }

    #[test]
    fn test_operate_relational_all_eq_errs_if_left_operand_not_a_list() {
        let two = NodeValue::Int32(2);

        assert!(two.operate(&OP_ALL_EQ, || provide(&two)).is_err());
    }

    #[test]
    fn test_operate_relational_all_eq_errs_if_operands_have_incompatible_types() {
        let bool_list = NodeValue::BoolList(vec![true, true, false]);
        let five = NodeValue::Float(5.5);

        assert!(bool_list.operate(&OP_ALL_EQ, || provide(&five)).is_err());
    }

    #[test]
    fn test_operate_relational_all_eq_bool_type_ok() {
        let bool_list_1 = NodeValue::BoolList(vec![true, true, true]);
        let bool_list_2 = NodeValue::BoolList(vec![true, false, false]);
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        assert_eq!(
            bool_list_1.operate(&OP_ALL_EQ, || provide(&true_val)),
            Ok(NodeValue::Bool(true))
        );
        assert_eq!(
            bool_list_1.operate(&OP_ALL_EQ, || provide(&false_val)),
            Ok(NodeValue::Bool(false))
        );
        assert_eq!(
            bool_list_2.operate(&OP_ALL_EQ, || provide(&true_val)),
            Ok(NodeValue::Bool(false))
        );
        assert_eq!(
            bool_list_2.operate(&OP_ALL_EQ, || provide(&false_val)),
            Ok(NodeValue::Bool(false))
        );
    }

    #[test]
    fn test_operate_relational_all_eq_int_type_ok() {
        let int_list_1 = NodeValue::Int64List(vec![3, 30, 300]);
        let int_list_2 = NodeValue::Int64List(vec![0, 0]);
        let zero = NodeValue::Int32(0);

        assert_eq!(int_list_1.operate(&OP_ALL_EQ, || provide(&zero)), Ok(NodeValue::Bool(false)));
        assert_eq!(int_list_2.operate(&OP_ALL_EQ, || provide(&zero)), Ok(NodeValue::Bool(true)));
    }

    #[test]
    fn test_operate_relational_all_eq_float_type_ok() {
        let float_list_1 = NodeValue::FloatList(vec![0.5, std::f32::consts::PI, 10.0]);
        let float_list_2 = NodeValue::FloatList(vec![0.5, 0.5]);

        let point_five = NodeValue::Double(0.5);

        assert_eq!(
            float_list_1.operate(&OP_ALL_EQ, || provide(&point_five)),
            Ok(NodeValue::Bool(false))
        );
        assert_eq!(
            float_list_2.operate(&OP_ALL_EQ, || provide(&point_five)),
            Ok(NodeValue::Bool(true))
        );
    }
}
