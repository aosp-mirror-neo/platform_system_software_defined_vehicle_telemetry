// Copyright 2023 Google LLC. All Rights Reserved.

//! Wrapper around a generic value.
//! Should be able to hold all the values in a ConstantLeafNode protobuf
//! message, as well as all the values that a FieldLeafNode can select.

use protobuf::reflect::{ReflectFieldRef, ReflectValueRef};
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
    /// If unary operator, then operates on self.
    /// If binary operator, then [self] <operator> [other].
    pub fn operate(&self, op: &OperatorProto, other: &NodeValue) -> Result<NodeValue, String> {
        match op {
            OperatorProto::ArithmeticOperator(op) => self.handle_arithmetic(
                &op.enum_value().map_err(|i| {
                    format!("Arithmetic operator with enum value {} not recognized", i)
                })?,
                other,
            ),
            OperatorProto::LogicalOperator(op) => self.handle_logical(
                &op.enum_value().map_err(|i| {
                    format!("Logical operator with enum value {} not recognized", i)
                })?,
                other,
            ),
            OperatorProto::RelationalOperator(op) => self.handle_relational(
                &op.enum_value().map_err(|i| {
                    format!("Relational operator with enum value {} not recognized", i)
                })?,
                other,
            ),
            _ => Err(format!("Operator {:?} not recognized.", op)),
        }
    }

    /// Handle logical operations.
    fn handle_logical(&self, op: &LogicalOperator, other: &NodeValue) -> Result<NodeValue, String> {
        if !self.is_bool() || !other.is_bool() {
            return Err("Logical operators must have boolean operands".to_string());
        }
        Ok(NodeValue::Bool(match op {
            LogicalOperator::AND => self.get_bool() && other.get_bool(),
            LogicalOperator::OR => self.get_bool() || other.get_bool(),
            LogicalOperator::XOR => self.get_bool() != other.get_bool(),
            LogicalOperator::NOT => !self.get_bool(),
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
            _ => Err("Other relational operators are not implemented yet".to_string()),
        }?))
    }

    /// Reads a field value from a protobuf message using the reflection.
    pub fn extract_field_value(
        msg: &dyn protobuf::MessageDyn,
        fields: &[String],
    ) -> Result<NodeValue, String> {
        let msg_descriptor = msg.descriptor_dyn();
        let field_name = fields
            .get(0)
            .ok_or("FieldLeafNodes should provide at least one field name".to_string())?;
        let field_descriptor = msg_descriptor.field_by_name(field_name).ok_or(format!(
            "No field descriptor exists for field {} in message {}. Typo in field name?",
            field_name,
            msg_descriptor.name()
        ))?;
        let field_value = field_descriptor.get_reflect(msg);

        // Base case for recursion: one field is provided, simply read the value
        if fields.len() == 1 {
            return Ok(match field_value {
                ReflectFieldRef::Optional(singular_field) => {
                    let field_value = singular_field.value().ok_or(format!(
                        "No value exists for field {} in message {}",
                        field_name,
                        msg_descriptor.name()
                    ))?;
                    match field_value {
                        ReflectValueRef::U32(v) => NodeValue::Int64(v as i64),
                        ReflectValueRef::U64(v) => NodeValue::Int64(v as i64),
                        ReflectValueRef::I32(v) => NodeValue::Int32(v),
                        ReflectValueRef::I64(v) => NodeValue::Int64(v),
                        ReflectValueRef::F32(v) => NodeValue::Float(v),
                        ReflectValueRef::F64(v) => NodeValue::Double(v),
                        ReflectValueRef::Bool(v) => NodeValue::Bool(v),
                        ReflectValueRef::Enum(_, v) => NodeValue::Int32(v),
                        _ => {
                            return Err("FieldLeafNode attempts to extract bytes, string, or \
                                       protobuf message values, which is not supported"
                                .to_string())
                        }
                    }
                }
                _ => return Err("not implemented yet".to_string()),
            });
        }

        // If fields length > 1, read field from nested messages
        if let ReflectFieldRef::Optional(singular_field) = field_value {
            let field_value = singular_field.value().ok_or(format!(
                "No value exists for field {} in message {}",
                field_name,
                msg_descriptor.name()
            ))?;
            if let ReflectValueRef::Message(msg) = field_value {
                Self::extract_field_value(&(*msg), &fields[1..])
            } else {
                Err("FieldLeafNode attempts to extract nested field value, but a \
                    non-terminal field is not a protobuf message"
                    .to_string())
            }
        } else {
            Err("FieldLeafNode attempts to extract nested field values, but a \
                non-terminal field is either repeated or a map"
                .to_string())
        }
    }

    // private functions and methods below

    /// Check if node holds a boolean-compatible value.
    fn is_bool(&self) -> bool {
        matches!(self, NodeValue::Bool(_) | NodeValue::None)
    }

    /// Returns the bool value or false by default.
    fn get_bool(&self) -> bool {
        if let NodeValue::Bool(val) = self {
            return *val;
        }
        false
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

    /// Returns the int value or error if NodeValue is not numeric.
    fn get_int(&self) -> Result<i64, String> {
        Ok(match self {
            NodeValue::Int32(val) => *val as i64,
            NodeValue::Int64(val) => *val,
            _ => return Err(format!("get_int() invoked on non-int NodeValue: {:?}", self)),
        })
    }

    /// Returns the double value or error if NodeValue is not numeric.
    fn get_double(&self) -> Result<f64, String> {
        Ok(match self {
            NodeValue::Int32(val) => *val as f64,
            NodeValue::Int64(val) => *val as f64,
            NodeValue::Float(val) => *val as f64,
            NodeValue::Double(val) => *val,
            _ => return Err(format!("get_double() invoked on non-numeric NodeValue: {:?}", self)),
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
    use sdv_telem_expr_test_proto::test_message::{TestEnum, TestMessage, TestNestedMessage};
    use sdvtelemetry_proto_rust::expression::{
        combination_node::{
            ArithmeticOperator, LogicalOperator, Operator as OperatorProto, RelationalOperator,
        },
        ConstantLeafNode,
    };

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
        static ref TEST_MESSAGE: TestMessage = {
            let mut test_message = TestMessage::new();
            test_message.int_field = 80;
            test_message.float_field = 100.0;
            test_message.string_field = "Hello world".to_string();
            test_message.enum_field = TestEnum::SOME.into();

            let mut test_nested_message = TestNestedMessage::new();
            test_nested_message.bool_field = true;
            test_nested_message.repeated_int_fields.push(1);
            test_nested_message.repeated_int_fields.push(2);
            test_message.message_field = protobuf::MessageField::some(test_nested_message);

            test_message
        };
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
    fn test_extract_field_value_no_field_names_should_err() {
        assert!(NodeValue::extract_field_value(&*TEST_MESSAGE, &[]).is_err());
    }

    #[test]
    fn test_extract_field_value_int_field_ok() {
        let field_names = ["int_field".to_string()];

        assert_eq!(
            NodeValue::extract_field_value(&*TEST_MESSAGE, &field_names),
            Ok(NodeValue::Int32(80))
        );
    }

    #[test]
    fn test_extract_field_value_float_field_ok() {
        let field_names = ["float_field".to_string()];

        assert_eq!(
            NodeValue::extract_field_value(&*TEST_MESSAGE, &field_names),
            Ok(NodeValue::Float(100.0))
        );
    }

    #[test]
    fn test_extract_field_value_enum_field_ok() {
        let field_names = ["enum_field".to_string()];

        assert_eq!(
            NodeValue::extract_field_value(&*TEST_MESSAGE, &field_names),
            Ok(NodeValue::Int32(1))
        );
    }

    #[test]
    fn test_extract_field_value_nested_field_ok() {
        let field_names = ["message_field".to_string(), "bool_field".to_string()];

        assert_eq!(
            NodeValue::extract_field_value(&*TEST_MESSAGE, &field_names),
            Ok(NodeValue::Bool(true))
        );
    }

    #[test]
    fn test_extract_field_value_string_field_should_err() {
        let field_names = ["string_field".to_string()];

        assert!(NodeValue::extract_field_value(&*TEST_MESSAGE, &field_names).is_err());
    }

    #[test]
    fn test_extract_field_value_message_field_should_err() {
        let field_names = ["message_field".to_string()];

        assert!(NodeValue::extract_field_value(&*TEST_MESSAGE, &field_names).is_err());
    }

    #[test]
    fn test_extract_field_value_repeated_field_should_err() {
        let field_names = ["message_field".to_string(), "repeated_int_fields".to_string()];

        assert!(NodeValue::extract_field_value(&*TEST_MESSAGE, &field_names).is_err());
    }

    #[test]
    fn test_extract_field_value_nonexistent_field_should_err() {
        let field_names = ["foo".to_string()];

        assert!(NodeValue::extract_field_value(&*TEST_MESSAGE, &field_names).is_err());
    }

    #[test]
    fn test_operate_logical_requires_boolean_types() {
        let true_val = NodeValue::Bool(true);
        let int_val = NodeValue::Int32(1);

        assert!(true_val.operate(&OP_AND, &int_val).is_err());
        assert!(int_val.operate(&OP_AND, &true_val).is_err());
    }

    #[test]
    fn test_operate_logical_and() {
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        // true && true = true
        assert_eq!(true_val.operate(&OP_AND, &true_val), Ok(NodeValue::Bool(true)));
        // true && false = false
        assert_eq!(true_val.operate(&OP_AND, &false_val), Ok(NodeValue::Bool(false)));
        // false && false = false
        assert_eq!(false_val.operate(&OP_AND, &false_val), Ok(NodeValue::Bool(false)));
    }

    #[test]
    fn test_operate_logical_or() {
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        // true || true = true
        assert_eq!(true_val.operate(&OP_OR, &true_val), Ok(NodeValue::Bool(true)));
        // true || false = true
        assert_eq!(true_val.operate(&OP_OR, &false_val), Ok(NodeValue::Bool(true)));
        // false || false = false
        assert_eq!(false_val.operate(&OP_OR, &false_val), Ok(NodeValue::Bool(false)));
    }

    #[test]
    fn test_operate_logical_xor() {
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        // true XOR true = false
        assert_eq!(true_val.operate(&OP_XOR, &true_val), Ok(NodeValue::Bool(false)));
        // true XOR false = true
        assert_eq!(true_val.operate(&OP_XOR, &false_val), Ok(NodeValue::Bool(true)));
        // false XOR false = false
        assert_eq!(false_val.operate(&OP_XOR, &false_val), Ok(NodeValue::Bool(false)));
    }

    #[test]
    fn test_operate_logical_not() {
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        // !true = false
        assert_eq!(true_val.operate(&OP_NOT, &NodeValue::None), Ok(NodeValue::Bool(false)));
        // !false = true
        assert_eq!(false_val.operate(&OP_NOT, &NodeValue::None), Ok(NodeValue::Bool(true)));
    }

    #[test]
    fn test_operate_arithmetic_requires_numeric_types() {
        let true_val = NodeValue::Bool(true);
        let int_val = NodeValue::Int32(1);

        assert!(true_val.operate(&OP_ADD, &int_val).is_err());
        assert!(int_val.operate(&OP_ADD, &true_val).is_err());
    }

    #[test]
    fn test_operate_arithmetic_add_integers() {
        let five = NodeValue::Int32(5);
        let twenty = NodeValue::Int32(20);

        // 5 + 20 = 25
        assert_eq!(five.operate(&OP_ADD, &twenty), Ok(NodeValue::Int64(25)));
    }

    #[test]
    fn test_operate_arithmetic_add_floats() {
        let five = NodeValue::Float(5.2);
        let twenty = NodeValue::Double(20.1);

        // 5.2 + 20.1 = 25.3
        assert_eq!(five.operate(&OP_ADD, &twenty), Ok(NodeValue::Double(25.3)));
    }

    #[test]
    fn test_operate_arithmetic_subtract_integers() {
        let five = NodeValue::Int32(5);
        let twenty = NodeValue::Int32(20);

        // 5 - 20 = -15
        assert_eq!(five.operate(&OP_SUBTRACT, &twenty), Ok(NodeValue::Int64(-15)));
    }

    #[test]
    fn test_operate_arithmetic_subtract_floats() {
        let five = NodeValue::Float(5.2);
        let twenty = NodeValue::Double(20.1);

        // 5.2 - 20.1 = -14.9
        assert_eq!(five.operate(&OP_SUBTRACT, &twenty), Ok(NodeValue::Double(-14.9)));
    }

    #[test]
    fn test_operate_arithmetic_multiply() {
        let five = NodeValue::Int32(5);
        let point_five = NodeValue::Float(0.5);

        // 5 * 0.5 = 2.5
        assert_eq!(five.operate(&OP_MULTIPLY, &point_five), Ok(NodeValue::Double(2.5)));
    }

    #[test]
    fn test_operate_arithmetic_divide_by_zero() {
        let five = NodeValue::Int32(5);
        let zero = NodeValue::Int32(0);

        assert!(five.operate(&OP_DIVIDE, &zero).is_err());
    }

    #[test]
    fn test_operate_arithmetic_divide() {
        let five = NodeValue::Int32(5);
        let two = NodeValue::Int32(2);

        // 5 / 2 = 2.5
        assert_eq!(five.operate(&OP_DIVIDE, &two), Ok(NodeValue::Double(2.5)));
    }

    #[test]
    fn test_operate_arithmetic_modulus_by_zero() {
        let five = NodeValue::Int32(5);
        let zero = NodeValue::Int32(0);

        assert!(five.operate(&OP_MODULUS, &zero).is_err());
    }

    #[test]
    fn test_operate_arithmetic_modulus() {
        let five = NodeValue::Int32(5);
        let two = NodeValue::Int32(2);

        // 5 % 2 = 1
        assert_eq!(five.operate(&OP_MODULUS, &two), Ok(NodeValue::Int64(1)));
    }

    #[test]
    fn test_operate_arithmetic_modulus_no_floating_points() {
        let five = NodeValue::Float(5.0);
        let two = NodeValue::Int32(2);

        // both operands must be integers
        assert!(five.operate(&OP_MODULUS, &two).is_err());
    }

    #[test]
    fn test_operate_arithmetic_power_integers() {
        let two = NodeValue::Int32(2);

        // 2^2 = 4
        assert_eq!(two.operate(&OP_POWER, &two), Ok(NodeValue::Int64(4)));
    }

    #[test]
    fn test_operate_arithmetic_power_floats() {
        let val = 2.5_f64;
        let expected = val.powf(val);
        let two_point_five = NodeValue::Double(val);

        assert_eq!(
            two_point_five.operate(&OP_POWER, &two_point_five),
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

        assert_eq!(two.operate(&OP_EQ, &two), Ok(NodeValue::Bool(true)));
        assert_eq!(two.operate(&OP_EQ, &two_i64), Ok(NodeValue::Bool(true)));
        assert_eq!(two.operate(&OP_EQ, &two_f32), Ok(NodeValue::Bool(true)));
        assert_eq!(two.operate(&OP_EQ, &two_f64), Ok(NodeValue::Bool(true)));
        assert_eq!(two.operate(&OP_EQ, &five), Ok(NodeValue::Bool(false)));
        assert_eq!(two.operate(&OP_EQ, &true_val), Ok(NodeValue::Bool(false)));
    }

    #[test]
    fn test_operate_relational_not_eq() {
        let two = NodeValue::Int32(2);
        let two_f32 = NodeValue::Float(2.0);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(two.operate(&OP_NOT_EQ, &two), Ok(NodeValue::Bool(false)));
        assert_eq!(two.operate(&OP_NOT_EQ, &two_f32), Ok(NodeValue::Bool(false)));
        assert_eq!(two.operate(&OP_NOT_EQ, &five), Ok(NodeValue::Bool(true)));
        assert_eq!(two.operate(&OP_NOT_EQ, &true_val), Ok(NodeValue::Bool(true)));
    }

    #[test]
    fn test_operate_relational_gt() {
        let two = NodeValue::Int32(2);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(two.operate(&OP_GT, &two), Ok(NodeValue::Bool(false)));
        assert_eq!(two.operate(&OP_GT, &five), Ok(NodeValue::Bool(false)));
        assert_eq!(five.operate(&OP_GT, &two), Ok(NodeValue::Bool(true)));
        assert!(five.operate(&OP_GT, &true_val).is_err());
    }

    #[test]
    fn test_operate_relational_gt_or_eq() {
        let two = NodeValue::Int32(2);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(two.operate(&OP_GT_OR_EQ, &two), Ok(NodeValue::Bool(true)));
        assert_eq!(two.operate(&OP_GT_OR_EQ, &five), Ok(NodeValue::Bool(false)));
        assert_eq!(five.operate(&OP_GT_OR_EQ, &two), Ok(NodeValue::Bool(true)));
        assert!(five.operate(&OP_GT_OR_EQ, &true_val).is_err());
    }

    #[test]
    fn test_operate_relational_lt() {
        let two = NodeValue::Int32(2);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(two.operate(&OP_LT, &two), Ok(NodeValue::Bool(false)));
        assert_eq!(two.operate(&OP_LT, &five), Ok(NodeValue::Bool(true)));
        assert_eq!(five.operate(&OP_LT, &two), Ok(NodeValue::Bool(false)));
        assert!(five.operate(&OP_LT, &true_val).is_err());
    }

    #[test]
    fn test_operate_relational_lt_or_eq() {
        let two = NodeValue::Int32(2);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(two.operate(&OP_LT_OR_EQ, &two), Ok(NodeValue::Bool(true)));
        assert_eq!(two.operate(&OP_LT_OR_EQ, &five), Ok(NodeValue::Bool(true)));
        assert_eq!(five.operate(&OP_LT_OR_EQ, &two), Ok(NodeValue::Bool(false)));
        assert!(five.operate(&OP_LT_OR_EQ, &true_val).is_err());
    }
}
