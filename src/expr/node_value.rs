// Copyright 2023 Google LLC. All Rights Reserved.

//! Wrapper around a generic value.
//! Should be able to hold all the values in a ConstantLeafNode protobuf
//! message, as well as all the values that a FieldLeafNode can select.

use sdvtelemetry_proto_rust::expression::{
    combination_node::{ArithmeticOperator, LogicalOperator, RelationalOperator},
    constant_leaf_node::Node_value,
    ConstantLeafNode,
};

const EPSILON: f32 = 1e-6;

/// Holds a generic value.
/// TODO(b/288481877): Add list value because FieldLeafNode can extract values
/// from repeated fields.
#[derive(Debug)]
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

#[allow(dead_code, unused_variables)]
impl NodeValue {
    /// Converts a ConstantLeafNode into a NodeValue.
    pub fn from_constant_leaf_node(constant_leaf_node: &ConstantLeafNode) -> NodeValue {
        match &constant_leaf_node.node_value {
            Some(Node_value::BoolValue(val)) => NodeValue::Bool(*val),
            Some(Node_value::Int32Value(val)) => NodeValue::Int32(*val),
            Some(Node_value::Int64Value(val)) => NodeValue::Int64(*val),
            Some(Node_value::FloatValue(val)) => NodeValue::Float(*val),
            Some(Node_value::DoubleValue(val)) => NodeValue::Double(*val),
            _ => NodeValue::None,
        }
    }

    /// Handle logical operations.
    pub fn handle_logical(
        &self,
        op: &LogicalOperator,
        other: &NodeValue,
    ) -> Result<NodeValue, String> {
        if !self.is_bool() || !other.is_bool() {
            return Err("Logical operators must have boolean operands".to_string());
        }
        match op {
            LogicalOperator::AND => Ok(NodeValue::Bool(self.get_bool() && other.get_bool())),
            LogicalOperator::OR => Ok(NodeValue::Bool(self.get_bool() || other.get_bool())),
            LogicalOperator::XOR => Ok(NodeValue::Bool(self.get_bool() != other.get_bool())),
            LogicalOperator::NOT => Ok(NodeValue::Bool(!self.get_bool())),
        }
    }

    /// Handle arithmetic operations.
    pub fn handle_arithmetic(
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
                if other.is_zero() {
                    return Err("Cannot divide by zero".to_string());
                }
                Ok(NodeValue::Double(self.get_double() / other.get_double()))
            }
            ArithmeticOperator::MODULUS => {
                if !self.is_int() || !other.is_int() {
                    return Err("Modulus operator requires integer operands".to_string());
                }
                if other.is_zero() {
                    return Err("Cannot perform mod 0. Divide by 0 is undefined.".to_string());
                }
                Ok(NodeValue::Int64(self.get_int() % other.get_int()))
            }
            ArithmeticOperator::POWER => self.apply_numeric(
                other,
                |a, b| NodeValue::Int64(a.pow(b as u32)),
                |a, b| NodeValue::Double(a.powf(b)),
            ),
        }
    }

    /// Handle relational operations.
    pub fn handle_relational(
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

    /// Returns the int value or 0 by default.
    fn get_int(&self) -> i64 {
        match self {
            NodeValue::Int32(val) => *val as i64,
            NodeValue::Int64(val) => *val,
            _ => 0,
        }
    }

    /// Returns the double value or 0 by default.
    fn get_double(&self) -> f64 {
        match self {
            NodeValue::Int32(val) => *val as f64,
            NodeValue::Int64(val) => *val as f64,
            NodeValue::Float(val) => *val as f64,
            NodeValue::Double(val) => *val,
            _ => 0.0,
        }
    }

    /// Checks whether the node value is zero.
    fn is_zero(&self) -> bool {
        match self {
            NodeValue::Int32(val) => *val == 0,
            NodeValue::Int64(val) => *val == 0,
            NodeValue::Float(val) => (*val).abs() < EPSILON,
            NodeValue::Double(val) => (*val).abs() < EPSILON.into(),
            _ => true, // if not a numeric type, their numeric value is by default 0
        }
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
            Ok(int_func(self.get_int(), other.get_int()))
        } else if self.is_numeric() && other.is_numeric() {
            Ok(float_func(self.get_double(), other.get_double()))
        } else {
            Err("Operator requires numeric operands but incompatible operands were provided"
                .to_string())
        }
    }
}

/// Per go/sdv-rust-testing, unit tests are to be written in the same file.
#[cfg(test)]
mod tests {

    use sdv_node_value::NodeValue;

    use sdvtelemetry_proto_rust::expression::{
        combination_node::{ArithmeticOperator, LogicalOperator, RelationalOperator},
        ConstantLeafNode,
    };

    // Static variables within the tests module scope that are reused across tests.
    static OP_AND: LogicalOperator = LogicalOperator::AND;
    static OP_OR: LogicalOperator = LogicalOperator::OR;
    static OP_XOR: LogicalOperator = LogicalOperator::XOR;
    static OP_NOT: LogicalOperator = LogicalOperator::NOT;
    static OP_ADD: ArithmeticOperator = ArithmeticOperator::ADD;
    static OP_SUBTRACT: ArithmeticOperator = ArithmeticOperator::SUBTRACT;
    static OP_MULTIPLY: ArithmeticOperator = ArithmeticOperator::MULTIPLY;
    static OP_DIVIDE: ArithmeticOperator = ArithmeticOperator::DIVIDE;
    static OP_MODULUS: ArithmeticOperator = ArithmeticOperator::MODULUS;
    static OP_POWER: ArithmeticOperator = ArithmeticOperator::POWER;
    static OP_EQ: RelationalOperator = RelationalOperator::EQ;
    static OP_NOT_EQ: RelationalOperator = RelationalOperator::NOT_EQ;
    static OP_GT: RelationalOperator = RelationalOperator::GT;
    static OP_GT_OR_EQ: RelationalOperator = RelationalOperator::GT_OR_EQ;
    static OP_LT: RelationalOperator = RelationalOperator::LT;
    static OP_LT_OR_EQ: RelationalOperator = RelationalOperator::LT_OR_EQ;

    #[test]
    fn test_from_constant_leaf_node() {
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
            NodeValue::from_constant_leaf_node(&constant_leaf_node_bool),
            NodeValue::Bool(true)
        );
        assert_eq!(
            NodeValue::from_constant_leaf_node(&constant_leaf_node_i32),
            NodeValue::Int32(32)
        );
        assert_eq!(
            NodeValue::from_constant_leaf_node(&constant_leaf_node_i64),
            NodeValue::Int64(64)
        );
        assert_eq!(
            NodeValue::from_constant_leaf_node(&constant_leaf_node_float),
            NodeValue::Float(1.5)
        );
        assert_eq!(
            NodeValue::from_constant_leaf_node(&constant_leaf_node_double),
            NodeValue::Double(1.5)
        );
    }

    #[test]
    fn test_handle_logical_requires_boolean_types() {
        let true_val = NodeValue::Bool(true);
        let int_val = NodeValue::Int32(1);

        assert!(true_val.handle_logical(&OP_AND, &int_val).is_err());
        assert!(int_val.handle_logical(&OP_AND, &true_val).is_err());
    }

    #[test]
    fn test_handle_logical_and() {
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        // true && true = true
        assert_eq!(true_val.handle_logical(&OP_AND, &true_val), Ok(NodeValue::Bool(true)));
        // true && false = false
        assert_eq!(true_val.handle_logical(&OP_AND, &false_val), Ok(NodeValue::Bool(false)));
        // false && false = false
        assert_eq!(false_val.handle_logical(&OP_AND, &false_val), Ok(NodeValue::Bool(false)));
    }

    #[test]
    fn test_handle_logical_or() {
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        // true || true = true
        assert_eq!(true_val.handle_logical(&OP_OR, &true_val), Ok(NodeValue::Bool(true)));
        // true || false = true
        assert_eq!(true_val.handle_logical(&OP_OR, &false_val), Ok(NodeValue::Bool(true)));
        // false || false = false
        assert_eq!(false_val.handle_logical(&OP_OR, &false_val), Ok(NodeValue::Bool(false)));
    }

    #[test]
    fn test_handle_logical_xor() {
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        // true XOR true = false
        assert_eq!(true_val.handle_logical(&OP_XOR, &true_val), Ok(NodeValue::Bool(false)));
        // true XOR false = true
        assert_eq!(true_val.handle_logical(&OP_XOR, &false_val), Ok(NodeValue::Bool(true)));
        // false XOR false = false
        assert_eq!(false_val.handle_logical(&OP_XOR, &false_val), Ok(NodeValue::Bool(false)));
    }

    #[test]
    fn test_handle_logical_not() {
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        // !true = false
        assert_eq!(true_val.handle_logical(&OP_NOT, &NodeValue::None), Ok(NodeValue::Bool(false)));
        // !false = true
        assert_eq!(false_val.handle_logical(&OP_NOT, &NodeValue::None), Ok(NodeValue::Bool(true)));
    }

    #[test]
    fn test_handle_arithmetic_requires_numeric_types() {
        let true_val = NodeValue::Bool(true);
        let int_val = NodeValue::Int32(1);

        assert!(true_val.handle_arithmetic(&OP_ADD, &int_val).is_err());
        assert!(int_val.handle_arithmetic(&OP_ADD, &true_val).is_err());
    }

    #[test]
    fn test_handle_arithmetic_add_integers() {
        let five = NodeValue::Int32(5);
        let twenty = NodeValue::Int32(20);

        // 5 + 20 = 25
        assert_eq!(five.handle_arithmetic(&OP_ADD, &twenty), Ok(NodeValue::Int64(25)));
    }

    #[test]
    fn test_handle_arithmetic_add_floats() {
        let five = NodeValue::Float(5.2);
        let twenty = NodeValue::Double(20.1);

        // 5.2 + 20.1 = 25.3
        assert_eq!(five.handle_arithmetic(&OP_ADD, &twenty), Ok(NodeValue::Double(25.3)));
    }

    #[test]
    fn test_handle_arithmetic_subtract_integers() {
        let five = NodeValue::Int32(5);
        let twenty = NodeValue::Int32(20);

        // 5 - 20 = -15
        assert_eq!(five.handle_arithmetic(&OP_SUBTRACT, &twenty), Ok(NodeValue::Int64(-15)));
    }

    #[test]
    fn test_handle_arithmetic_subtract_floats() {
        let five = NodeValue::Float(5.2);
        let twenty = NodeValue::Double(20.1);

        // 5.2 - 20.1 = -14.9
        assert_eq!(five.handle_arithmetic(&OP_SUBTRACT, &twenty), Ok(NodeValue::Double(-14.9)));
    }

    #[test]
    fn test_handle_arithmetic_multiply() {
        let five = NodeValue::Int32(5);
        let point_five = NodeValue::Float(0.5);

        // 5 * 0.5 = 2.5
        assert_eq!(five.handle_arithmetic(&OP_MULTIPLY, &point_five), Ok(NodeValue::Double(2.5)));
    }

    #[test]
    fn test_handle_arithmetic_divide_by_zero() {
        let five = NodeValue::Int32(5);
        let zero = NodeValue::Int32(0);

        assert!(five.handle_arithmetic(&OP_DIVIDE, &zero).is_err());
    }

    #[test]
    fn test_handle_arithmetic_divide() {
        let five = NodeValue::Int32(5);
        let two = NodeValue::Int32(2);

        // 5 / 2 = 2.5
        assert_eq!(five.handle_arithmetic(&OP_DIVIDE, &two), Ok(NodeValue::Double(2.5)));
    }

    #[test]
    fn test_handle_arithmetic_modulus_by_zero() {
        let five = NodeValue::Int32(5);
        let zero = NodeValue::Int32(0);

        assert!(five.handle_arithmetic(&OP_MODULUS, &zero).is_err());
    }

    #[test]
    fn test_handle_arithmetic_modulus() {
        let five = NodeValue::Int32(5);
        let two = NodeValue::Int32(2);

        // 5 % 2 = 1
        assert_eq!(five.handle_arithmetic(&OP_MODULUS, &two), Ok(NodeValue::Int64(1)));
    }

    #[test]
    fn test_handle_arithmetic_modulus_no_floating_points() {
        let five = NodeValue::Float(5.0);
        let two = NodeValue::Int32(2);

        // both operands must be integers
        assert!(five.handle_arithmetic(&OP_MODULUS, &two).is_err());
    }

    #[test]
    fn test_handle_arithmetic_power_integers() {
        let two = NodeValue::Int32(2);

        // 2^2 = 4
        assert_eq!(two.handle_arithmetic(&OP_POWER, &two), Ok(NodeValue::Int64(4)));
    }

    #[test]
    fn test_handle_arithmetic_power_floats() {
        let val = 2.5_f64;
        let expected = val.powf(val);
        let two_point_five = NodeValue::Double(val);

        assert_eq!(
            two_point_five.handle_arithmetic(&OP_POWER, &two_point_five),
            Ok(NodeValue::Double(expected))
        );
    }

    #[test]
    fn test_handle_relational_eq() {
        let two = NodeValue::Int32(2);
        let two_i64 = NodeValue::Int64(2);
        let two_f32 = NodeValue::Float(2.0);
        let two_f64 = NodeValue::Double(2.0);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(two.handle_relational(&OP_EQ, &two), Ok(NodeValue::Bool(true)));
        assert_eq!(two.handle_relational(&OP_EQ, &two_i64), Ok(NodeValue::Bool(true)));
        assert_eq!(two.handle_relational(&OP_EQ, &two_f32), Ok(NodeValue::Bool(true)));
        assert_eq!(two.handle_relational(&OP_EQ, &two_f64), Ok(NodeValue::Bool(true)));
        assert_eq!(two.handle_relational(&OP_EQ, &five), Ok(NodeValue::Bool(false)));
        assert_eq!(two.handle_relational(&OP_EQ, &true_val), Ok(NodeValue::Bool(false)));
    }

    #[test]
    fn test_handle_relational_not_eq() {
        let two = NodeValue::Int32(2);
        let two_f32 = NodeValue::Float(2.0);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(two.handle_relational(&OP_NOT_EQ, &two), Ok(NodeValue::Bool(false)));
        assert_eq!(two.handle_relational(&OP_NOT_EQ, &two_f32), Ok(NodeValue::Bool(false)));
        assert_eq!(two.handle_relational(&OP_NOT_EQ, &five), Ok(NodeValue::Bool(true)));
        assert_eq!(two.handle_relational(&OP_NOT_EQ, &true_val), Ok(NodeValue::Bool(true)));
    }

    #[test]
    fn test_handle_relational_gt() {
        let two = NodeValue::Int32(2);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(two.handle_relational(&OP_GT, &two), Ok(NodeValue::Bool(false)));
        assert_eq!(two.handle_relational(&OP_GT, &five), Ok(NodeValue::Bool(false)));
        assert_eq!(five.handle_relational(&OP_GT, &two), Ok(NodeValue::Bool(true)));
        assert!(five.handle_relational(&OP_GT, &true_val).is_err());
    }

    #[test]
    fn test_handle_relational_gt_or_eq() {
        let two = NodeValue::Int32(2);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(two.handle_relational(&OP_GT_OR_EQ, &two), Ok(NodeValue::Bool(true)));
        assert_eq!(two.handle_relational(&OP_GT_OR_EQ, &five), Ok(NodeValue::Bool(false)));
        assert_eq!(five.handle_relational(&OP_GT_OR_EQ, &two), Ok(NodeValue::Bool(true)));
        assert!(five.handle_relational(&OP_GT_OR_EQ, &true_val).is_err());
    }

    #[test]
    fn test_handle_relational_lt() {
        let two = NodeValue::Int32(2);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(two.handle_relational(&OP_LT, &two), Ok(NodeValue::Bool(false)));
        assert_eq!(two.handle_relational(&OP_LT, &five), Ok(NodeValue::Bool(true)));
        assert_eq!(five.handle_relational(&OP_LT, &two), Ok(NodeValue::Bool(false)));
        assert!(five.handle_relational(&OP_LT, &true_val).is_err());
    }

    #[test]
    fn test_handle_relational_lt_or_eq() {
        let two = NodeValue::Int32(2);
        let five = NodeValue::Float(5.5);
        let true_val = NodeValue::Bool(true);

        assert_eq!(two.handle_relational(&OP_LT_OR_EQ, &two), Ok(NodeValue::Bool(true)));
        assert_eq!(two.handle_relational(&OP_LT_OR_EQ, &five), Ok(NodeValue::Bool(true)));
        assert_eq!(five.handle_relational(&OP_LT_OR_EQ, &two), Ok(NodeValue::Bool(false)));
        assert!(five.handle_relational(&OP_LT_OR_EQ, &true_val).is_err());
    }
}
