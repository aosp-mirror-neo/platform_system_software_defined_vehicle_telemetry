// Copyright 2023 Google LLC. All Rights Reserved.

//! Wrapper around a generic value.
//! Should be able to hold all the values in a ConstantLeafNode protobuf
//! message, as well as all the values that a FieldLeafNode can select.

use log::info;
use sdvtelemetry_proto_rust::expression::{
    combination_node::{ArithmeticOperator, LogicalOperator, RelationalOperator},
    constant_leaf_node::Node_value,
    ConstantLeafNode,
};

/// Holds a generic value.
/// TODO(b/288481877): Add list value because FieldLeafNode can extract values
/// from repeated fields.
#[derive(Debug, PartialEq)]
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

    /// A string node value.
    String(String),
}

#[allow(dead_code, unused_variables)]
impl NodeValue {
    /// Converts a ConstantLeafNode into a NodeValue.
    pub fn from_constant_leaf_node(constant_leaf_node: &ConstantLeafNode) -> NodeValue {
        match constant_leaf_node.node_value {
            Some(Node_value::BoolValue(val)) => NodeValue::Bool(val),
            Some(Node_value::Int32Value(val)) => NodeValue::Int32(val),
            _ => NodeValue::String("Not implemented".to_string()),
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
            ArithmeticOperator::ADD => {
                info!("Only adding 32 bit integers right now");
                Ok(NodeValue::Int32(self.get_int32() + other.get_int32()))
            }
            _ => Err("Other arithmetic operators are not implemented yet".to_string()),
        }
    }

    /// Handle relational operations.
    pub fn handle_relational(
        &self,
        op: &RelationalOperator,
        other: &NodeValue,
    ) -> Result<NodeValue, String> {
        Err("Relational operators are not implemented yet".to_string())
    }

    // private functions and methods below

    /// Check if node holds a numeric value.
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            NodeValue::Int32(_) | NodeValue::Int64(_) | NodeValue::Float(_) | NodeValue::Double(_)
        )
    }

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

    /// Returns the int32 value or 0 by default.
    fn get_int32(&self) -> i32 {
        if let NodeValue::Int32(val) = self {
            return *val;
        }
        0
    }
}

/// Per go/sdv-rust-testing, unit tests are to be written in the same file.
#[cfg(test)]
mod tests {

    use sdv_node_value::NodeValue;

    use sdvtelemetry_proto_rust::expression::{
        combination_node::{ArithmeticOperator, LogicalOperator},
        ConstantLeafNode,
    };

    #[test]
    fn test_from_constant_leaf_node() {
        let mut constant_leaf_node_i32 = ConstantLeafNode::new();
        constant_leaf_node_i32.set_int32_value(32);

        assert_eq!(
            NodeValue::from_constant_leaf_node(&constant_leaf_node_i32),
            NodeValue::Int32(32)
        );

        let mut constant_leaf_node_bool = ConstantLeafNode::new();
        constant_leaf_node_bool.set_bool_value(true);
        assert_eq!(
            NodeValue::from_constant_leaf_node(&constant_leaf_node_bool),
            NodeValue::Bool(true)
        );

        let mut constant_leaf_node_float = ConstantLeafNode::new();
        constant_leaf_node_float.set_float_value(1.555);
        // not implemented
        assert_eq!(
            NodeValue::from_constant_leaf_node(&constant_leaf_node_float),
            NodeValue::String("Not implemented".to_string())
        );
    }

    #[test]
    fn test_handle_logical_requires_boolean_types() {
        let op = LogicalOperator::AND;
        let true_val = NodeValue::Bool(true);
        let int_val = NodeValue::Int32(1);

        assert!(true_val.handle_logical(&op, &int_val).is_err());
        assert!(int_val.handle_logical(&op, &true_val).is_err());
    }

    #[test]
    fn test_handle_logical_and() {
        let op = LogicalOperator::AND;
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        // true && true = true
        assert_eq!(true_val.handle_logical(&op, &true_val), Ok(NodeValue::Bool(true)));
        // true && false = false
        assert_eq!(true_val.handle_logical(&op, &false_val), Ok(NodeValue::Bool(false)));
        // false && false = false
        assert_eq!(false_val.handle_logical(&op, &false_val), Ok(NodeValue::Bool(false)));
    }

    #[test]
    fn test_handle_logical_or() {
        let op = LogicalOperator::OR;
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        // true || true = true
        assert_eq!(true_val.handle_logical(&op, &true_val), Ok(NodeValue::Bool(true)));
        // true || false = true
        assert_eq!(true_val.handle_logical(&op, &false_val), Ok(NodeValue::Bool(true)));
        // false || false = false
        assert_eq!(false_val.handle_logical(&op, &false_val), Ok(NodeValue::Bool(false)));
    }

    #[test]
    fn test_handle_logical_xor() {
        let op = LogicalOperator::XOR;
        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        // true XOR true = false
        assert_eq!(true_val.handle_logical(&op, &true_val), Ok(NodeValue::Bool(false)));
        // true XOR false = true
        assert_eq!(true_val.handle_logical(&op, &false_val), Ok(NodeValue::Bool(true)));
        // false XOR false = false
        assert_eq!(false_val.handle_logical(&op, &false_val), Ok(NodeValue::Bool(false)));
    }

    #[test]
    fn test_handle_logical_not() {
        let op = LogicalOperator::NOT;

        let true_val = NodeValue::Bool(true);
        let false_val = NodeValue::Bool(false);

        // !true = false
        assert_eq!(true_val.handle_logical(&op, &NodeValue::None), Ok(NodeValue::Bool(false)));
        // !false = true
        assert_eq!(false_val.handle_logical(&op, &NodeValue::None), Ok(NodeValue::Bool(true)));
    }

    #[test]
    fn test_handle_arithmetic_add() {
        let op = ArithmeticOperator::ADD;
        let five = NodeValue::Int32(5);
        let twenty = NodeValue::Int32(20);

        // 5 + 20 = 25
        assert_eq!(five.handle_arithmetic(&op, &twenty), Ok(NodeValue::Int32(25)));
    }
}
