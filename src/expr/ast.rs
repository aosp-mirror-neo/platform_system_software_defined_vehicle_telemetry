// Copyright 2023 Google LLC. All Rights Reserved.

//! Library for building and traversing the abstract syntax tree.

use log::info;
use sdv_node_value::NodeValue;
use sdvtelemetry_proto_rust::expression::{
    combination_node::LogicalOperator, node::Node_type, CombinationNode as CombinationNodeProto,
    Node as NodeProto,
};
use std::{cell::RefCell, rc::Rc};

/// A wrapper around a protobuf Node.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Node<'a> {
    proto_node: &'a NodeProto,
    // Option<T> means the variable can either hold nothing or T. Rust
    // variables cannot be null by default so we use Option<T>.
    // Rc<RefCell<T>> is to explicitly allow Nodes to reference each other.
    // Rc<T> means it is holding reference-counted pointer to T. You
    // cannot modify the object T in a Rc<T>, so we use Rc<RefCell<T>>
    // RefCell<T> means you can modify the object T.
    left: Option<Rc<RefCell<Node<'a>>>>,
    right: Option<Rc<RefCell<Node<'a>>>>,
}

impl<'a> Node<'a> {
    /// Convert protobuf Nodes into rust wrapper Nodes.
    /// Protobuf nodes use indices to represent left and right children.
    /// Rust nodes have a pointer to the actual node instead of an index.
    pub fn convert_nodes(proto_nodes: &'a [NodeProto]) -> Result<Vec<Rc<RefCell<Node>>>, String> {
        // Iterate over protobuf nodes and create rust wrapper nodes
        let rust_nodes: Vec<Rc<RefCell<Node>>> = proto_nodes
            .iter()
            .map(|proto_node| {
                Rc::new(RefCell::new(Node {
                    proto_node, // short hand for `proto_node: proto_node,`
                    left: None, // children to be initialized in the next loop
                    right: None,
                }))
            })
            .collect();

        // This loop sets up references to child nodes
        // TODO(b/289295418): type check the child nodes to ensure they have compatible types
        for (i, rust_node) in rust_nodes.iter().enumerate() {
            let proto_node = rust_node.borrow().proto_node;

            // We only care about non-leaf nodes because they have children
            // if-let syntax matches 1 enum variant - CombinationNode variant,
            // the enum is a generated type from the protobuf codegen
            if let Some(Node_type::CombinationNode(pb_combination_node)) = &proto_node.node_type {
                // set up reference to left child
                match pb_combination_node.left_index {
                    Some(left_index) => {
                        // `rust_nodes.get()` returns Option<T> enum
                        // Use if-let syntax to match the `Some` enum variant from the
                        // Option<T> enum
                        if let Some(left_node_ref) = rust_nodes.get(left_index as usize) {
                            // cloning Rc<T> just increments the reference count, the
                            // underlying node it points to is not cloned.
                            rust_node.borrow_mut().left = Some(left_node_ref.clone());
                        } else {
                            // rust_nodes.get() returns None, bad AST
                            return Err(format!(
                                "Bad AST. Left child index is invalid for node at index {} = {}",
                                i, proto_node
                            ));
                        }
                    }
                    None => {
                        return Err(format!("Bad AST. CombinationNode should always specify the left child. Node causing error is: {}", proto_node));
                    }
                }

                // set up reference to right child
                match pb_combination_node.right_index {
                    Some(right_index) => {
                        // nodes with 2 children must have binary operators
                        if Self::has_unary_operator(pb_combination_node) {
                            return Err(format!(
                                "Node {} should not specify right child because operator is unary",
                                proto_node
                            ));
                        }
                        // set up references to right child
                        if let Some(right_node_ref) = rust_nodes.get(right_index as usize) {
                            // set correct reference to right child
                            rust_node.borrow_mut().right = Some(right_node_ref.clone());
                        } else {
                            return Err(format!(
                                "Bad AST. Right child index is invalid for node at index {} = {}",
                                i, proto_node
                            ));
                        }
                    }
                    None => {
                        // if binary operator, right child should always be specified
                        if !Self::has_unary_operator(pb_combination_node) {
                            return Err(format!(
                                "Bad AST. Protobuf node did not specify right child for binary operator: {}",
                                proto_node
                            ));
                        }
                    }
                }
            }
        }
        Ok(rust_nodes)
    }

    /// Evaluate AST recursively using pre-order traversal.
    pub fn evaluate(&self) -> Result<NodeValue, String> {
        // Base case of recursion: node is a leaf node, return value
        match &self.proto_node.node_type {
            Some(Node_type::ConstantLeafNode(constant_leaf_node)) => {
                return Ok(NodeValue::from_constant_leaf_node(constant_leaf_node));
            }
            Some(Node_type::FieldLeafNode(_)) => {
                return Err("Not implemented".to_string());
            }
            _ => {
                info!("Node is a not a leaf node, will recursively evaluate its children");
            }
        }

        let proto_combination_node = self.proto_node.combination_node();

        // evaluate left branch recursively
        let left_val = match &self.left {
            Some(left) => left.borrow().evaluate(),
            None => Err("Incorrect Node! Every non-leaf Node should have a left child".to_string()),
        }?; // `?` returns early if error
        info!("left val = {:?}", left_val);

        // evaluate right branch recursively
        let right_val = match &self.right {
            Some(right) => right.borrow().evaluate(),
            None => {
                if Self::has_unary_operator(proto_combination_node) {
                    return Ok(NodeValue::None);
                }
                return Err("Incorrect AST, right child should always be specified for nodes with binary operators.".to_string());
            }
        }?; // `?` returns early if error
        info!("right val = {:?}", right_val);

        // If unary operator, then operates on self.
        // If binary operator, then [left] <operator> [right].
        if proto_combination_node.has_logical_operator() {
            left_val.handle_logical(&proto_combination_node.logical_operator(), &right_val)
        } else if proto_combination_node.has_arithmetic_operator() {
            left_val.handle_arithmetic(&proto_combination_node.arithmetic_operator(), &right_val)
        } else {
            left_val.handle_relational(&proto_combination_node.relational_operator(), &right_val)
        }
    }

    /// Helper function to check if protobuf CombinationNode has unary operator.
    fn has_unary_operator(node: &CombinationNodeProto) -> bool {
        node.has_logical_operator() && node.logical_operator() == LogicalOperator::NOT
    }
}

/// Per go/sdv-rust-testing, unit tests are to be written in the same file.
#[cfg(test)]
mod tests {
    use super::Node;
    use sdv_node_value::NodeValue;
    use sdvtelemetry_proto_rust::expression::{
        combination_node::LogicalOperator, CombinationNode, ConstantLeafNode, Node as NodeProto,
    };

    /// Helper function that returns this tree:
    ///            index 2 (OPERATOR)
    ///          /              \
    ///    index 0              index 1
    ///    TRUE                  FALSE
    fn create_true_false_branch(op: LogicalOperator) -> Vec<NodeProto> {
        // setup
        let mut leaf_true = NodeProto::new();
        let mut leaf_false = NodeProto::new();
        let mut root_node = NodeProto::new();

        let mut constant_leaf = ConstantLeafNode::new();
        constant_leaf.set_bool_value(true);
        leaf_true.set_constant_leaf_node(constant_leaf.clone());
        constant_leaf.set_bool_value(false);
        leaf_false.set_constant_leaf_node(constant_leaf.clone());

        let mut combination_node = CombinationNode::new();
        combination_node.left_index = Some(0);
        combination_node.right_index = Some(1);
        combination_node.set_logical_operator(op);
        root_node.set_combination_node(combination_node);

        vec![leaf_true, leaf_false, root_node]
    }

    #[test]
    fn test_convert_nodes_ok() {
        // setup
        let all_proto_nodes = create_true_false_branch(LogicalOperator::AND);

        // action
        let converted_nodes = Node::convert_nodes(&all_proto_nodes);

        // assert
        assert!(converted_nodes.is_ok());
        let converted_nodes = converted_nodes.ok().unwrap();
        // assert 1-1 correspondence between rust node and protobuf node
        for (i, rust_node_ref) in converted_nodes.iter().enumerate() {
            let rust_node = rust_node_ref.borrow();
            assert_eq!(*rust_node.proto_node, all_proto_nodes[i]);
            if rust_node.proto_node.has_combination_node() {
                assert_eq!(
                    *rust_node.left.as_ref().unwrap(),
                    converted_nodes
                        [rust_node.proto_node.combination_node().left_index.unwrap() as usize]
                );
                assert_eq!(
                    *rust_node.right.as_ref().unwrap(),
                    converted_nodes
                        [rust_node.proto_node.combination_node().right_index.unwrap() as usize]
                );
            } else {
                assert_eq!(rust_node.left, None);
                assert_eq!(rust_node.right, None);
            }
        }
    }

    #[test]
    fn test_convert_nodes_unary_operator_cannot_have_two_children() {
        // setup - root node has 2 child nodes but unary operator (NOT)
        let all_proto_nodes = create_true_false_branch(LogicalOperator::NOT);

        // action
        let converted_nodes = Node::convert_nodes(&all_proto_nodes);

        // assert
        assert!(converted_nodes.is_err());
    }

    #[test]
    fn test_convert_nodes_handles_missing_right_child() {
        // setup
        let mut leaf_true = NodeProto::new();
        let mut root_node = NodeProto::new();

        let mut constant_leaf = ConstantLeafNode::new();
        constant_leaf.set_bool_value(true);
        leaf_true.set_constant_leaf_node(constant_leaf.clone());

        //            index 1 (NOT)
        //          /              \
        //    index 0              no right child
        //    TRUE
        let mut combination_node = CombinationNode::new();
        combination_node.left_index = Some(0);
        combination_node.set_logical_operator(LogicalOperator::NOT);
        root_node.set_combination_node(combination_node.clone());

        let all_proto_nodes = vec![leaf_true.clone(), root_node.clone()];

        // action
        let converted_nodes = Node::convert_nodes(&all_proto_nodes);

        // assert ok because unary operator
        assert!(converted_nodes.is_ok());

        // if binary operator and right child is missing
        combination_node.set_logical_operator(LogicalOperator::AND);
        root_node.set_combination_node(combination_node);
        let all_proto_nodes = vec![leaf_true, root_node];

        // action
        let converted_nodes = Node::convert_nodes(&all_proto_nodes);

        // assert error because binary operator needs 2 children
        assert!(converted_nodes.is_err());
    }

    #[test]
    fn test_evaluate_and_or_ok() {
        // setup this tree:
        //            index 3 (AND)
        //          /              \
        //    index 0              index 2 (output of function `create_true_false_branch`)
        //    TRUE                  OR
        //                       /        \
        //               index 0 (T)      index 1 (F)
        // index 4 (AND), left child = index 1 (FALSE), right child = index 2
        let mut all_proto_nodes = create_true_false_branch(LogicalOperator::OR);
        let mut root_node = NodeProto::new();
        let mut combination_node = CombinationNode::new();
        combination_node.left_index = Some(0); // TRUE
        combination_node.right_index = Some(2); // TRUE || FALSE
                                                // operator = AND, combination node is T && (T || F)
        combination_node.set_logical_operator(LogicalOperator::AND);
        root_node.set_combination_node(combination_node.clone());
        all_proto_nodes.push(root_node.clone());

        combination_node.left_index = Some(1); // FALSE
                                               // combination node is  F && (T || F)
        root_node.set_combination_node(combination_node.clone());
        all_proto_nodes.push(root_node.clone());

        let converted_nodes = Node::convert_nodes(&all_proto_nodes).unwrap();

        // we will evaluate every node in `converted_nodes`, and we expect these results:
        let expected_results = vec![
            NodeValue::Bool(true),  // index 0 = constant value of T
            NodeValue::Bool(false), // index 1 = constant value of F
            NodeValue::Bool(true),  // index 2 = T || F = T
            NodeValue::Bool(true),  // index 3 = T && (T || F) = T
            NodeValue::Bool(false), // index 4 = F && (T || F)  = F
        ];

        for (i, rust_node_ref) in converted_nodes.iter().enumerate() {
            assert_eq!(rust_node_ref.borrow().evaluate().unwrap(), expected_results[i]);
        }
    }
}
