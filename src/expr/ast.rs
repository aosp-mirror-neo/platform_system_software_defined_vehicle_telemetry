// Copyright 2023 Google LLC. All Rights Reserved.

//! Library for building and traversing the abstract syntax tree.

mod node_value;

pub use node_value::NodeValue;

use protobuf::MessageDyn;
use sdvtelemetry_proto_rust::expression::{
    combination_node::LogicalOperator, combination_node::Operator as OperatorProto,
    node::Node_type as NodeTypeProto, CombinationNode as CombinationNodeProto, Node as NodeProto,
};
use std::{
    fmt,
    marker::{Send, Sync},
    sync::Arc,
};

const UNKNOWN_NODE_TYPE_MSG: &str = "Node did not specify a node_type";

/// Creates rust nodes from protobuf expression nodes.
pub trait NodeFactory {
    /// Creates rust Node from protobuf Node.
    /// Protobuf nodes use indices to represent left and right children.
    /// Rust nodes have a pointer to the actual children instead of an index.
    fn create(
        &self,
        proto_node: &NodeProto,
        context: &dyn NodeFactoryContext,
    ) -> Result<Node, String>;

    /// Validate the expression node to ensure it's possible to create a Rust node from it.
    fn validate(&self, proto_node: &NodeProto) -> Result<(), String>;
}

/// Provides context of the creation operation.
pub trait NodeFactoryContext {
    /// Creates a function that provides the latest message of the specified publisher.
    fn create_message_provider(
        &self,
        publisher_name: &str,
    ) -> Box<dyn Fn() -> Box<dyn MessageDyn> + Send + Sync>;

    /// Returns rust Node at a given index.
    fn get_node(&self, index: usize) -> &Arc<Node>;
}

/// Rust representation of a protobuf Node.
/// The enum variants have 1-1 correspondence with a protobuf Node.
/// For detailed explanation, see expression.proto.
pub enum Node {
    /// Corresponds to ConstantLeafNode message.
    ConstantLeaf {
        /// Constant value.
        value: NodeValue,
    },
    /// Leaf node that is the field value of a published message.
    FieldLeaf {
        /// Name of the publisher producing the data.
        publisher_name: String,

        /// Field names indicating which value to read.
        fields: Vec<String>,

        /// Function that returns the latest published message.
        message_provider: Box<dyn Fn() -> Box<dyn MessageDyn> + Send + Sync>,
    },
    /// Non-leaf variant holding a value computed from left and right children.
    /// Protobuf Nodes use index to represent left and right child nodes, Rust Nodes
    /// have direct references to the child nodes.
    Combination {
        /// Operator used to compute a value between left and right child nodes.
        operator: OperatorProto,

        /// Reference to the left child.
        /// Option<T> means the variable can either hold nothing or T. Rust
        /// variables cannot be null by default so we use Option<T>.
        /// Arc<T> means it is holding an atomic reference-counted pointer to T.
        left: Option<Arc<Node>>,

        /// Reference to the right child.
        right: Option<Arc<Node>>,
    },
}

/// Implementation of NodeFactory trait.
pub struct NodeFactoryImpl {}

impl NodeFactory for NodeFactoryImpl {
    fn create(
        &self,
        proto_node: &NodeProto,
        context: &dyn NodeFactoryContext,
    ) -> Result<Node, String> {
        // The `match` block creates a Rust Node enum and returns it.
        // TODO(b/289295418): type check the child nodes to ensure they have compatible types
        Ok(match &proto_node.node_type {
            Some(NodeTypeProto::ConstantLeafNode(constant_leaf)) => {
                // base case 1: constant leaf node, simply create Node and return
                Node::ConstantLeaf { value: NodeValue::from_constant_leaf_node(constant_leaf)? }
            }
            Some(NodeTypeProto::FieldLeafNode(field_leaf)) => {
                // base case 2: field leaf node, simply create Node and return
                Node::FieldLeaf {
                    publisher_name: field_leaf.publisher_name.clone(),
                    fields: field_leaf.field_names.clone(),
                    message_provider: context.create_message_provider(&field_leaf.publisher_name),
                }
            }
            Some(NodeTypeProto::CombinationNode(combination_node)) => {
                // ensure combination node specified an operator
                let op = combination_node.get_operator()?;

                // retrieve child nodes using indices. they must already exist
                let (left_idx, right_idx) = combination_node.get_child_indices(op)?;
                let left_child = context.get_node(left_idx).clone();
                let right_child = right_idx.map(|i| context.get_node(i).clone());

                // create Node of Combination variant for the protobuf node
                Node::Combination {
                    operator: op.clone(),
                    left: Some(left_child),
                    right: right_child,
                }
            }
            _ => {
                return Err(UNKNOWN_NODE_TYPE_MSG.to_owned());
            }
        })
    }

    fn validate(&self, proto_node: &NodeProto) -> Result<(), String> {
        match &proto_node.node_type {
            Some(NodeTypeProto::ConstantLeafNode(_)) | Some(NodeTypeProto::FieldLeafNode(_)) => {
                Ok(())
            }
            Some(NodeTypeProto::CombinationNode(combination_node)) => {
                // ensure combination node specified an operator
                let op = combination_node.get_operator()?;

                // verify required indices are present
                combination_node.get_child_indices(op)?;

                Ok(())
            }
            _ => Err(UNKNOWN_NODE_TYPE_MSG.to_owned()),
        }
    }
}

impl Node {
    /// Evaluate AST recursively.
    pub fn evaluate(&self) -> Result<NodeValue, String> {
        match &self {
            Node::ConstantLeaf { value } => {
                // Base case of recursion: node is a leaf node, return value
                Ok(value.clone())
            }
            Node::FieldLeaf { message_provider, fields, .. } => {
                // Base case of recursion: node is a leaf node, return value
                let msg = message_provider();
                NodeValue::extract_value(msg, fields)
            }
            Node::Combination { operator, left, right } => {
                // evaluate left branch recursively
                let left_val =
                    match left {
                        Some(left) => left.evaluate(),
                        None => Err("Incorrect Node! Every non-leaf Node should have a left child"
                            .to_string()),
                    }?; // `?` returns early if error

                // If unary operator, then operates on self.
                // If binary operator, then [left] <operator> [right]
                // The function short-circuits if possible. The closure provides right operand.
                left_val.operate(operator, || {
                    match right {
                        Some(right) => right.evaluate(),
                        None => {
                            if operator.is_unary_operator() {
                                return Ok(NodeValue::None);
                            }
                            Err("Incorrect AST, right child should always be specified for nodes with binary operators.".to_string())
                        }
                    }
                })
            }
        }
    }
}

/// Pretty print Node's debug string, reducing clutter caused by Option<Arc<T>>.
impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            Node::ConstantLeaf { value } => {
                write!(f, "Constant Leaf Node {{ value: {:?} }}", value)
            }
            Node::FieldLeaf { publisher_name, fields, .. } => write!(
                f,
                "Field Leaf Node {{ publisher_name: {}, fields: {:?} }}",
                publisher_name, fields
            ),
            Node::Combination { operator, left, right } => {
                let left_string = if let Some(left_child_ref) = left {
                    format!("{:#?}", left_child_ref)
                } else {
                    "Empty".to_string()
                };
                let right_string = if let Some(right_child_ref) = right {
                    format!("{:#?}", right_child_ref)
                } else {
                    "Empty".to_string()
                };
                write!(
                    f,
                    "Combination Node {{ operator: {:?},  left: {},  right: {} }}",
                    operator, left_string, right_string
                )
            }
        }
    }
}

trait OperatorProtoExtension {
    /// Helper function to check if operator is unary.
    fn is_unary_operator(&self) -> bool;

    /// Helper function to check if operator requires a right child.
    fn check_right_child(&self, right_index: &Option<u32>) -> Result<(), String>;
}

impl OperatorProtoExtension for OperatorProto {
    fn is_unary_operator(&self) -> bool {
        if let OperatorProto::LogicalOperator(op) = self {
            return op.enum_value_or_default() == LogicalOperator::NOT;
        }
        false
    }

    fn check_right_child(&self, right_index: &Option<u32>) -> Result<(), String> {
        let unary_op = self.is_unary_operator();
        let has_right_child = right_index.is_some();
        if unary_op && has_right_child {
            Err("Node has unary operator but specified two children".to_owned())
        } else if !unary_op && !has_right_child {
            Err("Node at has binary operator but did not specify right child".to_owned())
        } else {
            Ok(())
        }
    }
}

trait CombinationNodeProtoExtension {
    fn get_child_indices(&self, op: &OperatorProto) -> Result<(usize, Option<usize>), String>;

    fn get_operator(&self) -> Result<&OperatorProto, String>;
}

impl CombinationNodeProtoExtension for CombinationNodeProto {
    fn get_child_indices(&self, op: &OperatorProto) -> Result<(usize, Option<usize>), String> {
        let left_idx =
            self.left_index.ok_or_else(|| "Node did not specify left child.".to_owned())? as usize;
        // check if operator type requires a right child
        op.check_right_child(&self.right_index)?;
        Ok((left_idx, self.right_index.map(|idx| idx as usize)))
    }

    fn get_operator(&self) -> Result<&OperatorProto, String> {
        self.operator.as_ref().ok_or_else(|| "Node did not specify operator".to_owned())
    }
}

/// Per go/sdv-rust-testing, unit tests are to be written in the same file.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeValue;
    use lazy_static::lazy_static;
    use sdv_telem_expr_test_proto::test_message::{TestEnum, TestMessage, TestNestedMessage};
    use sdvtelemetry_proto_rust::expression::{
        combination_node::{
            ArithmeticOperator, LogicalOperator, Operator as OperatorProto, RelationalOperator,
        },
        CombinationNode, ConstantLeafNode, FieldLeafNode, Node as NodeProto,
    };
    use std::sync::Arc;

    // Use static variable to store the protobuf message that will be returned by the
    // `provide_test_message` function, which is an argument to `evaluate()`.
    // Cannot use local variable because Rust compiler complains that the proto msg
    // returned by the closure does not live long enough, so we must give it a
    // static lifetime to ensure it outlives evaluate().
    // Lazy static is used because Node::new() does not return a 'static lifetime, so its
    // return value cannot be stored in a static variable. Hence we use lazy_static
    lazy_static! {
        static ref TEST_MESSAGE: TestMessage = {
            let mut test_message = TestMessage::new();
            test_message.int_field = 80;
            test_message.float_field = 100.0;
            test_message.string_field = "Hello world".to_string();
            test_message.enum_field = TestEnum::SOME.into();

            let mut test_nested_message = TestNestedMessage::new();
            test_nested_message.bool_field = true;
            test_nested_message.repeated_i32s = vec![1, 2];
            test_message.message_field = protobuf::MessageField::some(test_nested_message);

            test_message.repeated_doubles =
                vec![std::f64::consts::PI, std::f64::consts::TAU, std::f64::consts::E];

            test_message
        };
    }

    struct MockNodeFactoryContext {
        nodes: Vec<Arc<Node>>,
    }

    impl MockNodeFactoryContext {
        fn new() -> Self {
            MockNodeFactoryContext { nodes: Vec::new() }
        }
    }

    impl NodeFactoryContext for MockNodeFactoryContext {
        fn create_message_provider(
            &self,
            _: &str,
        ) -> Box<dyn Fn() -> Box<dyn MessageDyn> + Send + Sync> {
            Box::new(|| Box::new((*TEST_MESSAGE).clone()))
        }

        fn get_node(&self, index: usize) -> &Arc<Node> {
            &self.nodes[index]
        }
    }

    fn create_nodes(
        node_factory: &NodeFactoryImpl,
        proto_nodes: &[NodeProto],
        context: &mut MockNodeFactoryContext,
    ) -> Result<Vec<Arc<Node>>, String> {
        proto_nodes
            .iter()
            .map(|proto_node| {
                let node = Arc::new(node_factory.create(proto_node, context)?);
                context.nodes.push(node.clone());
                Ok(node)
            })
            .collect()
    }

    fn validate_nodes(
        node_factory: &NodeFactoryImpl,
        proto_nodes: &[NodeProto],
    ) -> Result<(), String> {
        for proto_node in proto_nodes {
            node_factory.validate(proto_node)?;
        }
        Ok(())
    }

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
    fn test_validate_combination_node_did_not_specify_operator_fails() {
        let mut node_0 = NodeProto::new();
        node_0.set_combination_node(CombinationNode::new());
        let all_proto_nodes = vec![node_0];
        let node_factory = NodeFactoryImpl {};

        // action
        let err_msg = validate_nodes(&node_factory, &all_proto_nodes).err().unwrap();

        // assert
        assert!(err_msg.contains("did not specify operator"), "Unexpected error: {:?}", err_msg);
    }

    #[test]
    fn test_validate_missing_left_child_fails() {
        let mut node_0 = NodeProto::new();
        let mut combination_node = CombinationNode::new();
        combination_node.set_relational_operator(RelationalOperator::EQ);
        combination_node.right_index = Some(0);
        node_0.set_combination_node(combination_node);
        let all_proto_nodes = vec![node_0];
        let node_factory = NodeFactoryImpl {};

        // action
        let err_msg = validate_nodes(&node_factory, &all_proto_nodes).err().unwrap();

        // assert
        assert!(err_msg.contains("did not specify left child"), "Unexpected error: {:?}", err_msg);
    }

    #[test]
    fn test_validate_binary_operator_no_right_child_fails() {
        let mut node_0 = NodeProto::new();
        let mut node_1 = NodeProto::new();
        let mut constant_leaf = ConstantLeafNode::new();
        constant_leaf.set_bool_value(true);
        node_0.set_constant_leaf_node(constant_leaf);
        // binary operator but only specified left child index
        let mut combination_node = CombinationNode::new();
        combination_node.set_relational_operator(RelationalOperator::EQ);
        combination_node.left_index = Some(0);
        node_1.set_combination_node(combination_node);

        let all_proto_nodes = vec![node_0, node_1];
        let node_factory = NodeFactoryImpl {};

        // action
        let err_msg = validate_nodes(&node_factory, &all_proto_nodes).err().unwrap();

        // assert
        assert!(err_msg.contains("did not specify right child"), "Unexpected error: {:?}", err_msg);
    }

    #[test]
    fn test_validate_unary_operator_with_two_children_fails() {
        // setup - root node has 2 child nodes but unary operator (NOT)
        let all_proto_nodes = create_true_false_branch(LogicalOperator::NOT);
        let node_factory = NodeFactoryImpl {};

        // action
        let err_msg = validate_nodes(&node_factory, &all_proto_nodes).err().unwrap();

        // assert
        assert!(
            err_msg.contains("unary operator but specified two children"),
            "Unexpected error: {:?}",
            err_msg
        );
    }

    #[test]
    fn test_validate_unary_operator_no_right_child_ok() {
        // setup
        let node_factory = NodeFactoryImpl {};
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

        let all_proto_nodes = vec![leaf_true, root_node];

        // action
        let nodes = validate_nodes(&node_factory, &all_proto_nodes);

        // assert
        assert!(nodes.is_ok());
    }

    #[test]
    fn test_validate_ok() {
        // setup - root node has 2 child nodes
        let all_proto_nodes = create_true_false_branch(LogicalOperator::AND);
        let node_factory = NodeFactoryImpl {};

        // action
        let res = validate_nodes(&node_factory, &all_proto_nodes);

        // assert
        assert!(res.is_ok());
    }

    #[test]
    fn test_create_nodes_ok() -> Result<(), String> {
        // setup
        let op = LogicalOperator::AND;
        let node_factory = NodeFactoryImpl {};
        let mut context = MockNodeFactoryContext::new();
        let all_proto_nodes = create_true_false_branch(op);

        // action
        let nodes = create_nodes(&node_factory, &all_proto_nodes, &mut context)?;

        // assert 1-1 correspondence between rust node and protobuf node
        for (rust_node, proto_node) in nodes.iter().zip(all_proto_nodes) {
            match rust_node.as_ref() {
                Node::ConstantLeaf { value } => {
                    if let NodeValue::Bool(value) = value {
                        assert_eq!(*value, proto_node.constant_leaf_node().bool_value());
                    } else {
                        return Err(format!(
                            "Input Nodes contain only boolean constants, \
                                    output Node {:?} is a non-boolean constant",
                            value
                        ));
                    }
                }
                Node::Combination { operator, left, right } => {
                    assert_eq!(*operator, OperatorProto::LogicalOperator(op.into()));
                    assert!(Arc::ptr_eq(
                        left.as_ref().unwrap(),
                        &nodes[proto_node.combination_node().left_index.unwrap() as usize]
                    ));
                    assert!(Arc::ptr_eq(
                        right.as_ref().unwrap(),
                        &nodes[proto_node.combination_node().right_index.unwrap() as usize]
                    ));
                }
                _ => {
                    return Err("Output Nodes are expected to contain ConstantLeaf and \
                                Combination enum variants only"
                        .to_string())
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_create_nodes_unary_operator_cannot_have_two_children() {
        // setup - root node has 2 child nodes but unary operator (NOT)
        let all_proto_nodes = create_true_false_branch(LogicalOperator::NOT);
        let node_factory = NodeFactoryImpl {};
        let mut context = MockNodeFactoryContext::new();

        // action
        let nodes = create_nodes(&node_factory, &all_proto_nodes, &mut context);

        // assert
        assert!(nodes.is_err());
    }

    #[test]
    fn test_create_nodes_unary_operator_missing_right_child_ok() {
        // setup
        let node_factory = NodeFactoryImpl {};
        let mut context = MockNodeFactoryContext::new();
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

        let all_proto_nodes = vec![leaf_true, root_node];

        // action
        let nodes = create_nodes(&node_factory, &all_proto_nodes, &mut context);

        // assert
        assert!(nodes.is_ok());
    }

    #[test]
    fn test_create_nodes_binary_operator_missing_right_child_errors() {
        // setup
        let node_factory = NodeFactoryImpl {};
        let mut context = MockNodeFactoryContext::new();
        let mut leaf_true = NodeProto::new();
        let mut root_node = NodeProto::new();

        let mut constant_leaf = ConstantLeafNode::new();
        constant_leaf.set_bool_value(true);
        leaf_true.set_constant_leaf_node(constant_leaf.clone());

        //            index 1 (AND)
        //          /              \
        //    index 0              no right child
        //    TRUE
        let mut combination_node = CombinationNode::new();
        combination_node.left_index = Some(0);
        combination_node.set_logical_operator(LogicalOperator::AND);
        root_node.set_combination_node(combination_node.clone());

        let all_proto_nodes = vec![leaf_true, root_node];

        // action
        let nodes = create_nodes(&node_factory, &all_proto_nodes, &mut context);

        // assert error because binary operator needs 2 children
        assert!(nodes.is_err());
    }

    #[test]
    fn test_evaluate_logical_operator_ok() -> Result<(), String> {
        let node_factory = NodeFactoryImpl {};
        let mut context = MockNodeFactoryContext::new();
        // setup this tree:
        //            index 3 (XOR)
        //          /              \
        //    index 0              index 2 (output of function `create_true_false_branch`)
        //    TRUE                  OR
        //                       /        \
        //               index 0 (T)      index 1 (F)
        let mut all_proto_nodes = create_true_false_branch(LogicalOperator::OR);
        let mut node_index_3 = NodeProto::new();
        let mut xor_node = CombinationNode::new();
        xor_node.left_index = Some(0); // True
        xor_node.right_index = Some(2); // True || False
        xor_node.set_logical_operator(LogicalOperator::XOR);
        node_index_3.set_combination_node(xor_node.clone());
        all_proto_nodes.push(node_index_3.clone());

        let nodes = create_nodes(&node_factory, &all_proto_nodes, &mut context)?;

        // we will evaluate every node in `nodes`, and we expect these results:
        let expected_results = vec![
            NodeValue::Bool(true),  // index 0 = constant value of T
            NodeValue::Bool(false), // index 1 = constant value of F
            NodeValue::Bool(true),  // index 2 = T || F = T
            NodeValue::Bool(false), // index 3 = F = index 0 XOR index 2
        ];

        // assert that evaluated_result[i] == expected_result[i] for all index i
        for (rust_node, expected_val_ref) in nodes.iter().zip(expected_results.iter()) {
            assert_eq!(rust_node.evaluate()?, *expected_val_ref);
        }
        Ok(())
    }

    #[test]
    fn test_evaluate_arithmetic_operator_ok() -> Result<(), String> {
        let node_factory = NodeFactoryImpl {};
        let mut context = MockNodeFactoryContext::new();
        // setup this tree: 5 / 2 * 5^2
        //            index 4 (MULTIPLY)
        //          /              \
        //      index 2              index 3
        //       DIVIDE               POWER
        //    /         \          /        \
        // index 0   index 1   index 0     index 1
        //   5          2       5             2
        let mut node_0 = NodeProto::new();
        let mut node_1 = NodeProto::new();
        let mut node_2 = NodeProto::new();
        let mut node_3 = NodeProto::new();
        let mut node_4 = NodeProto::new();

        let mut constant_leaf_five = ConstantLeafNode::new();
        constant_leaf_five.set_int32_value(5);
        node_0.set_constant_leaf_node(constant_leaf_five);

        let mut constant_leaf_two = ConstantLeafNode::new();
        constant_leaf_two.set_int32_value(2);
        node_1.set_constant_leaf_node(constant_leaf_two);

        let mut combination_divide = CombinationNode::new();
        combination_divide.set_arithmetic_operator(ArithmeticOperator::DIVIDE);
        combination_divide.left_index = Some(0);
        combination_divide.right_index = Some(1);
        node_2.set_combination_node(combination_divide);

        let mut combination_pow = CombinationNode::new();
        combination_pow.set_arithmetic_operator(ArithmeticOperator::POWER);
        combination_pow.left_index = Some(0);
        combination_pow.right_index = Some(1);
        node_3.set_combination_node(combination_pow);

        let mut combination_multiply = CombinationNode::new();
        combination_multiply.set_arithmetic_operator(ArithmeticOperator::MULTIPLY);
        combination_multiply.left_index = Some(2);
        combination_multiply.right_index = Some(3);
        node_4.set_combination_node(combination_multiply);

        let all_proto_nodes = vec![node_0, node_1, node_2, node_3, node_4];
        let nodes = create_nodes(&node_factory, &all_proto_nodes, &mut context)?;

        // we will evaluate every node in `nodes`, and we expect these results:
        let expected_results = vec![
            NodeValue::Int32(5),     // index 0 = constant value of 5
            NodeValue::Int32(2),     // index 1 = constant value of 2
            NodeValue::Double(2.5),  // index 2 = 5 / 2 = 2.5
            NodeValue::Int64(25),    // index 3 = 5^2 = 25
            NodeValue::Double(62.5), // index 4 = 2.5 * 25 = 62.5
        ];

        // assert that evaluated_result[i] == expected_result[i]  for all index i
        for (rust_node, expected_val_ref) in nodes.iter().zip(expected_results.iter()) {
            assert_eq!(rust_node.evaluate()?, *expected_val_ref);
        }
        Ok(())
    }

    #[test]
    fn test_evaluate_relational_operator_ok() -> Result<(), String> {
        let node_factory = NodeFactoryImpl {};
        let mut context = MockNodeFactoryContext::new();
        // setup this tree which uses the protobuf message initialized in the lazy_static block
        //                             index 4 (EQ)
        //                           /              \
        //                      index 2                 index 3
        //                      CONTAINS                  true
        //               /                  \
        //          index 0                index 1
        // TestMessage.repeated_doubles       Pi
        let mut node_0 = NodeProto::new();
        let mut node_1 = NodeProto::new();
        let mut node_2 = NodeProto::new();
        let mut node_3 = NodeProto::new();
        let mut node_4 = NodeProto::new();

        let mut leaf_doubles = FieldLeafNode::new();
        leaf_doubles.publisher_name = "publisher_name".to_string();
        leaf_doubles.field_names.push("repeated_doubles".to_string());
        node_0.set_field_leaf_node(leaf_doubles);

        let mut constant_pi = ConstantLeafNode::new();
        constant_pi.set_float_value(std::f32::consts::PI);
        node_1.set_constant_leaf_node(constant_pi);

        let mut combination_contains = CombinationNode::new();
        combination_contains.set_relational_operator(RelationalOperator::CONTAINS);
        combination_contains.left_index = Some(0);
        combination_contains.right_index = Some(1);
        node_2.set_combination_node(combination_contains);

        let mut constant_true = ConstantLeafNode::new();
        constant_true.set_bool_value(true);
        node_3.set_constant_leaf_node(constant_true);

        let mut combination_eq = CombinationNode::new();
        combination_eq.set_relational_operator(RelationalOperator::EQ);
        combination_eq.left_index = Some(2);
        combination_eq.right_index = Some(3);
        node_4.set_combination_node(combination_eq);

        let all_proto_nodes = vec![node_0, node_1, node_2, node_3, node_4];
        let nodes = create_nodes(&node_factory, &all_proto_nodes, &mut context)?;

        // we will evaluate every node in `nodes`, and we expect these results:
        let expected_results = vec![
            // index 0 = [Pi, Tau, e]
            NodeValue::DoubleList(TEST_MESSAGE.repeated_doubles.clone()),
            NodeValue::Float(std::f32::consts::PI), // index 1 = constant value of Pi
            NodeValue::Bool(true),                  // index 2 = true because list contains Pi
            NodeValue::Bool(true),                  // index 3 = constant of true
            NodeValue::Bool(true),                  // index 4 = true because index 2 == index 3
        ];

        // assert that evaluated_result[i] == expected_result[i]  for all index i
        for (rust_node, expected_val_ref) in nodes.iter().zip(expected_results.iter()) {
            assert_eq!(rust_node.evaluate()?, *expected_val_ref);
        }
        Ok(())
    }

    #[test]
    fn test_evaluate_mixed_operators() -> Result<(), String> {
        let node_factory = NodeFactoryImpl {};
        let mut context = MockNodeFactoryContext::new();
        // setup tree containing all 3 types of operators
        //                       index 4 (&&)
        //                    /               \
        //                index 2             index 3
        //               LT_OR_EQ              TRUE
        //              /        \
        //     index 0 (5)      index 1 (+)
        //                       /         \
        //               index 0 (5)       index 0 (5)
        let mut node_0 = NodeProto::new();
        let mut node_1 = NodeProto::new();
        let mut node_2 = NodeProto::new();
        let mut node_3 = NodeProto::new();
        let mut node_4 = NodeProto::new();

        let mut constant_leaf_five = ConstantLeafNode::new();
        constant_leaf_five.set_int32_value(5);
        node_0.set_constant_leaf_node(constant_leaf_five);

        let mut add_node = CombinationNode::new();
        add_node.left_index = Some(0); // 5
        add_node.right_index = Some(0); // 5
        add_node.set_arithmetic_operator(ArithmeticOperator::ADD);
        node_1.set_combination_node(add_node);

        let mut gt_eq_node = CombinationNode::new();
        gt_eq_node.left_index = Some(0); // 5
        gt_eq_node.right_index = Some(1); // 5+5 = 10
        gt_eq_node.set_relational_operator(RelationalOperator::LT_OR_EQ);
        node_2.set_combination_node(gt_eq_node);

        let mut constant_leaf_true = ConstantLeafNode::new();
        constant_leaf_true.set_bool_value(true);
        node_3.set_constant_leaf_node(constant_leaf_true);

        let mut and_node = CombinationNode::new();
        and_node.left_index = Some(2); // 5 <= 10
        and_node.right_index = Some(3); // TRUE
        and_node.set_logical_operator(LogicalOperator::AND);
        node_4.set_combination_node(and_node);

        let all_proto_nodes = vec![node_0, node_1, node_2, node_3, node_4];
        let nodes = create_nodes(&node_factory, &all_proto_nodes, &mut context)?;

        // we will evaluate every node in `nodes`, and we expect these results:
        let expected_results = vec![
            NodeValue::Int32(5),   // index 0 = 5
            NodeValue::Int64(10),  // index 1 = 5+5 = 10
            NodeValue::Bool(true), // index 2 = 5 <= 10
            NodeValue::Bool(true), // index 3 = constant value of True
            NodeValue::Bool(true), // index 4 = True && True = True
        ];

        // assert that evaluated_result[i] == expected_result[i] for all index i
        for (rust_node, expected_val_ref) in nodes.iter().zip(expected_results.iter()) {
            assert_eq!(rust_node.evaluate()?, *expected_val_ref);
        }
        Ok(())
    }

    #[test]
    fn test_evaluate_mixed_leaf_node_types() -> Result<(), String> {
        let node_factory = NodeFactoryImpl {};
        let mut context = MockNodeFactoryContext::new();
        // setup this tree which uses the protobuf message initialized in the lazy_static block
        //                           index 4 (<)
        //                       /               \
        //                index 2                  index 3
        //                 DIVIDE                TestMessage.int_field
        //           /                \
        //     index 0               index 1
        // TestMessage.float_field       2
        let mut node_0 = NodeProto::new();
        let mut node_1 = NodeProto::new();
        let mut node_2 = NodeProto::new();
        let mut node_3 = NodeProto::new();
        let mut node_4 = NodeProto::new();

        let mut float_field_node = FieldLeafNode::new();
        float_field_node.publisher_name = "publisher_name".to_string();
        float_field_node.field_names.push("float_field".to_string());
        node_0.set_field_leaf_node(float_field_node);

        let mut constant_leaf_two = ConstantLeafNode::new();
        constant_leaf_two.set_int32_value(2);
        node_1.set_constant_leaf_node(constant_leaf_two);

        let mut combination_divide = CombinationNode::new();
        combination_divide.set_arithmetic_operator(ArithmeticOperator::DIVIDE);
        combination_divide.left_index = Some(0);
        combination_divide.right_index = Some(1);
        node_2.set_combination_node(combination_divide);

        let mut int_field_node = FieldLeafNode::new();
        int_field_node.publisher_name = "publisher_name".to_string();
        int_field_node.field_names.push("int_field".to_string());
        node_3.set_field_leaf_node(int_field_node);

        let mut combination_less_than = CombinationNode::new();
        combination_less_than.set_relational_operator(RelationalOperator::LT);
        combination_less_than.left_index = Some(2);
        combination_less_than.right_index = Some(3);
        node_4.set_combination_node(combination_less_than);

        let all_proto_nodes = vec![node_0, node_1, node_2, node_3, node_4];
        let nodes = create_nodes(&node_factory, &all_proto_nodes, &mut context)?;

        // we will evaluate every node in `nodes`, and we expect these results:
        let expected_results = vec![
            NodeValue::Float(100.0), // index 0 = field value of 100
            NodeValue::Int32(2),     // index 1 = constant value of 2
            NodeValue::Double(50.0), // index 2 = 100 / 2 = 50
            NodeValue::Int32(80),    // index 3 = field value of 80
            NodeValue::Bool(true),   // index 4 = 50 < 80 = true
        ];

        // assert that evaluated_result[i] == expected_result[i]  for all index i
        for (rust_node, expected_val_ref) in nodes.iter().zip(expected_results.iter()) {
            assert_eq!(rust_node.evaluate()?, *expected_val_ref);
        }
        Ok(())
    }

    #[test]
    fn test_evaluate_incompatible_types_should_fail() -> Result<(), String> {
        let node_factory = NodeFactoryImpl {};
        let mut context = MockNodeFactoryContext::new();
        let mut node_0 = NodeProto::new();
        let mut node_1 = NodeProto::new();

        let mut constant_leaf_true = ConstantLeafNode::new();
        constant_leaf_true.set_bool_value(true);
        node_0.set_constant_leaf_node(constant_leaf_true);

        let mut lt_node = CombinationNode::new();
        lt_node.left_index = Some(0); // True
        lt_node.right_index = Some(0); // True
        lt_node.set_relational_operator(RelationalOperator::LT);
        node_1.set_combination_node(lt_node);

        let all_proto_nodes = vec![node_0, node_1];
        let nodes = create_nodes(&node_factory, &all_proto_nodes, &mut context)?;

        // Index 1 expr is "True < True"
        let result = nodes[1].evaluate();

        // True cannot be less than True
        assert!(result.is_err());
        Ok(())
    }
}
