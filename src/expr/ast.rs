// Copyright 2023 Google LLC. All Rights Reserved.

//! Library for building and traversing the abstract syntax tree.

mod node_value;

pub use node_value::NodeValue;

use protobuf::MessageDyn;
use sdvtelemetry_proto_rust::expression::{
    combination_node::LogicalOperator, combination_node::Operator as OperatorProto,
    node::Node_type as NodeTypeProto, Node as NodeProto,
};
use std::{
    fmt,
    marker::{Send, Sync},
    sync::Arc,
};
use thiserror::Error;

/// Creates rust nodes from protobuf expression nodes.
pub trait NodeFactory {
    /// Validate the expression nodes to ensure they do not contain cyclic dependencies
    /// and all indices are valid.
    fn validate(&self, proto_nodes: &[NodeProto]) -> Result<(), String>;

    /// Creates rust Nodes from protobuf Nodes.
    /// Protobuf nodes use indices to represent left and right children.
    /// Rust nodes have a pointer to the actual children instead of an index.
    fn create_nodes(
        &self,
        proto_nodes: &[NodeProto],
        message_provider: &Arc<dyn MessageProvider>,
    ) -> Result<Vec<Arc<Node>>, String>;
}

/// Provides published messages.
pub trait MessageProvider: Send + Sync {
    /// Returns the latest published message for the specified publisher name.
    fn get_message(
        &self,
        publisher_name: &str,
    ) -> Result<Option<Box<dyn MessageDyn>>, MessageProviderError>;
}

/// Error returned by MessageProvider
#[derive(Error, Debug)]
pub enum MessageProviderError {
    /// Publisher name is invalid.
    #[error("Publisher name is invalid")]
    InvalidPublisherName,
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

        /// Provider of the latest published messages.
        message_provider: Arc<dyn MessageProvider>,
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
#[derive(Default)]
pub struct NodeFactoryImpl {}

/// Used for cycle detection in the expression nodes.
#[derive(Clone, PartialEq)]
enum VisitStatus {
    NotVisited,
    Visiting,
    Visited,
}

impl NodeFactoryImpl {
    /// Constructor.
    pub fn new() -> Self {
        NodeFactoryImpl {}
    }

    /// Vists the node at `index` using pre-order traversal.
    /// Ensures that the node specifies valid indices for child nodes, and its
    /// children do not contain cyclic dependencies.
    ///
    /// If the `nodes` argument is non-empty, in addition to validation,
    /// this function will also create the corresponding Rust Node and put it
    /// in the `nodes` vector at the given `index`.
    ///
    /// TODO(b/289295418): type check the child nodes to ensure they have compatible types
    fn visit(
        index: usize,
        proto_nodes: &[NodeProto],
        visit_status: &mut Vec<VisitStatus>,
        nodes: &mut Vec<Option<Arc<Node>>>,
        message_provider: &Option<&Arc<dyn MessageProvider>>,
    ) -> Result<Option<Arc<Node>>, String> {
        // get visit status and return error if index out of boound
        let status = visit_status.get(index).ok_or(format!(
            "Index {} out of bound. Expression nodes have size {}, double check that \
                all child nodes have valid indices",
            index,
            proto_nodes.len()
        ))?; // `?` returns early if error

        // detect cycles using visit status, and visit the node
        match status {
            VisitStatus::Visiting => {
                Err(format!("Cycle detected at expression node index {}", index))
            }
            VisitStatus::Visited => {
                if nodes.is_empty() {
                    Ok(None)
                } else {
                    Ok(nodes[index].clone())
                }
            }
            VisitStatus::NotVisited => {
                // mark index as "visiting", if visit() comes back to this index, there is a cycle
                visit_status[index] = VisitStatus::Visiting;

                // match on the node type and capture value returned by match block
                let rust_node = match &proto_nodes[index].node_type {
                    // base case 1: constant leaf node, can return from match block immediately
                    Some(NodeTypeProto::ConstantLeafNode(constant_leaf)) => {
                        Self::maybe_create_node(nodes, index, || {
                            Ok(Arc::new(Node::ConstantLeaf {
                                value: NodeValue::from_constant_leaf_node(constant_leaf)?,
                            }))
                        })?
                    }
                    // base case 2: field leaf node, can return from match block immediately
                    Some(NodeTypeProto::FieldLeafNode(field_leaf)) => {
                        Self::maybe_create_node(nodes, index, || {
                            Ok(Arc::new(Node::FieldLeaf {
                                publisher_name: field_leaf.publisher_name.clone(),
                                fields: field_leaf.field_names.clone(),
                                message_provider: message_provider.unwrap().clone(),
                            }))
                        })?
                    }
                    // recursive case: visit left, right, self
                    Some(NodeTypeProto::CombinationNode(combination_node)) => {
                        let op = combination_node
                            .operator
                            .as_ref()
                            .ok_or(format!("Node at index {} did not specify operator", index))?;
                        // before recursion, check if operator type requires a right child
                        op.check_right_child(&combination_node.right_index, index)?;

                        // visit left child
                        let left_child_index = combination_node
                            .left_index
                            .ok_or(format!("Node at index {} did not specify left child.", index))?
                            as usize;
                        let left = Self::visit(
                            left_child_index,
                            proto_nodes,
                            visit_status,
                            nodes,
                            message_provider,
                        )?;

                        // visit right child if right child is specified
                        let right = if let Some(right_idx) = combination_node.right_index {
                            Self::visit(
                                right_idx as usize,
                                proto_nodes,
                                visit_status,
                                nodes,
                                message_provider,
                            )?
                        } else {
                            None
                        };

                        // visit self
                        Self::maybe_create_node(nodes, index, move || {
                            Ok(Arc::new(Node::Combination { operator: op.clone(), left, right }))
                        })?
                    } // match arm `Some(CombinationNode)`
                    _ => {
                        return Err(format!("Node at index {} did not specify a node_type", index))
                    }
                };

                // mark index as visited and return Ok
                visit_status[index] = VisitStatus::Visited;
                Ok(rust_node)
            } // match arm `VisitStatus::NotVisited`
        } // `match` block
    }

    /// If the `nodes` vector is non-empty, it will call the `create_node_func` to create
    /// a Rust Node, put the node into the vector at the `index`, and return that node.
    /// Otherwise it will return a success status containing nothing.
    fn maybe_create_node(
        nodes: &mut Vec<Option<Arc<Node>>>,
        index: usize,
        create_node_func: impl FnOnce() -> Result<Arc<Node>, String>,
    ) -> Result<Option<Arc<Node>>, String> {
        if nodes.is_empty() {
            return Ok(None);
        }
        if index >= nodes.len() {
            return Err(format!(
                "maybe_create_node() called on invalid index {} on a vector of size {}",
                index,
                nodes.len()
            ));
        }
        nodes[index] = Some(create_node_func()?);
        Ok(nodes[index].clone())
    }
}

impl NodeFactory for NodeFactoryImpl {
    fn validate(&self, proto_nodes: &[NodeProto]) -> Result<(), String> {
        let n = proto_nodes.len();
        let mut visit_status = vec![VisitStatus::NotVisited; n];
        for i in 0..n {
            Self::visit(i, proto_nodes, &mut visit_status, &mut vec![], &None)?;
        }
        Ok(())
    }

    fn create_nodes(
        &self,
        proto_nodes: &[NodeProto],
        message_provider: &Arc<dyn MessageProvider>,
    ) -> Result<Vec<Arc<Node>>, String> {
        let n = proto_nodes.len();
        let mut visit_status = vec![VisitStatus::NotVisited; n];
        let mut rust_nodes: Vec<Option<Arc<Node>>> = vec![None; n];

        for i in 0..n {
            Self::visit(
                i,
                proto_nodes,
                &mut visit_status,
                &mut rust_nodes,
                &Some(message_provider),
            )?;
        }
        // flatten Vec<Option<T>> into Vec<T>, ignoring None values.
        let nodes: Vec<Arc<Node>> = rust_nodes.into_iter().flatten().collect();
        if nodes.len() != n {
            return Err(format!(
                "failed to initialize expression nodes, \
                expected {} nodes, but initialized {} nodes",
                n,
                nodes.len()
            ));
        }
        Ok(nodes)
    }
}

impl Node {
    /// Evaluate AST recursively with messages from the "message_provider".
    pub fn evaluate(&self) -> Result<NodeValue, String> {
        match &self {
            Node::ConstantLeaf { value } => {
                // Base case of recursion: node is a leaf node, return value
                Ok(value.clone())
            }
            Node::FieldLeaf { fields, message_provider, publisher_name } => {
                // Base case of recursion: node is a leaf node, return value
                message_provider
                    .get_message(publisher_name)
                    .map_err(|err| {
                        format!(
                            "Unable to retrieve latest published message for publisher {}: {}",
                            publisher_name, err
                        )
                    })?
                    .map_or_else(
                        || Ok(NodeValue::None),
                        |msg| NodeValue::extract_value(msg, fields),
                    )
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
    fn check_right_child(&self, right_index: &Option<u32>, index: usize) -> Result<(), String>;
}

impl OperatorProtoExtension for OperatorProto {
    fn is_unary_operator(&self) -> bool {
        if let OperatorProto::LogicalOperator(op) = self {
            return op.enum_value_or_default() == LogicalOperator::NOT;
        }
        false
    }

    fn check_right_child(&self, right_index: &Option<u32>, index: usize) -> Result<(), String> {
        let unary_op = self.is_unary_operator();
        let has_right_child = right_index.is_some();
        if unary_op && has_right_child {
            Err(format!("Node at index {} has unary operator but specified two children", index))
        } else if !unary_op && !has_right_child {
            Err(format!(
                "Node at index {} has binary operator but did not specify right child",
                index
            ))
        } else {
            Ok(())
        }
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

    struct MockMessageProvider {
        result_provider: Option<
            Box<
                dyn Fn() -> Result<Option<Box<dyn MessageDyn>>, MessageProviderError> + Send + Sync,
            >,
        >,
    }

    impl MockMessageProvider {
        fn new() -> Self {
            MockMessageProvider { result_provider: None }
        }
    }

    impl MessageProvider for MockMessageProvider {
        fn get_message(
            &self,
            _publisher_name: &str,
        ) -> Result<Option<Box<dyn MessageDyn>>, MessageProviderError> {
            if let Some(provider) = &self.result_provider {
                provider()
            } else {
                Ok(Some(Box::new((*TEST_MESSAGE).clone())))
            }
        }
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
    fn test_validate_child_is_self_fails() {
        // setup node_0 whose child is itself
        let mut node_0 = NodeProto::new();
        let mut combination_node = CombinationNode::new();
        combination_node.set_logical_operator(LogicalOperator::NOT);
        combination_node.left_index = Some(0);
        node_0.set_combination_node(combination_node);
        let all_proto_nodes = vec![node_0];
        let node_factory = NodeFactoryImpl {};

        // action
        let err_msg = node_factory.validate(&all_proto_nodes).err().unwrap();

        // assert
        assert!(err_msg.starts_with("Cycle detected"), "Unexpected error: {:?}", err_msg);
    }

    #[test]
    fn test_validate_cyclic_dependency_fails() {
        // setup node_0 depends on node_1 and node_1 depends on node_0
        let mut node_0 = NodeProto::new();
        let mut node_1 = NodeProto::new();
        let mut combination_node = CombinationNode::new();
        combination_node.set_logical_operator(LogicalOperator::NOT);
        combination_node.left_index = Some(1);
        node_0.set_combination_node(combination_node.clone());
        combination_node.left_index = Some(0);
        node_1.set_combination_node(combination_node);
        let all_proto_nodes = vec![node_0, node_1];
        let node_factory = NodeFactoryImpl {};

        // action
        let err_msg = node_factory.validate(&all_proto_nodes).err().unwrap();

        // assert
        assert!(err_msg.starts_with("Cycle detected"), "Unexpected error: {:?}", err_msg);
    }

    #[test]
    fn test_validate_child_node_index_out_of_bound_fails() {
        // setup node_0 whose child is at index 42
        let mut node_0 = NodeProto::new();
        let mut combination_node = CombinationNode::new();
        combination_node.set_logical_operator(LogicalOperator::NOT);
        combination_node.left_index = Some(42);
        node_0.set_combination_node(combination_node);
        let all_proto_nodes = vec![node_0];
        let node_factory = NodeFactoryImpl {};

        // action
        let err_msg = node_factory.validate(&all_proto_nodes).err().unwrap();

        // assert
        assert!(err_msg.contains("out of bound"), "Unexpected error: {:?}", err_msg);
    }

    #[test]
    fn test_validate_combination_node_did_not_specify_operator_fails() {
        let mut node_0 = NodeProto::new();
        node_0.set_combination_node(CombinationNode::new());
        let all_proto_nodes = vec![node_0];
        let node_factory = NodeFactoryImpl::new();

        // action
        let err_msg = node_factory.validate(&all_proto_nodes).err().unwrap();

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
        let node_factory = NodeFactoryImpl::new();

        // action
        let err_msg = node_factory.validate(&all_proto_nodes).err().unwrap();

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
        let node_factory = NodeFactoryImpl::new();

        // action
        let err_msg = node_factory.validate(&all_proto_nodes).err().unwrap();

        // assert
        assert!(err_msg.contains("did not specify right child"), "Unexpected error: {:?}", err_msg);
    }

    #[test]
    fn test_validate_unary_operator_with_two_children_fails() {
        // setup - root node has 2 child nodes but unary operator (NOT)
        let all_proto_nodes = create_true_false_branch(LogicalOperator::NOT);
        let node_factory = NodeFactoryImpl::new();

        // action
        let err_msg = node_factory.validate(&all_proto_nodes).err().unwrap();

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
        let node_factory = NodeFactoryImpl::new();
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
        let nodes = node_factory.validate(&all_proto_nodes);

        // assert
        assert!(nodes.is_ok());
    }

    #[test]
    fn test_validate_ok() {
        // setup - root node has 2 child nodes
        let all_proto_nodes = create_true_false_branch(LogicalOperator::AND);
        let node_factory = NodeFactoryImpl::new();

        // action
        let res = node_factory.validate(&all_proto_nodes);

        // assert
        assert!(res.is_ok());
    }

    #[test]
    fn test_create_nodes_ok() -> Result<(), String> {
        // setup
        let op = LogicalOperator::AND;
        let node_factory = NodeFactoryImpl {};
        let message_provider = Arc::new(MockMessageProvider::new()) as Arc<dyn MessageProvider>;
        let all_proto_nodes = create_true_false_branch(op);

        // action
        let nodes = node_factory.create_nodes(&all_proto_nodes, &message_provider)?;

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
        let message_provider = Arc::new(MockMessageProvider::new()) as Arc<dyn MessageProvider>;

        // action
        let nodes = node_factory.create_nodes(&all_proto_nodes, &message_provider);

        // assert
        assert!(nodes.is_err());
    }

    #[test]
    fn test_create_nodes_unary_operator_missing_right_child_ok() {
        // setup
        let node_factory = NodeFactoryImpl {};
        let message_provider = Arc::new(MockMessageProvider::new()) as Arc<dyn MessageProvider>;
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
        let nodes = node_factory.create_nodes(&all_proto_nodes, &message_provider);

        // assert
        assert!(nodes.is_ok());
    }

    #[test]
    fn test_create_nodes_binary_operator_missing_right_child_errors() {
        // setup
        let node_factory = NodeFactoryImpl {};
        let message_provider = Arc::new(MockMessageProvider::new()) as Arc<dyn MessageProvider>;
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
        let nodes = node_factory.create_nodes(&all_proto_nodes, &message_provider);

        // assert error because binary operator needs 2 children
        assert!(nodes.is_err());
    }

    #[test]
    fn test_create_nodes_handles_index_out_bound() {
        // setup root node with children at index 10 and 20 (out of bound)
        let node_factory = NodeFactoryImpl {};
        let message_provider = Arc::new(MockMessageProvider::new()) as Arc<dyn MessageProvider>;
        let mut proto_node = NodeProto::new();
        let mut combination_node = CombinationNode::new();
        combination_node.left_index = Some(10);
        combination_node.right_index = Some(20);
        combination_node.set_logical_operator(LogicalOperator::AND);
        proto_node.set_combination_node(combination_node);

        let all_proto_nodes = vec![proto_node];

        // action
        let nodes = node_factory.create_nodes(&all_proto_nodes, &message_provider);

        // assert
        assert!(nodes.is_err());
    }

    #[test]
    fn test_create_nodes_cyclic_dependency_fails() {
        // setup node_0 depends on node_1 and node_1 depends on node_0
        let mut node_0 = NodeProto::new();
        let mut node_1 = NodeProto::new();
        let mut combination_node = CombinationNode::new();
        combination_node.set_logical_operator(LogicalOperator::NOT);
        combination_node.left_index = Some(1);
        node_0.set_combination_node(combination_node.clone());
        combination_node.left_index = Some(0);
        node_1.set_combination_node(combination_node);
        let all_proto_nodes = vec![node_0, node_1];
        let node_factory = NodeFactoryImpl {};
        let message_provider = Arc::new(MockMessageProvider::new()) as Arc<dyn MessageProvider>;

        // action
        let err_msg = node_factory.create_nodes(&all_proto_nodes, &message_provider).err().unwrap();

        // assert
        assert!(err_msg.starts_with("Cycle detected"), "Unexpected error: {:?}", err_msg);
    }

    #[test]
    fn test_evaluate_logical_operator_ok() -> Result<(), String> {
        let node_factory = NodeFactoryImpl {};
        let message_provider = Arc::new(MockMessageProvider::new()) as Arc<dyn MessageProvider>;
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

        let nodes = node_factory.create_nodes(&all_proto_nodes, &message_provider)?;

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
        let message_provider = Arc::new(MockMessageProvider::new()) as Arc<dyn MessageProvider>;
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
        let nodes = node_factory.create_nodes(&all_proto_nodes, &message_provider)?;

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
        let message_provider = Arc::new(MockMessageProvider::new()) as Arc<dyn MessageProvider>;
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
        let nodes = node_factory.create_nodes(&all_proto_nodes, &message_provider)?;

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
        let message_provider = Arc::new(MockMessageProvider::new()) as Arc<dyn MessageProvider>;
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
        let nodes = node_factory.create_nodes(&all_proto_nodes, &message_provider)?;

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
        let message_provider = Arc::new(MockMessageProvider::new()) as Arc<dyn MessageProvider>;
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
        let nodes = node_factory.create_nodes(&all_proto_nodes, &message_provider)?;

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
        let message_provider = Arc::new(MockMessageProvider::new()) as Arc<dyn MessageProvider>;
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
        let nodes = node_factory.create_nodes(&all_proto_nodes, &message_provider)?;

        // Index 1 expr is "True < True"
        let result = nodes[1].evaluate();

        // True cannot be less than True
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn test_evaluate_message_provider_invalid_publisher_name_fails() {
        let mut message_provider = MockMessageProvider::new();
        message_provider.result_provider =
            Some(Box::new(|| Err(MessageProviderError::InvalidPublisherName)));
        let node = Node::FieldLeaf {
            publisher_name: String::default(),
            fields: vec![],
            message_provider: Arc::new(message_provider) as Arc<dyn MessageProvider>,
        };

        let err = node.evaluate().err().unwrap();

        assert!(err.contains("Unable to retrieve"));
    }

    #[test]
    fn test_evaluate_no_provided_message_returns_none() {
        let mut message_provider = MockMessageProvider::new();
        message_provider.result_provider = Some(Box::new(|| Ok(None)));
        let node = Node::FieldLeaf {
            publisher_name: String::default(),
            fields: vec![],
            message_provider: Arc::new(message_provider) as Arc<dyn MessageProvider>,
        };

        let res = node.evaluate().unwrap();

        assert_eq!(res, NodeValue::None);
    }
}
