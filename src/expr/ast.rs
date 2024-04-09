// Copyright 2023 Google LLC. All Rights Reserved.

//! Module for building and traversing the abstract syntax tree.

use protobuf::{reflect::MessageDescriptor, MessageDyn};
use sdvtelemetry_proto_rust::expression::{
    combination_node::Operator as OperatorProto, node::Node_type as NodeTypeProto,
    Node as NodeProto,
};
use std::{
    fmt,
    marker::{Send, Sync},
    sync::Arc,
};

use crate::expr::{
    node_errors::{DescriptorProviderError, MessageProviderError, NodeError},
    node_value::NodeValue,
    node_value_type::{
        FieldLeafNodeExtension, NodeValueType, NodeValueTypeExtension, OperatorProtoExtension,
    },
};

/// Creates rust nodes from protobuf expression nodes.
pub trait NodeFactory {
    /// Validate the expression nodes to ensure they do not contain cyclic dependencies
    /// and all indices are valid.
    fn validate(
        &self,
        proto_nodes: &[NodeProto],
        context: &NodeFactoryContext,
    ) -> Result<Vec<NodeValueType>, NodeError>;

    /// Creates rust Nodes from protobuf Nodes,.
    /// Protobuf nodes use indices to represent left and right children.
    /// Rust nodes have a pointer to the actual children instead of an index.
    /// Returns list of Nodes and their output value types. The two vectors have the
    /// same length.
    fn create_nodes(
        &self,
        proto_nodes: &[NodeProto],
        context: &NodeFactoryContext,
    ) -> Result<(Vec<Arc<Node>>, Vec<NodeValueType>), NodeError>;
}

/// Provides necessary information for node validation and creation.
pub struct NodeFactoryContext {
    /// Provides published data for FieldLeafNodes.
    pub message_provider: Option<Arc<dyn MessageProvider>>,

    /// Provides descriptors of published data for AST validation and evaluation.
    pub descriptor_provider: Box<dyn DescriptorProvider>,
}

/// Provides published messages.
pub trait MessageProvider: Send + Sync {
    /// Returns the latest published message for the specified publisher name.
    fn get_message(
        &self,
        publisher_name: &str,
    ) -> Result<Option<Box<dyn MessageDyn>>, MessageProviderError>;
}

/// Provides descriptors for published message types.
pub trait DescriptorProvider: Send + Sync {
    /// Provides protobuf message descriptor by publisher name.
    fn get_descriptor(
        &self,
        publisher_name: &str,
    ) -> Result<MessageDescriptor, DescriptorProviderError>;
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

/// Used for cycle detection in the expression nodes.
#[derive(Clone, PartialEq)]
enum VisitStatus {
    NotVisited,
    Visiting,
    Visited,
}

/// Implementation of NodeFactory trait.
#[derive(Default)]
pub struct NodeFactoryImpl {}

impl NodeFactoryImpl {
    /// Constructor.
    pub fn new() -> Self {
        Self {}
    }

    /// Vists the node at `index` using post-order traversal.
    /// Ensures that the node specifies valid indices for child nodes, and its
    /// children do not contain cyclic dependencies.
    ///
    /// # Arguments
    ///
    /// * `index` - The index of the node that will be visited.
    /// * `proto_nodes` - The list of expression nodes in the MetricsConfig.
    /// * `visit_status` - Tracks if a node has been visited by index.
    /// * `nodes` - If non-empty, this function will create Rust node with
    ///             1-1 correspondence with the protobuf node, put the Rust node at
    ///             the given `index` and return the Node. If `nodes` is empty, this
    ///             function only validate the tree rooted at the given `index`.
    /// * `node_value_types` - Tracks the output type of the node by index. Allows
    ///                        analysis on operator-operand compatibility as the tree
    ///                        is built.
    /// * `context` - Provides information for AST traversal, such as getter functions
    ///               for messages and descriptors for type-checking.
    fn visit(
        index: usize,
        proto_nodes: &[NodeProto],
        visit_status: &mut [VisitStatus],
        nodes: &mut [Option<Arc<Node>>],
        node_value_types: &mut [NodeValueType],
        context: &NodeFactoryContext,
    ) -> Result<Option<Arc<Node>>, NodeError> {
        // get visit status, `?` returns early if error
        let status = visit_status
            .get(index)
            .ok_or(NodeError::IndexOutOfBound { i: index, n: proto_nodes.len() })?;

        // detect cycles using visit status, and visit the node
        match status {
            VisitStatus::Visiting => Err(NodeError::CyclicDependency { i: index }),
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
                        node_value_types[index] = constant_leaf.get_node_value_type()?;
                        Self::maybe_create_node(nodes, index, || {
                            Ok(Arc::new(Node::ConstantLeaf {
                                value: NodeValue::from_constant_leaf_node(constant_leaf)?,
                            }))
                        })?
                    }
                    // base case 2: field leaf node, can return from match block immediately
                    Some(NodeTypeProto::FieldLeafNode(field_leaf)) => {
                        node_value_types[index] =
                            field_leaf.get_node_value_type(context.descriptor_provider.as_ref())?;
                        Self::maybe_create_node(nodes, index, || {
                            Ok(Arc::new(Node::FieldLeaf {
                                publisher_name: field_leaf.publisher_name.clone(),
                                fields: field_leaf.field_names.clone(),
                                message_provider: context
                                    .message_provider
                                    .as_ref()
                                    .unwrap()
                                    .clone(),
                            }))
                        })?
                    }
                    // recursive case: visit left, right, self
                    Some(NodeTypeProto::CombinationNode(combination_node)) => {
                        let op = combination_node
                            .operator
                            .as_ref()
                            .ok_or(NodeError::OperatorNotSet { i: index })?;
                        // before recursion, check if the child node is set up correctly
                        op.check_right_child(&combination_node.right_index)?;

                        // visit left child
                        let left_child_index = combination_node
                            .left_index
                            .ok_or(NodeError::LeftChildNotSet { i: index })?
                            as usize;
                        let left = Self::visit(
                            left_child_index,
                            proto_nodes,
                            visit_status,
                            nodes,
                            node_value_types,
                            context,
                        )?;

                        // visit right child if right child is specified
                        let right = if let Some(right_idx) = combination_node.right_index {
                            Self::visit(
                                right_idx as usize,
                                proto_nodes,
                                visit_status,
                                nodes,
                                node_value_types,
                                context,
                            )?
                        } else {
                            None
                        };

                        // check if right operand is compatible with operator and assign type
                        node_value_types[index] = op.get_node_value_type(
                            node_value_types,
                            left_child_index,
                            &combination_node.right_index,
                        )?;

                        // visit self
                        Self::maybe_create_node(nodes, index, move || {
                            Ok(Arc::new(Node::Combination { operator: op.clone(), left, right }))
                        })?
                    } // match arm `Some(CombinationNode)`
                    _ => return Err(NodeError::NodeTypeNotSet { i: index }),
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
        nodes: &mut [Option<Arc<Node>>],
        index: usize,
        create_node_func: impl FnOnce() -> Result<Arc<Node>, NodeError>,
    ) -> Result<Option<Arc<Node>>, NodeError> {
        if nodes.is_empty() {
            return Ok(None);
        }
        if index >= nodes.len() {
            return Err(NodeError::IndexOutOfBound { i: index, n: nodes.len() });
        }
        nodes[index] = Some(create_node_func()?);
        Ok(nodes[index].clone())
    }
}

impl NodeFactory for NodeFactoryImpl {
    fn validate(
        &self,
        proto_nodes: &[NodeProto],
        context: &NodeFactoryContext,
    ) -> Result<Vec<NodeValueType>, NodeError> {
        let n = proto_nodes.len();
        let mut visit_status = vec![VisitStatus::NotVisited; n];
        let mut node_value_types = vec![NodeValueType::None; n];
        for i in 0..n {
            Self::visit(
                i,
                proto_nodes,
                &mut visit_status,
                &mut [],
                &mut node_value_types,
                context,
            )?;
        }
        Ok(node_value_types)
    }

    fn create_nodes(
        &self,
        proto_nodes: &[NodeProto],
        context: &NodeFactoryContext,
    ) -> Result<(Vec<Arc<Node>>, Vec<NodeValueType>), NodeError> {
        let n = proto_nodes.len();
        let mut visit_status = vec![VisitStatus::NotVisited; n];
        let mut node_value_types = vec![NodeValueType::None; n];
        let mut rust_nodes: Vec<Option<Arc<Node>>> = vec![None; n];

        for i in 0..n {
            Self::visit(
                i,
                proto_nodes,
                &mut visit_status,
                &mut rust_nodes,
                &mut node_value_types,
                context,
            )?;
        }
        // flatten Vec<Option<T>> into Vec<T>, ignoring None values.
        let nodes: Vec<Arc<Node>> = rust_nodes.into_iter().flatten().collect();
        if nodes.len() != n {
            return Err(NodeError::InternalError {
                msg: format!("expected to create {} nodes, but created {} nodes", n, nodes.len()),
            });
        }
        Ok((nodes, node_value_types))
    }
}

impl Node {
    /// Evaluate AST recursively with messages from the "message_provider".
    pub fn evaluate(&self) -> Result<NodeValue, NodeError> {
        match &self {
            Node::ConstantLeaf { value } => {
                // Base case of recursion: node is a leaf node, return value
                Ok(value.clone())
            }
            Node::FieldLeaf { fields, message_provider, publisher_name } => {
                // Base case of recursion: node is a leaf node, return value
                message_provider
                    .get_message(publisher_name)
                    .map_err(|e| NodeError::MessageProviderError { e })?
                    .map_or_else(
                        || Ok(NodeValue::None),
                        |msg| NodeValue::extract_value(msg, fields),
                    )
            }
            Node::Combination { operator, left, right } => {
                // evaluate left branch recursively
                let left_val = match left {
                    Some(left) => left.evaluate(),
                    None => panic!(
                        "Incorrect Node! Node construction should have verified that \
                        every non-leaf Node has a left child, but evaluate() encountered \
                        a node without left child"
                    ),
                }?; // `?` returns early if error

                // If unary operator, then operates on self.
                // If binary operator, then [left] <operator> [right]
                // The function short-circuits if possible. The closure provides right operand.
                left_val.operate(operator, || match right {
                    Some(right) => right.evaluate(),
                    None => {
                        if operator.is_unary_operator() {
                            panic!(
                                "When the operator is unary, NodeValue should not invoke \
                                right operand's provider function"
                            );
                        } else {
                            panic!(
                                "Incorrect Node! Node construction should have verified that \
                                nodes with binary operator specify two children, but evaluate() \
                                encountered a node with binary operator but no right child"
                            );
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

#[cfg(test)]
mod tests {
    use sdvtelemetry_proto_rust::expression::{
        combination_node::{
            ArithmeticOperator, LogicalOperator, Operator as OperatorProto, RelationalOperator,
            RoundingOperator,
        },
        CombinationNode, ConstantLeafNode, FieldLeafNode, Node as NodeProto,
    };

    use super::*;
    use crate::expr::mock::*;

    const PUBLISHER_NAME: &str = "publisher_foo";

    fn create_factory_context() -> NodeFactoryContext {
        NodeFactoryContext {
            message_provider: Some(MOCK_MESSAGE_PROVIDER.clone()),
            descriptor_provider: Box::new(MockDescriptorProvider::new()),
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
    fn test_validate_node_type_not_set_fails() {
        let all_proto_nodes = vec![NodeProto::new()];
        let node_factory = NodeFactoryImpl::new();
        let context = create_factory_context();

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).err().unwrap();

        // assert
        assert!(matches!(err, NodeError::NodeTypeNotSet { .. }));
    }

    #[test]
    fn test_validate_node_value_not_set_fails() {
        let mut node = NodeProto::new();
        node.set_constant_leaf_node(ConstantLeafNode::new());
        let all_proto_nodes = vec![node];
        let node_factory = NodeFactoryImpl::new();
        let context = create_factory_context();

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).err().unwrap();

        // assert
        assert!(matches!(err, NodeError::ConstantValueNotSet { .. }));
    }

    #[test]
    fn test_validate_node_operator_not_set_fails() {
        let mut node = NodeProto::new();
        node.set_combination_node(CombinationNode::new());
        let all_proto_nodes = vec![node];
        let node_factory = NodeFactoryImpl::new();
        let context = create_factory_context();

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).err().unwrap();

        // assert
        assert!(matches!(err, NodeError::OperatorNotSet { .. }));
    }

    #[test]
    fn test_validate_node_has_string_field_fails() {
        let mut node_0 = NodeProto::new();
        let mut leaf = FieldLeafNode::new();
        leaf.publisher_name = PUBLISHER_NAME.to_string();
        leaf.field_names = vec!["string_field".to_string()];
        node_0.set_field_leaf_node(leaf);
        let all_proto_nodes = vec![node_0];
        let node_factory = NodeFactoryImpl::new();
        let context = create_factory_context();

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).err().unwrap();

        // assert
        assert!(matches!(err, NodeError::UnsupportedSingularFieldType { .. }));
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
        let context = create_factory_context();

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).err().unwrap();

        // assert
        assert!(matches!(err, NodeError::CyclicDependency { .. }));
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
        let context = create_factory_context();

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).err().unwrap();

        // assert
        assert!(matches!(err, NodeError::CyclicDependency { .. }));
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
        let context = create_factory_context();

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).err().unwrap();

        // assert
        assert!(matches!(err, NodeError::IndexOutOfBound { .. }));
    }

    #[test]
    fn test_validate_combination_node_did_not_specify_operator_fails() {
        let mut node_0 = NodeProto::new();
        node_0.set_combination_node(CombinationNode::new());
        let all_proto_nodes = vec![node_0];
        let node_factory = NodeFactoryImpl::new();
        let context = create_factory_context();

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).err().unwrap();

        // assert
        assert!(matches!(err, NodeError::OperatorNotSet { .. }));
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
        let context = create_factory_context();

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).err().unwrap();

        // assert
        assert!(matches!(err, NodeError::LeftChildNotSet { .. }));
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
        let context = create_factory_context();

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).err().unwrap();

        // assert
        assert!(matches!(err, NodeError::BinaryOperatorIncorrectChildren));
    }

    #[test]
    fn test_validate_unary_operator_with_two_children_fails() {
        // setup - root node has 2 child nodes but unary operator (NOT)
        let all_proto_nodes = create_true_false_branch(LogicalOperator::NOT);
        let node_factory = NodeFactoryImpl::new();
        let context = create_factory_context();

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).err().unwrap();

        // assert
        assert!(matches!(err, NodeError::UnaryOperatorIncorrectChildren));
    }

    #[test]
    fn test_validate_unary_operator_no_right_child_ok() {
        // setup
        let node_factory = NodeFactoryImpl::new();
        let context = create_factory_context();
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
        let nodes = node_factory.validate(&all_proto_nodes, &context);

        // assert
        assert!(nodes.is_ok());
    }

    #[test]
    fn test_validate_arithmetic_operator_incompatible_types_should_fail() {
        let node_factory = NodeFactoryImpl {};
        let context = create_factory_context();
        let mut node_0 = NodeProto::new();
        let mut node_1 = NodeProto::new();

        let mut constant_leaf_true = ConstantLeafNode::new();
        constant_leaf_true.set_bool_value(true);
        node_0.set_constant_leaf_node(constant_leaf_true);

        let mut add_node = CombinationNode::new();
        add_node.left_index = Some(0); // True
        add_node.right_index = Some(0); // True
        add_node.set_arithmetic_operator(ArithmeticOperator::ADD);
        node_1.set_combination_node(add_node);

        let all_proto_nodes = vec![node_0, node_1];

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).err().unwrap();

        assert!(matches!(err, NodeError::ArithmeticOperatorIncompatibleOperands));
    }

    #[test]
    fn test_validate_relational_operator_incompatible_types_should_fail() {
        let node_factory = NodeFactoryImpl {};
        let context = create_factory_context();
        let mut node_0 = NodeProto::new();
        let mut node_1 = NodeProto::new();
        let mut node_2 = NodeProto::new();

        let mut field_leaf = FieldLeafNode::new();
        field_leaf.publisher_name = PUBLISHER_NAME.to_string();
        field_leaf.field_names = vec!["message_field".to_string(), "repeated_i32s".to_string()];
        node_0.set_field_leaf_node(field_leaf);

        let mut constant_leaf_true = ConstantLeafNode::new();
        constant_leaf_true.set_bool_value(true);
        node_1.set_constant_leaf_node(constant_leaf_true);

        let mut contains_node = CombinationNode::new();
        contains_node.left_index = Some(0); // list of ints
        contains_node.right_index = Some(1); // a single boolean
        contains_node.set_relational_operator(RelationalOperator::CONTAINS);
        node_2.set_combination_node(contains_node);

        let all_proto_nodes = vec![node_0, node_1, node_2];

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).err().unwrap();

        assert!(matches!(err, NodeError::ContainsOperatorIncompatibleOperands));
    }

    #[test]
    fn test_validate_logical_operator_incompatible_types_should_fail() {
        let node_factory = NodeFactoryImpl {};
        let context = create_factory_context();
        let mut node_0 = NodeProto::new();
        let mut node_1 = NodeProto::new();

        let mut constant_leaf_one = ConstantLeafNode::new();
        constant_leaf_one.set_int32_value(1);
        node_0.set_constant_leaf_node(constant_leaf_one);

        let mut or_node = CombinationNode::new();
        or_node.left_index = Some(0);
        or_node.right_index = Some(0);
        or_node.set_logical_operator(LogicalOperator::OR);
        node_1.set_combination_node(or_node);

        let all_proto_nodes = vec![node_0, node_1];

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).err().unwrap();

        assert!(matches!(err, NodeError::LogicalOperatorIncompatibleOperands));
    }

    #[test]
    fn test_validate_rounding_operator_incompatible_types_should_fail() {
        let node_factory = NodeFactoryImpl {};
        let context = create_factory_context();
        let mut node_0 = NodeProto::new();
        let mut node_1 = NodeProto::new();

        let mut constant_leaf_one = ConstantLeafNode::new();
        constant_leaf_one.set_int32_value(10);
        node_0.set_constant_leaf_node(constant_leaf_one);

        let mut floor_node = CombinationNode::new();
        floor_node.left_index = Some(0); // fails because rounding requires floating point
        floor_node.set_rounding_operator(RoundingOperator::FLOOR);
        node_1.set_combination_node(floor_node);

        let all_proto_nodes = vec![node_0, node_1];

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).err().unwrap();

        assert!(matches!(err, NodeError::RoundingOperatorIncompatibleOperands));
    }

    #[test]
    fn test_validate_rounding_operator_has_two_children_should_fail() {
        let node_factory = NodeFactoryImpl {};
        let context = create_factory_context();
        let mut node_0 = NodeProto::new();
        let mut node_1 = NodeProto::new();

        let mut constant_leaf_one = ConstantLeafNode::new();
        constant_leaf_one.set_int32_value(1);
        node_0.set_constant_leaf_node(constant_leaf_one);

        let mut floor_node = CombinationNode::new();
        floor_node.left_index = Some(0);
        floor_node.right_index = Some(0); // fails for having right child
        floor_node.set_rounding_operator(RoundingOperator::FLOOR);
        node_1.set_combination_node(floor_node);

        let all_proto_nodes = vec![node_0, node_1];

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).err().unwrap();

        assert!(matches!(err, NodeError::UnaryOperatorIncorrectChildren));
    }

    #[test]
    fn test_validate_published_message_no_descriptor_should_fail() {
        let mut node = NodeProto::new();
        let mut field_leaf = FieldLeafNode::new();
        field_leaf.publisher_name = PUBLISHER_NAME.to_string();
        field_leaf.field_names = vec!["int_field".to_string()];
        node.set_field_leaf_node(field_leaf);
        let all_proto_nodes = vec![node];

        let node_factory = NodeFactoryImpl {};
        // make descriptor provider return error
        let context = NodeFactoryContext {
            message_provider: None,
            descriptor_provider: Box::new(MockDescriptorProvider {
                result_provider: Some(Box::new(|| {
                    Err(DescriptorProviderError::InvalidPublisherName)
                })),
            }),
        };

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).err().unwrap();

        assert!(matches!(err, NodeError::DescriptorProviderError { .. }));
    }

    #[test]
    fn test_validate_ok() {
        let node_factory = NodeFactoryImpl::new();
        let context = create_factory_context();
        // setup tree containing all 4 types of operators
        //                    index 4 (NOT)
        //                    /
        //                index 3
        //               LT_OR_EQ
        //              /          \
        //         index 1        index 2
        //          ROUND          MULTIPLY
        //           /           /         \
        //    index 0          index 0    index 0
        //       0.5             0.5         0.5
        let mut node_0 = NodeProto::new();
        let mut node_1 = NodeProto::new();
        let mut node_2 = NodeProto::new();
        let mut node_3 = NodeProto::new();
        let mut node_4 = NodeProto::new();

        let mut constant_leaf_point_five = ConstantLeafNode::new();
        constant_leaf_point_five.set_double_value(0.5);
        node_0.set_constant_leaf_node(constant_leaf_point_five);

        let mut round_node = CombinationNode::new();
        round_node.left_index = Some(0); // 0.5
        round_node.set_rounding_operator(RoundingOperator::ROUND);
        node_1.set_combination_node(round_node);

        let mut multiply_node = CombinationNode::new();
        multiply_node.left_index = Some(0); // 0.5
        multiply_node.right_index = Some(0); // 0.5
        multiply_node.set_arithmetic_operator(ArithmeticOperator::MULTIPLY);
        node_2.set_combination_node(multiply_node);

        let mut lt_eq_node = CombinationNode::new();
        lt_eq_node.left_index = Some(1); // 0.5 rounded = 1
        lt_eq_node.right_index = Some(2); // 0.5 * 0.5 = 0.25
        lt_eq_node.set_relational_operator(RelationalOperator::LT_OR_EQ);
        node_3.set_combination_node(lt_eq_node);

        let mut not_node = CombinationNode::new();
        not_node.left_index = Some(3); // 1 <= 0.25
        not_node.set_logical_operator(LogicalOperator::NOT);
        node_4.set_combination_node(not_node);

        let all_proto_nodes = vec![node_0, node_1, node_2, node_3, node_4];
        let expected_types = vec![
            NodeValueType::Float,
            NodeValueType::Int,
            NodeValueType::Float,
            NodeValueType::Bool,
            NodeValueType::Bool,
        ];

        // action
        let node_value_types = node_factory.validate(&all_proto_nodes, &context).unwrap();

        // assert
        for (actual_type, expected_type) in node_value_types.into_iter().zip(expected_types) {
            assert_eq!(actual_type, expected_type);
        }
    }

    #[test]
    fn test_create_nodes_ok() -> Result<(), NodeError> {
        // setup
        let op = LogicalOperator::AND;
        let node_factory = NodeFactoryImpl {};
        let context = create_factory_context();
        let all_proto_nodes = create_true_false_branch(op);

        // action
        let (nodes, node_types) = node_factory.create_nodes(&all_proto_nodes, &context)?;

        // assert 1-1 correspondence between rust node and protobuf node
        for (rust_node, proto_node) in nodes.iter().zip(all_proto_nodes) {
            match rust_node.as_ref() {
                Node::ConstantLeaf { value } => {
                    if let NodeValue::Bool(value) = value {
                        assert_eq!(*value, proto_node.constant_leaf_node().bool_value());
                    } else {
                        panic!(
                            "Input Nodes contain only boolean constants, \
                                    output Node {:?} is a non-boolean constant",
                            value
                        );
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
                    panic!(
                        "Output Nodes are expected to contain ConstantLeaf and \
                                Combination enum variants only"
                    )
                }
            }
        }
        // assert all nodes in this tree output boolean results
        assert!(node_types.iter().all(|value_type| *value_type == NodeValueType::Bool));
        Ok(())
    }

    #[test]
    fn test_create_nodes_unary_operator_cannot_have_two_children() {
        // setup - root node has 2 child nodes but unary operator (NOT)
        let all_proto_nodes = create_true_false_branch(LogicalOperator::NOT);
        let node_factory = NodeFactoryImpl {};
        let context = create_factory_context();

        // action
        let res = node_factory.create_nodes(&all_proto_nodes, &context);

        // assert
        assert!(matches!(res.err().unwrap(), NodeError::UnaryOperatorIncorrectChildren));
    }

    #[test]
    fn test_create_nodes_unary_operator_missing_right_child_ok() {
        // setup
        let node_factory = NodeFactoryImpl {};
        let context = create_factory_context();
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
        let res = node_factory.create_nodes(&all_proto_nodes, &context);

        // assert
        assert!(res.is_ok());
    }

    #[test]
    fn test_create_nodes_binary_operator_missing_right_child_errors() {
        // setup
        let node_factory = NodeFactoryImpl {};
        let context = create_factory_context();
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
        let res = node_factory.create_nodes(&all_proto_nodes, &context);

        // assert error because binary operator needs 2 children
        assert!(matches!(res.err().unwrap(), NodeError::BinaryOperatorIncorrectChildren));
    }

    #[test]
    fn test_create_nodes_handles_index_out_bound() {
        // setup root node with children at index 10 and 20 (out of bound)
        let node_factory = NodeFactoryImpl {};
        let context = create_factory_context();
        let mut proto_node = NodeProto::new();
        let mut combination_node = CombinationNode::new();
        combination_node.left_index = Some(10);
        combination_node.right_index = Some(20);
        combination_node.set_logical_operator(LogicalOperator::AND);
        proto_node.set_combination_node(combination_node);

        let all_proto_nodes = vec![proto_node];

        // action
        let res = node_factory.create_nodes(&all_proto_nodes, &context);

        // assert
        assert!(matches!(res.err().unwrap(), NodeError::IndexOutOfBound { .. }));
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
        let context = create_factory_context();

        // action
        let err = node_factory.create_nodes(&all_proto_nodes, &context).err().unwrap();

        // assert
        assert!(matches!(err, NodeError::CyclicDependency { .. }));
    }

    #[test]
    fn test_evaluate_logical_operator_ok() -> Result<(), NodeError> {
        // setup this tree:
        //               XOR
        //          /             \
        //        True             OR
        //                       /        \
        //                    True       False
        let true_node = Arc::new(Node::ConstantLeaf { value: NodeValue::Bool(true) });
        let false_node = Arc::new(Node::ConstantLeaf { value: NodeValue::Bool(false) });
        let or_node = Arc::new(Node::Combination {
            operator: OP_OR.clone(),
            left: Some(true_node.clone()),
            right: Some(false_node.clone()),
        });
        let xor_node = Node::Combination {
            operator: OP_XOR.clone(),
            left: Some(true_node.clone()),
            right: Some(or_node.clone()),
        };
        let nodes = [true_node, false_node, or_node, xor_node.into()];

        // we will evaluate every node in `nodes`, and we expect these results:
        let expected_results = [
            NodeValue::Bool(true),  // true_node
            NodeValue::Bool(false), // false_node
            NodeValue::Bool(true),  // true || false = true
            NodeValue::Bool(false), // true ^ (true || false) = false
        ];

        // assert that evaluated_result[i] == expected_result[i] for all index i
        for (rust_node, expected_val_ref) in nodes.iter().zip(expected_results.iter()) {
            // action + assertion
            assert_eq!(rust_node.evaluate()?, *expected_val_ref);
        }
        Ok(())
    }

    #[test]
    fn test_evaluate_arithmetic_operator_ok() -> Result<(), NodeError> {
        // setup this tree: 5 / 2 * 5^2
        //              MULTIPLY
        //          /             \
        //       DIVIDE            POWER
        //    /         \        /        \
        //   5          2       5           2
        let five_node = Arc::new(Node::ConstantLeaf { value: NodeValue::Int32(5) });
        let two_node = Arc::new(Node::ConstantLeaf { value: NodeValue::Int32(2) });
        let divide_node = Arc::new(Node::Combination {
            operator: OP_DIVIDE.clone(),
            left: Some(five_node.clone()),
            right: Some(two_node.clone()),
        });
        let power_node = Arc::new(Node::Combination {
            operator: OP_POWER.clone(),
            left: Some(five_node.clone()),
            right: Some(two_node.clone()),
        });
        let multiply_node = Arc::new(Node::Combination {
            operator: OP_MULTIPLY.clone(),
            left: Some(divide_node.clone()),
            right: Some(power_node.clone()),
        });
        let nodes = [five_node, two_node, divide_node, power_node, multiply_node];

        // we will evaluate every node in `nodes`, and we expect these results:
        let expected_results = [
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
    fn test_evaluate_relational_operator_ok() -> Result<(), NodeError> {
        // setup this tree which uses the protobuf message in the expr/mock.rs
        //                                 EQ
        //                           /              \
        //                      CONTAINS                  true
        //               /                  \
        // TestMessage.repeated_doubles       Pi
        let repeated_doubles_node = Arc::new(Node::FieldLeaf {
            publisher_name: PUBLISHER_NAME.to_string(),
            fields: vec!["repeated_doubles".to_string()],
            message_provider: MOCK_MESSAGE_PROVIDER.clone(),
        });
        let pi_node =
            Arc::new(Node::ConstantLeaf { value: NodeValue::Float(std::f32::consts::PI) });
        let contains_node = Arc::new(Node::Combination {
            operator: OP_CONTAINS.clone(),
            left: Some(repeated_doubles_node.clone()),
            right: Some(pi_node.clone()),
        });
        let eq_node = Arc::new(Node::Combination {
            operator: OP_EQ.clone(),
            left: Some(contains_node.clone()),
            right: Some(Arc::new(Node::ConstantLeaf { value: NodeValue::Bool(true) })),
        });
        let nodes = [repeated_doubles_node, pi_node, contains_node, eq_node];

        // we will evaluate every node in `nodes`, and we expect these results:
        let expected_results = [
            // index 0 = [Pi, Tau, e]
            NodeValue::DoubleList(TEST_MESSAGE.repeated_doubles.clone()),
            NodeValue::Float(std::f32::consts::PI), // constant value of Pi
            NodeValue::Bool(true),                  // list contains Pi = true
            NodeValue::Bool(true),                  // true == true
        ];

        // assert that evaluated_result[i] == expected_result[i]  for all index i
        for (rust_node, expected_val_ref) in nodes.iter().zip(expected_results.iter()) {
            assert_eq!(rust_node.evaluate()?, *expected_val_ref);
        }
        Ok(())
    }

    #[test]
    fn test_evaluate_rounding_operator_ok() -> Result<(), NodeError> {
        let pi_node =
            Arc::new(Node::ConstantLeaf { value: NodeValue::Float(std::f32::consts::PI) });
        let round_node = Arc::new(Node::Combination {
            operator: OP_ROUND.clone(),
            left: Some(pi_node.clone()),
            right: None,
        });
        let nodes = vec![pi_node, round_node];

        // we will evaluate every node in `nodes`, and we expect these results:
        let expected_results = vec![
            NodeValue::Float(std::f32::consts::PI), // constant value of Pi
            NodeValue::Int64(3),                    // Pi rounded to the nearest int
        ];

        // assert that evaluated_result[i] == expected_result[i]  for all index i
        for (rust_node, expected_val_ref) in nodes.iter().zip(expected_results.iter()) {
            assert_eq!(rust_node.evaluate()?, *expected_val_ref);
        }
        Ok(())
    }

    #[test]
    fn test_evaluate_mixed_operators() -> Result<(), NodeError> {
        // setup tree containing all 4 types of operators
        //                       logical (&&)
        //                    /               \
        //     relational (LT_OR_EQ)              True
        //            /              \
        // rounding (ceil)      arithmetic (+)
        //         /                   /         \
        //        0.3                5            5
        let point_three_node = Arc::new(Node::ConstantLeaf { value: NodeValue::Float(0.3) });
        let ceil_node = Arc::new(Node::Combination {
            operator: OP_CEIL.clone(),
            left: Some(point_three_node.clone()),
            right: None,
        });
        let five_node = Arc::new(Node::ConstantLeaf { value: NodeValue::Int32(5) });
        let add_node = Arc::new(Node::Combination {
            operator: OP_ADD.clone(),
            left: Some(five_node.clone()),
            right: Some(five_node.clone()),
        });
        let lt_or_eq_node = Arc::new(Node::Combination {
            operator: OP_LT_OR_EQ.clone(),
            left: Some(five_node.clone()),
            right: Some(add_node.clone()),
        });
        let and_node = Arc::new(Node::Combination {
            operator: OP_AND.clone(),
            left: Some(lt_or_eq_node.clone()),
            right: Some(Arc::new(Node::ConstantLeaf { value: NodeValue::Bool(true) })),
        });
        let nodes = vec![point_three_node, ceil_node, five_node, add_node, lt_or_eq_node, and_node];

        // we will evaluate every node in `nodes`, and we expect these results:
        let expected_results = vec![
            NodeValue::Float(0.3), // constant of 0.3
            NodeValue::Int64(1),   // 0.3 rounded up
            NodeValue::Int32(5),   // constant of 5
            NodeValue::Int64(10),  // 5+5 = 10
            NodeValue::Bool(true), // True because 5 <= 10
            NodeValue::Bool(true), // True && True = True
        ];

        // assert that evaluated_result[i] == expected_result[i] for all index i
        for (rust_node, expected_val_ref) in nodes.iter().zip(expected_results.iter()) {
            assert_eq!(rust_node.evaluate()?, *expected_val_ref);
        }
        Ok(())
    }

    #[test]
    fn test_evaluate_mixed_leaf_node_types() -> Result<(), NodeError> {
        // setup this tree which uses the message initialized in expr/mock.rs
        //                           index 4 (<)
        //                       /               \
        //                index 2                  index 3
        //                 DIVIDE                TestMessage.int_field
        //           /                \
        //     index 0               index 1
        // TestMessage.float_field       2
        let float_field_node = Arc::new(Node::FieldLeaf {
            publisher_name: PUBLISHER_NAME.to_string(),
            fields: vec!["float_field".to_string()],
            message_provider: MOCK_MESSAGE_PROVIDER.clone(),
        });
        let divide_node = Arc::new(Node::Combination {
            operator: OP_DIVIDE.clone(),
            left: Some(float_field_node.clone()),
            right: Some(Arc::new(Node::ConstantLeaf { value: NodeValue::Int32(2) })),
        });
        let int_field_node = Arc::new(Node::FieldLeaf {
            publisher_name: PUBLISHER_NAME.to_string(),
            fields: vec!["int_field".to_string()],
            message_provider: MOCK_MESSAGE_PROVIDER.clone(),
        });
        let lt_node = Arc::new(Node::Combination {
            operator: OP_LT.clone(),
            left: Some(divide_node.clone()),
            right: Some(int_field_node.clone()),
        });

        let nodes = vec![float_field_node, divide_node, int_field_node, lt_node];

        // we will evaluate every node in `nodes`, and we expect these results:
        let expected_results = [
            NodeValue::Float(100.0), // field_field has value of 100
            NodeValue::Double(50.0), // 100 / 2 = 50
            NodeValue::Int32(80),    // int_field has value 80
            NodeValue::Bool(true),   // 50 < 80 = true
        ];

        // assert that evaluated_result[i] == expected_result[i]  for all index i
        for (rust_node, expected_val_ref) in nodes.iter().zip(expected_results.iter()) {
            assert_eq!(rust_node.evaluate()?, *expected_val_ref);
        }
        Ok(())
    }

    #[test]
    fn test_evaluate_incompatible_types_should_fail() {
        let zero_node = Arc::new(Node::ConstantLeaf { value: NodeValue::Int32(0) });
        let true_node = Arc::new(Node::ConstantLeaf { value: NodeValue::Bool(true) });
        let lt_node = Arc::new(Node::Combination {
            operator: OperatorProto::RelationalOperator(RelationalOperator::LT.into()),
            left: Some(zero_node),
            right: Some(true_node),
        });

        // expr is "0 < True"
        let result = lt_node.evaluate();

        // 0 cannot be less than True
        assert!(matches!(result.err().unwrap(), NodeError::ComparisonOperatorIncomptaibleOperands));
    }

    #[test]
    fn test_evaluate_message_provider_invalid_publisher_name_fails() {
        let message_provider = Arc::new(MockMessageProvider::new());
        message_provider.set_next_result(Err(MessageProviderError::InvalidPublisherName));
        let node =
            Node::FieldLeaf { publisher_name: String::default(), fields: vec![], message_provider };

        let err = node.evaluate().err().unwrap();

        assert!(matches!(err, NodeError::MessageProviderError { .. }));
    }

    #[test]
    fn test_evaluate_no_provided_message_returns_none() {
        let message_provider = Arc::new(MockMessageProvider::new());
        message_provider.set_next_result(Ok(None));
        let node =
            Node::FieldLeaf { publisher_name: String::default(), fields: vec![], message_provider };

        let res = node.evaluate().unwrap();

        assert_eq!(res, NodeValue::None);
    }
}
