// Copyright 2024 Google LLC

use protobuf::{reflect::MessageDescriptor, MessageDyn};
use sdv_telemetry_proto_rust::expression::{
    function_leaf_node::Function_type as FunctionLeafNodeTypeProto,
    get_current_timestamp_function::TimeSource as TimeSourceProto,
    node::Node_type as NodeTypeProto, FunctionLeafNode as FunctionLeafNodeProto, Node as NodeProto,
};
use sdv_telemetry_utils::operation_result::OperationResult;
use std::marker::{Send, Sync};
use std::sync::Arc;

use crate::expr::node::{FunctionType, Node, TimeSource};
use crate::expr::node_error::{
    DescriptorProviderError, FunctionLeafNodeArgumentError,
    FunctionLeafNodeGetCurrentTimestampArgumentError, MessageProviderError, NodeError,
    NodeEvaluationError,
};
use crate::expr::node_value::NodeValue;
use crate::expr::node_value_type::{
    FieldLeafNodeExtension, NodeValueType, NodeValueTypeExtension, OperatorProtoExtension,
};

/// Creates rust nodes from protobuf expression nodes.
#[cfg_attr(test, sdv_telemetry_utils_macros::with_super_trait(downcast_rs::Downcast))]
pub trait NodeFactory: Send + Sync {
    /// Creates rust Nodes from protobuf Nodes,.
    /// Protobuf nodes use indices to represent left and right children.
    /// Rust nodes have a pointer to the actual children instead of an index.
    /// Returns list of Nodes and their output value types. The two vectors have the
    /// same length.
    fn create_nodes(
        &self,
        proto_nodes: &[NodeProto],
        context: &NodeFactoryContext,
    ) -> Result<(Vec<Arc<Node>>, Vec<Option<NodeValueType>>), NodeError>;

    /// Validate the expression nodes to ensure they do not contain cyclic dependencies
    /// and all indices are valid.
    fn validate(
        &self,
        proto_nodes: &[NodeProto],
        context: &NodeFactoryContext, // Copyright 2024 Google LLC
    ) -> Result<Vec<Option<NodeValueType>>, NodeError>;
}

#[cfg(test)]
downcast_rs::impl_downcast!(NodeFactory);

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
    ) -> OperationResult<'static, Option<Arc<dyn MessageDyn>>, MessageProviderError>;
}

/// Provides descriptors for published message types.
pub trait DescriptorProvider: Send + Sync {
    /// Provides protobuf message descriptor by publisher name.
    fn get_descriptor(
        &self,
        publisher_name: &str,
    ) -> Result<Option<&MessageDescriptor>, DescriptorProviderError>;
}

/// Used for cycle detection in the expression nodes.
#[derive(Clone)]
enum VisitStatus {
    NotVisited,
    Visiting,
    Visited,
}

#[derive(Clone)]
pub struct VisitInfo {
    visit_status: VisitStatus,
    parent_index: Option<usize>,
}

/// Implementation of NodeFactory trait.
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
    /// * `parent_index` - The index of the node that was visited previously or None if the current
    ///   node is the first one to visit.
    /// * `proto_nodes` - The list of expression nodes in the MetricsConfig.
    /// * `visit_info` - Tracks if a node has been visited by index, together with tracking node's parent.
    ///                  This information is used to detect and restore cyclic dependencies.
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
        parent_index: Option<usize>,
        proto_nodes: &[NodeProto],
        visit_info: &mut [VisitInfo],
        nodes: &mut [Option<Arc<Node>>],
        node_value_types: &mut [Option<NodeValueType>],
        context: &NodeFactoryContext,
    ) -> Result<Option<Arc<Node>>, NodeError> {
        let status = &visit_info
            .get(index)
            .ok_or(NodeError {
                index,
                err: NodeEvaluationError::IndexOutOfBound { n: proto_nodes.len() },
            })?
            .visit_status;

        // detect cycles using visit status, and visit the node
        match status {
            VisitStatus::Visiting => Err(NodeError {
                index,
                err: NodeEvaluationError::CyclicDependency {
                    cycle: Self::restore_cycle(
                        parent_index.expect("parent index should be set in order to restore cycle"),
                        index,
                        visit_info,
                    ),
                },
            }),
            VisitStatus::Visited => {
                if nodes.is_empty() {
                    Ok(None)
                } else {
                    Ok(nodes[index].clone())
                }
            }
            VisitStatus::NotVisited => {
                // mark index as "visiting", if visit() comes back to this index, there is a cycle
                visit_info[index].visit_status = VisitStatus::Visiting;
                visit_info[index].parent_index = parent_index;

                // match on the node type and capture value returned by match block
                let rust_node = match &proto_nodes[index].node_type {
                    // base case 1: constant leaf node, can return from match block immediately
                    Some(NodeTypeProto::ConstantLeafNode(constant_leaf)) => {
                        node_value_types[index] = Some(
                            constant_leaf
                                .get_node_value_type()
                                .map_err(|err| NodeError { index, err })?,
                        );
                        Self::maybe_create_node(nodes, index, || {
                            Ok(Arc::new(Node::ConstantLeaf {
                                value: NodeValue::from_constant_leaf_node(constant_leaf)?,
                            }))
                        })?
                    }
                    // base case 2: field leaf node, can return from match block immediately
                    Some(NodeTypeProto::FieldLeafNode(field_leaf)) => {
                        node_value_types[index] = field_leaf
                            .get_node_value_type(context.descriptor_provider.as_ref())
                            .map_err(|err| NodeError { index, err })?;
                        Self::maybe_create_node(nodes, index, || {
                            Ok(Arc::new(Node::FieldLeaf {
                                publisher_name: field_leaf.publisher_name.clone(),
                                fields: field_leaf.field_names.clone(),
                                message_provider: Arc::clone(
                                    context.message_provider.as_ref().unwrap(),
                                ),
                            }))
                        })?
                    }
                    // base case 3: function leaf node, can return from match block immediately
                    Some(NodeTypeProto::FunctionLeafNode(function_leaf_node)) => {
                        node_value_types[index] = Some(
                            function_leaf_node
                                .get_node_value_type()
                                .map_err(|err| NodeError { index, err })?,
                        );
                        let function = Self::to_function_type(function_leaf_node)
                            .map_err(|err| NodeError { index, err })?;
                        Self::maybe_create_node(nodes, index, move || {
                            Ok(Arc::new(Node::FunctionLeaf { function }))
                        })?
                    }
                    // recursive case: visit left, right, self
                    Some(NodeTypeProto::CombinationNode(combination_node)) => {
                        let op = combination_node
                            .operator
                            .as_ref()
                            .ok_or(NodeError { index, err: NodeEvaluationError::OperatorNotSet })?;
                        // before recursion, check if the child node is set up correctly
                        op.check_right_child(combination_node.right_index)
                            .map_err(|err| NodeError { index, err })?;

                        // visit left child
                        let left_child_index = combination_node
                            .left_index
                            .ok_or(NodeError { index, err: NodeEvaluationError::LeftChildNotSet })?
                            as usize;
                        let left = Self::visit(
                            left_child_index,
                            Some(index),
                            proto_nodes,
                            visit_info,
                            nodes,
                            node_value_types,
                            context,
                        )?;

                        // visit right child if right child is specified
                        let right = if let Some(right_child_index) = combination_node.right_index {
                            Self::visit(
                                right_child_index as usize,
                                Some(index),
                                proto_nodes,
                                visit_info,
                                nodes,
                                node_value_types,
                                context,
                            )?
                        } else {
                            None
                        };

                        // check if right operand is compatible with operator and assign type
                        node_value_types[index] = op
                            .get_node_value_type(
                                node_value_types,
                                left_child_index,
                                combination_node.right_index,
                            )
                            .map_err(|err| NodeError { index, err })?;

                        // visit self
                        Self::maybe_create_node(nodes, index, move || {
                            Ok(Arc::new(Node::Combination { operator: op.clone(), left, right }))
                        })?
                    } // match arm `Some(CombinationNode)`
                    Some(_) => {
                        return Err(NodeError {
                            index,
                            err: NodeEvaluationError::NodeTypeNotSupported,
                        })
                    }
                    None => {
                        return Err(NodeError { index, err: NodeEvaluationError::NodeTypeNotSet })
                    }
                };

                // mark index as visited and return Ok
                visit_info[index].visit_status = VisitStatus::Visited;
                Ok(rust_node)
            } // match arm `VisitStatus::NotVisited`
        } // `match` block
    }

    fn to_function_type(
        function_leaf_node: &FunctionLeafNodeProto,
    ) -> Result<FunctionType, NodeEvaluationError> {
        let function_type = function_leaf_node
            .function_type
            .as_ref()
            .ok_or(NodeEvaluationError::FunctionLeafNodeNotSet)?;
        match function_type {
            FunctionLeafNodeTypeProto::GetCurrentTimestamp(get_current_timestamp) => {
                let time_source = get_current_timestamp.time_source.enum_value().map_err(|_| {
                    NodeEvaluationError::FunctionLeafNodeArgumentError {
                        error: FunctionLeafNodeArgumentError::GetCurrentTimestampError(
                            FunctionLeafNodeGetCurrentTimestampArgumentError::TimeSourceNotSupported,
                        ),
                    }
                })?;
                let time_source = match time_source {
                    TimeSourceProto::TIME_SOURCE_UNSPECIFIED => {
                        return Err(NodeEvaluationError::FunctionLeafNodeArgumentError {
                            error: FunctionLeafNodeArgumentError::GetCurrentTimestampError(
                                FunctionLeafNodeGetCurrentTimestampArgumentError::TimeSourceNotSet,
                            ),
                        })
                    }
                    TimeSourceProto::REALTIME_CLOCK => TimeSource::RealtimeClock,
                    TimeSourceProto::MONOTONIC_TIME_SINCE_BOOT => {
                        TimeSource::MonotonicTimeSinceBoot
                    }
                };
                Ok(FunctionType::GetCurrentTimestamp { time_source })
            }
            _ => Err(NodeEvaluationError::FunctionLeafNodeNotSupported),
        }
    }

    /// If the `nodes` vector is non-empty, it will call the `create_node_func` to create
    /// a Rust Node, put the node into the vector at the `index`, and return that node.
    /// Otherwise it will return a success status containing nothing.
    fn maybe_create_node(
        nodes: &mut [Option<Arc<Node>>],
        index: usize,
        create_node_func: impl FnOnce() -> Result<Arc<Node>, NodeEvaluationError>,
    ) -> Result<Option<Arc<Node>>, NodeError> {
        if nodes.is_empty() {
            return Ok(None);
        }
        if index >= nodes.len() {
            return Err(NodeError {
                index,
                err: NodeEvaluationError::IndexOutOfBound { n: nodes.len() },
            });
        }
        match create_node_func() {
            Ok(node) => {
                nodes[index] = Some(node);
                Ok(nodes[index].clone())
            }
            Err(err) => Err(NodeError { index, err }),
        }
    }

    fn restore_cycle(
        start_index: usize,
        finish_index: usize,
        visit_info: &mut [VisitInfo],
    ) -> Vec<usize> {
        let mut cycle = vec![];
        let mut current_index = start_index;
        loop {
            cycle.push(current_index);
            if current_index == finish_index {
                break;
            }
            current_index = visit_info[current_index]
                .parent_index
                .expect("each cycle node should have its parent set");
        }

        // For the cycle to look like (0 -> 1 -> 2 -> 0)
        cycle.push(start_index);
        cycle.reverse();
        cycle
    }
}

impl NodeFactory for NodeFactoryImpl {
    fn validate(
        &self,
        proto_nodes: &[NodeProto],
        context: &NodeFactoryContext,
    ) -> Result<Vec<Option<NodeValueType>>, NodeError> {
        let n = proto_nodes.len();
        let mut visit_info =
            vec![VisitInfo { visit_status: VisitStatus::NotVisited, parent_index: None }; n];
        let mut node_value_types = vec![None; n];
        for i in 0..n {
            Self::visit(
                i,
                None,
                proto_nodes,
                &mut visit_info,
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
    ) -> Result<(Vec<Arc<Node>>, Vec<Option<NodeValueType>>), NodeError> {
        let n = proto_nodes.len();
        let mut visit_info =
            vec![VisitInfo { visit_status: VisitStatus::NotVisited, parent_index: None }; n];
        let mut node_value_types = vec![None; n];
        let mut rust_nodes: Vec<Option<Arc<Node>>> = vec![None; n];

        for i in 0..n {
            Self::visit(
                i,
                None,
                proto_nodes,
                &mut visit_info,
                &mut rust_nodes,
                &mut node_value_types,
                context,
            )?;
        }
        // flatten Vec<Option<T>> into Vec<T>, ignoring None values.
        let nodes: Vec<Arc<Node>> = rust_nodes.into_iter().flatten().collect();
        if nodes.len() != n {
            return Err(NodeError {
                index: 0,
                err: NodeEvaluationError::InternalError {
                    msg: format!(
                        "expected to create {} nodes, but created {} nodes",
                        n,
                        nodes.len()
                    ),
                },
            });
        }
        Ok((nodes, node_value_types))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use protobuf::{EnumOrUnknown, MessageFull};
    use sdv_telemetry_proto_rust::expression::combination_node::{
        ArithmeticOperator, LogicalOperator, Operator as OperatorProto, RelationalOperator,
        RoundingOperator,
    };
    use sdv_telemetry_proto_rust::expression::{
        CombinationNode, ConstantLeafNode, FieldLeafNode, Node as NodeProto,
    };
    use sdv_telemetry_test_proto::test_message::TestMessage;
    use sdv_telemetry_test_utils::parameterized_test;

    use crate::expr::mock::*;

    const BOOL_FIELD_NAME: &str = "bool_field";
    const FLOAT_FIELD_NAME: &str = "float_field";
    const MESSAGE_FIELD_NAME: &str = "message_field";
    const INT_FIELD_NAME: &str = "int_field";
    const REPEATED_INT_FIELD_NAME: &str = "repeated_i64s";
    const KNOWN_PUBLISHER_NAME: &str = "publisher_foo";
    const UNKNOWN_PUBLISHER_NAME: &str = "publisher_bar";

    fn create_factory_context() -> NodeFactoryContext {
        let mut descriptor_provider = MockDescriptorProvider::new();
        descriptor_provider
            .descriptors
            .insert(KNOWN_PUBLISHER_NAME.to_owned(), TestMessage::descriptor());
        NodeFactoryContext {
            message_provider: Some(Arc::new(MockMessageProvider::new())),
            descriptor_provider: Box::new(descriptor_provider),
        }
    }

    /// Create a combination node with two leaf nodes.
    fn create_combination_node(
        operator: OperatorProto,
        is_left_known: bool,
        left_field_name: &str,
        is_right_known: bool,
        right_field_name: &str,
    ) -> Vec<NodeProto> {
        let mut left_node = NodeProto::new();
        let left_field_node = left_node.mut_field_leaf_node();

        if is_left_known { KNOWN_PUBLISHER_NAME } else { UNKNOWN_PUBLISHER_NAME }
            .clone_into(&mut left_field_node.publisher_name);
        left_field_node.field_names = vec![left_field_name.to_owned()];

        let mut right_node = NodeProto::new();
        let right_field_node = right_node.mut_field_leaf_node();
        if is_right_known { KNOWN_PUBLISHER_NAME } else { UNKNOWN_PUBLISHER_NAME }
            .clone_into(&mut right_field_node.publisher_name);
        right_field_node.field_names = vec![right_field_name.to_owned()];

        let mut node = NodeProto::new();
        let combination_node = node.mut_combination_node();
        combination_node.left_index = Some(0);
        combination_node.right_index = Some(1);
        combination_node.operator = Some(operator);

        vec![left_node, right_node, node]
    }

    /// Create a combination node that checks whether the specified message list
    /// contains a message.
    fn create_contains_message_node(message_list_field_names: &[&str]) -> Vec<NodeProto> {
        let mut node_0 = NodeProto::new();
        let mut node_1 = NodeProto::new();
        let mut node_2 = NodeProto::new();

        let mut message_list_field_leaf = FieldLeafNode::new();
        KNOWN_PUBLISHER_NAME.clone_into(&mut message_list_field_leaf.publisher_name);
        message_list_field_leaf.field_names =
            message_list_field_names.iter().map(|s| s.to_string()).collect();
        node_0.set_field_leaf_node(message_list_field_leaf);

        let mut message_field_leaf = FieldLeafNode::new();
        KNOWN_PUBLISHER_NAME.clone_into(&mut message_field_leaf.publisher_name);
        message_field_leaf.field_names = vec![MESSAGE_FIELD_NAME.to_owned()];
        node_1.set_field_leaf_node(message_field_leaf);

        let mut contains_node = CombinationNode::new();
        contains_node.left_index = Some(0); // list of messages
        contains_node.right_index = Some(1); // a single message
        contains_node.set_relational_operator(RelationalOperator::CONTAINS);
        node_2.set_combination_node(contains_node);

        vec![node_0, node_1, node_2]
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
    fn create_nodes_success() -> Result<(), NodeError> {
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
        assert!(node_types.iter().all(|value_type| *value_type == Some(NodeValueType::Bool)));
        Ok(())
    }

    #[test]
    fn create_nodes_unary_operator_two_children_fails() {
        // setup - root node has 2 child nodes but unary operator (NOT)
        let all_proto_nodes = create_true_false_branch(LogicalOperator::NOT);
        let node_factory = NodeFactoryImpl {};
        let context = create_factory_context();

        // action
        let err = node_factory.create_nodes(&all_proto_nodes, &context).unwrap_err();

        // assert
        assert!(
            matches!(
                &err,
                NodeError { err: NodeEvaluationError::UnaryOperatorIncorrectChildren, .. }
            ),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn create_nodes_unary_operator_success() {
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
        root_node.set_combination_node(combination_node);

        let all_proto_nodes = vec![leaf_true, root_node];

        // action
        node_factory.create_nodes(&all_proto_nodes, &context).unwrap();
    }

    #[test]
    fn create_nodes_binary_operator_no_right_child_fails() {
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
        let err = node_factory.create_nodes(&all_proto_nodes, &context).unwrap_err();

        // assert error because binary operator needs 2 children
        assert!(
            matches!(
                &err,
                NodeError { index: 1, err: NodeEvaluationError::BinaryOperatorIncorrectChildren }
            ),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn create_nodes_index_out_of_bounds_fails() {
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
        let err = node_factory.create_nodes(&all_proto_nodes, &context).unwrap_err();

        // assert
        assert!(
            matches!(
                &err,
                NodeError { index: 10, err: NodeEvaluationError::IndexOutOfBound { .. } }
            ),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn create_nodes_cyclic_dependency_fails() {
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
        let err = node_factory.create_nodes(&all_proto_nodes, &context).unwrap_err();

        // assert
        assert!(
            matches!(
                &err,
                NodeError {
                    err: NodeEvaluationError::CyclicDependency { cycle },
                    ..
                }
                if cycle == &vec![1, 0, 1]),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_node_type_not_set_fails() {
        let all_proto_nodes = vec![NodeProto::new()];
        let node_factory = NodeFactoryImpl::new();
        let context = create_factory_context();

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).unwrap_err();

        // assert
        assert!(
            matches!(&err, NodeError { index: 0, err: NodeEvaluationError::NodeTypeNotSet }),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_node_value_not_set_fails() {
        let mut node = NodeProto::new();
        node.set_constant_leaf_node(ConstantLeafNode::new());
        let all_proto_nodes = vec![node];
        let node_factory = NodeFactoryImpl::new();
        let context = create_factory_context();

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).unwrap_err();

        // assert
        assert!(
            matches!(&err, NodeError { err: NodeEvaluationError::ConstantValueNotSet { .. }, .. }),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_node_operator_not_set_fails() {
        let mut node = NodeProto::new();
        node.set_combination_node(CombinationNode::new());
        let all_proto_nodes = vec![node];
        let node_factory = NodeFactoryImpl::new();
        let context = create_factory_context();

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).unwrap_err();

        // assert
        assert!(
            matches!(&err, NodeError { err: NodeEvaluationError::OperatorNotSet, .. }),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_node_has_string_field_success() {
        let mut node_0 = NodeProto::new();
        let mut leaf = FieldLeafNode::new();
        leaf.publisher_name = KNOWN_PUBLISHER_NAME.to_string();
        leaf.field_names = vec!["string_field".to_string()];
        node_0.set_field_leaf_node(leaf);
        let all_proto_nodes = vec![node_0];
        let node_factory = NodeFactoryImpl::new();
        let context = create_factory_context();

        // action
        let nodes = node_factory.validate(&all_proto_nodes, &context);

        // assert
        assert!(nodes.is_ok());
    }

    #[test]
    fn validate_child_is_self_fails() {
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
        let err = node_factory.validate(&all_proto_nodes, &context).unwrap_err();

        // assert
        assert!(
            matches!(
                &err,
                NodeError {
                    err: NodeEvaluationError::CyclicDependency{cycle}, ..
                }
                if cycle == &vec![0, 0]),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_cyclic_dependency_fails() {
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
        let err = node_factory.validate(&all_proto_nodes, &context).unwrap_err();

        // assert
        assert!(
            matches!(
                &err,
                NodeError {
                    err: NodeEvaluationError::CyclicDependency{cycle}, ..
                }
                if cycle == &vec![1, 0, 1]),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_child_node_index_out_of_bound_fails() {
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
        let err = node_factory.validate(&all_proto_nodes, &context).unwrap_err();

        // assert
        assert!(
            matches!(
                &err,
                NodeError { index: 42, err: NodeEvaluationError::IndexOutOfBound { .. } }
            ),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_combination_node_did_not_specify_operator_fails() {
        let mut node_0 = NodeProto::new();
        node_0.set_combination_node(CombinationNode::new());
        let all_proto_nodes = vec![node_0];
        let node_factory = NodeFactoryImpl::new();
        let context = create_factory_context();

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).unwrap_err();

        // assert
        assert!(
            matches!(&err, NodeError { err: NodeEvaluationError::OperatorNotSet, .. }),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_missing_left_child_fails() {
        let mut node_0 = NodeProto::new();
        let mut combination_node = CombinationNode::new();
        combination_node.set_relational_operator(RelationalOperator::EQ);
        combination_node.right_index = Some(0);
        node_0.set_combination_node(combination_node);
        let all_proto_nodes = vec![node_0];
        let node_factory = NodeFactoryImpl::new();
        let context = create_factory_context();

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).unwrap_err();

        // assert
        assert!(
            matches!(&err, NodeError { index: 0, err: NodeEvaluationError::LeftChildNotSet }),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_binary_operator_no_right_child_fails() {
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
        let err = node_factory.validate(&all_proto_nodes, &context).unwrap_err();

        // assert
        assert!(
            matches!(
                &err,
                NodeError { index: 1, err: NodeEvaluationError::BinaryOperatorIncorrectChildren }
            ),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_unary_operator_with_two_children_fails() {
        // setup - root node has 2 child nodes but unary operator (NOT)
        let all_proto_nodes = create_true_false_branch(LogicalOperator::NOT);
        let node_factory = NodeFactoryImpl::new();
        let context = create_factory_context();

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).unwrap_err();

        // assert
        assert!(
            matches!(
                &err,
                NodeError { err: NodeEvaluationError::UnaryOperatorIncorrectChildren, .. }
            ),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_unary_operator_no_right_child_ok() {
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
        root_node.set_combination_node(combination_node);

        let all_proto_nodes = vec![leaf_true, root_node];

        // action
        let nodes = node_factory.validate(&all_proto_nodes, &context);

        // assert
        assert!(nodes.is_ok());
    }

    #[test]
    fn validate_unary_arithmetic_operator_no_right_child_ok() {
        // setup
        let node_factory = NodeFactoryImpl::new();
        let context = create_factory_context();
        let mut leaf_num = NodeProto::new();
        let mut root_node = NodeProto::new();

        let mut constant_leaf_negative_four_point_eight = ConstantLeafNode::new();
        constant_leaf_negative_four_point_eight.set_float_value(-4.8);
        leaf_num.set_constant_leaf_node(constant_leaf_negative_four_point_eight);

        //            index 1 (ABSOLUTE)
        //          /              \
        //    index 0              no right child
        //    -4.8
        let mut absolute_node = CombinationNode::new();
        absolute_node.left_index = Some(0);
        absolute_node.set_arithmetic_operator(ArithmeticOperator::ABSOLUTE);
        root_node.set_combination_node(absolute_node);

        let all_proto_nodes = vec![leaf_num, root_node];

        // action
        let nodes = node_factory.validate(&all_proto_nodes, &context);

        // assert
        assert!(nodes.is_ok());
    }

    #[test]
    fn validate_arithmetic_operator_incompatible_types_fails() {
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
        let err = node_factory.validate(&all_proto_nodes, &context).unwrap_err();

        assert!(
            matches!(
                &err,
                NodeError {
                    index: 1,
                    err: NodeEvaluationError::ArithmeticOperatorIncompatibleOperands
                }
            ),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_relational_operator_incompatible_types_fails() {
        let node_factory = NodeFactoryImpl {};
        let context = create_factory_context();
        let all_proto_nodes = create_contains_message_node(&[MESSAGE_FIELD_NAME, "repeated_i32s"]);

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).unwrap_err();

        assert!(
            matches!(
                &err,
                NodeError { err: NodeEvaluationError::ContainsOperatorIncompatibleOperands, .. }
            ),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_relational_operator_incompatible_message_types_fails() {
        let node_factory = NodeFactoryImpl {};
        let context = create_factory_context();
        let all_proto_nodes = create_contains_message_node(&["repeated_empty"]);

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).unwrap_err();

        assert!(
            matches!(
                &err,
                NodeError { err: NodeEvaluationError::ContainsOperatorIncompatibleOperands, .. }
            ),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_relational_operator_message_types_success() {
        let node_factory = NodeFactoryImpl {};
        let context = create_factory_context();
        let all_proto_nodes = create_contains_message_node(&["repeated_messages"]);

        // action
        node_factory.validate(&all_proto_nodes, &context).unwrap();
    }

    #[test]
    fn validate_logical_operator_incompatible_types_fails() {
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
        let err = node_factory.validate(&all_proto_nodes, &context).unwrap_err();

        assert!(
            matches!(
                &err,
                NodeError {
                    index: 1,
                    err: NodeEvaluationError::LogicalOperatorIncompatibleOperands
                }
            ),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_rounding_operator_incompatible_types_fails() {
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
        let err = node_factory.validate(&all_proto_nodes, &context).unwrap_err();

        assert!(
            matches!(
                &err,
                NodeError {
                    index: 1,
                    err: NodeEvaluationError::RoundingOperatorIncompatibleOperands
                }
            ),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_rounding_operator_has_two_children_fails() {
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
        let err = node_factory.validate(&all_proto_nodes, &context).unwrap_err();

        assert!(
            matches!(
                &err,
                NodeError { index: 1, err: NodeEvaluationError::UnaryOperatorIncorrectChildren }
            ),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn validate_rounding_operator_no_right_child_ok() {
        // setup
        let node_factory = NodeFactoryImpl::new();
        let context = create_factory_context();
        let mut node_0 = NodeProto::new();
        let mut node_1 = NodeProto::new();

        let mut constant_leaf_negative_four_point_eight = ConstantLeafNode::new();
        constant_leaf_negative_four_point_eight.set_float_value(-4.8);
        node_0.set_constant_leaf_node(constant_leaf_negative_four_point_eight);

        //            index 1 (CEIL)
        //          /              \
        //    index 0              no right child
        //    -4.8
        let mut ceil_node = CombinationNode::new();
        ceil_node.left_index = Some(0);
        ceil_node.set_rounding_operator(RoundingOperator::CEIL);
        node_1.set_combination_node(ceil_node);

        let all_proto_nodes = vec![node_0, node_1];

        // action
        let nodes = node_factory.validate(&all_proto_nodes, &context);

        // assert
        assert!(nodes.is_ok());
    }

    #[test]
    fn validate_field_leaf_node_no_descriptor_success() {
        let node_factory = NodeFactoryImpl {};
        let context = create_factory_context();

        let mut node = NodeProto::new();
        let field_leaf = node.mut_field_leaf_node();
        UNKNOWN_PUBLISHER_NAME.clone_into(&mut field_leaf.publisher_name);
        field_leaf.field_names = vec![INT_FIELD_NAME.to_owned()];

        // action
        let res = node_factory.validate(&[node], &context).unwrap();

        assert_eq!(&res, &[None]);
    }

    #[test]
    fn validate_field_leaf_node_descriptor_provider_error_fails() {
        let node_factory = NodeFactoryImpl {};
        // make descriptor provider return error
        let mut descriptor_provider = MockDescriptorProvider::new();
        descriptor_provider
            .error_provider
            .replace(Box::new(|| DescriptorProviderError::InvalidPublisherName));
        let context = NodeFactoryContext {
            message_provider: None,
            descriptor_provider: Box::new(descriptor_provider),
        };

        let mut node = NodeProto::new();
        let mut field_leaf = FieldLeafNode::new();
        KNOWN_PUBLISHER_NAME.clone_into(&mut field_leaf.publisher_name);
        field_leaf.field_names = vec![INT_FIELD_NAME.to_owned()];
        node.set_field_leaf_node(field_leaf);
        let all_proto_nodes = vec![node];

        // action
        let err = node_factory.validate(&all_proto_nodes, &context).unwrap_err();

        assert!(
            matches!(
                &err,
                NodeError { index: 0, err: NodeEvaluationError::DescriptorProviderError { .. } }
            ),
            "Unexpected error: {:?}",
            err
        );
    }

    // Validates relational operators with different operand availability combinations.
    parameterized_test!(
        #[test]
        validate_combination_node,

        |operator: RelationalOperator, is_left_known, is_right_known, is_list| {
            let node_factory = NodeFactoryImpl {};
            let context = create_factory_context();
            let all_proto_nodes = create_combination_node(
                OperatorProto::RelationalOperator(operator.into()),
                is_left_known,
                if is_list { REPEATED_INT_FIELD_NAME } else { INT_FIELD_NAME },
                is_right_known,
                INT_FIELD_NAME);

            let res = node_factory.validate(&all_proto_nodes, &context).unwrap();

            assert_eq!(&res[2], &Some(NodeValueType::Bool));
        },

        gt_left_unknown_type_success: (RelationalOperator::GT, false, true, false,),
        gt_right_unknown_type_success: (RelationalOperator::GT, true, false, false,),
        gt_both_unknown_types_success: (RelationalOperator::GT, false, false, false,),
        gt_or_eq_left_unknown_type_success: (RelationalOperator::GT_OR_EQ, false, true, false,),
        gt_or_eq_right_unknown_type_success: (RelationalOperator::GT_OR_EQ, true, false, false,),
        gt_or_eq_both_unknown_types_success: (RelationalOperator::GT_OR_EQ, false, false, false,),
        lt_left_unknown_type_success: (RelationalOperator::LT, false, true, false,),
        lt_right_unknown_type_success: (RelationalOperator::LT, true, false, false,),
        lt_both_unknown_types_success: (RelationalOperator::LT, false, false, false,),
        lt_or_eq_left_unknown_type_success: (RelationalOperator::LT_OR_EQ, false, true, false,),
        lt_or_eq_right_unknown_type_success: (RelationalOperator::LT_OR_EQ, true, false, false,),
        lt_or_eq_both_unknown_types_success: (RelationalOperator::LT_OR_EQ, false, false, false,),
        contains_left_unknown_type_success: (RelationalOperator::CONTAINS, false, true, true,),
        contains_right_unknown_type_success: (RelationalOperator::CONTAINS, true, false, true,),
        contains_both_unknown_types_success: (RelationalOperator::CONTAINS, false, false, true,),
        does_not_contain_left_unknown_type_success: (RelationalOperator::DOES_NOT_CONTAIN, false, true, true,),
        does_not_contain_right_unknown_type_success: (RelationalOperator::DOES_NOT_CONTAIN, true, false, true,),
        does_not_contain_both_unknown_types_success: (RelationalOperator::DOES_NOT_CONTAIN, false, false, true,),
        all_eq_left_unknown_type_success: (RelationalOperator::ALL_EQ, false, true, true,),
        all_eq_right_unknown_type_success: (RelationalOperator::ALL_EQ, true, false, true,),
        all_eq_both_unknown_types_success: (RelationalOperator::ALL_EQ, false, false, true,),
    );

    // Validates logical operators with different operand availability combinations.
    parameterized_test!(
        #[test]
        validate_combination_node,

        |operator: LogicalOperator, is_left_known, is_right_known| {
            let node_factory = NodeFactoryImpl {};
            let context = create_factory_context();
            let all_proto_nodes = create_combination_node(
                OperatorProto::LogicalOperator(operator.into()),
                is_left_known,
                BOOL_FIELD_NAME,
                is_right_known,
                BOOL_FIELD_NAME);

            let res = node_factory.validate(&all_proto_nodes, &context).unwrap();

            assert_eq!(&res[2], &Some(NodeValueType::Bool));
        },

        and_left_unknown_type_success: (LogicalOperator::AND, false, true,),
        and_right_unknown_type_success: (LogicalOperator::AND, true, false,),
        and_both_unknown_types_success: (LogicalOperator::AND, false, false,),
        or_left_unknown_type_success: (LogicalOperator::OR, false, true,),
        or_right_unknown_type_success: (LogicalOperator::OR, true, false,),
        or_both_unknown_types_success: (LogicalOperator::OR, false, false,),
        xor_left_unknown_type_success: (LogicalOperator::XOR, false, true,),
        xor_right_unknown_type_success: (LogicalOperator::XOR, true, false,),
        xor_both_unknown_types_success: (LogicalOperator::XOR, false, false,),
    );

    #[test]
    fn validate_combination_node_not_unknown_type_success() {
        let node_factory = NodeFactoryImpl {};
        let context = create_factory_context();

        let mut left_node = NodeProto::new();
        let left_field_node = left_node.mut_field_leaf_node();
        UNKNOWN_PUBLISHER_NAME.clone_into(&mut left_field_node.publisher_name);
        left_field_node.field_names = vec![BOOL_FIELD_NAME.to_owned()];

        let mut node = NodeProto::new();
        let combination_node = node.mut_combination_node();
        combination_node.left_index = Some(0);
        combination_node.set_logical_operator(LogicalOperator::NOT);

        let all_proto_nodes = &[left_node, node];

        let res = node_factory.validate(all_proto_nodes, &context).unwrap();

        assert_eq!(&res[1], &Some(NodeValueType::Bool));
    }

    // Validates arithmetic operators with different operand availability combinations.
    parameterized_test!(
        #[test]
        validate_combination_node,

        |operator: ArithmeticOperator, are_float_operands, is_left_known, is_right_known, expected_result| {
            let node_factory = NodeFactoryImpl {};
            let context = create_factory_context();
            let all_proto_nodes = create_combination_node(
                OperatorProto::ArithmeticOperator(operator.into()),
                is_left_known,
                if are_float_operands { FLOAT_FIELD_NAME } else { INT_FIELD_NAME },
                is_right_known,
                if are_float_operands { FLOAT_FIELD_NAME } else { INT_FIELD_NAME });

            let res = node_factory.validate(&all_proto_nodes, &context).unwrap();

            assert_eq!(&res[2], &expected_result);
        },

        add_int_left_unknown_type_success: (ArithmeticOperator::ADD, false, false, true, None,),
        add_int_right_unknown_type_success: (ArithmeticOperator::ADD, false, true, false, None,),
        add_int_both_unknown_types_success: (ArithmeticOperator::ADD, false, false, false, None,),
        add_float_left_unknown_type_success: (ArithmeticOperator::ADD, true, false, true, Some(NodeValueType::Float),),
        add_float_right_unknown_type_success: (ArithmeticOperator::ADD, true, true, false, Some(NodeValueType::Float),),
        add_float_both_unknown_types_success: (ArithmeticOperator::ADD, true, false, false, None,),
        subtract_int_left_unknown_type_success: (ArithmeticOperator::SUBTRACT, false, false, true, None,),
        subtract_int_right_unknown_type_success: (ArithmeticOperator::SUBTRACT, false, true, false, None,),
        subtract_int_both_unknown_types_success: (ArithmeticOperator::SUBTRACT, false, false, false, None,),
        subtract_float_left_unknown_type_success: (ArithmeticOperator::SUBTRACT, true, false, true, Some(NodeValueType::Float),),
        subtract_float_right_unknown_type_success: (ArithmeticOperator::SUBTRACT, true, true, false, Some(NodeValueType::Float),),
        subtract_float_both_unknown_types_success: (ArithmeticOperator::SUBTRACT, true, false, false, None,),
        multiply_int_left_unknown_type_success: (ArithmeticOperator::MULTIPLY, false, false, true, None,),
        multiply_int_right_unknown_type_success: (ArithmeticOperator::MULTIPLY, false, true, false, None,),
        multiply_int_both_unknown_types_success: (ArithmeticOperator::MULTIPLY, false, false, false, None,),
        multiply_float_left_unknown_type_success: (ArithmeticOperator::MULTIPLY, true, false, true, Some(NodeValueType::Float),),
        multiply_float_right_unknown_type_success: (ArithmeticOperator::MULTIPLY, true, true, false, Some(NodeValueType::Float),),
        multiply_float_both_unknown_types_success: (ArithmeticOperator::MULTIPLY, true, false, false, None,),
        power_int_left_unknown_type_success: (ArithmeticOperator::POWER, false, false, true, None,),
        power_int_right_unknown_type_success: (ArithmeticOperator::POWER, false, true, false, None,),
        power_int_both_unknown_types_success: (ArithmeticOperator::POWER, false, false, false, None,),
        power_float_left_unknown_type_success: (ArithmeticOperator::POWER, true, false, true, Some(NodeValueType::Float),),
        power_float_right_unknown_type_success: (ArithmeticOperator::POWER, true, true, false, Some(NodeValueType::Float),),
        power_float_both_unknown_types_success: (ArithmeticOperator::POWER, true, false, false, None,),
        divide_left_unknown_type_success: (ArithmeticOperator::DIVIDE, false, false, true, Some(NodeValueType::Float),),
        divide_right_unknown_type_success: (ArithmeticOperator::DIVIDE, false, true, false, Some(NodeValueType::Float),),
        divide_both_unknown_types_success: (ArithmeticOperator::DIVIDE, false, false, false, Some(NodeValueType::Float),),
        modulo_trunc_left_unknown_type_success: (ArithmeticOperator::MODULO_TRUNC, false, false, true, Some(NodeValueType::Int),),
        modulo_trunc_right_unknown_type_success: (ArithmeticOperator::MODULO_TRUNC, false, true, false, Some(NodeValueType::Int),),
        modulo_trunc_both_unknown_types_success: (ArithmeticOperator::MODULO_TRUNC, false, false, false, Some(NodeValueType::Int),),
    );

    #[test]
    fn validate_combination_node_rounding_unknown_type_success() {
        let node_factory = NodeFactoryImpl {};
        let context = create_factory_context();

        let mut left_node = NodeProto::new();
        let left_field_node = left_node.mut_field_leaf_node();
        UNKNOWN_PUBLISHER_NAME.clone_into(&mut left_field_node.publisher_name);
        left_field_node.field_names = vec![INT_FIELD_NAME.to_owned()];

        let mut node = NodeProto::new();
        let combination_node = node.mut_combination_node();
        combination_node.left_index = Some(0);
        combination_node.set_rounding_operator(RoundingOperator::ROUND);

        let all_proto_nodes = &[left_node, node];

        let res = node_factory.validate(all_proto_nodes, &context).unwrap();

        assert_eq!(&res[1], &Some(NodeValueType::Int));
    }

    #[test]
    fn validate_function_node_timestamp_time_source_not_set_fails() {
        let node_factory = NodeFactoryImpl {};
        let context = create_factory_context();

        let mut node = NodeProto::new();
        node.mut_function_leaf_node().mut_get_current_timestamp();

        let result = node_factory.validate(&[node], &context);

        assert!(
            matches!(
                result,
                Err(NodeError {
                    index: 0,
                    err: NodeEvaluationError::FunctionLeafNodeArgumentError {
                        error: FunctionLeafNodeArgumentError::GetCurrentTimestampError(
                            FunctionLeafNodeGetCurrentTimestampArgumentError::TimeSourceNotSet,
                        ),
                    }
                })
            ),
            "Unexpected result: {result:?}"
        );
    }

    #[test]
    fn validate_function_node_timestamp_time_source_not_supported_fails() {
        let node_factory = NodeFactoryImpl {};
        let context = create_factory_context();

        let mut node = NodeProto::new();
        let timestamp_node = node.mut_function_leaf_node().mut_get_current_timestamp();
        timestamp_node.time_source = EnumOrUnknown::from_i32(9999);

        let result = node_factory.validate(&[node], &context);

        assert!(
            matches!(
                result,
                Err(NodeError {
                    index: 0,
                    err: NodeEvaluationError::FunctionLeafNodeArgumentError {
                        error: FunctionLeafNodeArgumentError::GetCurrentTimestampError(
                            FunctionLeafNodeGetCurrentTimestampArgumentError::TimeSourceNotSupported,
                        ),
                    }
                })
            ),
            "Unexpected result: {result:?}"
        );
    }

    #[test]
    fn validate_function_node_timestamp_time_source_realtime_success() {
        let node_factory = NodeFactoryImpl {};
        let context = create_factory_context();

        let mut node = NodeProto::new();
        let timestamp_node = node.mut_function_leaf_node().mut_get_current_timestamp();
        timestamp_node.time_source = TimeSourceProto::REALTIME_CLOCK.into();

        let result = node_factory.validate(&[node], &context);

        assert_eq!(
            result.as_ref().unwrap(),
            &vec![Some(NodeValueType::Int)],
            "Unexpected result: {result:?}"
        );
    }

    #[test]
    fn validate_function_node_timestamp_time_source_monotonic_success() {
        let node_factory = NodeFactoryImpl {};
        let context = create_factory_context();

        let mut node = NodeProto::new();
        let timestamp_node = node.mut_function_leaf_node().mut_get_current_timestamp();
        timestamp_node.time_source = TimeSourceProto::MONOTONIC_TIME_SINCE_BOOT.into();

        let result = node_factory.validate(&[node], &context);

        assert_eq!(
            result.as_ref().unwrap(),
            &vec![Some(NodeValueType::Int)],
            "Unexpected result: {result:?}"
        );
    }

    #[test]
    fn validate_success() {
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
            assert_eq!(actual_type, Some(expected_type));
        }
    }
}
