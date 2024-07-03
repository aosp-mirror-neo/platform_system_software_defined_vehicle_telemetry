// Copyright 2024 Google LLC

use sdv_telemetry_proto_rust::expression::combination_node::Operator as OperatorProto;
use sdv_telemetry_time_wrapper::Microseconds;
use sdv_telemetry_utils::operation_result::OperationResult;
use std::fmt;
use std::sync::Arc;

#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};

use crate::expr::node_error::NodeError;
use crate::expr::node_factory::MessageProvider;
use crate::expr::node_value::NodeValue;
use crate::expr::node_value_type::OperatorProtoExtension;

const EMPTY: &str = "Empty";

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

    FunctionLeaf {
        function: FunctionType,
    },
}

pub enum FunctionType {
    GetCurrentTimestamp { time_source: TimeSource },
}

#[derive(Debug)]
pub enum TimeSource {
    RealtimeClock,
    MonotonicTimeSinceBoot,
}

#[cfg(test)]
static NEXT_NODE_EVALUATION_CONTEXT_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
#[cfg_attr(test, derive(Debug))]
pub struct NodeEvaluationContext {
    #[cfg(test)]
    id: u64,

    // By only querying the time once and putting it into the context, we ensure that it remains
    // constant while nodes are being evaluated, even if multiple nodes as part of the same
    // evaluation query the time.
    unix_timestamp: Microseconds,
    monotonic_time_since_boot: Microseconds,
}

impl NodeEvaluationContext {
    pub fn new() -> Self {
        Self {
            #[cfg(test)]
            id: NEXT_NODE_EVALUATION_CONTEXT_ID.fetch_add(1, Ordering::SeqCst),

            unix_timestamp: sdv_telemetry_time_wrapper::get_unix_timestamp(),
            monotonic_time_since_boot: sdv_telemetry_time_wrapper::get_monotonic_time_since_boot(),
        }
    }

    #[cfg(test)]
    pub fn id(&self) -> u64 {
        self.id
    }
}

/// Equality checks of the context really only make sense in tests.
#[cfg(test)]
impl PartialEq for NodeEvaluationContext {
    fn eq(&self, other: &NodeEvaluationContext) -> bool {
        let result = self.id.eq(&other.id);
        if result {
            // These asserts should never fail, because each `NodeEvaluationContext::id` is only
            // used once.
            debug_assert_eq!(self.unix_timestamp, other.unix_timestamp);
            debug_assert_eq!(self.monotonic_time_since_boot, other.monotonic_time_since_boot);
        }
        result
    }
}

#[cfg(test)]
impl Eq for NodeEvaluationContext {}

impl Node {
    /// Evaluate AST recursively with messages from the "message_provider".
    pub fn evaluate(
        self: &Arc<Self>,
        context: &Arc<NodeEvaluationContext>,
    ) -> OperationResult<'static, NodeValue, NodeError> {
        match self.as_ref() {
            Node::ConstantLeaf { value } => {
                // Base case of recursion: node is a leaf node, return value
                Ok(value.clone()).into()
            }
            Node::FieldLeaf { message_provider, publisher_name, .. } => {
                // Base case of recursion: node is a leaf node, return value
                let self_clone = Arc::clone(self);
                message_provider.get_message(publisher_name).err_into().and_then_sync(
                    move |maybe_msg| {
                        let Node::FieldLeaf { fields, .. } = self_clone.as_ref() else {
                            panic!("Captured node is not a field leaf node: {:?}", self_clone);
                        };

                        maybe_msg.map_or_else(
                            || Ok(NodeValue::None),
                            |msg| NodeValue::extract_value(msg, fields),
                        )
                    },
                )
            }
            Node::Combination { left, .. } => {
                // evaluate left branch recursively
                let context_clone = Arc::clone(context);
                let self_clone = Arc::clone(self);
                left
                    .as_ref()
                    .expect(
                        "Incorrect Node! Node construction should have verified that \
                        every non-leaf Node has a left child, but evaluate() encountered \
                        a node without left child",
                    )
                    .evaluate(context)
                    .and_then(move |left_value| {
                        let Node::Combination { operator, right, .. } = self_clone.as_ref() else {
                            panic!("Captured node is not a combination node: {:?}", self_clone);
                        };

                        // If unary operator, then operate on self.
                        // If binary operator, then [left] <operator> [right]
                        // The function short-circuits if possible. The closure provides right operand.
                        let right_clone = right.clone();
                        left_value.operate(operator, move || {
                            right_clone.as_ref()
                                .unwrap_or_else(|| {
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
                                })
                                .evaluate(&context_clone)
                        })
                    })
            }
            Node::FunctionLeaf { function } => match function {
                FunctionType::GetCurrentTimestamp { time_source } => {
                    let timestamp = match time_source {
                        TimeSource::RealtimeClock => context.unix_timestamp,
                        TimeSource::MonotonicTimeSinceBoot => context.monotonic_time_since_boot,
                    };
                    Ok(NodeValue::Int64(timestamp.0)).into()
                }
            },
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
                let format_node = |node: &Option<Arc<Node>>| {
                    node.as_ref().map_or_else(|| EMPTY.to_owned(), |node| format!("{:#?}", node))
                };
                write!(
                    f,
                    "Combination Node {{ operator: {:?},  left: {},  right: {} }}",
                    operator,
                    format_node(left),
                    format_node(right)
                )
            }
            Node::FunctionLeaf { function } => write!(
                f,
                "Function Leaf Node {{ function: {} }}",
                match function {
                    FunctionType::GetCurrentTimestamp { time_source } =>
                        format!("GetCurrentTimestamp {{ time_source: {:?} }}", time_source),
                }
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use protobuf::MessageDyn;
    use sdv_telemetry_proto_rust::expression::combination_node::{
        Operator as OperatorProto, RelationalOperator,
    };
    use std::time::SystemTime;

    use super::*;
    use crate::expr::mock::*;
    use crate::expr::node_error::MessageProviderError;

    const FLOAT_FIELD_NAME: &str = "float_field";
    const INT_FIELD_NAME: &str = "int_field";
    const KNOWN_PUBLISHER_NAME: &str = "publisher_foo";

    #[test]
    fn node_evaluation_context_ids_are_ascending() {
        let context1 = NodeEvaluationContext::new();
        let context2 = NodeEvaluationContext::new();
        assert!(context1.id < context2.id);
        assert!(context1.monotonic_time_since_boot <= context2.monotonic_time_since_boot);
    }

    #[test]
    fn evaluate_logical_operator_success() -> Result<(), NodeError> {
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
            left: Some(Arc::clone(&true_node)),
            right: Some(Arc::clone(&false_node)),
        });
        let xor_node = Node::Combination {
            operator: OP_XOR.clone(),
            left: Some(Arc::clone(&true_node)),
            right: Some(Arc::clone(&or_node)),
        };
        let nodes = [true_node, false_node, or_node, xor_node.into()];

        // we will evaluate every node in `nodes`, and we expect these results:
        let expected_results = [
            NodeValue::Bool(true),  // true_node
            NodeValue::Bool(false), // false_node
            NodeValue::Bool(true),  // true || false = true
            NodeValue::Bool(false), // true ^ (true || false) = false
        ];

        let context = Arc::new(NodeEvaluationContext::new());
        // assert that evaluated_result[i] == expected_result[i] for all index i
        for (rust_node, expected_val_ref) in nodes.iter().zip(expected_results.iter()) {
            // action + assertion
            assert_eq!(rust_node.evaluate(&context).unwrap_sync()?, *expected_val_ref);
        }
        Ok(())
    }

    #[test]
    fn evaluate_arithmetic_operator_success() -> Result<(), NodeError> {
        // setup this tree: |-( 5 / 2 * 5^2 )|
        //             ABSOLUTE
        //                 |
        //            UNARY_MINUS
        //                 |
        //              MULTIPLY
        //          /             \
        //       DIVIDE            POWER
        //    /         \        /        \
        //   5          2       5           2
        let five_node = Arc::new(Node::ConstantLeaf { value: NodeValue::Int32(5) });
        let two_node = Arc::new(Node::ConstantLeaf { value: NodeValue::Int32(2) });
        let divide_node = Arc::new(Node::Combination {
            operator: OP_DIVIDE.clone(),
            left: Some(Arc::clone(&five_node)),
            right: Some(Arc::clone(&two_node)),
        });
        let power_node = Arc::new(Node::Combination {
            operator: OP_POWER.clone(),
            left: Some(Arc::clone(&five_node)),
            right: Some(Arc::clone(&two_node)),
        });
        let multiply_node = Arc::new(Node::Combination {
            operator: OP_MULTIPLY.clone(),
            left: Some(Arc::clone(&divide_node)),
            right: Some(Arc::clone(&power_node)),
        });
        let unary_minus_node = Arc::new(Node::Combination {
            operator: OP_UNARY_MINUS.clone(),
            left: Some(Arc::clone(&multiply_node)),
            right: None,
        });
        let absolute_node = Arc::new(Node::Combination {
            operator: OP_ABSOLUTE.clone(),
            left: Some(Arc::clone(&unary_minus_node)),
            right: None,
        });
        let nodes = [
            five_node,
            two_node,
            divide_node,
            power_node,
            multiply_node,
            unary_minus_node,
            absolute_node,
        ];

        // we will evaluate every node in `nodes`, and we expect these results:
        let expected_results = [
            NodeValue::Int32(5),      // index 0 = constant value of 5
            NodeValue::Int32(2),      // index 1 = constant value of 2
            NodeValue::Double(2.5),   // index 2 = 5 / 2 = 2.5
            NodeValue::Int64(25),     // index 3 = 5^2 = 25
            NodeValue::Double(62.5),  // index 4 = 2.5 * 25 = 62.5
            NodeValue::Double(-62.5), // index 5 = -(62.5) = -62.5
            NodeValue::Double(62.5),  // index 6 = |-62.5|= 62.5
        ];

        let context = Arc::new(NodeEvaluationContext::new());
        // assert that evaluated_result[i] == expected_result[i]  for all index i
        for (rust_node, expected_val_ref) in nodes.iter().zip(expected_results.iter()) {
            assert_eq!(rust_node.evaluate(&context).unwrap_sync()?, *expected_val_ref);
        }
        Ok(())
    }

    #[test]
    fn evaluate_relational_operator_success() -> Result<(), NodeError> {
        // setup this tree which uses the protobuf message in the expr/mock.rs
        //                                 EQ
        //                           /              \
        //                      CONTAINS                  true
        //               /                  \
        // TestMessage.repeated_doubles       Pi
        let repeated_doubles_node = Arc::new(Node::FieldLeaf {
            publisher_name: KNOWN_PUBLISHER_NAME.to_string(),
            fields: vec!["repeated_doubles".to_string()],
            message_provider: MOCK_MESSAGE_PROVIDER.clone(),
        });
        let pi_node =
            Arc::new(Node::ConstantLeaf { value: NodeValue::Float(std::f32::consts::PI) });
        let contains_node = Arc::new(Node::Combination {
            operator: OP_CONTAINS.clone(),
            left: Some(Arc::clone(&repeated_doubles_node)),
            right: Some(Arc::clone(&pi_node)),
        });
        let eq_node = Arc::new(Node::Combination {
            operator: OP_EQ.clone(),
            left: Some(Arc::clone(&contains_node)),
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

        let context = Arc::new(NodeEvaluationContext::new());
        // assert that evaluated_result[i] == expected_result[i]  for all index i
        for (rust_node, expected_val_ref) in nodes.iter().zip(expected_results.iter()) {
            assert_eq!(rust_node.evaluate(&context).unwrap_sync()?, *expected_val_ref);
        }
        Ok(())
    }

    #[test]
    fn evaluate_rounding_operator_success() -> Result<(), NodeError> {
        let pi_node =
            Arc::new(Node::ConstantLeaf { value: NodeValue::Float(std::f32::consts::PI) });
        let round_node = Arc::new(Node::Combination {
            operator: OP_ROUND.clone(),
            left: Some(Arc::clone(&pi_node)),
            right: None,
        });
        let nodes = [pi_node, round_node];

        // we will evaluate every node in `nodes`, and we expect these results:
        let expected_results = [
            NodeValue::Float(std::f32::consts::PI), // constant value of Pi
            NodeValue::Int64(3),                    // Pi rounded to the nearest int
        ];

        let context = Arc::new(NodeEvaluationContext::new());
        // assert that evaluated_result[i] == expected_result[i]  for all index i
        for (rust_node, expected_val_ref) in nodes.iter().zip(expected_results.iter()) {
            assert_eq!(rust_node.evaluate(&context).unwrap_sync()?, *expected_val_ref);
        }
        Ok(())
    }

    #[test]
    fn evaluate_mixed_operators_success() -> Result<(), NodeError> {
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
            left: Some(Arc::clone(&point_three_node)),
            right: None,
        });
        let five_node = Arc::new(Node::ConstantLeaf { value: NodeValue::Int32(5) });
        let add_node = Arc::new(Node::Combination {
            operator: OP_ADD.clone(),
            left: Some(Arc::clone(&five_node)),
            right: Some(Arc::clone(&five_node)),
        });
        let lt_or_eq_node = Arc::new(Node::Combination {
            operator: OP_LT_OR_EQ.clone(),
            left: Some(Arc::clone(&five_node)),
            right: Some(Arc::clone(&add_node)),
        });
        let and_node = Arc::new(Node::Combination {
            operator: OP_AND.clone(),
            left: Some(Arc::clone(&lt_or_eq_node)),
            right: Some(Arc::new(Node::ConstantLeaf { value: NodeValue::Bool(true) })),
        });
        let nodes = [point_three_node, ceil_node, five_node, add_node, lt_or_eq_node, and_node];

        // we will evaluate every node in `nodes`, and we expect these results:
        let expected_results = [
            NodeValue::Float(0.3), // constant of 0.3
            NodeValue::Int64(1),   // 0.3 rounded up
            NodeValue::Int32(5),   // constant of 5
            NodeValue::Int64(10),  // 5+5 = 10
            NodeValue::Bool(true), // True because 5 <= 10
            NodeValue::Bool(true), // True && True = True
        ];

        let context = Arc::new(NodeEvaluationContext::new());
        // assert that evaluated_result[i] == expected_result[i] for all index i
        for (rust_node, expected_val_ref) in nodes.iter().zip(expected_results.iter()) {
            assert_eq!(rust_node.evaluate(&context).unwrap_sync()?, *expected_val_ref);
        }
        Ok(())
    }

    #[test]
    fn evaluate_mixed_leaf_node_types_success() -> Result<(), NodeError> {
        // setup this tree which uses the message initialized in expr/mock.rs
        //                           index 4 (<)
        //                       /               \
        //                index 2                  index 3
        //                 DIVIDE                TestMessage.int_field
        //           /                \
        //     index 0               index 1
        // TestMessage.float_field       2
        let float_field_node = Arc::new(Node::FieldLeaf {
            publisher_name: KNOWN_PUBLISHER_NAME.to_string(),
            fields: vec![FLOAT_FIELD_NAME.to_owned()],
            message_provider: MOCK_MESSAGE_PROVIDER.clone(),
        });
        let divide_node = Arc::new(Node::Combination {
            operator: OP_DIVIDE.clone(),
            left: Some(Arc::clone(&float_field_node)),
            right: Some(Arc::new(Node::ConstantLeaf { value: NodeValue::Int32(2) })),
        });
        let int_field_node = Arc::new(Node::FieldLeaf {
            publisher_name: KNOWN_PUBLISHER_NAME.to_string(),
            fields: vec!["int_field".to_string()],
            message_provider: MOCK_MESSAGE_PROVIDER.clone(),
        });
        let lt_node = Arc::new(Node::Combination {
            operator: OP_LT.clone(),
            left: Some(Arc::clone(&divide_node)),
            right: Some(Arc::clone(&int_field_node)),
        });

        let nodes = [float_field_node, divide_node, int_field_node, lt_node];

        // we will evaluate every node in `nodes`, and we expect these results:
        let expected_results = [
            NodeValue::Float(100.0), // field_field has value of 100
            NodeValue::Double(50.0), // 100 / 2 = 50
            NodeValue::Int32(80),    // int_field has value 80
            NodeValue::Bool(true),   // 50 < 80 = true
        ];

        let context = Arc::new(NodeEvaluationContext::new());
        // assert that evaluated_result[i] == expected_result[i]  for all index i
        for (rust_node, expected_val_ref) in nodes.iter().zip(expected_results.iter()) {
            assert_eq!(rust_node.evaluate(&context).unwrap_sync()?, *expected_val_ref);
        }
        Ok(())
    }

    #[test]
    fn evaluate_incompatible_types_fails() {
        let zero_node = Arc::new(Node::ConstantLeaf { value: NodeValue::Int32(0) });
        let true_node = Arc::new(Node::ConstantLeaf { value: NodeValue::Bool(true) });
        let lt_node = Arc::new(Node::Combination {
            operator: OperatorProto::RelationalOperator(RelationalOperator::LT.into()),
            left: Some(zero_node),
            right: Some(true_node),
        });

        let context = Arc::new(NodeEvaluationContext::new());
        // expr is "0 < True"
        let err = lt_node.evaluate(&context).unwrap_sync().unwrap_err();

        // 0 cannot be less than True
        assert!(
            matches!(&err, NodeError::ComparisonOperatorIncomptaibleOperands),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn evaluate_message_provider_invalid_publisher_name_fails() {
        let message_provider = Arc::new(MockMessageProvider::new());
        message_provider.set_next_sync_result(Err(MessageProviderError::InvalidPublisherName));
        let node = Arc::new(Node::FieldLeaf {
            publisher_name: String::default(),
            fields: vec![],
            message_provider,
        });

        let context = Arc::new(NodeEvaluationContext::new());
        let err = node.evaluate(&context).unwrap_sync().unwrap_err();

        assert!(
            matches!(&err, NodeError::MessageProviderError { .. }),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn evaluate_no_provided_message_returns_none() {
        let message_provider = Arc::new(MockMessageProvider::new());
        message_provider.set_next_sync_result(Ok(None));
        let node = Arc::new(Node::FieldLeaf {
            publisher_name: String::default(),
            fields: vec![],
            message_provider,
        });

        let context = Arc::new(NodeEvaluationContext::new());
        let res = node.evaluate(&context).unwrap_sync().unwrap();

        assert_eq!(res, NodeValue::None);
    }

    #[test]
    fn evaluate_function_timestamp_time_source_realtime_success() {
        let timestamp_node = Arc::new(Node::FunctionLeaf {
            function: FunctionType::GetCurrentTimestamp { time_source: TimeSource::RealtimeClock },
        });

        let context = Arc::new(NodeEvaluationContext::new());
        let res = timestamp_node.evaluate(&context).unwrap_sync().unwrap();

        assert_eq!(res, NodeValue::Int64(context.unix_timestamp.0));
    }

    #[test]
    fn evaluate_function_timestamp_time_source_realtime_without_mock_success() {
        let timestamp_node = Arc::new(Node::FunctionLeaf {
            function: FunctionType::GetCurrentTimestamp { time_source: TimeSource::RealtimeClock },
        });

        let context = Arc::new(NodeEvaluationContext::new());
        let expected_timestamp = i64::try_from(
            SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros(),
        )
        .unwrap();
        let result = timestamp_node.evaluate(&context).unwrap_sync();

        let Ok(NodeValue::Int64(actual_timestamp)) = result else {
            panic!("Unexpected result: {:?}", result);
        };
        assert!((expected_timestamp - actual_timestamp).abs() < 100_000);
    }

    #[test]
    fn evaluate_function_timestamp_time_source_monotonic_success() {
        let timestamp_node = Arc::new(Node::FunctionLeaf {
            function: FunctionType::GetCurrentTimestamp {
                time_source: TimeSource::MonotonicTimeSinceBoot,
            },
        });

        let context = Arc::new(NodeEvaluationContext::new());
        let res = timestamp_node.evaluate(&context).unwrap_sync().unwrap();

        assert_eq!(res, NodeValue::Int64(context.monotonic_time_since_boot.0));
    }

    #[tokio::test]
    async fn evaluate_asynchronous_provider_left_node_propagates_error() {
        let message_provider = Arc::new(MockMessageProvider::new());
        message_provider.set_next_async_result(
            async { Err(MessageProviderError::InvalidPublisherName) }.boxed(),
        );
        let field_node = Arc::new(Node::FieldLeaf {
            publisher_name: String::default(),
            fields: vec![],
            message_provider,
        });
        let constant_node = Arc::new(Node::ConstantLeaf { value: NodeValue::None });
        let combination_node = Arc::new(Node::Combination {
            operator: OperatorProto::RelationalOperator(RelationalOperator::LT.into()),
            left: Some(field_node),
            right: Some(constant_node),
        });

        let context = Arc::new(NodeEvaluationContext::new());
        let err = combination_node.evaluate(&context).unwrap_async().await.unwrap_err();

        assert!(
            matches!(&err, NodeError::MessageProviderError { e } if matches!(e, MessageProviderError::InvalidPublisherName)),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn evaluate_asynchronous_provider_right_node_propagates_error() {
        let message_provider = Arc::new(MockMessageProvider::new());
        message_provider.set_next_async_result(
            async { Err(MessageProviderError::InvalidPublisherName) }.boxed(),
        );
        let field_node = Arc::new(Node::FieldLeaf {
            publisher_name: String::default(),
            fields: vec![],
            message_provider,
        });
        let constant_node = Arc::new(Node::ConstantLeaf { value: NodeValue::Int32(0) });
        let combination_node = Arc::new(Node::Combination {
            operator: OperatorProto::RelationalOperator(RelationalOperator::LT.into()),
            left: Some(constant_node),
            right: Some(field_node),
        });

        let context = Arc::new(NodeEvaluationContext::new());
        let err = combination_node.evaluate(&context).unwrap_async().await.unwrap_err();

        assert!(
            matches!(&err, NodeError::MessageProviderError { e } if matches!(e, MessageProviderError::InvalidPublisherName)),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn evaluate_asynchronous_provider_result_success() {
        let left_message_provider = Arc::new(MockMessageProvider::new());
        left_message_provider.set_next_async_result(
            async { Ok(Some(Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>)) }.boxed(),
        );
        let left_field_node = Arc::new(Node::FieldLeaf {
            publisher_name: KNOWN_PUBLISHER_NAME.to_owned(),
            fields: vec![FLOAT_FIELD_NAME.to_owned()],
            message_provider: left_message_provider,
        });
        let right_message_provider = Arc::new(MockMessageProvider::new());
        right_message_provider.set_next_async_result(
            async { Ok(Some(Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>)) }.boxed(),
        );
        let right_field_node = Arc::new(Node::FieldLeaf {
            publisher_name: KNOWN_PUBLISHER_NAME.to_owned(),
            fields: vec![INT_FIELD_NAME.to_owned()],
            message_provider: right_message_provider,
        });
        let combination_node = Arc::new(Node::Combination {
            operator: OperatorProto::RelationalOperator(RelationalOperator::GT.into()),
            left: Some(left_field_node),
            right: Some(right_field_node),
        });

        let context = Arc::new(NodeEvaluationContext::new());
        let res = combination_node.evaluate(&context).unwrap_async().await.unwrap();

        assert_eq!(&res, &NodeValue::Bool(true));
    }
}
