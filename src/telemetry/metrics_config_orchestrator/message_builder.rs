// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod value_aggregator;
pub(super) mod value_aggregator_factory;

use log::trace;
use parking_lot::Mutex;
use protobuf::reflect::MessageDescriptor;
use sdv_telemetry_expr::{
    dynamic_message::DynamicMessage, node::Node, node_error::NodeEvaluationError,
    node_evaluation_context::NodeEvaluationContext, node_evaluation_state::NodeEvaluationState,
    node_value::NodeValue,
};
use sdv_telemetry_utils::collection_extension::HashMapExtension;
use sdv_telemetry_utils::value_holder::ValueHolder;
use std::collections::HashMap;
use std::sync::Arc;

use crate::metrics_config_orchestrator::node_value_dynamic_message::NodeValueDynamicMessage;

pub(super) use value_aggregator::AggregationError;
use value_aggregator::ValueAggregator;

pub(crate) trait MessageBuilder: Send + Sync {
    /// Builds a protobuf message dynamically.
    fn build(
        self: Arc<Self>,
        context: &Arc<NodeEvaluationContext>,
    ) -> ValueHolder<'static, (Option<Box<dyn DynamicMessage>>, Vec<MessageBuilderError>)>;

    /// Resets internal aggregation state of the builder.
    fn reset(&self);
}

pub(crate) struct MessageBuilderImpl {
    /// Pairs of a field name and the aggregator that is used to populate the new value of the field.
    field_assignments: Mutex<Vec<(String, Box<dyn ValueAggregator>)>>,
    /// Descriptor of the protocol buffer message that this builder is creating.
    message_descriptor: MessageDescriptor,
    /// The name of the owner for tracing purposes.
    owner_name: String,
    /// Pairs of a field name and a node that provides data that is used for field aggregation.
    /// A field may have no matching node if it's not required for aggregation.
    value_providers: Vec<(String, Arc<Node>)>,
}

#[derive(thiserror::Error, Debug)]
#[error("Message build failed for field {field_name}: {error}")]
pub struct MessageBuilderError {
    pub field_name: String,
    pub error: MessageBuilderErrorType,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum MessageBuilderErrorType {
    #[error("Unable to assign value to field: {e}")]
    BadAggregation { e: AggregationError },
    #[error("Evaluation of expression node failed: {e}")]
    ExpressionEvaluationError { e: NodeEvaluationError },
}

impl MessageBuilder for MessageBuilderImpl {
    fn build(
        self: Arc<Self>,
        context: &Arc<NodeEvaluationContext>,
    ) -> ValueHolder<'static, (Option<Box<dyn DynamicMessage>>, Vec<MessageBuilderError>)> {
        let node_evaluation_state = Arc::new(NodeEvaluationState::new(Arc::clone(context)));
        let eval_results = self.value_providers.iter().map(|(field_name, node)| {
            let name_clone = field_name.to_owned();
            node.evaluate_with_state(Arc::clone(&node_evaluation_state))
                .map(move |res| (name_clone, res.map(|(value, _state)| value)))
        });
        let self_clone = Arc::clone(&self);
        ValueHolder::collect_unordered(eval_results).map(move |eval_results| {
            let mut eval_results = eval_results.into_iter().collect::<HashMap<_, _>>();

            let mut message_values = HashMap::new();
            let mut builder_errors = vec![];

            let mut field_assignments = self_clone.field_assignments.lock();
            for (field_name, field_aggregator) in &mut *field_assignments {
                if let Err(err) = self.build_field(
                    field_name,
                    field_aggregator,
                    &mut eval_results,
                    &mut message_values,
                ) {
                    builder_errors.push(err);
                }
            }

            if field_assignments.is_empty() || field_assignments.len() != builder_errors.len() {
                let dynamic_message = NodeValueDynamicMessage::new(
                    self_clone.message_descriptor.clone(),
                    message_values,
                );
                (Some(Box::new(dynamic_message) as Box<dyn DynamicMessage>), builder_errors)
            } else {
                // Don't return the message if all field assignments failed
                (None, builder_errors)
            }
        })
    }

    fn reset(&self) {
        self.field_assignments.lock().iter_mut().for_each(|(_, aggregator)| aggregator.reset());
    }
}

impl MessageBuilderImpl {
    pub(crate) fn new(
        owner_name: String,
        message_descriptor: MessageDescriptor,
        field_assignments: Vec<(String, Box<dyn ValueAggregator>)>,
        value_providers: Vec<(String, Arc<Node>)>,
    ) -> Self {
        Self {
            field_assignments: field_assignments.into(),
            message_descriptor,
            owner_name,
            value_providers,
        }
    }

    fn build_field(
        &self,
        field_name: &str,
        field_aggregator: &mut Box<dyn ValueAggregator>,
        eval_results: &mut HashMap<String, Result<NodeValue, NodeEvaluationError>>,
        message_values: &mut HashMap<String, NodeValue>,
    ) -> Result<(), MessageBuilderError> {
        let input_value =
            eval_results.remove(field_name).transpose().map_err(|e| MessageBuilderError {
                field_name: field_name.to_owned(),
                error: MessageBuilderErrorType::ExpressionEvaluationError { e },
            })?;

        let aggregated_value =
            field_aggregator.compute(input_value).map_err(|e| MessageBuilderError {
                field_name: field_name.to_owned(),
                error: MessageBuilderErrorType::BadAggregation { e },
            })?;

        // if aggregated_value is none, skip populating this field
        if aggregated_value.is_none() {
            // TODO: b/307547570 - properly handle absent values
            return Ok(());
        }

        trace!("Assigning value to field {} of {}", field_name, self.owner_name);

        message_values
            .try_insert_value(field_name.to_owned(), aggregated_value)
            .expect("Field assignments should be unique");

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
mod tests {
    use super::*;

    use futures::{future, FutureExt};
    use googletest::assert_that;
    use lazy_static::lazy_static;
    use protobuf::well_known_types::any::Any;
    use protobuf::{reflect::ReflectEqMode, MessageField, MessageFull};
    use sdv_telemetry_expr::dynamic_message::DynamicMessage;
    use sdv_telemetry_expr::mock::MockMessageProvider;
    use sdv_telemetry_expr::node_error::MessageProviderError;
    use sdv_telemetry_expr::node_value::NodeValue;
    use sdv_telemetry_proto_rust::expression::combination_node::{ArithmeticOperator, Operator};
    use sdv_telemetry_test_proto::test_message::{TestEnum, TestMessage, TestNestedMessage};
    use sdv_telemetry_test_utils::{msg_dyn_eq, TEST_MESSAGE};
    use std::sync::Arc;
    use value_aggregator::mock::MockValueAggregator;

    use crate::metrics_config_orchestrator::protobuf_dynamic_message::ProtobufDynamicMessage;

    lazy_static! {
        static ref REFLECT_EQ_MODE: ReflectEqMode = ReflectEqMode::default();
        static ref TEST_NESTED_MESSAGE: TestNestedMessage = {
            let mut msg = TestNestedMessage::new();
            msg.bool_field = true;
            msg.repeated_i32s = vec![1, 2];
            msg
        };
    }

    // For all available field names, see src/test_utils/test_message.proto
    const ANY_FIELD_NAME: &str = "any_field";
    const INT_FIELD_NAME: &str = "int_field";
    const FLOAT_FIELD_NAME: &str = "float_field";
    const ENUM_FIELD_NAME: &str = "enum_field";
    const MESSAGE_FIELD_NAME: &str = "message_field";
    const REPEATED_BOOL_FIELD_NAME: &str = "repeated_bools";
    const REPEATED_INT_FIELD_NAME: &str = "repeated_i64s";
    const REPEATED_DOUBLE_FIELD_NAME: &str = "repeated_doubles";
    const REPEATED_ENUM_FIELD_NAME: &str = "repeated_enums";
    const REPEATED_MESSAGE_FIELD_NAME: &str = "repeated_messages";
    const OPTIONAL_BOOL_FIELD_NAME: &str = "optional_bool_field";
    const OPTIONAL_INT64_FIELD_NAME: &str = "optional_int64_field";
    const PUBLISHER_NAME: &str = "test_publisher";

    /// Creates a MessageBuilderImpl struct object using the descriptor and
    /// field_name + node pairs as field assignments.
    fn create_message_builder(
        message_descriptor: MessageDescriptor,
        field_name_and_value_pairs: Vec<(&str, Node)>,
    ) -> Arc<MessageBuilderImpl> {
        let (field_assignments, value_providers) = field_name_and_value_pairs
            .into_iter()
            .map(|(field, value)| {
                // the mocked field value aggregator always returns the node value
                // that is passed to it.
                let mock_aggregator: Box<dyn ValueAggregator> =
                    Box::new(MockValueAggregator::new());
                ((field.to_owned(), mock_aggregator), (field.to_owned(), Arc::new(value)))
            })
            .unzip();
        Arc::new(MessageBuilderImpl::new(
            String::default(),
            message_descriptor,
            field_assignments,
            value_providers,
        ))
    }

    /// Creates a MessageBuilderImpl struct object using the descriptor and
    /// field_name + node_value pairs as field assignments.
    fn create_message_builder_with_constant_leaf_nodes(
        message_descriptor: MessageDescriptor,
        field_name_and_value_pairs: Vec<(&str, NodeValue)>,
    ) -> Arc<MessageBuilderImpl> {
        let field_name_and_value_pairs = field_name_and_value_pairs
            .into_iter()
            .map(|(field, value)| (field, Node::ConstantLeaf { value }))
            .collect::<Vec<_>>();
        create_message_builder(message_descriptor, field_name_and_value_pairs)
    }

    #[test]
    fn build_no_fields_ok() {
        let expected_message = TestMessage::new();

        // create MessageBuilderImpl struct object, this is the subject of the test
        let builder = create_message_builder(TestMessage::descriptor(), vec![]);

        // action
        let context = Arc::new(NodeEvaluationContext::new(None));
        let (message, errors) = builder.build(&context).unwrap_sync();

        assert!(errors.is_empty());
        assert_that!(
            message.unwrap().to_protobuf().unwrap(),
            msg_dyn_eq(&expected_message, &REFLECT_EQ_MODE)
        );
    }

    #[test]
    fn build_singular_fields_ok() {
        let mut expected_message = TestMessage::new();
        expected_message.int_field = 5;
        expected_message.float_field = 0.0;
        expected_message.enum_field = TestEnum::SOME.into(); // this enum is equal to int 1

        // create MessageBuilderImpl struct object, this is the subject of the test
        let builder = create_message_builder_with_constant_leaf_nodes(
            TestMessage::descriptor(),
            vec![
                (INT_FIELD_NAME, NodeValue::Int32(5)),
                (FLOAT_FIELD_NAME, NodeValue::Float(0.0)),
                (ENUM_FIELD_NAME, NodeValue::Int32(1)),
            ],
        );

        // action
        let context = Arc::new(NodeEvaluationContext::new(None));
        let (message, errors) = builder.build(&context).unwrap_sync();

        assert!(errors.is_empty());
        assert_that!(
            message.unwrap().to_protobuf().unwrap(),
            msg_dyn_eq(&expected_message, &REFLECT_EQ_MODE)
        );
    }

    #[test]
    fn build_message_field_ok() {
        let mut expected_message = TestMessage::new();
        expected_message.any_field = MessageField::some(Any::pack(&*TEST_NESTED_MESSAGE).unwrap());
        expected_message.message_field = MessageField::some(TEST_NESTED_MESSAGE.clone());

        // create MessageBuilderImpl struct object, this is the subject of the test
        let builder = create_message_builder_with_constant_leaf_nodes(
            TestMessage::descriptor(),
            vec![
                (
                    ANY_FIELD_NAME,
                    NodeValue::Message(Arc::new(ProtobufDynamicMessage::new(Arc::new(
                        TEST_NESTED_MESSAGE.clone(),
                    )))),
                ),
                (
                    MESSAGE_FIELD_NAME,
                    NodeValue::Message(Arc::new(ProtobufDynamicMessage::new(Arc::new(
                        TEST_NESTED_MESSAGE.clone(),
                    )))),
                ),
            ],
        );

        // action
        let context = Arc::new(NodeEvaluationContext::new(None));
        let (message, errors) = builder.build(&context).unwrap_sync();

        assert!(errors.is_empty());
        assert_that!(
            message.unwrap().to_protobuf().unwrap(),
            msg_dyn_eq(&expected_message, &REFLECT_EQ_MODE)
        );
    }

    #[test]
    fn build_repeated_primitive_fields_ok() {
        let booleans = vec![true, false, true, false];
        let integers = vec![42, 100, -1];
        let doubles = vec![0.0, 1.0];
        let enums = vec![1, 1, 0]; // SOME, SOME, NONE
        let mut expected_message = TestMessage::new();
        expected_message.repeated_bools.clone_from(&booleans);
        expected_message.repeated_i64s.clone_from(&integers);
        expected_message.repeated_doubles.clone_from(&doubles);
        expected_message.repeated_enums =
            vec![TestEnum::SOME.into(), TestEnum::SOME.into(), TestEnum::NONE.into()];

        // create MessageBuilderImpl struct object, this is the subject of the test
        let builder = create_message_builder_with_constant_leaf_nodes(
            TestMessage::descriptor(),
            vec![
                (REPEATED_BOOL_FIELD_NAME, NodeValue::BoolList(booleans)),
                (REPEATED_INT_FIELD_NAME, NodeValue::Int64List(integers)),
                (REPEATED_DOUBLE_FIELD_NAME, NodeValue::DoubleList(doubles)),
                (REPEATED_ENUM_FIELD_NAME, NodeValue::Int64List(enums)),
            ],
        );

        // action
        let context = Arc::new(NodeEvaluationContext::new(None));
        let (message, errors) = builder.build(&context).unwrap_sync();

        assert!(errors.is_empty());
        assert_that!(
            message.unwrap().to_protobuf().unwrap(),
            msg_dyn_eq(&expected_message, &REFLECT_EQ_MODE)
        );
    }

    #[test]
    fn build_repeated_message_field_ok() {
        let mut expected_message = TestMessage::new();
        expected_message.int_field = 42;
        expected_message.repeated_messages = vec![TEST_NESTED_MESSAGE.clone(); 2];

        // create MessageBuilderImpl struct object, this is the subject of the test
        let msg = Arc::new(ProtobufDynamicMessage::new(Arc::new(TEST_NESTED_MESSAGE.clone())));
        let builder = create_message_builder_with_constant_leaf_nodes(
            TestMessage::descriptor(),
            vec![
                (INT_FIELD_NAME, NodeValue::Int64(42)),
                (
                    // field name
                    REPEATED_MESSAGE_FIELD_NAME,
                    // value to assign to this field = a vector of 2 messages
                    NodeValue::MessageList(vec![msg; 2]),
                ),
            ],
        );

        // action
        let context = Arc::new(NodeEvaluationContext::new(None));
        let (message, errors) = builder.build(&context).unwrap_sync();

        assert!(errors.is_empty());
        // verify the newly-built message does set the fields to the expected values
        assert_that!(
            message.unwrap().to_protobuf().unwrap(),
            msg_dyn_eq(&expected_message, &REFLECT_EQ_MODE)
        );
    }

    #[test]
    fn build_handles_none_value_ok() {
        let mut expected_message = TestMessage::new();
        expected_message.int_field = 5;

        // create MessageBuilderImpl struct object, this is the subject of the test
        let builder = create_message_builder_with_constant_leaf_nodes(
            TestMessage::descriptor(),
            vec![
                (INT_FIELD_NAME, NodeValue::Int32(5)),
                (FLOAT_FIELD_NAME, NodeValue::None),
                (OPTIONAL_BOOL_FIELD_NAME, NodeValue::None),
            ],
        );

        // action
        let context = Arc::new(NodeEvaluationContext::new(None));
        let (message, errors) = builder.build(&context).unwrap_sync();

        assert!(errors.is_empty());
        assert_that!(
            message.unwrap().to_protobuf().unwrap(),
            msg_dyn_eq(&expected_message, &REFLECT_EQ_MODE)
        );
    }

    #[test]
    fn build_zeros_are_not_encoded_in_implicitly_optional_fields() {
        // expect empty message because zeros are not encoded in implicitly optional fields
        // that means 0 is indistinguishable from field-not-set
        let expected_message = TestMessage::new();

        // create MessageBuilderImpl struct object, this is the subject of the test
        let builder = create_message_builder_with_constant_leaf_nodes(
            TestMessage::descriptor(),
            vec![(INT_FIELD_NAME, NodeValue::Int32(0))],
        );

        // action
        let context = Arc::new(NodeEvaluationContext::new(None));
        let (message, errors) = builder.build(&context).unwrap_sync();

        assert!(errors.is_empty());
        assert_that!(
            message.unwrap().to_protobuf().unwrap(),
            msg_dyn_eq(&expected_message, &REFLECT_EQ_MODE)
        );
    }

    #[test]
    fn build_zeros_are_encoded_in_explicitly_optional_fields() {
        let mut expected_message = TestMessage::new();
        expected_message.optional_bool_field = Some(false);
        expected_message.optional_int64_field = Some(0);

        // create MessageBuilderImpl struct object, this is the subject of the test
        let builder = create_message_builder_with_constant_leaf_nodes(
            TestMessage::descriptor(),
            vec![
                (OPTIONAL_BOOL_FIELD_NAME, NodeValue::Bool(false)),
                (OPTIONAL_INT64_FIELD_NAME, NodeValue::Int64(0)),
            ],
        );

        // action
        let context = Arc::new(NodeEvaluationContext::new(None));
        let (message, errors) = builder.build(&context).unwrap_sync();

        assert!(errors.is_empty());
        assert_that!(
            message.unwrap().to_protobuf().unwrap(),
            msg_dyn_eq(&expected_message, &REFLECT_EQ_MODE)
        );
    }

    #[test]
    fn build_fields_partially_ok() {
        let mut expected_message = TestMessage::new();
        expected_message.int_field = 5;

        // create MessageBuilderImpl struct object, this is the subject of the test
        let builder = create_message_builder(
            TestMessage::descriptor(),
            vec![
                (INT_FIELD_NAME, Node::ConstantLeaf { value: NodeValue::Int32(5) }),
                // Fails to evaluate
                (
                    FLOAT_FIELD_NAME,
                    Node::Combination {
                        operator: Operator::ArithmeticOperator(ArithmeticOperator::DIVIDE.into()),
                        left: Some(Arc::new(Node::ConstantLeaf { value: NodeValue::Float(5.0) })),
                        right: Some(Arc::new(Node::ConstantLeaf { value: NodeValue::Float(0.0) })),
                    },
                ),
            ],
        );

        // action
        let context = Arc::new(NodeEvaluationContext::new(None));
        let (message, errors) = builder.build(&context).unwrap_sync();

        assert_that!(
            message.unwrap().to_protobuf().unwrap(),
            msg_dyn_eq(&expected_message, &REFLECT_EQ_MODE)
        );

        assert_eq!(errors.len(), 1);
        let err = &errors[0].error;
        assert!(
            matches!(
                err,
                MessageBuilderErrorType::ExpressionEvaluationError {
                    e: NodeEvaluationError::DivideByZero,
                }
            ),
            "Unexpected error: {err:?}"
        );
    }

    #[test]
    fn build_aggregated_field_value_compute_error_fails() {
        let field_assignments = vec![(
            INT_FIELD_NAME.to_owned(),
            Box::new(MockValueAggregator::new().fail_compute()) as Box<dyn ValueAggregator>,
        )];
        let builder = Arc::new(MessageBuilderImpl::new(
            String::default(),
            TestMessage::descriptor(),
            field_assignments,
            vec![],
        ));

        // action
        let context = Arc::new(NodeEvaluationContext::new(None));
        let (message, errors) = builder.build(&context).unwrap_sync();

        assert!(message.is_none());
        assert_eq!(errors.len(), 1);

        let err = &errors[0].error;
        assert!(
            matches!(err, MessageBuilderErrorType::BadAggregation { .. }),
            "Unexpected error: {err:?}"
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn build_from_asynchronous_result_propagates_error() {
        // Setup a field node that always returns an error.
        let message_provider = Arc::new(MockMessageProvider::new());
        message_provider.set_next_async_result(
            future::ready(Err(MessageProviderError::InvalidPublisherName)).boxed(),
        );
        let field_node = Arc::new(Node::FieldLeaf {
            publisher_name: String::default(),
            fields: vec![],
            message_provider,
        });

        // Setup builder to retrieve value from the field node and pass it through aggregator.
        let builder = Arc::new(MessageBuilderImpl::new(
            String::default(),
            TestMessage::descriptor(),
            vec![(
                INT_FIELD_NAME.to_owned(),
                Box::new(MockValueAggregator::new()) as Box<dyn ValueAggregator>,
            )],
            vec![(INT_FIELD_NAME.to_owned(), field_node)],
        ));

        let (message, errors) =
            builder.build(&Arc::new(NodeEvaluationContext::new(None))).unwrap_async().await;

        assert!(message.is_none());
        assert_eq!(errors.len(), 1);

        let err = &errors[0].error;
        assert!(
            matches!(err, MessageBuilderErrorType::ExpressionEvaluationError { .. }),
            "Unexpected error: {err:?}"
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn build_from_asynchronous_result_success() {
        let expected_message = TestMessage { int_field: 80, ..Default::default() };

        // Setup a field node that returns the value of the test message int field.
        let message_provider = Arc::new(MockMessageProvider::new());
        message_provider.set_next_async_result(
            future::ready(Ok(Some(Arc::new(ProtobufDynamicMessage::new(Arc::new(
                TEST_MESSAGE.clone(),
            ))) as Arc<dyn DynamicMessage>)))
            .boxed(),
        );
        let field_node = Arc::new(Node::FieldLeaf {
            publisher_name: String::default(),
            fields: vec![INT_FIELD_NAME.to_owned()],
            message_provider,
        });

        // Setup builder to retrieve value from the field node and pass it through aggregator.
        let builder = Arc::new(MessageBuilderImpl::new(
            String::default(),
            TestMessage::descriptor(),
            vec![(
                INT_FIELD_NAME.to_owned(),
                Box::new(MockValueAggregator::new()) as Box<dyn ValueAggregator>,
            )],
            vec![(INT_FIELD_NAME.to_owned(), field_node)],
        ));

        let (message, errors) =
            builder.build(&Arc::new(NodeEvaluationContext::new(None))).unwrap_async().await;

        assert!(errors.is_empty());
        assert_that!(
            message.unwrap().to_protobuf().unwrap(),
            msg_dyn_eq(&expected_message, &REFLECT_EQ_MODE)
        );
    }

    #[test]
    fn build_shares_node_evaluation_context() {
        let expected_message =
            TestMessage { int_field: 80, float_field: 100.0, ..Default::default() };

        let message_provider = Arc::new(MockMessageProvider::new());
        message_provider.set_next_sync_result(Ok(Some(Arc::new(ProtobufDynamicMessage::new(
            Arc::new(TEST_MESSAGE.clone()),
        )) as Arc<dyn DynamicMessage>)));
        let field_node1 = Arc::new(Node::FieldLeaf {
            publisher_name: PUBLISHER_NAME.to_owned(),
            fields: vec![INT_FIELD_NAME.to_owned()],
            message_provider,
        });
        let field_node2 = Arc::new(Node::FieldLeaf {
            publisher_name: PUBLISHER_NAME.to_owned(),
            fields: vec![FLOAT_FIELD_NAME.to_owned()],
            message_provider: Arc::new(MockMessageProvider::new()),
        });

        let builder = Arc::new(MessageBuilderImpl::new(
            String::default(),
            TestMessage::descriptor(),
            vec![
                (
                    INT_FIELD_NAME.to_owned(),
                    Box::new(MockValueAggregator::new()) as Box<dyn ValueAggregator>,
                ),
                (
                    FLOAT_FIELD_NAME.to_owned(),
                    Box::new(MockValueAggregator::new()) as Box<dyn ValueAggregator>,
                ),
            ],
            vec![
                (INT_FIELD_NAME.to_owned(), field_node1),
                (FLOAT_FIELD_NAME.to_owned(), field_node2),
            ],
        ));

        let (message, errors) =
            builder.build(&Arc::new(NodeEvaluationContext::new(None))).unwrap_sync();

        assert!(errors.is_empty());
        assert_that!(
            message.unwrap().to_protobuf().unwrap(),
            msg_dyn_eq(&expected_message, &REFLECT_EQ_MODE)
        );
    }

    #[test]
    fn reset_ok() {
        let aggregator = MockValueAggregator::new();
        let builder = Arc::new(MessageBuilderImpl::new(
            String::default(),
            TestMessage::descriptor(),
            vec![(
                INT_FIELD_NAME.to_owned(),
                Box::new(aggregator.clone()) as Box<dyn ValueAggregator>,
            )],
            vec![],
        ));

        builder.reset();

        assert_eq!(aggregator.get_num_reset_calls(), 1);
    }
}
