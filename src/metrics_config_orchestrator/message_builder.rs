// Copyright 2023 Google LLC

mod value_aggregator;
pub(super) mod value_aggregator_factory;

pub(super) use value_aggregator::AggregationError;

use log::trace;
use parking_lot::Mutex;
use protobuf::{reflect::MessageDescriptor, MessageDyn};
use sdv_telemetry_utils::{operation_result::OperationResult, value_holder::ValueHolder};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

use crate::expr::node::Node;
use crate::expr::node_error::NodeError;
use crate::expr::node_evaluation_context::NodeEvaluationContext;
use crate::expr::proto_node_value_converter::node_value_to_protobuf_reflection::{
    ReflectRepeatedMutExt, ToReflectValueBox,
};

use value_aggregator::ValueAggregator;

pub(crate) trait MessageBuilder: Send + Sync {
    /// Builds a protobuf message dynamically.
    fn build(
        self: Arc<Self>,
        context: &Arc<NodeEvaluationContext>,
    ) -> OperationResult<'static, Box<dyn MessageDyn>, MessageBuilderError>;

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

#[derive(Error, Debug)]
#[non_exhaustive]
pub(crate) enum MessageBuilderError {
    #[error("Unable to assign value to field {field_name} because of: {e}")]
    BadAggregation { field_name: String, e: AggregationError },
    #[error("Evaluation of expression node for field {field_name} failed: {e}")]
    ExpressionEvaluationError { field_name: String, e: NodeError },
    #[error("Failed to assign node value to field {field_name} because of: {e}")]
    FailedToAssignValueToField { field_name: String, e: NodeError },
}

impl MessageBuilder for MessageBuilderImpl {
    fn build(
        self: Arc<Self>,
        context: &Arc<NodeEvaluationContext>,
    ) -> OperationResult<'static, Box<dyn MessageDyn>, MessageBuilderError> {
        let operation_results = self.value_providers.iter().map(|(field_name, node)| {
            let name_clone = field_name.to_owned();
            node.evaluate(Arc::clone(context)).map(move |res| (name_clone, res))
        });
        let self_clone = Arc::clone(&self);
        ValueHolder::collect_unordered(operation_results).map(move |results| {
            let mut results_map = results.into_iter().collect::<HashMap<_, _>>();
            let mut msg = self_clone.message_descriptor.new_instance();
            for (field_name, field_aggregator) in &mut *self_clone.field_assignments.lock() {
                let input_value = results_map.remove(field_name).transpose().map_err(|e| {
                    MessageBuilderError::ExpressionEvaluationError {
                        field_name: field_name.to_owned(),
                        e,
                    }
                })?;
                let aggregated_value = field_aggregator.compute(input_value).map_err(|e| {
                    MessageBuilderError::BadAggregation { field_name: field_name.to_owned(), e }
                })?;
                // if aggregated_value is none, skip populating this field
                if aggregated_value.is_none() {
                    // TODO: b/307547570 - properly handle absent values
                    continue;
                }

                let field_descriptor = self
                    .message_descriptor
                    .field_by_name(field_name)
                    .expect("All fields should have been validated to exist");
                trace!("Assigning value to field {} of {}", field_name, self.owner_name);
                if field_descriptor.is_singular() {
                    field_descriptor.set_singular_field(
                        msg.as_mut(),
                        aggregated_value
                            .to_reflect_value_box(&field_descriptor.singular_runtime_type())
                            .map_err(|e| MessageBuilderError::FailedToAssignValueToField {
                                field_name: field_name.to_owned(),
                                e,
                            })?,
                    );
                } else if field_descriptor.is_repeated() {
                    field_descriptor
                        .mut_repeated(msg.as_mut())
                        .push_node_value_content(&aggregated_value)
                        .map_err(|e| MessageBuilderError::FailedToAssignValueToField {
                            field_name: field_name.to_owned(),
                            e,
                        })?;
                } else {
                    panic!("Validation should have ensured map fields are not allowed");
                }
            }
            Ok(msg)
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
}

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
mod tests {
    use super::*;

    use futures::{future, FutureExt};
    use lazy_static::lazy_static;
    use protobuf::well_known_types::{any::Any, timestamp::Timestamp};
    use protobuf::{reflect::ReflectEqMode, MessageField, MessageFull};
    use sdv_telemetry_test_proto::test_message::{TestEnum, TestMessage, TestNestedMessage};
    use std::sync::Arc;

    use crate::expr::mock::{MockMessageProvider, TEST_MESSAGE};
    use crate::expr::node_error::MessageProviderError;
    use crate::expr::node_value::NodeValue;
    use value_aggregator::mock::MockValueAggregator;

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

    /// Creates a MessageBuilderImpl struct object using the descriptor and
    /// field_name + node_value pairs as field assignments.
    fn create_message_builder(
        message_descriptor: MessageDescriptor,
        field_name_and_value_pairs: Vec<(&str, NodeValue)>,
    ) -> Arc<MessageBuilderImpl> {
        let (field_assignments, value_providers) = field_name_and_value_pairs
            .into_iter()
            .map(|(field, value)| {
                // the mocked field value aggregator always returns the node value
                // that is passed to it.
                let mock_aggregator: Box<dyn ValueAggregator> =
                    Box::new(MockValueAggregator::new());
                (
                    (field.to_owned(), mock_aggregator),
                    (field.to_owned(), Arc::new(Node::ConstantLeaf { value })),
                )
            })
            .unzip();
        Arc::new(MessageBuilderImpl::new(
            String::default(),
            message_descriptor,
            field_assignments,
            value_providers,
        ))
    }

    #[test]
    fn build_singular_fields_ok() {
        let mut expected_message = TestMessage::new();
        expected_message.int_field = 5;
        expected_message.float_field = 0.0;
        expected_message.enum_field = TestEnum::SOME.into(); // this enum is equal to int 1

        // create MessageBuilderImpl struct object, this is the subject of the test
        let builder = create_message_builder(
            TestMessage::descriptor(),
            vec![
                (INT_FIELD_NAME, NodeValue::Int32(5)),
                (FLOAT_FIELD_NAME, NodeValue::Float(0.0)),
                (ENUM_FIELD_NAME, NodeValue::Int32(1)),
            ],
        );

        // action
        let context = Arc::new(NodeEvaluationContext::new());
        let res = builder.build(&context).unwrap_sync().unwrap();

        // compare `Box<dyn MessageDyn>` with `MessageFull` using reflect_eq_dyn
        assert!(res.reflect_eq_dyn(&expected_message, &REFLECT_EQ_MODE));
    }

    #[test]
    fn build_message_field_ok() {
        let mut expected_message = TestMessage::new();
        expected_message.any_field = MessageField::some(Any::pack(&*TEST_NESTED_MESSAGE).unwrap());
        expected_message.message_field = MessageField::some(TEST_NESTED_MESSAGE.clone());

        // create MessageBuilderImpl struct object, this is the subject of the test
        let builder = create_message_builder(
            TestMessage::descriptor(),
            vec![
                (ANY_FIELD_NAME, NodeValue::Message(Arc::new(TEST_NESTED_MESSAGE.clone()))),
                (MESSAGE_FIELD_NAME, NodeValue::Message(Arc::new(TEST_NESTED_MESSAGE.clone()))),
            ],
        );

        // action
        let context = Arc::new(NodeEvaluationContext::new());
        let res = builder.build(&context).unwrap_sync().unwrap();

        // compare `Box<dyn MessageDyn>` with `MessageFull` using reflect_eq_dyn
        assert!(res.reflect_eq_dyn(&expected_message, &REFLECT_EQ_MODE));
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
        let builder = create_message_builder(
            TestMessage::descriptor(),
            vec![
                (REPEATED_BOOL_FIELD_NAME, NodeValue::BoolList(booleans)),
                (REPEATED_INT_FIELD_NAME, NodeValue::Int64List(integers)),
                (REPEATED_DOUBLE_FIELD_NAME, NodeValue::DoubleList(doubles)),
                (REPEATED_ENUM_FIELD_NAME, NodeValue::Int64List(enums)),
            ],
        );

        // action
        let context = Arc::new(NodeEvaluationContext::new());
        let res = builder.build(&context).unwrap_sync().unwrap();

        // compare `Box<dyn MessageDyn>` with `MessageFull` using reflect_eq_dyn
        assert!(res.reflect_eq_dyn(&expected_message, &REFLECT_EQ_MODE));
    }

    #[test]
    fn build_repeated_message_field_ok() {
        let mut expected_message = TestMessage::new();
        expected_message.int_field = 42;
        expected_message.repeated_messages = vec![TEST_NESTED_MESSAGE.clone(); 2];

        // create MessageBuilderImpl struct object, this is the subject of the test
        let builder = create_message_builder(
            TestMessage::descriptor(),
            vec![
                (INT_FIELD_NAME, NodeValue::Int64(42)),
                (
                    // field name
                    REPEATED_MESSAGE_FIELD_NAME,
                    // value to assign to this field = a vector of 2 messages
                    NodeValue::MessageList(vec![
                        Arc::new(TEST_NESTED_MESSAGE.clone())
                            as Arc<dyn MessageDyn>;
                        2
                    ]),
                ),
            ],
        );

        // action
        let context = Arc::new(NodeEvaluationContext::new());
        let res = builder.build(&context).unwrap_sync().unwrap();

        // verify the newly-built message does set the fields to the expected values
        assert!(res.reflect_eq_dyn(&expected_message, &REFLECT_EQ_MODE));
    }

    #[test]
    fn build_handles_none_value_ok() {
        let mut expected_message = TestMessage::new();
        expected_message.int_field = 5;

        // create MessageBuilderImpl struct object, this is the subject of the test
        let builder = create_message_builder(
            TestMessage::descriptor(),
            vec![
                (INT_FIELD_NAME, NodeValue::Int32(5)),
                (FLOAT_FIELD_NAME, NodeValue::None),
                (OPTIONAL_BOOL_FIELD_NAME, NodeValue::None),
            ],
        );

        // action
        let context = Arc::new(NodeEvaluationContext::new());
        let res = builder.build(&context).unwrap_sync().unwrap();

        assert!(res.reflect_eq_dyn(&expected_message, &REFLECT_EQ_MODE));
    }

    #[test]
    fn build_zeros_are_not_encoded_in_implicitly_optional_fields() {
        // expect empty message because zeros are not encoded in implicitly optional fields
        // that means 0 is indistinguishable from field-not-set
        let expected_message = TestMessage::new();

        // create MessageBuilderImpl struct object, this is the subject of the test
        let builder = create_message_builder(
            TestMessage::descriptor(),
            vec![(INT_FIELD_NAME, NodeValue::Int32(0))],
        );

        // action
        let context = Arc::new(NodeEvaluationContext::new());
        let res = builder.build(&context).unwrap_sync().unwrap();

        assert!(res.reflect_eq_dyn(&expected_message, &REFLECT_EQ_MODE));
    }

    #[test]
    fn build_zeros_are_encoded_in_explicitly_optional_fields() {
        let mut expected_message = TestMessage::new();
        expected_message.optional_bool_field = Some(false);
        expected_message.optional_int64_field = Some(0);

        // create MessageBuilderImpl struct object, this is the subject of the test
        let builder = create_message_builder(
            TestMessage::descriptor(),
            vec![
                (OPTIONAL_BOOL_FIELD_NAME, NodeValue::Bool(false)),
                (OPTIONAL_INT64_FIELD_NAME, NodeValue::Int64(0)),
            ],
        );

        // action
        let context = Arc::new(NodeEvaluationContext::new());
        let res = builder.build(&context).unwrap_sync().unwrap();

        assert!(res.reflect_eq_dyn(&expected_message, &REFLECT_EQ_MODE));
    }

    #[test]
    fn build_node_value_incompatible_with_singular_field_fails() {
        let builder = create_message_builder(
            TestMessage::descriptor(),
            // field expects integer, actual node value is a float
            vec![(INT_FIELD_NAME, NodeValue::Float(0.5))],
        );

        // action
        let context = Arc::new(NodeEvaluationContext::new());
        let err = builder.build(&context).unwrap_sync().unwrap_err();

        assert!(
            matches!(&err, MessageBuilderError::FailedToAssignValueToField { .. }),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn build_node_value_incompatible_with_repeated_field_fails() {
        let builder = create_message_builder(
            TestMessage::descriptor(),
            // field expects repeated integers, actual node value is repeated floats
            vec![(REPEATED_INT_FIELD_NAME, NodeValue::FloatList(vec![0.5]))],
        );

        // action
        let context = Arc::new(NodeEvaluationContext::new());
        let err = builder.build(&context).unwrap_sync().unwrap_err();

        assert!(
            matches!(&err, MessageBuilderError::FailedToAssignValueToField { .. }),
            "Unexpected error: {:?}",
            err
        );
    }

    #[test]
    fn build_node_value_incompatible_with_message_field_fails() {
        let builder = create_message_builder(
            TestMessage::descriptor(),
            vec![(MESSAGE_FIELD_NAME, NodeValue::Message(Arc::new(Timestamp::new())))],
        );

        // action
        let context = Arc::new(NodeEvaluationContext::new());
        let err = builder.build(&context).unwrap_sync().unwrap_err();

        assert!(
            matches!(&err, MessageBuilderError::FailedToAssignValueToField { .. }),
            "Unexpected error: {:?}",
            err
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
        let context = Arc::new(NodeEvaluationContext::new());
        let err = builder.build(&context).unwrap_sync().unwrap_err();

        assert!(
            matches!(&err, MessageBuilderError::BadAggregation { .. }),
            "Unexpected error: {:?}",
            err
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

        let err = builder
            .build(&Arc::new(NodeEvaluationContext::new()))
            .unwrap_async()
            .await
            .unwrap_err();

        assert!(
            matches!(&err, MessageBuilderError::ExpressionEvaluationError { .. }),
            "Unexpected error: {:?}",
            err
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn build_from_asynchronous_result_success() {
        // Setup a field node that returns the value of the test message int field.
        let message_provider = Arc::new(MockMessageProvider::new());
        message_provider.set_next_async_result(
            future::ready(Ok(Some(Arc::new(TEST_MESSAGE.clone()) as Arc<dyn MessageDyn>))).boxed(),
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

        let res =
            builder.build(&Arc::new(NodeEvaluationContext::new())).unwrap_async().await.unwrap();

        let expected_message = TestMessage { int_field: 80, ..Default::default() };
        assert_eq!(&res, &(Box::new(expected_message) as Box<dyn MessageDyn>));
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
