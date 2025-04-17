// Copyright 2024 Google LLC

use async_trait::async_trait;
use sdv_telemetry_expr::dynamic_message::DynamicMessage;
use sdv_telemetry_expr::node_evaluation_context::NodeEvaluationContext;
use sdv_telemetry_service_publisher_accessor::ServicePublisherAccessorError;
use sdv_telemetry_utils::{
    collection_extension::HashMapExtension, operation_result::OperationResult,
    value_holder::ValueHolder,
};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{Arc, Weak};
use tokio::sync::{mpsc, Mutex as AsyncMutex};
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::{base::BasePublisher, CallbackFn, Publisher};
use crate::metrics_config_orchestrator::async_event_processor::{
    AsyncEventHandler, AsyncEventProcessor,
};
use crate::metrics_config_orchestrator::error_status_codes::ErrorStatusCodes;
use crate::metrics_config_orchestrator::message_builder::{MessageBuilder, MessageBuilderError};
use crate::metrics_config_orchestrator::trigger::{Trigger, TriggerSubscriber};
use crate::status_reporter::StatusReporter;

use google_sdv_telemetry::aidl::google::sdv::telemetry::status::{
    StatusDetails::AggregationPublisherErrorDetails::AggregationPublisherErrorDetails,
    StatusDetails::StatusDetails,
};

pub(crate) struct AggregationPublisher {
    base: BasePublisher,
    message_builder: Arc<dyn MessageBuilder>,
    reset_on_get: bool,
    state: AsyncMutex<AggregationPublisherState>,
    triggers: Vec<Arc<dyn Trigger>>,
    trigger_subscriber: TriggerSubscriber,
    status_reporter: Arc<dyn StatusReporter>,
}

#[derive(strum_macros::Display)]
#[strum(serialize_all = "title_case")]
enum AggregationPublisherState {
    /// The publisher is not subscribed to the underlying data source
    /// and doesn't provide the latest message.
    Inactive,
    /// The publisher is subscribed to the underlying data source
    /// and maintains the latest message.
    Active {
        // Map of unique data trigger names to the subscribed callbacks.
        callback_map: HashMap<String, CallbackFn>,

        // Processor of incoming trigger events.
        _event_processor: AsyncEventProcessor,
    },
}

#[async_trait]
impl Publisher for AggregationPublisher {
    fn get_name(&self) -> &str {
        self.base.get_name()
    }

    fn get_metrics_config_uuid(&self) -> &str {
        self.base.get_metrics_config_uuid()
    }

    fn is_subscribable(&self) -> bool {
        true
    }

    fn get_latest_message(
        self: Arc<Self>,
    ) -> OperationResult<'static, Option<Arc<dyn DynamicMessage>>, ServicePublisherAccessorError>
    {
        let latest_message = self.base.get_latest_message();
        if self.reset_on_get {
            self.base.reset_latest_message();
            self.message_builder.reset();
        }
        Ok(latest_message).into()
    }

    async fn activate(self: Arc<Self>) -> Result<(), ServicePublisherAccessorError> {
        // Lock state mutex and hold it until the state transition is complete.
        let mut state_lock = self.state.lock().await;
        assert!(
            matches!(*state_lock, AggregationPublisherState::Inactive),
            "Unable to activate publisher {}: it's in {} state",
            self.get_name(),
            state_lock
        );

        // Subscribe to parent triggers.
        let (sender, receiver) = mpsc::unbounded_channel();
        for trigger in &self.triggers {
            let self_reference = Arc::downgrade(&self);
            let sender_clone = sender.clone();
            Arc::clone(trigger)
                .subscribe(
                    &self.trigger_subscriber,
                    Box::new(move |context| {
                        // Synchronously build aggregated message and enqueue the result.
                        let self_clone = self_reference.upgrade().expect(
                            "Callback should only be called when aggregation publisher is active",
                        );
                        let eval_context =
                            if let Some(latest_message) = self_clone.base.get_latest_message() {
                                Cow::Owned(Arc::new(context.to_child_context(
                                    self_clone.base.get_name().to_owned(),
                                    latest_message,
                                )))
                            } else {
                                Cow::Borrowed(&context)
                            };
                        let build_result =
                            Arc::clone(&self_clone.message_builder).build(&eval_context);
                        sender_clone
                            .send((build_result, context))
                            .expect("Callback should only receive messages when subscribed")
                    }),
                )
                .await;
        }

        struct AggregationPublisherHandler {
            aggregation_publisher: Weak<AggregationPublisher>,
        }

        #[async_trait]
        impl
            AsyncEventHandler<(
                ValueHolder<'static, (Option<Box<dyn DynamicMessage>>, Vec<MessageBuilderError>)>,
                Arc<NodeEvaluationContext>,
            )> for AggregationPublisherHandler
        {
            async fn handle(
                &mut self,
                (build_result, context): (
                    ValueHolder<
                        'static,
                        (Option<Box<dyn DynamicMessage>>, Vec<MessageBuilderError>),
                    >,
                    Arc<NodeEvaluationContext>,
                ),
            ) {
                let Some(aggregation_publisher) = self.aggregation_publisher.upgrade() else {
                    return;
                };
                aggregation_publisher.on_new_message(build_result, context).await;
            }
        }

        let event_processor = AsyncEventProcessor::new(
            UnboundedReceiverStream::new(receiver),
            AggregationPublisherHandler { aggregation_publisher: Arc::downgrade(&self) },
        );

        // Set the new state and release the mutex.
        *state_lock = AggregationPublisherState::Active {
            callback_map: HashMap::new(),
            _event_processor: event_processor,
        };

        Ok(())
    }

    async fn deactivate(&self) -> anyhow::Result<()> {
        // Lock state mutex and hold it until the state transition is complete.
        let mut state_lock = self.state.lock().await;
        match &*state_lock {
            AggregationPublisherState::Active { callback_map, .. } => {
                assert!(
                    callback_map.is_empty(),
                    "Unable to deactivate publisher {} with active subscriptions",
                    self.get_name()
                );
            }
            AggregationPublisherState::Inactive => {
                panic!("Unable to deactivate publisher {}: it's inactive", self.get_name())
            }
        }

        // Perform deactivation logic specific to the publisher type.

        // Verify that the publisher is in the subscribed state.
        assert!(
            matches!(&*state_lock, AggregationPublisherState::Active { .. }),
            "Unable to deactivate Aggregation publisher {} because it's in {} state",
            self.get_name(),
            state_lock
        );

        // Unsubscribe from parent triggers.
        for trigger in &self.triggers {
            trigger.unsubscribe(&self.trigger_subscriber).await;
        }

        // Reset latest message.
        // The publisher is unsubscribed from underlying data sources, so no callback will change it back to some stale message.
        self.base.reset_latest_message();

        // Change state to Inactive and release the mutex.
        *state_lock = AggregationPublisherState::Inactive;

        Ok(())
    }

    async fn subscribe(&self, data_trigger_name: &str, callback: CallbackFn) {
        // Lock state mutex and hold it until operation is complete.
        let mut state_lock = self.state.lock().await;

        if let AggregationPublisherState::Active { callback_map, .. } = &mut *state_lock {
            callback_map.try_insert_value(data_trigger_name.to_owned(), callback).unwrap_or_else(|_| panic!("Unable to subscribe to publisher {}: data trigger {} is already subscribed", self.get_name(), data_trigger_name));
        } else {
            panic!(
                "Unable to subscribe to publisher {}: it's in {} state",
                self.get_name(),
                state_lock
            );
        }
    }

    async fn unsubscribe(&self, data_trigger_name: &str) {
        // Lock state mutex and hold it until operation is complete.
        let mut state_lock = self.state.lock().await;

        if let AggregationPublisherState::Active { callback_map, .. } = &mut *state_lock {
            let _ = callback_map.remove(data_trigger_name).unwrap_or_else(|| {
                panic!(
                    "Unable to unsubscribe from publisher {}: data trigger {} wasn't subscribed",
                    self.get_name(),
                    data_trigger_name
                )
            });
        } else {
            panic!(
                "Unable to unsubscribe from publisher {}: it's in {} state",
                self.get_name(),
                state_lock
            );
        }
    }
}

impl AggregationPublisher {
    pub(crate) fn new(
        name: String,
        message_builder: Arc<dyn MessageBuilder>,
        metrics_config_uuid: String,
        reset_on_get: bool,
        triggers: Vec<Arc<dyn Trigger>>,

        // Dependencies
        status_reporter: Arc<dyn StatusReporter>,
    ) -> Self {
        Self {
            base: BasePublisher::new(name.clone(), metrics_config_uuid),
            message_builder,
            reset_on_get,
            state: AggregationPublisherState::Inactive.into(),
            status_reporter,
            trigger_subscriber: TriggerSubscriber::AggregationPublisher { name },
            triggers,
        }
    }

    fn report_error<T: ErrorStatusCodes + Display>(
        &self,
        err: T,
        field_name_provider: impl FnOnce(T) -> String,
    ) {
        let metrics_config_uuid = self.get_metrics_config_uuid();
        let publisher_name = self.get_name();
        self.status_reporter.report(
            err.get_status_code(),
            format!("Metrics config {metrics_config_uuid} aggregation publisher {publisher_name} failed to build message: {err}"),
            StatusDetails::AggregationPublisherErrorDetails(
                AggregationPublisherErrorDetails {
                    metricsConfigUUID: metrics_config_uuid.to_owned(),
                    publisherName: publisher_name.to_owned(),
                    fieldName: field_name_provider(err),
                },
        ));
    }

    async fn on_new_message(
        &self,
        build_result: ValueHolder<
            'static,
            (Option<Box<dyn DynamicMessage>>, Vec<MessageBuilderError>),
        >,
        context: Arc<NodeEvaluationContext>,
    ) {
        // Lock state mutex and hold it to make sure the publisher is not deactivated.
        let mut state_lock = self.state.lock().await;

        let AggregationPublisherState::Active { callback_map, .. } = &mut *state_lock else {
            return;
        };

        let (message, builder_errors) = build_result.into_future().await;
        if !builder_errors.is_empty() {
            for err in builder_errors {
                self.report_error(err, |err| err.field_name);
            }

            // Don't set new message if at least one error has occurred
            return;
        }

        let message = message.expect("Message should be Some if no builder errors occurred").into();
        let context = context.to_child_context(self.get_name().to_owned(), Arc::clone(&message));

        self.base.set_new_message(message, &context.into(), callback_map);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::FutureExt;
    use google_sdv_telemetry::aidl::google::sdv::telemetry::status::{
        TelemetryServiceStatus::TelemetryServiceStatus,
        TelemetryServiceStatusCodes::TelemetryServiceStatusCodes as StatusCodes,
    };
    use protobuf::well_known_types::wrappers::Int32Value;
    use protobuf::MessageDyn;
    use sdv_telemetry_expr::dynamic_message::DynamicMessage;
    use sdv_telemetry_expr::node_error::NodeEvaluationError;
    use sdv_telemetry_test_proto::test_message::TestMessage;
    use sdv_telemetry_test_utils::oneshot_callback;
    use std::{future, sync::Arc};

    use crate::metrics_config_orchestrator::message_builder::mock::MockMessageBuilder;
    use crate::metrics_config_orchestrator::message_builder::MessageBuilderErrorType;
    use crate::metrics_config_orchestrator::protobuf_dynamic_message::ProtobufDynamicMessage;
    use crate::metrics_config_orchestrator::trigger::mock::MockTrigger;
    use crate::StatusReporterImpl;

    const CONFIG_UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";
    const DATA_TRIGGER_NAME: &str = "datatrigger";
    const PUBLISHER_NAME: &str = "publisher";
    const PUBLISHER_NAME_2: &str = "publisher2";
    const TRIGGER_NAME: &str = "trigger";
    const FIELD_NAME: &str = "field_name";

    fn create_inactive_aggregation_publisher(
        triggers: Vec<Arc<dyn Trigger>>,
    ) -> (Arc<dyn Publisher>, Arc<MockMessageBuilder>) {
        let message_builder = Arc::new(MockMessageBuilder::new());
        let publisher = Arc::new(AggregationPublisher::new(
            PUBLISHER_NAME.to_owned(),
            Arc::<MockMessageBuilder>::clone(&message_builder),
            CONFIG_UUID.to_owned(),
            false,
            triggers,
            Arc::new(StatusReporterImpl::new(|err| panic!("Unexpected error: {err:?}"))),
        ));
        (publisher, message_builder)
    }

    async fn create_active_aggregation_publisher(
        triggers: Vec<Arc<dyn Trigger>>,
    ) -> (Arc<dyn Publisher>, Arc<MockMessageBuilder>) {
        let (publisher, message_builder) = create_inactive_aggregation_publisher(triggers);

        Arc::clone(&publisher).activate().await.unwrap();

        (publisher, message_builder)
    }

    async fn create_aggregation_publisher_with_subscription(
        triggers: Vec<Arc<dyn Trigger>>,
    ) -> Arc<dyn Publisher> {
        let (publisher, _) = create_active_aggregation_publisher(triggers).await;

        publisher.subscribe(DATA_TRIGGER_NAME, create_empty_fn_box()).await;
        publisher
    }

    /// Creates a Box with a callback that accepts context and does nothing.
    pub fn create_empty_fn_box() -> CallbackFn {
        Box::new(|_| {})
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn get_latest_message_resets() {
        let trigger = Arc::new(MockTrigger::new_for_subscriber(
            TRIGGER_NAME.to_owned(),
            CONFIG_UUID.to_owned(),
            TriggerSubscriber::AggregationPublisher { name: PUBLISHER_NAME.to_owned() },
        ));
        let message_builder = Arc::new(MockMessageBuilder::new());
        let publisher = Arc::new(AggregationPublisher::new(
            PUBLISHER_NAME.to_owned(),
            Arc::<MockMessageBuilder>::clone(&message_builder),
            CONFIG_UUID.to_owned(),
            true,
            vec![Arc::<MockTrigger>::clone(&trigger)],
            Arc::new(StatusReporterImpl::new(|err| panic!("Unexpected error: {err:?}"))),
        ));

        Arc::clone(&publisher).activate().await.unwrap();
        trigger.fire().await;
        let msg1 = Arc::clone(&publisher)
            .get_latest_message()
            .unwrap_sync()
            .expect("Message retrieval should be successful");
        let msg2 = publisher
            .get_latest_message()
            .unwrap_sync()
            .expect("Message retrieval should be successful");

        assert!(msg1.is_some());
        assert!(msg2.is_none(), "Unexpected message: {:?}", msg2.unwrap());
        assert_eq!(message_builder.get_num_reset_calls(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn activate_when_active_panics() {
        let (publisher, _) = create_active_aggregation_publisher(vec![]).await;

        let _ = publisher.activate().await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn deactivate_when_inactive_panics() {
        let (publisher, _) = create_inactive_aggregation_publisher(vec![]);

        let _ = publisher.deactivate().await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn deactivate_with_subscriptions_panics() {
        let publisher = create_aggregation_publisher_with_subscription(vec![]).await;

        let _ = publisher.deactivate().await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn subscribe_when_inactive_panics() {
        let (publisher, _) = create_inactive_aggregation_publisher(vec![]);

        publisher.subscribe(DATA_TRIGGER_NAME, create_empty_fn_box()).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn subscribe_already_subscribed_panics() {
        let publisher = create_aggregation_publisher_with_subscription(vec![]).await;

        publisher.subscribe(DATA_TRIGGER_NAME, create_empty_fn_box()).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribe_notifies_subscriber_when_triggered() {
        let trigger = Arc::new(MockTrigger::new_for_subscriber(
            TRIGGER_NAME.to_owned(),
            CONFIG_UUID.to_owned(),
            TriggerSubscriber::AggregationPublisher { name: PUBLISHER_NAME.to_owned() },
        ));
        let (publisher, _) =
            create_active_aggregation_publisher(vec![Arc::<MockTrigger>::clone(&trigger)]).await;
        let (tx, rx) = oneshot_callback();

        publisher
            .subscribe(
                DATA_TRIGGER_NAME,
                Box::new(move |context| {
                    let msg = context
                        .get_message(PUBLISHER_NAME)
                        .expect("Publisher message should be present in the context");
                    tx(Arc::clone(msg));
                }),
            )
            .await;
        trigger.fire().await;

        let messages = [
            rx.await.to_protobuf().unwrap(),
            Arc::clone(&publisher)
                .get_latest_message()
                .unwrap_sync()
                .unwrap()
                .unwrap()
                .to_protobuf()
                .unwrap(),
        ];
        for message in messages {
            let test_message =
                <dyn MessageDyn>::downcast_ref::<TestMessage>(message.as_ref()).unwrap();
            assert_eq!(test_message.int_field, 1);
        }
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribe_passes_latest_message_to_builder_when_triggered() {
        let trigger = Arc::new(MockTrigger::new_for_subscriber(
            TRIGGER_NAME.to_owned(),
            CONFIG_UUID.to_owned(),
            TriggerSubscriber::AggregationPublisher { name: PUBLISHER_NAME.to_owned() },
        ));
        let message_builder = Arc::new(MockMessageBuilder::new());
        let (tx, rx) = oneshot_callback();
        // Capture the context passed to the message builder
        message_builder.set_result_callback(move |ctx| {
            tx(Arc::clone(ctx));
            ValueHolder::Synchronous((None, vec![]))
        });

        let publisher = Arc::new(AggregationPublisher::new(
            PUBLISHER_NAME.to_owned(),
            Arc::<MockMessageBuilder>::clone(&message_builder),
            CONFIG_UUID.to_owned(),
            false,
            vec![Arc::<MockTrigger>::clone(&trigger)],
            Arc::new(StatusReporterImpl::new(|err| panic!("Unexpected error: {err:?}"))),
        ));

        // Set a latest message before activation
        let latest_msg = Arc::new(ProtobufDynamicMessage::new(Arc::new(Int32Value {
            value: 99,
            ..Default::default()
        }))) as Arc<dyn DynamicMessage>;
        publisher.base.set_new_message(
            Arc::clone(&latest_msg),
            &Arc::new(NodeEvaluationContext::new(None)),
            &mut HashMap::new(),
        );

        // Activate and fire trigger
        Arc::clone(&publisher).activate().await.unwrap();
        trigger.fire().await;

        // Verify the context received by the message builder
        let received_context = rx.await;
        let context_msg = received_context.get_message(PUBLISHER_NAME).unwrap();
        assert!(Arc::ptr_eq(context_msg, &latest_msg), "Unexpected message: {context_msg:?}");
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribe_passes_trigger_context_when_triggered() {
        let trigger = Arc::new(MockTrigger::new_for_subscriber(
            TRIGGER_NAME.to_owned(),
            CONFIG_UUID.to_owned(),
            TriggerSubscriber::AggregationPublisher { name: PUBLISHER_NAME.to_owned() },
        ));
        let (publisher, _) =
            create_active_aggregation_publisher(vec![Arc::<MockTrigger>::clone(&trigger)]).await;
        let msg = Arc::new(ProtobufDynamicMessage::new(Arc::new(Int32Value {
            value: 42,
            ..Default::default()
        }))) as Arc<dyn DynamicMessage>;

        let (tx, rx) = oneshot_callback();
        publisher.subscribe(DATA_TRIGGER_NAME, Box::new(tx)).await;

        let trigger_context = Arc::new(NodeEvaluationContext::with_publisher_message(
            None,
            PUBLISHER_NAME_2.to_owned(),
            Arc::clone(&msg),
        ));
        trigger.fire_with_context(Arc::clone(&trigger_context)).await;

        let context = rx.await;

        assert_eq!(context.root_id(), trigger_context.root_id());
        let context_msg = context
            .get_message(PUBLISHER_NAME_2)
            .expect("Publisher message should be present in the context");
        assert!(Arc::ptr_eq(context_msg, &msg), "Unexpected message: {context_msg:?}");
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribe_handles_async_expression_result() {
        let trigger = Arc::new(MockTrigger::new_for_subscriber(
            TRIGGER_NAME.to_owned(),
            CONFIG_UUID.to_owned(),
            TriggerSubscriber::AggregationPublisher { name: PUBLISHER_NAME.to_owned() },
        ));
        let (publisher, message_builder) =
            create_active_aggregation_publisher(vec![Arc::<MockTrigger>::clone(&trigger)]).await;
        let (tx, rx) = oneshot_callback();
        publisher
            .subscribe(
                DATA_TRIGGER_NAME,
                Box::new(move |context| {
                    let msg = context
                        .get_message(PUBLISHER_NAME)
                        .expect("Publisher message should be present in the context");
                    tx(Arc::clone(msg));
                }),
            )
            .await;

        message_builder.set_next_async_result(
            future::ready((
                Some(Box::new(ProtobufDynamicMessage::new(Arc::new(TestMessage {
                    int_field: 2,
                    ..Default::default()
                }))) as Box<dyn DynamicMessage>),
                vec![],
            ))
            .boxed(),
        );
        trigger.fire().await;

        let messages = [
            rx.await.to_protobuf().unwrap(),
            publisher.get_latest_message().unwrap_sync().unwrap().unwrap().to_protobuf().unwrap(),
        ];
        for message in messages {
            let test_message =
                <dyn MessageDyn>::downcast_ref::<TestMessage>(message.as_ref()).unwrap();
            assert_eq!(test_message.int_field, 2);
        }
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn unsubscribe_when_inactive_panics() {
        let (publisher, _) = create_inactive_aggregation_publisher(vec![]);

        publisher.unsubscribe(DATA_TRIGGER_NAME).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn unsubscribe_not_subscribed_panics() {
        let (publisher, _) = create_active_aggregation_publisher(vec![]).await;

        publisher.unsubscribe(DATA_TRIGGER_NAME).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn unsubscribe_success() {
        let publisher = create_aggregation_publisher_with_subscription(vec![]).await;

        publisher.unsubscribe(DATA_TRIGGER_NAME).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn on_new_message_reports_builder_error() {
        let trigger = Arc::new(MockTrigger::new_for_subscriber(
            TRIGGER_NAME.to_owned(),
            CONFIG_UUID.to_owned(),
            TriggerSubscriber::AggregationPublisher { name: PUBLISHER_NAME.to_owned() },
        ));

        let message_builder = Arc::new(MockMessageBuilder::new());
        message_builder.set_next_error(MessageBuilderError {
            field_name: FIELD_NAME.to_owned(),
            error: MessageBuilderErrorType::ExpressionEvaluationError {
                e: NodeEvaluationError::DivideByZero,
            },
        });

        let (tx, rx) = oneshot_callback();
        let publisher = Arc::new(AggregationPublisher::new(
            PUBLISHER_NAME.to_owned(),
            Arc::clone(&message_builder) as Arc<dyn MessageBuilder>,
            CONFIG_UUID.to_owned(),
            false,
            vec![Arc::<MockTrigger>::clone(&trigger)],
            Arc::new(StatusReporterImpl::new(tx)),
        ));

        Arc::clone(&publisher).activate().await.unwrap();

        trigger.fire().await;

        // Wait for error status to be delivered.
        let status = rx.await;
        assert!(
            matches!(&status, TelemetryServiceStatus {
                id: 0,
                code: StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR,
                message,
                details: StatusDetails::AggregationPublisherErrorDetails(
                    AggregationPublisherErrorDetails { metricsConfigUUID, publisherName, fieldName }
                ),
            }
            if message.contains("failed to build message") && metricsConfigUUID == CONFIG_UUID && publisherName == PUBLISHER_NAME && fieldName == FIELD_NAME),
            "Unexpected status: {:?}",
            status
        );
    }
}
