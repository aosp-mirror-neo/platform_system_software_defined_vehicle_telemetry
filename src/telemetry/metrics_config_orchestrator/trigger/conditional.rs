// Copyright 2024 Google LLC

use async_trait::async_trait;
use sdv_telemetry_utils::operation_result::OperationResult;
use std::sync::{Arc, Weak};
use tokio::sync::mpsc;

use super::base::{BaseTrigger, TriggerState};
use super::{Trigger, TriggerCallbackFn, TriggerSubscriber};
use crate::expr::node::Node;
use crate::expr::node_error::NodeEvaluationError;
use crate::expr::node_evaluation_context::NodeEvaluationContext;
use crate::expr::node_value::NodeValue;
use crate::metrics_config_orchestrator::async_event_processor::{
    AsyncEventHandler, AsyncEventProcessor, AsyncReceiver,
};
use crate::metrics_config_orchestrator::error_status_codes::ErrorStatusCodes;
use crate::metrics_config_orchestrator::subscription_manager::{Activatable, ActivationId};
use crate::metrics_config_orchestrator::trigger::condition_checker::{
    ConditionChecker, ConstantConditionChecker, PreviousValueRelationalConditionChecker,
};

use google_sdv_telemetry::aidl::google::sdv::telemetry::status::{
    StatusDetails::ConditionalTriggerErrorDetails::ConditionalTriggerErrorDetails,
    StatusDetails::StatusDetails, TelemetryServiceStatus::TelemetryServiceStatus,
};

pub(crate) enum ConditionType {
    RisingEdge,
    FallingEdge,
    AllChanges,
    IsTrue,
    IsFalse,
}

pub(crate) struct ConditionalTrigger {
    base: BaseTrigger,

    pub(crate) triggers: Vec<Arc<dyn Trigger>>,
    pub(crate) selector_expression: Arc<Node>,

    condition_type: ConditionType,
    subscriber: TriggerSubscriber,
    error_callback: Arc<dyn Fn(TelemetryServiceStatus) + Send + Sync>,
}

#[async_trait]
impl Trigger for ConditionalTrigger {
    fn get_name(&self) -> &str {
        self.base.get_name()
    }

    fn get_metrics_config_uuid(&self) -> &str {
        self.base.get_metrics_config_uuid()
    }

    async fn subscribe(
        self: Arc<Self>,
        subscriber: &TriggerSubscriber,
        on_fired_callback: TriggerCallbackFn,
    ) {
        self.base.subscription_manager.subscribe(subscriber.clone(), on_fired_callback).await;
    }

    async fn unsubscribe(&self, subscriber: &TriggerSubscriber) {
        self.base.subscription_manager.unsubscribe(subscriber).await;
    }
}

#[async_trait]
impl Activatable<TriggerSubscriber, Arc<NodeEvaluationContext>> for ConditionalTrigger {
    async fn activate(self: Arc<Self>, activation_id: ActivationId) {
        // Lock state mutex and hold it until the state transition is complete.
        let mut state_lock = self.base.state.lock().await;
        assert!(
            matches!(*state_lock, TriggerState::Inactive),
            "Unable to activate trigger {}: it's in {} state",
            self.get_name(),
            state_lock
        );

        let (sender, receiver) = mpsc::unbounded_channel();
        for trigger in &self.triggers {
            let self_reference = Arc::downgrade(&self);
            let sender_clone = sender.clone();
            Arc::clone(trigger)
                .subscribe(
                    &self.subscriber,
                    Box::new(move |context| {
                        // Synchronously evaluate selector expression and enqueue the result.
                        let self_clone = &self_reference.upgrade().expect(
                            "Callback should only be called when conditional trigger is active",
                        );
                        let evaluation_result =
                            self_clone.selector_expression.evaluate(Arc::clone(&context));
                        sender_clone
                            .send((evaluation_result, context))
                            .expect("Callback should only receive messages when subscribed")
                    }),
                )
                .await;
        }

        let condition_checker = match self.condition_type {
            ConditionType::RisingEdge => {
                Box::new(PreviousValueRelationalConditionChecker::new_rising_edge())
                    as Box<dyn ConditionChecker>
            }
            ConditionType::FallingEdge => {
                Box::new(PreviousValueRelationalConditionChecker::new_falling_edge())
                    as Box<dyn ConditionChecker>
            }
            ConditionType::AllChanges => {
                Box::new(PreviousValueRelationalConditionChecker::new_all_changes())
                    as Box<dyn ConditionChecker>
            }
            ConditionType::IsTrue => Box::new(ConstantConditionChecker::new_is_true_or_non_zero())
                as Box<dyn ConditionChecker>,
            ConditionType::IsFalse => Box::new(ConstantConditionChecker::new_is_false_or_zero())
                as Box<dyn ConditionChecker>,
        };

        struct ConditionalTriggerHandler {
            activation_id: ActivationId,
            conditional_trigger: Weak<ConditionalTrigger>,
            condition_checker: Box<dyn ConditionChecker>,
        }

        #[async_trait]
        impl
            AsyncEventHandler<(
                OperationResult<'static, NodeValue, NodeEvaluationError>,
                Arc<NodeEvaluationContext>,
            )> for ConditionalTriggerHandler
        {
            async fn handle(
                &mut self,
                (evaluation_result, context): (
                    OperationResult<'static, NodeValue, NodeEvaluationError>,
                    Arc<NodeEvaluationContext>,
                ),
            ) {
                let Some(conditional_trigger) = self.conditional_trigger.upgrade() else {
                    return;
                };
                conditional_trigger
                    .on_new_message(
                        self.activation_id,
                        evaluation_result,
                        context,
                        self.condition_checker.as_mut(),
                    )
                    .await;
            }
        }

        let event_processor = AsyncEventProcessor::new(
            AsyncReceiver::Unbounded(receiver),
            ConditionalTriggerHandler {
                activation_id,
                conditional_trigger: Arc::downgrade(&self),
                condition_checker,
            },
        );

        // Set the new state and release the mutex.
        *state_lock = TriggerState::Active { _event_processor: event_processor };
    }

    async fn deactivate(&self) {
        // Lock state mutex and hold it until the state transition is complete.
        let mut state_lock = self.base.state.lock().await;
        assert!(
            matches!(*state_lock, TriggerState::Active { .. }),
            "Unable to deactivate trigger {}: it's in {} state",
            self.get_name(),
            state_lock
        );

        for trigger in &self.triggers {
            trigger.unsubscribe(&self.subscriber).await;
        }

        // Set the new state and release the mutex.
        *state_lock = TriggerState::Inactive;
    }
}

impl ConditionalTrigger {
    pub(crate) fn new(
        name: String,
        metrics_config_uuid: String,
        condition_type: ConditionType,
        triggers: Vec<Arc<dyn Trigger>>,
        selector_expression: Arc<Node>,
        error_callback: Arc<dyn Fn(TelemetryServiceStatus) + Send + Sync>,
    ) -> Arc<Self> {
        Arc::new_cyclic(|me: &Weak<Self>| Self {
            base: BaseTrigger::new(
                name.clone(),
                metrics_config_uuid,
                Weak::clone(me) as Weak<dyn Activatable<_, _>>,
            ),
            triggers,
            selector_expression,
            condition_type,
            subscriber: TriggerSubscriber::ConditionalTrigger { name },
            error_callback,
        })
    }

    /// A callback to handle a new message generated from the selector expression as a result of
    /// a fire event from conditional trigger's associated parent trigger.
    async fn on_new_message(
        &self,
        activation_id: ActivationId,
        evaluation_result: OperationResult<'static, NodeValue, NodeEvaluationError>,
        context: Arc<NodeEvaluationContext>,
        condition_checker: &mut dyn ConditionChecker,
    ) {
        let node_value = match evaluation_result.into_future().await {
            Ok(node_value) => node_value,
            Err(err) => {
                let trigger_name = self.get_name().to_owned();
                let metrics_config_uuid = self.get_metrics_config_uuid().to_owned();
                (self.error_callback)(TelemetryServiceStatus {
                    code: err.get_status_code(),
                    message: format!("Metrics config {metrics_config_uuid} conditional trigger '{trigger_name}' failed to evaluate condition: {err}"),
                    details: StatusDetails::ConditionalTriggerErrorDetails(ConditionalTriggerErrorDetails {
                        metricsConfigUUID: metrics_config_uuid,
                        triggerName: trigger_name,
                    }),
                });
                return;
            }
        };

        let condition_result = condition_checker.check(node_value).unwrap_or_else(|err| {
            panic!("Failed to perform comparison for trigger {}: {err:#}", self.get_name())
        });
        if condition_result {
            self.base.fire_trigger(activation_id, context).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use anyhow::anyhow;
    use futures::FutureExt;
    use google_sdv_telemetry::aidl::google::sdv::telemetry::status::TelemetryServiceStatusCodes::TelemetryServiceStatusCodes as StatusCodes;
    use googletest::matchers::{eq, none, result_of_ref, some};
    use googletest::{assert_that, expect_that};
    use lazy_static::lazy_static;
    use protobuf::reflect::ReflectEqMode;
    use protobuf::well_known_types::wrappers::{BoolValue, Int32Value};
    use protobuf::MessageDyn;
    use sdv_telemetry_test_utils::oneshot_callback;
    use sdv_telemetry_test_utils::{msg_dyn_eq, parameterized_test};
    use sdv_telemetry_utils::conversions::u32_into_usize;

    use std::collections::HashMap;
    use std::future;

    use crate::expr::mock::MockMessageProvider;
    use crate::expr::node::Node;
    use crate::expr::node_error::MessageProviderError;
    use crate::metrics_config_orchestrator::trigger::mock::{
        MockCallbackListener, MockSubscriptionManager, MockTrigger,
    };
    use crate::metrics_config_orchestrator::ServicePublisherAccessorError;

    const CONFIG_UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";
    const TRIGGER_NAME: &str = "trigger";
    const PARENT_TRIGGER_NAME: &str = "parent_trigger";
    const PUBLISHER_NAME: &str = "publisher";
    const ACTIVATION_ID: ActivationId = ActivationId::new(3);
    const ERROR_MESSAGE: &str = "Error 42";

    lazy_static! {
        static ref FALSE_MSG: Arc<dyn MessageDyn> =
            Arc::new(BoolValue { value: false, ..Default::default() });
        static ref TRUE_MSG: Arc<dyn MessageDyn> =
            Arc::new(BoolValue { value: true, ..Default::default() });
        static ref INT_0_MSG: Arc<dyn MessageDyn> =
            Arc::new(Int32Value { value: 0, ..Default::default() });
        static ref INT_1_MSG: Arc<dyn MessageDyn> =
            Arc::new(Int32Value { value: 1, ..Default::default() });
        static ref INT_2_MSG: Arc<dyn MessageDyn> =
            Arc::new(Int32Value { value: 2, ..Default::default() });
    }

    fn create_field_node(field_name: String, message_provider: Arc<MockMessageProvider>) -> Node {
        Node::FieldLeaf {
            publisher_name: PUBLISHER_NAME.to_owned(),
            fields: vec![field_name],
            message_provider,
        }
    }

    fn create_valid_conditional_trigger(
        condition_type: ConditionType,
        parent_trigger: Option<Arc<MockTrigger>>,
    ) -> (Arc<ConditionalTrigger>, Arc<MockMessageProvider>, Arc<MockTrigger>) {
        let parent_trigger = parent_trigger.unwrap_or_else(|| {
            Arc::new(MockTrigger::new_for_subscriber(
                PARENT_TRIGGER_NAME.to_owned(),
                CONFIG_UUID.to_owned(),
                TriggerSubscriber::ConditionalTrigger { name: TRIGGER_NAME.to_owned() },
            ))
        });
        let message_provider = Arc::new(MockMessageProvider::new());
        let trigger = ConditionalTrigger::new(
            TRIGGER_NAME.to_owned(),
            CONFIG_UUID.to_owned(),
            condition_type,
            vec![Arc::<MockTrigger>::clone(&parent_trigger)],
            Arc::new(create_field_node("value".to_owned(), Arc::clone(&message_provider))),
            Arc::new(|err| panic!("Unexpected error: {err:?}")),
        );

        (trigger, message_provider, parent_trigger)
    }

    fn create_valid_conditional_trigger_with_mock_subscription_manager(
        condition_type: ConditionType,
        parent_trigger: Option<Arc<MockTrigger>>,
    ) -> (
        Arc<ConditionalTrigger>,
        Arc<MockMessageProvider>,
        Arc<MockTrigger>,
        MockSubscriptionManager,
    ) {
        let (trigger, message_provider, parent_trigger) =
            create_valid_conditional_trigger(condition_type, parent_trigger);

        let mut trigger = Arc::into_inner(trigger).unwrap();
        let subscription_manager = MockSubscriptionManager::new();
        trigger.base.subscription_manager = Box::new(subscription_manager.clone());

        (Arc::new(trigger), message_provider, parent_trigger, subscription_manager)
    }

    async fn setup_active_conditional_trigger(
        condition_type: ConditionType,
    ) -> (
        Arc<ConditionalTrigger>,
        Arc<MockMessageProvider>,
        Arc<MockTrigger>,
        Arc<MockCallbackListener<(ActivationId, Arc<NodeEvaluationContext>)>>,
    ) {
        let (trigger, message_provider, parent_trigger, subscription_manager) =
            create_valid_conditional_trigger_with_mock_subscription_manager(condition_type, None);

        let callback_listener = Arc::new(MockCallbackListener::new());
        subscription_manager
            .subscribe_for_test(
                TriggerSubscriber::MetricsConfigStart,
                callback_listener.create_callback(),
            )
            .await;

        Arc::clone(&trigger).activate(ACTIVATION_ID).await;

        (trigger, message_provider, parent_trigger, callback_listener)
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_subscribes_to_parents() {
        let parent_trigger1 = Arc::new(MockTrigger::new_for_subscriber(
            PARENT_TRIGGER_NAME.to_owned(),
            CONFIG_UUID.to_owned(),
            TriggerSubscriber::ConditionalTrigger { name: TRIGGER_NAME.to_owned() },
        ));
        let parent_trigger2 = Arc::new(MockTrigger::new_for_subscriber(
            PARENT_TRIGGER_NAME.to_owned(),
            CONFIG_UUID.to_owned(),
            TriggerSubscriber::ConditionalTrigger { name: TRIGGER_NAME.to_owned() },
        ));
        let message_provider = Arc::new(MockMessageProvider::new());
        let trigger = ConditionalTrigger::new(
            TRIGGER_NAME.to_owned(),
            CONFIG_UUID.to_owned(),
            ConditionType::AllChanges,
            vec![
                Arc::<MockTrigger>::clone(&parent_trigger1),
                Arc::<MockTrigger>::clone(&parent_trigger2),
            ],
            Arc::new(create_field_node("value".to_owned(), Arc::clone(&message_provider))),
            Arc::new(|err| panic!("Unexpected error: {err:?}")),
        );
        let mut trigger = Arc::into_inner(trigger).unwrap();
        let subscription_manager = MockSubscriptionManager::new();
        trigger.base.subscription_manager = Box::new(subscription_manager.clone());
        let trigger = Arc::new(trigger);

        let callback_listener = Arc::new(MockCallbackListener::new());
        subscription_manager
            .subscribe_for_test(
                TriggerSubscriber::MetricsConfigStart,
                callback_listener.create_callback(),
            )
            .await;
        Arc::clone(&trigger).activate(ACTIVATION_ID).await;

        let msg1 = Arc::new(Int32Value { value: 1, ..Default::default() });
        let msg2 = Arc::new(Int32Value { value: 2, ..Default::default() });

        // Set the first message and fire the first parent trigger.
        message_provider.set_next_sync_result(Ok(Some(msg1)));
        parent_trigger1.fire().await;

        // Set a different message and fire the second parent trigger.
        message_provider.set_next_sync_result(Ok(Some(msg2)));
        parent_trigger2.fire().await;

        // Wait for trigger to fire once.
        callback_listener.wait_for_callback_fired().await;

        // Verify that the trigger fired once.
        assert_eq!(callback_listener.get_fired_count(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn shares_context_across_subscribers() {
        let parent_trigger = Arc::new(MockTrigger::new_for_subscriber(
            PARENT_TRIGGER_NAME.to_owned(),
            CONFIG_UUID.to_owned(),
            TriggerSubscriber::ConditionalTrigger { name: TRIGGER_NAME.to_owned() },
        ));
        let parent_context = Arc::new(NodeEvaluationContext::new());
        let message_provider = Arc::new(MockMessageProvider::new());
        let trigger = ConditionalTrigger::new(
            TRIGGER_NAME.to_owned(),
            CONFIG_UUID.to_owned(),
            ConditionType::IsTrue,
            vec![Arc::<MockTrigger>::clone(&parent_trigger)],
            Arc::new(create_field_node("value".to_owned(), Arc::clone(&message_provider))),
            Arc::new(|err| panic!("Unexpected error: {err:?}")),
        );

        let callback_listener1 = Arc::new(MockCallbackListener::new());
        let callback_listener2 = Arc::new(MockCallbackListener::new());

        Arc::clone(&trigger)
            .subscribe(&TriggerSubscriber::MetricsConfigStart, callback_listener1.create_callback())
            .await;
        Arc::clone(&trigger)
            .subscribe(&TriggerSubscriber::MetricsConfigEnd, callback_listener2.create_callback())
            .await;

        message_provider.set_next_sync_result(Ok(Some(Arc::new(Int32Value {
            value: 1,
            ..Default::default()
        }))));
        parent_trigger.fire_with_context(Arc::clone(&parent_context)).await;

        let child_context1 = callback_listener1.wait_for_callback_fired().await;
        let child_context2 = callback_listener2.wait_for_callback_fired().await;

        assert_eq!(callback_listener1.get_fired_count(), 1);
        assert_eq!(callback_listener2.get_fired_count(), 1);
        assert!(Arc::ptr_eq(&parent_context, &child_context1));
        assert!(Arc::ptr_eq(&parent_context, &child_context2));
    }

    parameterized_test!(
        #[googletest::test]
        #[sdv_telemetry_test_utils::timeout]
        #[tokio::test]
        activate,

        |condition_type, messages: &[Option<&Arc<dyn MessageDyn>>], expected| async move {
            let (_trigger, message_provider, parent_trigger, callback_listener) =
                setup_active_conditional_trigger(condition_type).await;

            let mut ctx_to_msg_map = HashMap::new();
            for msg in messages.iter() {
                message_provider.set_next_sync_result(Ok(msg.cloned()));

                let context = NodeEvaluationContext::new();
                if let Some(msg) = msg {
                    assert_that!(ctx_to_msg_map.insert(context.root_id(), msg.as_ref()), none());
                }
                parent_trigger.fire_with_context(context.into()).await;
            }

            for msg in expected.iter() {
                expect_that!(
                    callback_listener.wait_for_callback_fired().await,
                    (
                        eq(&ACTIVATION_ID),
                        result_of_ref!(
                            |context: &Arc<NodeEvaluationContext>| ctx_to_msg_map
                                .get(&context.root_id())
                                .cloned(),
                            some(msg_dyn_eq(msg.as_ref(), &ReflectEqMode::default()))
                        )
                    )
                );
            }
            expect_that!(u32_into_usize(callback_listener.get_fired_count()), eq(expected.len()));
        },

        all_changes:      (ConditionType::AllChanges, &[Some(&*INT_1_MSG), /* change */ Some(&*INT_2_MSG), /* no change */ Some(&*INT_2_MSG)], &[&*INT_2_MSG],),
        all_changes_none: (ConditionType::AllChanges, &[Some(&*INT_1_MSG), None, /* change */ Some(&*INT_2_MSG)], &[&*INT_2_MSG],),

        falling_edge: (ConditionType::FallingEdge, &[Some(&*INT_1_MSG), /* rising */ Some(&*INT_2_MSG), /* falling */ Some(&*INT_1_MSG)], &[&*INT_1_MSG],),

        rising_edge: (ConditionType::RisingEdge, &[Some(&*INT_2_MSG), /* falling */ Some(&*INT_1_MSG), /* rising */ Some(&*INT_2_MSG)], &[&*INT_2_MSG],),

        is_true_bool_1:    (ConditionType::IsTrue, &[Some(&*TRUE_MSG)], &[&*TRUE_MSG],),
        is_true_bool_2:    (ConditionType::IsTrue, &[Some(&*FALSE_MSG), Some(&*TRUE_MSG), Some(&*FALSE_MSG), Some(&*TRUE_MSG)], &[&*TRUE_MSG, &*TRUE_MSG],),
        is_true_numeric_1: (ConditionType::IsTrue, &[Some(&*INT_1_MSG)], &[&*INT_1_MSG],),
        is_true_numeric_2: (ConditionType::IsTrue, &[Some(&*INT_0_MSG), Some(&*INT_1_MSG), Some(&*INT_0_MSG), Some(&*INT_1_MSG)], &[&*INT_1_MSG, &*INT_1_MSG],),
        is_true_none:      (ConditionType::IsTrue, &[None, Some(&*TRUE_MSG), None, Some(&*INT_1_MSG)], &[&*TRUE_MSG, &*INT_1_MSG],),

        is_false_bool_1:    (ConditionType::IsFalse, &[Some(&*FALSE_MSG)], &[&*FALSE_MSG],),
        is_false_bool_2:    (ConditionType::IsFalse, &[Some(&*TRUE_MSG), Some(&*FALSE_MSG), Some(&*TRUE_MSG), Some(&*FALSE_MSG)], &[&*FALSE_MSG, &*FALSE_MSG],),
        is_false_numeric_1: (ConditionType::IsFalse, &[Some(&*INT_0_MSG)], &[&*INT_0_MSG],),
        is_false_numeric_2: (ConditionType::IsFalse, &[Some(&*INT_1_MSG), Some(&*INT_0_MSG), Some(&*INT_1_MSG), Some(&*INT_0_MSG)], &[&*INT_0_MSG, &*INT_0_MSG],),
        is_false_none:      (ConditionType::IsFalse, &[None, Some(&*FALSE_MSG), None, Some(&*INT_0_MSG)], &[&*FALSE_MSG, &*INT_0_MSG],),
    );

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_handles_async_expression_result() {
        let (_trigger, message_provider, parent_trigger, callback_listener) =
            setup_active_conditional_trigger(ConditionType::IsTrue).await;

        let msg = Arc::new(Int32Value { value: 1, ..Default::default() });
        message_provider
            .set_next_async_result(future::ready(Ok(Some(msg as Arc<dyn MessageDyn>))).boxed());
        parent_trigger.fire().await;
        callback_listener.wait_for_callback_fired().await;

        assert_eq!(callback_listener.get_fired_count(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn activate_active_panics() {
        let (trigger, _, _, _) = setup_active_conditional_trigger(ConditionType::AllChanges).await;

        // Activate the trigger again.
        trigger.activate(ActivationId::new(1)).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn deactivate_unsubscribes_from_parents() {
        let mut parent_trigger = MockTrigger::new_for_subscriber(
            PARENT_TRIGGER_NAME.to_owned(),
            CONFIG_UUID.to_owned(),
            TriggerSubscriber::ConditionalTrigger { name: TRIGGER_NAME.to_owned() },
        );
        let callback_listener = Arc::new(MockCallbackListener::new());
        let callback_listener_clone = Arc::clone(&callback_listener);
        parent_trigger
            .set_unsubscribe_callback(move |_| callback_listener_clone.on_callback_fired(()));
        let (trigger, _, _, _) = create_valid_conditional_trigger_with_mock_subscription_manager(
            ConditionType::AllChanges,
            Some(Arc::new(parent_trigger)),
        );

        // Activate the trigger.
        Arc::clone(&trigger).activate(ActivationId::new(1)).await;

        // Deactivate the trigger.
        trigger.deactivate().await;

        // Verify that the trigger was unsubscribed from once.
        assert_eq!(callback_listener.get_fired_count(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn deactivate_inactive_panics() {
        let (trigger, _, _, _) = create_valid_conditional_trigger_with_mock_subscription_manager(
            ConditionType::AllChanges,
            None,
        );

        // Deactivate the trigger.
        trigger.deactivate().await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn on_new_message_reports_error() {
        let (trigger, message_provider, parent_trigger) =
            create_valid_conditional_trigger(ConditionType::AllChanges, None);

        let (tx, rx) = oneshot_callback();
        let mut trigger = Arc::into_inner(trigger).unwrap();
        trigger.error_callback = Arc::new(tx);
        let trigger = Arc::new(trigger);

        Arc::clone(&trigger).activate(ACTIVATION_ID).await;

        message_provider.set_next_sync_result(Err(MessageProviderError::MessageRetrievalError {
            e: ServicePublisherAccessorError::MessageRetrievalFailed(anyhow!(ERROR_MESSAGE)),
        }));

        parent_trigger.fire().await;

        // Wait for error status to be delivered
        let status = rx.await;
        assert!(
            matches!(&status, TelemetryServiceStatus {
                code: StatusCodes::METRICS_CONFIG_RUNTIME_SERVICE_MESSAGE_RETRIEVAL_ERROR,
                message,
                details: StatusDetails::ConditionalTriggerErrorDetails(
                    ConditionalTriggerErrorDetails { metricsConfigUUID, triggerName }
                ),
            }
            if message.contains("failed to evaluate condition") && message.contains(ERROR_MESSAGE) && metricsConfigUUID == CONFIG_UUID && triggerName == TRIGGER_NAME),
            "Unexpected status: {:?}",
            status
        );
    }
}
