// Copyright 2024 Google LLC

use anyhow::anyhow;
use async_trait::async_trait;
use sdv_telemetry_proto_rust::expression::combination_node::{Operator, RelationalOperator};
use sdv_telemetry_utils::operation_result::OperationResult;
use std::sync::{Arc, Weak};
use tokio::sync::mpsc;

use super::base::{BaseTrigger, TriggerState};
use super::{Trigger, TriggerCallbackFn, TriggerSubscriber};
use crate::expr::node::Node;
use crate::expr::node_error::NodeError;
use crate::expr::node_evaluation_context::NodeEvaluationContext;
use crate::expr::node_value::NodeValue;
use crate::metrics_config_orchestrator::async_event_processor::{
    AsyncEventHandler, AsyncEventProcessor, AsyncReceiver,
};
use crate::metrics_config_orchestrator::subscription_manager::{Activatable, ActivationId};
use crate::metrics_config_runtime_error::ConditionalTriggerRuntimeError;

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

    condition_checker: fn(NodeValue, &mut Option<NodeValue>) -> anyhow::Result<bool>,
    subscriber: TriggerSubscriber,
    error_callback: Arc<dyn Fn(ConditionalTriggerRuntimeError) + Send + Sync>,
}

#[async_trait]
impl Trigger for ConditionalTrigger {
    fn get_name(&self) -> &str {
        self.base.get_name()
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

        struct ConditionalTriggerHandler {
            activation_id: ActivationId,
            conditional_trigger: Weak<ConditionalTrigger>,
            maybe_previous_value: Option<NodeValue>,
        }

        #[async_trait]
        impl
            AsyncEventHandler<(
                OperationResult<'static, NodeValue, NodeError>,
                Arc<NodeEvaluationContext>,
            )> for ConditionalTriggerHandler
        {
            async fn handle(
                &mut self,
                (evaluation_result, context): (
                    OperationResult<'static, NodeValue, NodeError>,
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
                        &mut self.maybe_previous_value,
                    )
                    .await;
            }
        }

        let event_processor = AsyncEventProcessor::new(
            AsyncReceiver::Unbounded(receiver),
            ConditionalTriggerHandler {
                activation_id,
                conditional_trigger: Arc::downgrade(&self),
                maybe_previous_value: None,
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
        condition_type: ConditionType,
        triggers: Vec<Arc<dyn Trigger>>,
        selector_expression: Arc<Node>,
        error_callback: Arc<dyn Fn(ConditionalTriggerRuntimeError) + Send + Sync>,
    ) -> Arc<Self> {
        let condition_checker: for<'a> fn(
            NodeValue,
            &'a mut Option<NodeValue>,
        ) -> anyhow::Result<bool> = match condition_type {
            ConditionType::RisingEdge => |current_value, previous_value_holder| {
                Self::compare_with_previous(
                    current_value,
                    previous_value_holder,
                    &Operator::RelationalOperator(RelationalOperator::GT.into()),
                )
            },
            ConditionType::FallingEdge => |current_value, previous_value_holder| {
                Self::compare_with_previous(
                    current_value,
                    previous_value_holder,
                    &Operator::RelationalOperator(RelationalOperator::LT.into()),
                )
            },
            ConditionType::AllChanges => |current_value, previous_value_holder| {
                Self::compare_with_previous(
                    current_value,
                    previous_value_holder,
                    &Operator::RelationalOperator(RelationalOperator::NOT_EQ.into()),
                )
            },
            ConditionType::IsTrue => |current_value, _| Self::is_true(&current_value),
            ConditionType::IsFalse => |current_value, _| Self::is_false(&current_value),
        };

        Arc::new_cyclic(|me: &Weak<Self>| Self {
            base: BaseTrigger::new(name.clone(), Weak::clone(me) as Weak<dyn Activatable<_, _>>),
            triggers,
            selector_expression,
            condition_checker,
            subscriber: TriggerSubscriber::ConditionalTrigger { name },
            error_callback,
        })
    }

    /// Compares current value with the previous value using provided operator, stores
    /// current value in the new previous value holder and returns comparison result.
    fn compare_with_previous(
        current_value: NodeValue,
        previous_value_holder: &mut Option<NodeValue>,
        operator: &Operator,
    ) -> anyhow::Result<bool> {
        // Extract previous value and save current value.
        let maybe_previous_value = previous_value_holder.replace(current_value.clone());

        // Perform comparison or return false if there's no previous value.
        if let Some(previous_value) = maybe_previous_value {
            let result = current_value.operate_sync(operator, || Ok(previous_value)).unwrap();
            if let NodeValue::Bool(bool_result) = result {
                Ok(bool_result)
            } else {
                Err(anyhow!("Relational operator evaluation returned non-bool result {:?}", result))
            }
        } else {
            Ok(false)
        }
    }

    /// Returns true if the current value is a false boolean or a zero numeric value.
    fn is_false(current_value: &NodeValue) -> anyhow::Result<bool> {
        let res = match current_value {
            NodeValue::Bool(boolean) => !*boolean,
            NodeValue::None => false,
            // If the value is not a bool or None, it's numeric.
            // Otherwise validation would fail.
            numeric_value => numeric_value.is_zero()?,
        };
        Ok(res)
    }

    /// Returns true if the current value is a true boolean or a non-zero numeric value.
    fn is_true(current_value: &NodeValue) -> anyhow::Result<bool> {
        let res = match current_value {
            NodeValue::Bool(boolean) => *boolean,
            NodeValue::None => false,
            // If the value is not a bool or None, it's numeric.
            // Otherwise validation would fail.
            numeric_value => !numeric_value.is_zero()?,
        };
        Ok(res)
    }

    /// A callback to handle a new message generated from the selector expression as a result of
    /// a fire event from conditional trigger's associated parent trigger.
    async fn on_new_message(
        &self,
        activation_id: ActivationId,
        evaluation_result: OperationResult<'static, NodeValue, NodeError>,
        context: Arc<NodeEvaluationContext>,
        maybe_previous_value: &mut Option<NodeValue>,
    ) {
        let node_value = match evaluation_result.into_future().await {
            Ok(node_value) => node_value,
            Err(err) => {
                (self.error_callback)(ConditionalTriggerRuntimeError {
                    trigger_name: self.get_name().to_owned(),
                    node_error: err,
                });
                return;
            }
        };

        if node_value.is_none() {
            // TODO: b/307547570 - properly handle absent values
            return;
        }

        let condition_result = (self.condition_checker)(node_value, maybe_previous_value)
            .unwrap_or_else(|err| {
                panic!("Failed to perform comparison for trigger {}: {}", self.get_name(), err)
            });
        if condition_result {
            self.base.fire_trigger(activation_id, context).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::FutureExt;
    use protobuf::well_known_types::wrappers::{BoolValue, Int32Value};
    use protobuf::MessageDyn;
    use std::future;

    use crate::expr::mock::MockMessageProvider;
    use crate::expr::node::Node;
    use crate::metrics_config_orchestrator::trigger::mock::{
        MockCallbackListener, MockSubscriptionManager, MockTrigger,
    };

    const TRIGGER_NAME: &str = "trigger";
    const PARENT_TRIGGER_NAME: &str = "parent_trigger";
    const PUBLISHER_NAME: &str = "publisher";
    const ACTIVATION_ID: ActivationId = ActivationId::new(3);

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
                TriggerSubscriber::ConditionalTrigger { name: TRIGGER_NAME.to_owned() },
            ))
        });
        let message_provider = Arc::new(MockMessageProvider::new());
        let trigger = ConditionalTrigger::new(
            TRIGGER_NAME.to_owned(),
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
        let (trigger, message_provider, parent_trigger) =
            create_valid_conditional_trigger(condition_type, None);

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

        (trigger, message_provider, parent_trigger, callback_listener)
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_subscribes_to_parents() {
        let parent_trigger1 = Arc::new(MockTrigger::new_for_subscriber(
            PARENT_TRIGGER_NAME.to_owned(),
            TriggerSubscriber::ConditionalTrigger { name: TRIGGER_NAME.to_owned() },
        ));
        let parent_trigger2 = Arc::new(MockTrigger::new_for_subscriber(
            PARENT_TRIGGER_NAME.to_owned(),
            TriggerSubscriber::ConditionalTrigger { name: TRIGGER_NAME.to_owned() },
        ));
        let message_provider = Arc::new(MockMessageProvider::new());
        let trigger = ConditionalTrigger::new(
            TRIGGER_NAME.to_owned(),
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

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn shares_context_across_subscribers() {
        let parent_trigger = Arc::new(MockTrigger::new_for_subscriber(
            PARENT_TRIGGER_NAME.to_owned(),
            TriggerSubscriber::ConditionalTrigger { name: TRIGGER_NAME.to_owned() },
        ));
        let parent_context = Arc::new(NodeEvaluationContext::new());
        let message_provider = Arc::new(MockMessageProvider::new());
        let trigger = ConditionalTrigger::new(
            TRIGGER_NAME.to_owned(),
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

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_filters_none() {
        let (_trigger, message_provider, parent_trigger, callback_listener) =
            setup_active_conditional_trigger(ConditionType::AllChanges).await;

        let msg1 = Arc::new(Int32Value { value: 1, ..Default::default() });
        let msg2 = Arc::new(Int32Value { value: 2, ..Default::default() });

        // Fire parent trigger three times.
        message_provider.set_next_sync_result(Ok(Some(msg1)));
        parent_trigger.fire().await;

        message_provider.set_next_sync_result(Ok(None));
        parent_trigger.fire().await;

        message_provider.set_next_sync_result(Ok(Some(msg2)));
        parent_trigger.fire().await;

        // Wait for trigger to fire once.
        let (activation_id, _context) = callback_listener.wait_for_callback_fired().await;

        assert_eq!(activation_id, ACTIVATION_ID);
        assert_eq!(callback_listener.get_fired_count(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_filters_all_changes() {
        let (_trigger, message_provider, parent_trigger, callback_listener) =
            setup_active_conditional_trigger(ConditionType::AllChanges).await;

        let msg1 = Arc::new(Int32Value { value: 1, ..Default::default() });
        let msg2 = Arc::new(Int32Value { value: 2, ..Default::default() });

        // Fire parent trigger three times with the message changing once.
        message_provider.set_next_sync_result(Ok(Some(msg1)));
        parent_trigger.fire().await;

        message_provider.set_next_sync_result(Ok(Some(Arc::clone(&msg2) as Arc<dyn MessageDyn>)));
        parent_trigger.fire().await;

        message_provider.set_next_sync_result(Ok(Some(msg2)));
        parent_trigger.fire().await;

        // Wait for trigger to fire once.
        callback_listener.wait_for_callback_fired().await;

        // Verify that the trigger fired once.
        assert_eq!(callback_listener.get_fired_count(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_filters_falling_edge() {
        let (_trigger, message_provider, parent_trigger, callback_listener) =
            setup_active_conditional_trigger(ConditionType::FallingEdge).await;

        let msg1 = Arc::new(Int32Value { value: 1, ..Default::default() });
        let msg2 = Arc::new(Int32Value { value: 2, ..Default::default() });

        // Fire parent trigger three times.
        // The first change is the rising edge and the second change is the falling edge.
        message_provider.set_next_sync_result(Ok(Some(Arc::clone(&msg1) as Arc<dyn MessageDyn>)));
        parent_trigger.fire().await;

        message_provider.set_next_sync_result(Ok(Some(msg2)));
        parent_trigger.fire().await;

        message_provider.set_next_sync_result(Ok(Some(msg1)));
        parent_trigger.fire().await;

        // Wait for trigger to fire once.
        callback_listener.wait_for_callback_fired().await;

        assert_eq!(callback_listener.get_fired_count(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_filters_rising_edge() {
        let (_trigger, message_provider, parent_trigger, callback_listener) =
            setup_active_conditional_trigger(ConditionType::RisingEdge).await;

        let msg1 = Arc::new(Int32Value { value: 1, ..Default::default() });
        let msg2 = Arc::new(Int32Value { value: 2, ..Default::default() });

        // Fire parent trigger three times.
        // The first change is the falling edge and the second change is the rising edge.
        message_provider.set_next_sync_result(Ok(Some(Arc::clone(&msg2) as Arc<dyn MessageDyn>)));
        parent_trigger.fire().await;

        message_provider.set_next_sync_result(Ok(Some(msg1)));
        parent_trigger.fire().await;

        message_provider.set_next_sync_result(Ok(Some(msg2)));
        parent_trigger.fire().await;

        // Wait for trigger to fire once.
        callback_listener.wait_for_callback_fired().await;

        assert_eq!(callback_listener.get_fired_count(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_is_true_filters_true() {
        let (_trigger, message_provider, parent_trigger, callback_listener) =
            setup_active_conditional_trigger(ConditionType::IsTrue).await;

        let false_msg = Arc::new(BoolValue { value: false, ..Default::default() });
        let true_msg = Arc::new(BoolValue { value: true, ..Default::default() });

        // Fire parent trigger three times: [false, true, true].
        // This is to verify it filters out false and lets repeated true go through.
        message_provider.set_next_sync_result(Ok(Some(false_msg)));
        parent_trigger.fire().await;

        message_provider
            .set_next_sync_result(Ok(Some(Arc::clone(&true_msg) as Arc<dyn MessageDyn>)));
        parent_trigger.fire().await;

        message_provider.set_next_sync_result(Ok(Some(true_msg)));
        parent_trigger.fire().await;

        // Wait for trigger to fire twice.
        callback_listener.wait_for_callback_fired().await;
        callback_listener.wait_for_callback_fired().await;

        assert_eq!(callback_listener.get_fired_count(), 2);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_is_false_filters_false() {
        let (_trigger, message_provider, parent_trigger, callback_listener) =
            setup_active_conditional_trigger(ConditionType::IsFalse).await;

        let false_msg = Arc::new(BoolValue { value: false, ..Default::default() });
        let true_msg = Arc::new(BoolValue { value: true, ..Default::default() });

        // Fire parent trigger three times: [true, false, false].
        // This is to verify it filters out false and lets repeated true go through.
        message_provider.set_next_sync_result(Ok(Some(true_msg)));
        parent_trigger.fire().await;

        message_provider
            .set_next_sync_result(Ok(Some(Arc::clone(&false_msg) as Arc<dyn MessageDyn>)));
        parent_trigger.fire().await;

        message_provider.set_next_sync_result(Ok(Some(false_msg)));
        parent_trigger.fire().await;

        // Wait for trigger to fire twice.
        callback_listener.wait_for_callback_fired().await;
        callback_listener.wait_for_callback_fired().await;

        assert_eq!(callback_listener.get_fired_count(), 2);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_is_true_filters_non_zero() {
        let (_trigger, message_provider, parent_trigger, callback_listener) =
            setup_active_conditional_trigger(ConditionType::IsTrue).await;

        let msg0 = Arc::new(Int32Value { value: 0, ..Default::default() });
        let msg1 = Arc::new(Int32Value { value: 1, ..Default::default() });

        // Fire parent trigger three times: [0, 1, 1].
        // This is to verify it filters out zeo and lets repeated non-zeroes go through.
        message_provider.set_next_sync_result(Ok(Some(msg0)));
        parent_trigger.fire().await;

        message_provider.set_next_sync_result(Ok(Some(Arc::clone(&msg1) as Arc<dyn MessageDyn>)));
        parent_trigger.fire().await;

        message_provider.set_next_sync_result(Ok(Some(msg1)));
        parent_trigger.fire().await;

        // Wait for trigger to fire twice.
        callback_listener.wait_for_callback_fired().await;
        callback_listener.wait_for_callback_fired().await;

        assert_eq!(callback_listener.get_fired_count(), 2);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_is_false_filters_zero() {
        let (_trigger, message_provider, parent_trigger, callback_listener) =
            setup_active_conditional_trigger(ConditionType::IsFalse).await;

        let msg0 = Arc::new(Int32Value { value: 0, ..Default::default() });
        let msg1 = Arc::new(Int32Value { value: 1, ..Default::default() });

        // Fire parent trigger three times: [1, 0, 0].
        // This is to verify it filters out non-zero and lets repeated zero go through.
        message_provider.set_next_sync_result(Ok(Some(msg1)));
        parent_trigger.fire().await;

        message_provider.set_next_sync_result(Ok(Some(Arc::clone(&msg0) as Arc<dyn MessageDyn>)));
        parent_trigger.fire().await;

        message_provider.set_next_sync_result(Ok(Some(msg0)));
        parent_trigger.fire().await;

        // Wait for trigger to fire twice.
        callback_listener.wait_for_callback_fired().await;
        callback_listener.wait_for_callback_fired().await;

        assert_eq!(callback_listener.get_fired_count(), 2);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_is_true_filters_none() {
        let (_trigger, message_provider, parent_trigger, callback_listener) =
            setup_active_conditional_trigger(ConditionType::IsTrue).await;

        let msg = Arc::new(Int32Value { value: 1, ..Default::default() });

        // Fire parent trigger three times: [None, 1, 1].
        // This is to verify it filters out None and lets repeated non-zeroes go through.
        message_provider.set_next_sync_result(Ok(None));
        parent_trigger.fire().await;

        message_provider.set_next_sync_result(Ok(Some(Arc::clone(&msg) as Arc<dyn MessageDyn>)));
        parent_trigger.fire().await;

        message_provider.set_next_sync_result(Ok(Some(msg)));
        parent_trigger.fire().await;

        // Wait for trigger to fire twice.
        callback_listener.wait_for_callback_fired().await;
        callback_listener.wait_for_callback_fired().await;

        assert_eq!(callback_listener.get_fired_count(), 2);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_is_false_filters_none() {
        let (_trigger, message_provider, parent_trigger, callback_listener) =
            setup_active_conditional_trigger(ConditionType::IsFalse).await;

        let msg = Arc::new(Int32Value { value: 0, ..Default::default() });

        // Fire parent trigger three times: [None, 0, 0].
        // This is to verify it filters out None and lets repeated zero go through.
        message_provider.set_next_sync_result(Ok(None));
        parent_trigger.fire().await;

        message_provider.set_next_sync_result(Ok(Some(Arc::clone(&msg) as Arc<dyn MessageDyn>)));
        parent_trigger.fire().await;

        message_provider.set_next_sync_result(Ok(Some(msg)));
        parent_trigger.fire().await;

        // Wait for trigger to fire twice.
        callback_listener.wait_for_callback_fired().await;
        callback_listener.wait_for_callback_fired().await;

        assert_eq!(callback_listener.get_fired_count(), 2);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
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

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    #[should_panic]
    async fn activate_active_panics() {
        let (trigger, _, _, _) = setup_active_conditional_trigger(ConditionType::AllChanges).await;

        // Activate the trigger again.
        trigger.activate(ActivationId::new(1)).await;
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn deactivate_unsubscribes_from_parents() {
        let mut parent_trigger = MockTrigger::new_for_subscriber(
            PARENT_TRIGGER_NAME.to_owned(),
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

    #[sdv_telemetry_test_utils_macros::timeout]
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
}
