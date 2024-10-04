// Copyright 2024 Google LLC

use anyhow::anyhow;
use async_trait::async_trait;
use log::error;
use parking_lot::Mutex;
use sdv_telemetry_proto_rust::expression::combination_node::{Operator, RelationalOperator};
use sdv_telemetry_utils::operation_result::OperationResult;
use std::collections::HashMap;
use std::marker::Send;
use std::sync::{Arc, Weak};
use tokio::sync::Mutex as AsyncMutex;

use super::{base::BaseTrigger, Trigger, TriggerCallbackFn, TriggerSubscriber};
use crate::expr::node::Node;
use crate::expr::node_error::NodeError;
use crate::expr::node_evaluation_context::NodeEvaluationContext;
use crate::expr::node_value::NodeValue;
use crate::metrics_config_orchestrator::subscription_manager::Activatable;

pub(crate) enum ConditionType {
    RisingEdge,
    FallingEdge,
    AllChanges,
    IsTrue,
    IsFalse,
}

pub(crate) struct ConditionalTrigger {
    base: BaseTrigger,

    pub(crate) condition_type: ConditionType,
    pub(crate) triggers: Vec<Arc<dyn Trigger>>,
    pub(crate) selector_expression: Arc<Node>,

    maybe_previous_value: Mutex<Option<NodeValue>>,
    subscriber: TriggerSubscriber,
}

impl ConditionalTrigger {
    pub(crate) fn new(
        name: String,
        condition_type: ConditionType,
        triggers: Vec<Arc<dyn Trigger>>,
        selector_expression: Arc<Node>,
    ) -> Arc<Self> {
        Arc::new_cyclic(|me: &Weak<Self>| Self {
            base: BaseTrigger::new(name.clone(), Weak::clone(me) as Weak<dyn Activatable<_, _>>),
            condition_type,
            triggers,
            selector_expression,
            maybe_previous_value: None.into(),
            subscriber: TriggerSubscriber::ConditionalTrigger { name },
        })
    }

    /// Compares current value with the previous value using provided operator, stores
    /// current value in the new previous value holder and returns comparison result.
    fn compare_with_previous(
        current_value: NodeValue,
        previous_value_holder: &Mutex<Option<NodeValue>>,
        operator: &Operator,
    ) -> anyhow::Result<bool> {
        // Extract previous value and save current value.
        let maybe_previous_value = previous_value_holder.lock().replace(current_value.clone());

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

    /// A callback to handle a fire event from conditional trigger's associated parent trigger.
    /// Passes the result of expression evaluation and the previous value holder to
    /// condition_checker and fires the trigger if it returns true.
    fn on_conditional_trigger_parent_trigger_fired(
        self: &Arc<Self>,
        callback_map: &Arc<AsyncMutex<HashMap<TriggerSubscriber, TriggerCallbackFn>>>,
        condition_checker: impl FnOnce(NodeValue, &Mutex<Option<NodeValue>>) -> anyhow::Result<bool>
            + Send
            + 'static,
        context: Arc<NodeEvaluationContext>,
    ) {
        fn check_evaluation_result(
            trigger: &Arc<ConditionalTrigger>,
            result: Result<NodeValue, NodeError>,
            condition_checker: impl FnOnce(NodeValue, &Mutex<Option<NodeValue>>) -> anyhow::Result<bool>,
        ) -> bool {
            let node_value = match result {
                Ok(node_value) => node_value,
                Err(err) => {
                    error!(
                        "Error when performing expression evaluation for trigger {}: {}",
                        trigger.get_name(),
                        err
                    );
                    return false;
                }
            };

            if node_value.is_none() {
                // TODO: b/307547570 - properly handle absent values
                return false;
            }

            condition_checker(node_value, &trigger.maybe_previous_value).unwrap_or_else(|err| {
                panic!("Failed to perform comparison for trigger {}: {}", trigger.get_name(), err)
            })
        }

        let result_holder = self.selector_expression.evaluate(Arc::clone(&context));
        match result_holder {
            OperationResult::Asynchronous(future) => {
                // Spawn a task that awaits the async result, checks it and fires the trigger if it's true.
                let self_clone = Arc::clone(self);
                let callback_map_clone = Arc::clone(callback_map);
                tokio::spawn(async move {
                    if check_evaluation_result(&self_clone, future.await, condition_checker) {
                        BaseTrigger::fire_trigger(
                            self_clone.get_name(),
                            &callback_map_clone,
                            &context,
                        )
                        .await;
                    }
                });
            }
            OperationResult::Synchronous(result) => {
                // Check result in the current thread and spawn a task to fire the trigger if it's true.
                if check_evaluation_result(self, result, condition_checker) {
                    let callback_map_clone = Arc::clone(callback_map);
                    let trigger_name_clone = self.get_name().to_owned();
                    tokio::spawn(async move {
                        BaseTrigger::fire_trigger(
                            &trigger_name_clone,
                            &callback_map_clone,
                            &context,
                        )
                        .await;
                    });
                }
            }
        };
    }
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
    async fn activate(
        self: Arc<Self>,
        callback_map: Arc<AsyncMutex<HashMap<TriggerSubscriber, TriggerCallbackFn>>>,
    ) {
        let condition_checker: for<'a> fn(
            NodeValue,
            &'a Mutex<Option<NodeValue>>,
        ) -> anyhow::Result<bool> = match self.condition_type {
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

        for trigger in &self.triggers {
            let self_clone = Arc::clone(&self);
            let callback_map = Arc::clone(&callback_map);
            Arc::clone(trigger)
                .subscribe(
                    &self.subscriber,
                    Box::new(move |context| {
                        self_clone.on_conditional_trigger_parent_trigger_fired(
                            &callback_map,
                            condition_checker,
                            context,
                        )
                    }),
                )
                .await;
        }
    }

    async fn deactivate(&self) {
        for trigger in &self.triggers {
            trigger.unsubscribe(&self.subscriber).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use protobuf::well_known_types::wrappers::{BoolValue, Int32Value};
    use protobuf::MessageDyn;
    use std::collections::HashMap;
    use std::future;
    use tokio::sync::oneshot;
    use tokio::sync::Mutex as AsyncMutex;

    use super::*;
    use crate::expr::mock::MockMessageProvider;
    use crate::expr::node::Node;
    use crate::metrics_config_orchestrator::trigger::mock::{MockCallbackListener, MockTrigger};

    const TRIGGER_NAME: &str = "trigger";
    const PARENT_TRIGGER_NAME: &str = "parent_trigger";
    const PUBLISHER_NAME: &str = "publisher";

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
        );

        (trigger, message_provider, parent_trigger)
    }

    async fn setup_active_conditional_trigger(
        condition_type: ConditionType,
    ) -> (
        Arc<MockMessageProvider>,
        Arc<MockTrigger>,
        Arc<MockCallbackListener<Arc<NodeEvaluationContext>>>,
    ) {
        let (trigger, message_provider, parent_trigger) =
            create_valid_conditional_trigger(condition_type, None);

        let callback_listener = Arc::new(MockCallbackListener::new());
        trigger.activate(callback_listener.create_callback_map()).await;

        (message_provider, parent_trigger, callback_listener)
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_conditional_trigger_subscribes_to_parents() {
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
        );
        let callback_listener = Arc::new(MockCallbackListener::new());

        trigger.activate(callback_listener.create_callback_map()).await;

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
    async fn conditional_trigger_shares_context_across_subscribers() {
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
        );
        let callback_listener = Arc::new(MockCallbackListener::new());
        let callback_map = callback_listener.create_callback_map();

        // Add a second callback to `callback_map`.
        let (child_context1_tx, child_context1_rx) = oneshot::channel();
        let mut child_context1_tx = Some(child_context1_tx);
        callback_map.lock().await.insert(
            TriggerSubscriber::AggregationPublisher { name: "publisher-name".to_owned() },
            Box::new(move |context| child_context1_tx.take().unwrap().send(context).unwrap()),
        );

        trigger.activate(callback_map).await;

        message_provider.set_next_sync_result(Ok(Some(Arc::new(Int32Value {
            value: 1,
            ..Default::default()
        }))));
        parent_trigger.fire_with_context(Arc::clone(&parent_context)).await;

        let child_context1 = child_context1_rx.await.unwrap();
        let child_context2 = callback_listener.wait_for_callback_fired().await;

        assert_eq!(callback_listener.get_fired_count(), 1);
        assert!(Arc::ptr_eq(&parent_context, &child_context1));
        assert!(Arc::ptr_eq(&parent_context, &child_context2));
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_conditional_trigger_filters_none() {
        let (message_provider, parent_trigger, callback_listener) =
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
        callback_listener.wait_for_callback_fired().await;

        assert_eq!(callback_listener.get_fired_count(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_conditional_trigger_filters_all_changes() {
        let (message_provider, parent_trigger, callback_listener) =
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
    async fn activate_conditional_trigger_filters_falling_edge() {
        let (message_provider, parent_trigger, callback_listener) =
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
    async fn activate_conditional_trigger_filters_rising_edge() {
        let (message_provider, parent_trigger, callback_listener) =
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
    async fn activate_conditional_trigger_is_true_filters_true() {
        let (message_provider, parent_trigger, callback_listener) =
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
    async fn activate_conditional_trigger_is_false_filters_false() {
        let (message_provider, parent_trigger, callback_listener) =
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
    async fn activate_conditional_trigger_is_true_filters_non_zero() {
        let (message_provider, parent_trigger, callback_listener) =
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
    async fn activate_conditional_trigger_is_false_filters_zero() {
        let (message_provider, parent_trigger, callback_listener) =
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
    async fn activate_conditional_trigger_is_true_filters_none() {
        let (message_provider, parent_trigger, callback_listener) =
            setup_active_conditional_trigger(ConditionType::IsTrue).await;

        let msg = Arc::new(Int32Value { value: 1, ..Default::default() });

        // Fire parent trigger three times: [None, 1, 1].
        // This is to verify it filters out None and lets repeated non-zeroes go through.
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
    async fn activate_conditional_trigger_is_false_filters_none() {
        let (message_provider, parent_trigger, callback_listener) =
            setup_active_conditional_trigger(ConditionType::IsFalse).await;

        let msg = Arc::new(Int32Value { value: 0, ..Default::default() });

        // Fire parent trigger three times: [None, 0, 0].
        // This is to verify it filters out None and lets repeated zero go through.
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
    async fn activate_conditional_trigger_handles_async_expression_result() {
        let (message_provider, parent_trigger, callback_listener) =
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
    async fn deactivate_conditional_trigger_unsubscribes_from_parents() {
        let mut parent_trigger = MockTrigger::new_for_subscriber(
            PARENT_TRIGGER_NAME.to_owned(),
            TriggerSubscriber::ConditionalTrigger { name: TRIGGER_NAME.to_owned() },
        );
        let callback_listener = Arc::new(MockCallbackListener::new());
        let callback_listener_clone = Arc::clone(&callback_listener);
        parent_trigger
            .set_unsubscribe_callback(move |_| callback_listener_clone.on_callback_fired(()));
        let (trigger, _, _) = create_valid_conditional_trigger(
            ConditionType::AllChanges,
            Some(Arc::new(parent_trigger)),
        );

        // Activate the trigger.
        Arc::clone(&trigger).activate(Arc::new(AsyncMutex::new(HashMap::new()))).await;

        // Deactivate the trigger.
        trigger.deactivate().await;

        // Verify that the trigger was unsubscribed from once.
        assert_eq!(callback_listener.get_fired_count(), 1);
    }
}
