// Copyright 2023 Google LLC. All Rights Reserved.

use anyhow::anyhow;
use async_trait::async_trait;
use log::{error, trace};
use protobuf::MessageDyn;
use sdvtelemetry_proto_rust::{
    expression::combination_node::{Operator, RelationalOperator},
    metrics_configuration::conditional_trigger::ConditionType,
};
use std::{
    collections::HashMap,
    fmt,
    marker::{Send, Sync},
    sync::{Arc, Mutex, Weak},
};
use tokio::{sync::Mutex as AsyncMutex, time::Duration};

use crate::expr::{ast::Node, node_value::NodeValue};
use crate::metrics_config_orchestrator::subscription_manager::{
    Activatable, CallbackFn, SubscriptionManager, SubscriptionManagerImpl,
};
use crate::metrics_config_orchestrator::Publisher;

#[async_trait]
pub(crate) trait Trigger: Send + Sync {
    /// Returns the name of the trigger.
    fn get_name(&self) -> &str;

    /// Subscribe to trigger fire events.
    ///
    /// First subscription activates the trigger.
    /// The callback mustn't block because it's executed on the trigger thread.
    async fn subscribe(
        self: Arc<Self>,
        subscriber: &TriggerSubscriber,
        on_fired_callback: CallbackFn,
    );

    /// Unsubscribe from trigger fire events.
    ///
    /// Last unsubscription deactivates the trigger.
    /// After asynchronous function is complete, the callback is guaranteed not to be called.
    async fn unsubscribe(&self, subscriber: &TriggerSubscriber);
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub(crate) enum TriggerSubscriber {
    AggregationPublisher { name: String },
    ConditionalTrigger { name: String },
    MetricsConfigStart,
    MetricsConfigEnd,
    MetricsConfigFinish,
    MetricsReportGenerator { name: String },
}

pub(crate) struct TriggerImpl {
    pub(super) name: String,
    pub(super) trigger_type: TriggerType,

    // Manages trigger subscriptions.
    // The first subscription to TriggerImpl activates the trigger, passing a reference
    // to the callback map. The last unsubscription deactivates the trigger.
    subscription_manager: Box<dyn SubscriptionManager<TriggerSubscriber>>,
}

pub(crate) enum TriggerType {
    Data {
        publisher: Arc<dyn Publisher>,
    },
    Periodic {
        interval_ms: u64,
    },
    Conditional {
        condition_type: ConditionType,
        maybe_previous_value: Mutex<Option<NodeValue>>,
        selector_expression: Arc<Node>,
        subscriber: TriggerSubscriber,
        triggers: Vec<Arc<dyn Trigger>>,
    },
}

impl fmt::Display for TriggerSubscriber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TriggerSubscriber::AggregationPublisher { name } => {
                write!(f, "Aggregation Publisher {}", name)
            }
            TriggerSubscriber::ConditionalTrigger { name } => {
                write!(f, "Periodic Trigger {}", name)
            }
            TriggerSubscriber::MetricsConfigStart => write!(f, "Metrics Config Start"),
            TriggerSubscriber::MetricsConfigEnd => write!(f, "Metrics Config End"),
            TriggerSubscriber::MetricsConfigFinish => write!(f, "Metrics Config Finish"),
            TriggerSubscriber::MetricsReportGenerator { name } => {
                write!(f, "Metrics Report Generator {}", name)
            }
        }
    }
}

#[async_trait]
impl Trigger for TriggerImpl {
    fn get_name(&self) -> &str {
        &self.name
    }

    async fn subscribe(
        self: Arc<Self>,
        subscriber: &TriggerSubscriber,
        on_fired_callback: CallbackFn,
    ) {
        self.subscription_manager.subscribe(subscriber.clone(), on_fired_callback).await;
    }

    async fn unsubscribe(&self, subscriber: &TriggerSubscriber) {
        self.subscription_manager.unsubscribe(subscriber).await;
    }
}

#[async_trait]
impl Activatable<TriggerSubscriber> for TriggerImpl {
    /// Performs activation operations specific to each trigger type.
    async fn activate(
        self: Arc<Self>,
        callback_map: Arc<AsyncMutex<HashMap<TriggerSubscriber, CallbackFn>>>,
    ) {
        match &self.trigger_type {
            TriggerType::Data { publisher } => {
                let trigger_name_clone = self.name.clone();
                publisher
                    .subscribe(
                        &self.name,
                        Box::new(move |message| {
                            Self::on_data_trigger_publisher_new_message(
                                &trigger_name_clone,
                                &callback_map,
                                message,
                            )
                        }),
                    )
                    .await;
            }
            TriggerType::Periodic { interval_ms } => {
                let trigger_name_clone = self.name.clone();
                let interval_ms = *interval_ms;
                tokio::spawn(async move {
                    Self::call_periodically(&trigger_name_clone, interval_ms, callback_map).await;
                });
            }
            TriggerType::Conditional { condition_type, subscriber, triggers, .. } => {
                let condition_checker: for<'a, 'b> fn(
                    &'a NodeValue,
                    &'b Mutex<Option<NodeValue>>,
                ) -> anyhow::Result<bool> = match condition_type {
                    ConditionType::CHANGE_TYPE_UNSPECIFIED => {
                        panic!("Change type for trigger {} is not specified", self.name)
                    }
                    ConditionType::RISING_EDGE => |current_value, previous_value_holder| {
                        Self::compare_with_previous(
                            current_value,
                            previous_value_holder,
                            &Operator::RelationalOperator(RelationalOperator::GT.into()),
                        )
                    },
                    ConditionType::FALLING_EDGE => |current_value, previous_value_holder| {
                        Self::compare_with_previous(
                            current_value,
                            previous_value_holder,
                            &Operator::RelationalOperator(RelationalOperator::LT.into()),
                        )
                    },
                    ConditionType::ALL_CHANGES => |current_value, previous_value_holder| {
                        Self::compare_with_previous(
                            current_value,
                            previous_value_holder,
                            &Operator::RelationalOperator(RelationalOperator::NOT_EQ.into()),
                        )
                    },
                    ConditionType::IS_TRUE => |current_value, _| Self::is_true(current_value),
                    ConditionType::IS_FALSE => |current_value, _| Self::is_false(current_value),
                };
                for trigger in triggers {
                    let self_clone = self.clone();
                    let callback_map_clone = callback_map.clone();
                    trigger
                        .clone()
                        .subscribe(
                            subscriber,
                            Box::new(move || {
                                self_clone.on_conditional_trigger_parent_trigger_fired(
                                    &callback_map_clone,
                                    condition_checker,
                                )
                            }),
                        )
                        .await;
                }
            }
        };
    }

    /// Performs deactivation operations specific to each trigger type.
    async fn deactivate(&self) {
        match &self.trigger_type {
            TriggerType::Data { publisher } => publisher.unsubscribe(&self.name).await,
            TriggerType::Conditional { subscriber, triggers, .. } => {
                for trigger in triggers {
                    trigger.unsubscribe(subscriber).await;
                }
            }
            _ => {}
        }
    }
}

impl TriggerImpl {
    /// Creates a new instance of trigger of the provided trigger type.
    pub(super) fn new(name: String, trigger_type: TriggerType) -> Arc<Self> {
        Arc::new_cyclic(|me: &Weak<TriggerImpl>| TriggerImpl {
            name,
            trigger_type,
            subscription_manager: Box::new(SubscriptionManagerImpl::new(me.clone())),
        })
    }

    /// Creates a new instance of conditional trigger.
    pub(super) fn new_conditional_trigger(
        name: String,
        condition_type: ConditionType,
        triggers: Vec<Arc<dyn Trigger>>,
        selector_expression: Arc<Node>,
    ) -> Arc<Self> {
        let subscriber = TriggerSubscriber::ConditionalTrigger { name: name.clone() };
        Self::new(
            name,
            TriggerType::Conditional {
                condition_type,
                maybe_previous_value: None.into(),
                triggers,
                selector_expression,
                subscriber,
            },
        )
    }

    /// Periodically invokes callbacks until the weak reference is no longer valid.
    async fn call_periodically(
        trigger_name: &str,
        interval_ms: u64,
        callback_map: Arc<AsyncMutex<HashMap<TriggerSubscriber, CallbackFn>>>,
    ) {
        let mut timer = tokio::time::interval(Duration::from_millis(interval_ms));

        // Trigger first tick that returns immediately.
        timer.tick().await;

        loop {
            // Verify that the map hasn't been cleared.
            if callback_map.lock().await.is_empty() {
                return;
            }

            // Sleep until next interval tick.
            timer.tick().await;

            // Fire trigger.
            Self::fire_trigger(trigger_name, &callback_map).await;
        }
    }

    /// Fires the trigger by invoking provided callback.
    /// The operation is performed after asynchronously awaiting on the mutex.
    async fn fire_trigger(
        trigger_name: &str,
        callback_map: &Arc<AsyncMutex<HashMap<TriggerSubscriber, CallbackFn>>>,
    ) {
        // Retrieve callbacks.
        let mut callbacks = callback_map.lock().await;

        // Verify that the map hasn't been cleared.
        if callbacks.is_empty() {
            return;
        }

        trace!("Trigger {} is firing", trigger_name);

        // Synchronously invoke callbacks.
        // The loop needs to wait before they finish to release the mutex lock when they're
        // done because they're allowed to mutate themselves.
        for callback in callbacks.values_mut() {
            callback();
        }
    }

    /// Compares current value with the previous value using provided operator, stores
    /// current value in the new previous value holder and returns comparison result.
    fn compare_with_previous(
        current_value: &NodeValue,
        previous_value_holder: &Mutex<Option<NodeValue>>,
        operator: &Operator,
    ) -> anyhow::Result<bool> {
        // Extract previous value and save current value.
        let maybe_previous_value =
            previous_value_holder.lock().unwrap().replace(current_value.clone());

        // Perform comparison or return false if there's no previous value.
        if let Some(previous_value) = maybe_previous_value {
            let result = current_value.operate(operator, || Ok(previous_value)).unwrap();
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

    /// A callback to handle a new message published by the data trigger's associated publisher.
    fn on_data_trigger_publisher_new_message(
        trigger_name: &str,
        callback_map: &Arc<AsyncMutex<HashMap<TriggerSubscriber, CallbackFn>>>,
        _message: &dyn MessageDyn,
    ) {
        let trigger_name_clone = trigger_name.to_owned();
        let callback_map_clone = callback_map.clone();
        tokio::spawn(async move {
            Self::fire_trigger(&trigger_name_clone, &callback_map_clone).await;
        });
    }

    /// A callback to handle a fire event from conditional trigger's associated parent trigger.
    /// Passes the result of expression evaluation and the previous value holder to
    /// condition_checker and fires the trigger if it returns true.
    fn on_conditional_trigger_parent_trigger_fired(
        self: &Arc<Self>,
        callback_map: &Arc<AsyncMutex<HashMap<TriggerSubscriber, CallbackFn>>>,
        condition_checker: impl FnOnce(
            &NodeValue,
            &Mutex<Option<NodeValue>>,
        ) -> Result<bool, anyhow::Error>,
    ) {
        let TriggerType::Conditional { maybe_previous_value, selector_expression, .. } =
            &self.trigger_type
        else {
            panic!("Unexpected trigger type");
        };

        let node_value = match selector_expression.evaluate() {
            Ok(value) => value,
            Err(err) => {
                error!(
                    "Error when performing expression evaluation for trigger {}: {}",
                    self.name, err
                );
                return;
            }
        };

        let check_succeeded =
            condition_checker(&node_value, maybe_previous_value).unwrap_or_else(|err| {
                panic!("Failed to perform comparison for trigger {}: {:?}", self.name, err)
            });
        if check_succeeded {
            let trigger_name_clone = self.name.clone();
            let callback_map_clone = callback_map.clone();
            tokio::spawn(async move {
                Self::fire_trigger(&trigger_name_clone, &callback_map_clone).await;
            });
        }
    }
}

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
mod tests {
    use protobuf::well_known_types::wrappers::{BoolValue, Int32Value};
    use sdv_telemetry_utils::test_utils::with_timeout;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tokio::sync::Notify;

    use super::*;
    use crate::expr::mock::MockMessageProvider;
    use crate::metrics_config_orchestrator::{
        publisher::mock::MockPublisher, trigger::mock::MockTrigger,
    };

    const INTERVAL_MS: u64 = 10;
    const PARENT_TRIGGER_NAME: &str = "parent_trigger";
    const PUBLISHER_NAME: &str = "publisher";
    const TRIGGER_NAME: &str = "trigger";

    struct CallbackListener {
        callback_notify: Notify,
        fired_count: AtomicU32,
    }

    impl CallbackListener {
        fn new() -> CallbackListener {
            CallbackListener { callback_notify: Notify::new(), fired_count: 0.into() }
        }

        fn get_fired_count(&self) -> u32 {
            self.fired_count.load(Ordering::Acquire)
        }

        // Creates a map with a callback that is handled by this listener instance.
        fn create_callback_map(
            self: &Arc<Self>,
        ) -> Arc<AsyncMutex<HashMap<TriggerSubscriber, CallbackFn>>> {
            let self_clone = self.clone();
            let callback: CallbackFn = Box::new(move || self_clone.on_callback_fired());
            Arc::new(AsyncMutex::new(HashMap::from([(
                TriggerSubscriber::MetricsConfigStart,
                callback,
            )])))
        }

        /// Increases fired count and triggers the notify.
        fn on_callback_fired(&self) {
            self.fired_count.fetch_add(1, Ordering::SeqCst);
            self.callback_notify.notify_one();
        }
    }

    #[derive(Clone)]
    struct MockSubscriptionManager {
        key: Arc<Mutex<Option<TriggerSubscriber>>>,
        callback: Arc<Mutex<Option<CallbackFn>>>,
    }

    #[async_trait]
    impl SubscriptionManager<TriggerSubscriber> for MockSubscriptionManager {
        async fn subscribe(&self, key: TriggerSubscriber, callback: CallbackFn) {
            self.key.lock().unwrap().replace(key);
            self.callback.lock().unwrap().replace(callback);
        }

        async fn unsubscribe(&self, key: &TriggerSubscriber) {
            self.key.lock().unwrap().replace(key.to_owned());
        }
    }

    impl MockSubscriptionManager {
        fn new() -> MockSubscriptionManager {
            MockSubscriptionManager {
                key: Arc::new(Mutex::new(None)),
                callback: Arc::new(Mutex::new(None)),
            }
        }
    }

    fn create_empty_callback_map() -> Arc<AsyncMutex<HashMap<TriggerSubscriber, CallbackFn>>> {
        Arc::new(AsyncMutex::new(HashMap::new()))
    }

    fn create_empty_fn_box() -> CallbackFn {
        Box::new(|| {})
    }

    fn create_field_node(field_name: String, message_provider: Arc<MockMessageProvider>) -> Node {
        Node::FieldLeaf {
            publisher_name: PUBLISHER_NAME.to_owned(),
            fields: vec![field_name],
            message_provider,
        }
    }

    fn create_trigger(trigger_type: TriggerType) -> (Arc<TriggerImpl>, MockSubscriptionManager) {
        let trigger = TriggerImpl::new(TRIGGER_NAME.to_owned(), trigger_type);
        let mut trigger = Arc::into_inner(trigger).unwrap();
        let subscription_manager = MockSubscriptionManager::new();
        trigger.subscription_manager = Box::new(subscription_manager.clone());
        (Arc::new(trigger), subscription_manager)
    }

    fn create_valid_data_trigger(publisher: Arc<MockPublisher>) -> Arc<TriggerImpl> {
        let (trigger, _) = create_trigger(TriggerType::Data { publisher });
        trigger
    }

    fn create_valid_periodic_trigger() -> (Arc<TriggerImpl>, MockSubscriptionManager) {
        create_trigger(TriggerType::Periodic { interval_ms: INTERVAL_MS })
    }

    fn create_valid_conditional_trigger(
        condition_type: ConditionType,
        parent_trigger: Option<Arc<MockTrigger>>,
    ) -> (Arc<TriggerImpl>, Arc<MockMessageProvider>, Arc<MockTrigger>) {
        let parent_trigger = parent_trigger.unwrap_or_else(|| {
            Arc::new(MockTrigger::new_for_subscriber(
                PARENT_TRIGGER_NAME.to_owned(),
                TriggerSubscriber::ConditionalTrigger { name: TRIGGER_NAME.to_owned() },
            ))
        });
        let message_provider = Arc::new(MockMessageProvider::new());
        let trigger = TriggerImpl::new_conditional_trigger(
            TRIGGER_NAME.to_owned(),
            condition_type,
            vec![parent_trigger.clone()],
            Arc::new(create_field_node("value".to_owned(), message_provider.clone())),
        );

        (trigger, message_provider, parent_trigger)
    }

    fn create_valid_mock_publisher() -> MockPublisher {
        let mut publisher = MockPublisher::new(PUBLISHER_NAME.to_owned());
        publisher.expected_trigger_names.insert(TRIGGER_NAME.to_owned());
        publisher
    }

    async fn setup_active_conditional_trigger(
        condition_type: ConditionType,
    ) -> (Arc<MockMessageProvider>, Arc<MockTrigger>, Arc<CallbackListener>) {
        let (trigger, message_provider, parent_trigger) =
            create_valid_conditional_trigger(condition_type, None);

        let callback_listener = Arc::new(CallbackListener::new());
        trigger.activate(callback_listener.create_callback_map()).await;

        (message_provider, parent_trigger, callback_listener)
    }

    #[tokio::test]
    async fn subscribe_calls_subscription_manager() {
        let (trigger, subscription_manager) = create_valid_periodic_trigger();

        trigger
            .clone()
            .subscribe(&TriggerSubscriber::MetricsConfigStart, create_empty_fn_box())
            .await;

        // Verify that the key and the callback are set.
        let subscription_key = subscription_manager.key.lock().unwrap().take().unwrap();
        assert!(
            subscription_key == TriggerSubscriber::MetricsConfigStart,
            "{} != {}",
            subscription_key,
            TriggerSubscriber::MetricsConfigStart
        );
        assert!(subscription_manager.callback.lock().unwrap().is_some());
    }

    #[tokio::test]
    async fn unsubscribe_calls_subscription_manager() {
        let (trigger, subscription_manager) = create_valid_periodic_trigger();

        trigger.unsubscribe(&TriggerSubscriber::MetricsConfigStart).await;

        // Verify that the key is set and the callback is empty.
        let subscription_key = subscription_manager.key.lock().unwrap().take().unwrap();
        assert!(
            subscription_key == TriggerSubscriber::MetricsConfigStart,
            "{} != {}",
            subscription_key,
            TriggerSubscriber::MetricsConfigStart
        );
        assert!(subscription_manager.callback.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn activate_data_trigger_subscribes_to_publisher() {
        let publisher = Arc::new(create_valid_mock_publisher());
        let trigger = create_valid_data_trigger(publisher.clone());
        let callback_listener = Arc::new(CallbackListener::new());

        // Activate the trigger.
        trigger.activate(callback_listener.create_callback_map()).await;

        // Fire publisher message. This should notify subscribed trigger.
        publisher.fire_new_message(&Int32Value::new()).await;

        // Wait for notification.
        with_timeout(callback_listener.callback_notify.notified()).await;

        // Verify that the trigger fired once.
        assert_eq!(callback_listener.get_fired_count(), 1);
    }

    #[tokio::test]
    async fn activate_periodic_trigger_fires_twice() {
        let (trigger, _) = create_valid_periodic_trigger();
        let callback_listener = Arc::new(CallbackListener::new());

        // Subscribe to trigger.
        trigger.clone().activate(callback_listener.create_callback_map()).await;

        // Wait for trigger to fire twice.
        with_timeout(callback_listener.callback_notify.notified()).await;
        with_timeout(callback_listener.callback_notify.notified()).await;

        // Unsubscribe from trigger.
        trigger.deactivate().await;

        // Verify that the trigger fired twice.
        assert_eq!(callback_listener.get_fired_count(), 2);
    }

    #[tokio::test]
    async fn activate_periodic_trigger_deactivate_from_callback_success() {
        let (trigger, _) = create_valid_periodic_trigger();
        let mut trigger_clone = Some(trigger.clone());
        let callback: CallbackFn = Box::new(move || {
            let trigger_clone = trigger_clone.take().unwrap();
            tokio::spawn(async move {
                trigger_clone.deactivate().await;
            });
        });
        let callback_map = Arc::new(AsyncMutex::new(HashMap::from([(
            TriggerSubscriber::MetricsConfigStart,
            callback,
        )])));

        // Verify that there's no deadlock.
        trigger.activate(callback_map).await;
    }

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
        let trigger = TriggerImpl::new_conditional_trigger(
            TRIGGER_NAME.to_owned(),
            ConditionType::ALL_CHANGES,
            vec![parent_trigger1.clone(), parent_trigger2.clone()],
            Arc::new(create_field_node("value".to_owned(), message_provider.clone())),
        );
        let callback_listener = Arc::new(CallbackListener::new());

        trigger.activate(callback_listener.create_callback_map()).await;

        let mut msg1 = Box::new(Int32Value::new());
        msg1.value = 1;
        let mut msg2 = Box::new(Int32Value::new());
        msg2.value = 2;

        // Set the first message and fire the first parent trigger.
        message_provider.set_next_result(Ok(Some(msg1)));
        parent_trigger1.fire().await;

        // Set a different message and fire the second parent trigger.
        message_provider.set_next_result(Ok(Some(msg2)));
        parent_trigger2.fire().await;

        // Wait for trigger to fire once.
        with_timeout(callback_listener.callback_notify.notified()).await;

        // Verify that the trigger fired once.
        assert_eq!(callback_listener.get_fired_count(), 1);
    }

    #[tokio::test]
    async fn activate_conditional_trigger_filters_all_changes() {
        let (message_provider, parent_trigger, callback_listener) =
            setup_active_conditional_trigger(ConditionType::ALL_CHANGES).await;

        let mut msg1 = Box::new(Int32Value::new());
        msg1.value = 1;
        let mut msg2 = Box::new(Int32Value::new());
        msg2.value = 2;

        // Fire parent trigger three times with the message changing once.
        message_provider.set_next_result(Ok(Some(msg1)));
        parent_trigger.fire().await;

        message_provider.set_next_result(Ok(Some(msg2.clone())));
        parent_trigger.fire().await;

        message_provider.set_next_result(Ok(Some(msg2)));
        parent_trigger.fire().await;

        // Wait for trigger to fire once.
        with_timeout(callback_listener.callback_notify.notified()).await;

        // Verify that the trigger fired once.
        assert_eq!(callback_listener.get_fired_count(), 1);
    }

    #[tokio::test]
    async fn activate_conditional_trigger_filters_falling_edge() {
        let (message_provider, parent_trigger, callback_listener) =
            setup_active_conditional_trigger(ConditionType::FALLING_EDGE).await;

        let mut msg1 = Box::new(Int32Value::new());
        msg1.value = 1;
        let mut msg2 = Box::new(Int32Value::new());
        msg2.value = 2;

        // Fire parent trigger three times.
        // The first change is the rising edge and the second change is the falling edge.
        message_provider.set_next_result(Ok(Some(msg1.clone())));
        parent_trigger.fire().await;

        message_provider.set_next_result(Ok(Some(msg2)));
        parent_trigger.fire().await;

        message_provider.set_next_result(Ok(Some(msg1)));
        parent_trigger.fire().await;

        // Wait for trigger to fire once.
        with_timeout(callback_listener.callback_notify.notified()).await;

        assert_eq!(callback_listener.get_fired_count(), 1);
    }

    #[tokio::test]
    async fn activate_conditional_trigger_filters_rising_edge() {
        let (message_provider, parent_trigger, callback_listener) =
            setup_active_conditional_trigger(ConditionType::RISING_EDGE).await;

        let mut msg1 = Box::new(Int32Value::new());
        msg1.value = 1;
        let mut msg2 = Box::new(Int32Value::new());
        msg2.value = 2;

        // Fire parent trigger three times.
        // The first change is the falling edge and the second change is the rising edge.
        message_provider.set_next_result(Ok(Some(msg2.clone())));
        parent_trigger.fire().await;

        message_provider.set_next_result(Ok(Some(msg1)));
        parent_trigger.fire().await;

        message_provider.set_next_result(Ok(Some(msg2)));
        parent_trigger.fire().await;

        // Wait for trigger to fire once.
        with_timeout(callback_listener.callback_notify.notified()).await;

        assert_eq!(callback_listener.get_fired_count(), 1);
    }

    #[tokio::test]
    async fn activate_conditional_trigger_is_true_filters_true() {
        let (message_provider, parent_trigger, callback_listener) =
            setup_active_conditional_trigger(ConditionType::IS_TRUE).await;

        let mut false_msg = Box::new(BoolValue::new());
        false_msg.value = false;
        let mut true_msg = Box::new(BoolValue::new());
        true_msg.value = true;

        // Fire parent trigger three times: [false, true, true].
        // This is to verify it filters out false and lets repeated true go through.
        message_provider.set_next_result(Ok(Some(false_msg)));
        parent_trigger.fire().await;

        message_provider.set_next_result(Ok(Some(true_msg.clone())));
        parent_trigger.fire().await;

        message_provider.set_next_result(Ok(Some(true_msg)));
        parent_trigger.fire().await;

        // Wait for trigger to fire twice.
        with_timeout(callback_listener.callback_notify.notified()).await;
        with_timeout(callback_listener.callback_notify.notified()).await;

        assert_eq!(callback_listener.get_fired_count(), 2);
    }

    #[tokio::test]
    async fn activate_conditional_trigger_is_false_filters_false() {
        let (message_provider, parent_trigger, callback_listener) =
            setup_active_conditional_trigger(ConditionType::IS_FALSE).await;

        let mut false_msg = Box::new(BoolValue::new());
        false_msg.value = false;
        let mut true_msg = Box::new(BoolValue::new());
        true_msg.value = true;

        // Fire parent trigger three times: [true, false, false].
        // This is to verify it filters out false and lets repeated true go through.
        message_provider.set_next_result(Ok(Some(true_msg)));
        parent_trigger.fire().await;

        message_provider.set_next_result(Ok(Some(false_msg.clone())));
        parent_trigger.fire().await;

        message_provider.set_next_result(Ok(Some(false_msg)));
        parent_trigger.fire().await;

        // Wait for trigger to fire twice.
        with_timeout(callback_listener.callback_notify.notified()).await;
        with_timeout(callback_listener.callback_notify.notified()).await;

        assert_eq!(callback_listener.get_fired_count(), 2);
    }

    #[tokio::test]
    async fn activate_conditional_trigger_is_true_filters_non_zero() {
        let (message_provider, parent_trigger, callback_listener) =
            setup_active_conditional_trigger(ConditionType::IS_TRUE).await;

        let mut msg0 = Box::new(Int32Value::new());
        msg0.value = 0;
        let mut msg1 = Box::new(Int32Value::new());
        msg1.value = 1;

        // Fire parent trigger three times: [0, 1, 1].
        // This is to verify it filters out zeo and lets repeated non-zeroes go through.
        message_provider.set_next_result(Ok(Some(msg0)));
        parent_trigger.fire().await;

        message_provider.set_next_result(Ok(Some(msg1.clone())));
        parent_trigger.fire().await;

        message_provider.set_next_result(Ok(Some(msg1)));
        parent_trigger.fire().await;

        // Wait for trigger to fire twice.
        with_timeout(callback_listener.callback_notify.notified()).await;
        with_timeout(callback_listener.callback_notify.notified()).await;

        assert_eq!(callback_listener.get_fired_count(), 2);
    }

    #[tokio::test]
    async fn activate_conditional_trigger_is_false_filters_zero() {
        let (message_provider, parent_trigger, callback_listener) =
            setup_active_conditional_trigger(ConditionType::IS_FALSE).await;

        let mut msg0 = Box::new(Int32Value::new());
        msg0.value = 0;
        let mut msg1 = Box::new(Int32Value::new());
        msg1.value = 1;

        // Fire parent trigger three times: [1, 0, 0].
        // This is to verify it filters out non-zero and lets repeated zero go through.
        message_provider.set_next_result(Ok(Some(msg1)));
        parent_trigger.fire().await;

        message_provider.set_next_result(Ok(Some(msg0.clone())));
        parent_trigger.fire().await;

        message_provider.set_next_result(Ok(Some(msg0)));
        parent_trigger.fire().await;

        // Wait for trigger to fire twice.
        with_timeout(callback_listener.callback_notify.notified()).await;
        with_timeout(callback_listener.callback_notify.notified()).await;

        assert_eq!(callback_listener.get_fired_count(), 2);
    }

    #[tokio::test]
    async fn activate_conditional_trigger_is_true_filters_none() {
        let (message_provider, parent_trigger, callback_listener) =
            setup_active_conditional_trigger(ConditionType::IS_TRUE).await;

        let mut msg = Box::new(Int32Value::new());
        msg.value = 1;

        // Fire parent trigger three times: [None, 1, 1].
        // This is to verify it filters out None and lets repeated non-zeroes go through.
        parent_trigger.fire().await;

        message_provider.set_next_result(Ok(Some(msg.clone())));
        parent_trigger.fire().await;

        message_provider.set_next_result(Ok(Some(msg)));
        parent_trigger.fire().await;

        // Wait for trigger to fire twice.
        with_timeout(callback_listener.callback_notify.notified()).await;
        with_timeout(callback_listener.callback_notify.notified()).await;

        assert_eq!(callback_listener.get_fired_count(), 2);
    }

    #[tokio::test]
    async fn activate_conditional_trigger_is_false_filters_none() {
        let (message_provider, parent_trigger, callback_listener) =
            setup_active_conditional_trigger(ConditionType::IS_FALSE).await;

        let mut msg = Box::new(Int32Value::new());
        msg.value = 0;

        // Fire parent trigger three times: [None, 0, 0].
        // This is to verify it filters out None and lets repeated zero go through.
        parent_trigger.fire().await;

        message_provider.set_next_result(Ok(Some(msg.clone())));
        parent_trigger.fire().await;

        message_provider.set_next_result(Ok(Some(msg)));
        parent_trigger.fire().await;

        // Wait for trigger to fire twice.
        with_timeout(callback_listener.callback_notify.notified()).await;
        with_timeout(callback_listener.callback_notify.notified()).await;

        assert_eq!(callback_listener.get_fired_count(), 2);
    }

    #[tokio::test]
    async fn deactivate_data_trigger_unsubscribes_from_publisher() {
        let callback_listener = Arc::new(CallbackListener::new());
        let callback_listener_clone = callback_listener.clone();
        let mut publisher = create_valid_mock_publisher();
        publisher.set_unsubscribe_callback(move || callback_listener_clone.on_callback_fired());
        let publisher = Arc::new(publisher);
        let trigger = create_valid_data_trigger(publisher.clone());

        // Activate the trigger.
        trigger.clone().activate(create_empty_callback_map()).await;

        // Deactivate the trigger.
        trigger.deactivate().await;

        // Verify that publisher was unsubscribed from once.
        assert_eq!(callback_listener.get_fired_count(), 1);
    }

    #[tokio::test]
    async fn deactivate_conditional_trigger_unsubscribes_from_parents() {
        let mut parent_trigger = MockTrigger::new_for_subscriber(
            PARENT_TRIGGER_NAME.to_owned(),
            TriggerSubscriber::ConditionalTrigger { name: TRIGGER_NAME.to_owned() },
        );
        let callback_listener = Arc::new(CallbackListener::new());
        let callback_listener_clone = callback_listener.clone();
        parent_trigger
            .set_unsubscribe_callback(move |_| callback_listener_clone.on_callback_fired());
        let (trigger, _, _) = create_valid_conditional_trigger(
            ConditionType::ALL_CHANGES,
            Some(Arc::new(parent_trigger)),
        );

        // Activate the trigger.
        trigger.clone().activate(create_empty_callback_map()).await;

        // Deactivate the trigger.
        trigger.deactivate().await;

        // Verify that the trigger was unsubscribed from once.
        assert_eq!(callback_listener.get_fired_count(), 1);
    }
}
