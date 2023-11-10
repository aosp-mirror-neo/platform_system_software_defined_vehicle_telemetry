// Copyright 2023 Google LLC. All Rights Reserved.

use anyhow::anyhow;
use async_trait::async_trait;
use log::error;
use protobuf::MessageDyn;
use sdvtelemetry_proto_rust::{
    expression::combination_node::{Operator, RelationalOperator},
    metrics_configuration::conditional_trigger::ConditionType,
};
use std::{
    collections::HashMap,
    fmt,
    marker::{Send, Sync},
    sync::{Arc, Mutex},
};
use thiserror::Error;
use tokio::{sync::Mutex as AsyncMutex, time::Duration};

use crate::metrics_config_orchestrator::{
    publisher::{PublisherSubscribeError, PublisherUnsubscribeError},
    Publisher,
};
use crate::{
    collection_extension::HashMapExtension,
    expr::{ast::Node, node_value::NodeValue},
};

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
        on_fired_callback: Box<dyn FnMut() + Send + Sync>,
    ) -> Result<(), TriggerSubscribeError>;

    /// Unsubscribe from trigger fire events.
    ///
    /// Last unsubscription deactivates the trigger.
    /// After asynchronous function is complete, the callback is guaranteed not to be called.
    async fn unsubscribe(
        &self,
        subscriber: &TriggerSubscriber,
    ) -> Result<(), TriggerUnsubscribeError>;
}

#[derive(Error, Debug)]
pub enum TriggerSubscribeError {
    #[error("Subscriber {subscriber_name} is already subscribed to the trigger. Duplicate subscriptions are not allowed.")]
    DuplicateSubscription { subscriber_name: String },
    #[error("Error when subscribing to the underlying publisher: {error}")]
    PublisherSubscribeError {
        #[from]
        error: PublisherSubscribeError,
    },
}

#[derive(Error, Debug)]
pub enum TriggerUnsubscribeError {
    #[error("Subscriber {subscriber_name} wasn't subscribed to the trigger")]
    NotSubscribed { subscriber_name: String },
    #[error("Error when unsubscribing from the underlying publisher: {error}")]
    PublisherUnsubscribeError {
        #[from]
        error: PublisherUnsubscribeError,
    },
    #[error("Unable to fully unsubscribe from underlying triggers: {errors:?}")]
    ParentTriggerUnsubscribeErrors { errors: Vec<(String, TriggerUnsubscribeError)> },
}

#[allow(dead_code)]
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

    // The map of functions that need to be called when the trigger fires.
    // The first subscription to TriggerImpl creates a new map and activates underlying trigger
    // type, passing a reference to the map. Last unsubscription removes the map and deactivates
    // underlying trigger type.
    callback_map:
        AsyncMutex<Arc<AsyncMutex<HashMap<TriggerSubscriber, Box<dyn FnMut() + Send + Sync>>>>>,
}

#[allow(dead_code)]
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
        on_fired_callback: Box<dyn FnMut() + Send + Sync>,
    ) -> Result<(), TriggerSubscribeError> {
        let maybe_callbacks_container = {
            let callbacks_container = self.callback_map.lock().await;
            let mut callbacks = callbacks_container.lock().await;
            if callbacks.try_insert_value(subscriber.clone(), on_fired_callback).is_err() {
                return Err(TriggerSubscribeError::DuplicateSubscription {
                    subscriber_name: subscriber.to_string(),
                });
            }

            // Return a reference to the container if this is the first subscription.
            if callbacks.len() == 1 {
                Some(callbacks_container.clone())
            } else {
                None
            }
        };

        if let Some(callbacks_container) = maybe_callbacks_container {
            // If a container was returned, this is the first subscription and underlying
            // trigger type needs to be activated.
            self.activate(callbacks_container).await?;
        }

        Ok(())
    }

    async fn unsubscribe(
        &self,
        subscriber: &TriggerSubscriber,
    ) -> Result<(), TriggerUnsubscribeError> {
        let mut callbacks_container = self.callback_map.lock().await;
        let is_last_unsubscription = {
            let mut callbacks = callbacks_container.lock().await;
            if callbacks.remove(subscriber).is_none() {
                return Err(TriggerUnsubscribeError::NotSubscribed {
                    subscriber_name: subscriber.to_string(),
                });
            }

            callbacks.is_empty()
        };

        if is_last_unsubscription {
            // Create a new map to ensure no new items will be added to the old one.
            *callbacks_container = Arc::new(AsyncMutex::new(HashMap::new()));

            // Deactivate the trigger.
            self.deactivate().await?;
        }

        Ok(())
    }
}

impl TriggerImpl {
    /// Creates a new instance of trigger of the provided trigger type.
    pub(super) fn new(name: String, trigger_type: TriggerType) -> Self {
        TriggerImpl {
            name,
            trigger_type,
            callback_map: AsyncMutex::new(Arc::new(AsyncMutex::new(HashMap::new()))),
        }
    }

    /// Creates a new instance of conditional trigger.
    pub(super) fn new_conditional_trigger(
        name: String,
        condition_type: ConditionType,
        triggers: Vec<Arc<dyn Trigger>>,
        selector_expression: Arc<Node>,
    ) -> Self {
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

    /// Performs activation operations specific to each trigger type.
    async fn activate(
        self: Arc<Self>,
        callbacks_container: Arc<
            AsyncMutex<HashMap<TriggerSubscriber, Box<dyn FnMut() + Send + Sync>>>,
        >,
    ) -> Result<(), TriggerSubscribeError> {
        match &self.trigger_type {
            TriggerType::Data { publisher } => {
                publisher
                    .subscribe(
                        &self.name,
                        Box::new(move |msg| {
                            Self::on_data_trigger_publisher_new_message(&callbacks_container, msg)
                        }),
                    )
                    .await?;
            }
            TriggerType::Periodic { interval_ms } => {
                tokio::spawn(Self::call_periodically(*interval_ms, callbacks_container));
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
                    ConditionType::IS_FALSE => {
                        |current_value, _| Self::is_true(current_value).map(|is_true| !is_true)
                    }
                };
                for trigger in triggers {
                    let self_clone = self.clone();
                    let callbacks_container_clone = callbacks_container.clone();
                    if let Err(err) = trigger
                        .clone()
                        .subscribe(
                            subscriber,
                            Box::new(move || {
                                self_clone.on_conditional_trigger_parent_trigger_fired(
                                    &callbacks_container_clone,
                                    condition_checker,
                                )
                            }),
                        )
                        .await
                    {
                        error!("Error when trying to activate trigger {} for conditional trigger {}: {}.", trigger.get_name(), self.name, err);
                    }
                }
            }
        };
        Ok(())
    }

    /// Performs deactivation operations specific to each trigger type.
    async fn deactivate(&self) -> Result<(), TriggerUnsubscribeError> {
        match &self.trigger_type {
            TriggerType::Data { publisher } => publisher.unsubscribe(&self.name).await?,
            TriggerType::Conditional { subscriber, triggers, .. } => {
                let mut errors = Vec::new();

                for trigger in triggers {
                    if let Err(err) = trigger.unsubscribe(subscriber).await {
                        errors.push((trigger.get_name().to_owned(), err));
                    }
                }

                if !errors.is_empty() {
                    return Err(TriggerUnsubscribeError::ParentTriggerUnsubscribeErrors { errors });
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Periodically invokes callbacks until the weak reference is no longer valid.
    async fn call_periodically(
        interval_ms: u64,
        callbacks_container: Arc<
            AsyncMutex<HashMap<TriggerSubscriber, Box<dyn FnMut() + Send + Sync>>>,
        >,
    ) {
        let mut timer = tokio::time::interval(Duration::from_millis(interval_ms));

        // Trigger first tick that returns immediately.
        timer.tick().await;

        loop {
            // Verify that the map hasn't been cleared.
            if callbacks_container.lock().await.is_empty() {
                return;
            }

            // Sleep until next interval tick.
            timer.tick().await;

            // Fire trigger.
            Self::fire_trigger(&callbacks_container).await;
        }
    }

    /// Fires the trigger by invoking provided callback.
    /// The operation is performed after asynchronously awaiting on the mutex.
    async fn fire_trigger(
        callbacks_container: &Arc<
            AsyncMutex<HashMap<TriggerSubscriber, Box<dyn FnMut() + Send + Sync>>>,
        >,
    ) {
        // Retrieve callbacks.
        let mut callbacks = callbacks_container.lock().await;

        // Verify that the map hasn't been cleared.
        if callbacks.is_empty() {
            return;
        }

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

    /// Returns true if the current value is a true boolean or a non-zero numeric value.
    fn is_true(current_value: &NodeValue) -> anyhow::Result<bool> {
        if let NodeValue::Bool(boolean) = current_value {
            Ok(*boolean)
        } else {
            Ok(current_value.is_zero().map(|is_zero| !is_zero)?)
        }
    }

    /// A callback to handle a new message published by the data trigger's associated publisher.
    fn on_data_trigger_publisher_new_message(
        callbacks_container: &Arc<
            AsyncMutex<HashMap<TriggerSubscriber, Box<dyn FnMut() + Send + Sync>>>,
        >,
        _message: &dyn MessageDyn,
    ) {
        let callbacks_container_clone = callbacks_container.clone();
        tokio::spawn(async move {
            Self::fire_trigger(&callbacks_container_clone).await;
        });
    }

    /// A callback to handle a fire event from conditional trigger's associated parent trigger.
    /// Passes the result of expression evaluation and the previous value holder to
    /// condition_checker and fires the trigger if it returns true.
    pub fn on_conditional_trigger_parent_trigger_fired(
        self: &Arc<Self>,
        callbacks_container: &Arc<
            AsyncMutex<HashMap<TriggerSubscriber, Box<dyn FnMut() + Send + Sync>>>,
        >,
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
            let callbacks_container_clone = callbacks_container.clone();
            tokio::spawn(async move {
                Self::fire_trigger(&callbacks_container_clone).await;
            });
        }
    }
}

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
mod tests {
    use protobuf::well_known_types::wrappers::{BoolValue, Int32Value};
    use std::sync::atomic::{AtomicU32, Ordering};
    use tokio::{sync::Notify, time};

    use super::*;
    use crate::expr::mock::MockMessageProvider;
    use crate::metrics_config_orchestrator::{
        publisher::mock::MockPublisher, trigger::mock::MockTrigger,
    };
    use crate::test_utils::with_timeout;

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

        /// Increases fired count and returns whether the fired limit has been reached if it's set.
        fn on_callback_fired(&self) {
            self.fired_count.fetch_add(1, Ordering::SeqCst);
            self.callback_notify.notify_one();
        }
    }

    fn empty_fn() {}

    fn create_empty_fn_box() -> Box<dyn FnMut() + Send + Sync> {
        Box::new(empty_fn)
    }

    fn create_field_node(field_name: String, message_provider: Arc<MockMessageProvider>) -> Node {
        Node::FieldLeaf {
            publisher_name: PUBLISHER_NAME.to_owned(),
            fields: vec![field_name],
            message_provider,
        }
    }

    fn create_valid_data_trigger(publisher: Arc<MockPublisher>) -> TriggerImpl {
        TriggerImpl::new(TRIGGER_NAME.to_owned(), TriggerType::Data { publisher })
    }

    fn create_valid_periodic_trigger() -> TriggerImpl {
        TriggerImpl::new(
            TRIGGER_NAME.to_owned(),
            TriggerType::Periodic { interval_ms: INTERVAL_MS },
        )
    }

    fn create_valid_mock_publisher() -> MockPublisher {
        let mut publisher = MockPublisher::new(PUBLISHER_NAME.to_owned());
        publisher.expected_trigger_names.insert(TRIGGER_NAME.to_owned());
        publisher
    }

    async fn setup_subscribed_conditional_trigger(
        condition_type: ConditionType,
    ) -> (Arc<MockMessageProvider>, Arc<MockTrigger>, Arc<CallbackListener>) {
        let parent_trigger = Arc::new(MockTrigger::new_for_subscriber(
            PARENT_TRIGGER_NAME.to_owned(),
            TriggerSubscriber::ConditionalTrigger { name: TRIGGER_NAME.to_owned() },
        ));
        let message_provider = Arc::new(MockMessageProvider::new());
        let trigger = Arc::new(TriggerImpl::new_conditional_trigger(
            TRIGGER_NAME.to_owned(),
            condition_type,
            vec![parent_trigger.clone()],
            Arc::new(create_field_node("value".to_owned(), message_provider.clone())),
        ));
        let callback_listener = Arc::new(CallbackListener::new());
        let callback_listener_clone = callback_listener.clone();
        trigger
            .subscribe(
                &TriggerSubscriber::MetricsConfigStart,
                Box::new(move || callback_listener_clone.on_callback_fired()),
            )
            .await
            .unwrap();

        (message_provider, parent_trigger, callback_listener)
    }

    #[tokio::test]
    async fn subscribe_already_subscribed_fails() {
        let trigger = Arc::new(create_valid_periodic_trigger());

        trigger
            .clone()
            .subscribe(&TriggerSubscriber::MetricsConfigStart, create_empty_fn_box())
            .await
            .unwrap();
        let err = trigger
            .subscribe(&TriggerSubscriber::MetricsConfigStart, create_empty_fn_box())
            .await
            .err()
            .unwrap();

        assert!(
            matches!(&err, TriggerSubscribeError::DuplicateSubscription { subscriber_name } if subscriber_name == &TriggerSubscriber::MetricsConfigStart.to_string()),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn subscribe_to_data_trigger_subscribe_returns_error_fails() {
        let mut publisher = create_valid_mock_publisher();
        publisher.set_subscribe_callback(|| {
            Some(PublisherSubscribeError::DuplicateSubscription {
                data_trigger_name: TRIGGER_NAME.to_owned(),
            })
        });
        let trigger = Arc::new(create_valid_data_trigger(Arc::new(publisher)));

        let err = trigger
            .subscribe(&TriggerSubscriber::MetricsConfigStart, create_empty_fn_box())
            .await
            .err()
            .unwrap();

        let TriggerSubscribeError::PublisherSubscribeError { error } = &err else {
            panic!("Unexpected error: {:?}", err);
        };
        assert!(
            matches!(error, PublisherSubscribeError::DuplicateSubscription { data_trigger_name } if data_trigger_name == TRIGGER_NAME),
            "Unexpected inner error: {:?}",
            error
        );
    }

    #[tokio::test]
    async fn subscribe_to_data_trigger_subscribes_to_publisher() {
        let publisher = Arc::new(create_valid_mock_publisher());
        let trigger = Arc::new(create_valid_data_trigger(publisher.clone()));
        let callback_listener = Arc::new(CallbackListener::new());
        let callback_listener_clone = callback_listener.clone();

        trigger
            .subscribe(
                &TriggerSubscriber::MetricsConfigStart,
                Box::new(move || callback_listener_clone.on_callback_fired()),
            )
            .await
            .unwrap();

        publisher.fire_new_message(&Int32Value::new()).await;

        with_timeout(callback_listener.callback_notify.notified()).await;

        assert_eq!(callback_listener.get_fired_count(), 1);
    }

    #[tokio::test]
    async fn subscribe_to_periodic_trigger_immediately_unsubscribe_doesnt_fire() {
        let trigger = Arc::new(create_valid_periodic_trigger());
        let callback_listener = Arc::new(CallbackListener::new());
        let callback_listener_clone = callback_listener.clone();

        trigger
            .clone()
            .subscribe(
                &TriggerSubscriber::MetricsConfigStart,
                Box::new(move || callback_listener_clone.on_callback_fired()),
            )
            .await
            .unwrap();
        trigger.unsubscribe(&TriggerSubscriber::MetricsConfigStart).await.unwrap();
        // Sleep for 2 intervals to ensure the trigger won't fire.
        // TODO(b/300703055): Find a way to remove sleeps.
        time::sleep(Duration::from_millis(2 * INTERVAL_MS)).await;

        assert_eq!(callback_listener.get_fired_count(), 0);
    }

    #[tokio::test]
    async fn subscribe_to_periodic_trigger_fires_twice() {
        let trigger = Arc::new(create_valid_periodic_trigger());
        let callback_listener = Arc::new(CallbackListener::new());
        let callback_listener_clone = callback_listener.clone();

        // Subscribe to trigger.
        trigger
            .clone()
            .subscribe(
                &TriggerSubscriber::MetricsConfigStart,
                Box::new(move || callback_listener_clone.on_callback_fired()),
            )
            .await
            .unwrap();

        // Wait for trigger to fire twice.
        with_timeout(callback_listener.callback_notify.notified()).await;
        with_timeout(callback_listener.callback_notify.notified()).await;

        // Unsubscribe from trigger.
        trigger.unsubscribe(&TriggerSubscriber::MetricsConfigStart).await.unwrap();

        assert_eq!(callback_listener.get_fired_count(), 2);
    }

    #[tokio::test]
    async fn subscribe_to_periodic_trigger_unsubscribe_from_callback_success() {
        let trigger = Arc::new(create_valid_periodic_trigger());
        let mut trigger_clone = Some(trigger.clone());

        trigger
            .subscribe(
                &TriggerSubscriber::MetricsConfigStart,
                Box::new(move || {
                    let trigger_clone = trigger_clone.take().unwrap();
                    tokio::spawn(async move {
                        trigger_clone
                            .unsubscribe(&TriggerSubscriber::MetricsConfigStart)
                            .await
                            .unwrap();
                    });
                }),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn subscribe_to_periodic_trigger_one_async_loop() {
        let trigger = Arc::new(create_valid_periodic_trigger());
        let callback_listener1 = Arc::new(CallbackListener::new());
        let callback_listener1_clone = callback_listener1.clone();
        let callback_listener2 = Arc::new(CallbackListener::new());
        let callback_listener2_clone = callback_listener2.clone();

        // Subscribe to trigger.
        trigger
            .clone()
            .subscribe(
                &TriggerSubscriber::MetricsConfigStart,
                Box::new(move || callback_listener1_clone.on_callback_fired()),
            )
            .await
            .unwrap();

        // Increase interval so that new async loops won't generate messages any time soon.
        let mut trigger = Arc::into_inner(trigger).unwrap();
        if let TriggerType::Periodic { interval_ms } = &mut trigger.trigger_type {
            *interval_ms = 100 * INTERVAL_MS;
        } else {
            panic!("Unexpected trigger type");
        }
        let trigger = Arc::new(trigger);

        // Create another subscription.
        trigger
            .subscribe(
                &TriggerSubscriber::MetricsConfigEnd,
                Box::new(move || callback_listener2_clone.on_callback_fired()),
            )
            .await
            .unwrap();

        // Wait for trigger to fire once.
        with_timeout(callback_listener1.callback_notify.notified()).await;
        with_timeout(callback_listener2.callback_notify.notified()).await;

        // Verify that both subscriptions were notified.
        assert_eq!(callback_listener1.get_fired_count(), 1);
        assert_eq!(callback_listener2.get_fired_count(), 1);
    }

    #[tokio::test]
    async fn subscribe_to_conditional_trigger_subscribes_to_parents() {
        let parent_trigger1 = Arc::new(MockTrigger::new_for_subscriber(
            PARENT_TRIGGER_NAME.to_owned(),
            TriggerSubscriber::ConditionalTrigger { name: TRIGGER_NAME.to_owned() },
        ));
        let parent_trigger2 = Arc::new(MockTrigger::new_for_subscriber(
            PARENT_TRIGGER_NAME.to_owned(),
            TriggerSubscriber::ConditionalTrigger { name: TRIGGER_NAME.to_owned() },
        ));
        let message_provider = Arc::new(MockMessageProvider::new());
        let trigger = Arc::new(TriggerImpl::new_conditional_trigger(
            TRIGGER_NAME.to_owned(),
            ConditionType::ALL_CHANGES,
            vec![parent_trigger1.clone(), parent_trigger2.clone()],
            Arc::new(create_field_node("value".to_owned(), message_provider.clone())),
        ));
        let callback_listener = Arc::new(CallbackListener::new());
        let callback_listener_clone = callback_listener.clone();

        trigger
            .subscribe(
                &TriggerSubscriber::MetricsConfigStart,
                Box::new(move || callback_listener_clone.on_callback_fired()),
            )
            .await
            .unwrap();

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

        assert_eq!(callback_listener.get_fired_count(), 1);
    }

    #[tokio::test]
    async fn subscribe_to_conditional_trigger_filters_all_changes() {
        let (message_provider, parent_trigger, callback_listener) =
            setup_subscribed_conditional_trigger(ConditionType::ALL_CHANGES).await;

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

        assert_eq!(callback_listener.get_fired_count(), 1);
    }

    #[tokio::test]
    async fn subscribe_to_conditional_trigger_filters_falling_edge() {
        let (message_provider, parent_trigger, callback_listener) =
            setup_subscribed_conditional_trigger(ConditionType::FALLING_EDGE).await;

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
    async fn subscribe_to_conditional_trigger_filters_rising_edge() {
        let (message_provider, parent_trigger, callback_listener) =
            setup_subscribed_conditional_trigger(ConditionType::RISING_EDGE).await;

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
    async fn subscribe_to_conditional_trigger_filters_true() {
        let (message_provider, parent_trigger, callback_listener) =
            setup_subscribed_conditional_trigger(ConditionType::IS_TRUE).await;

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
    async fn subscribe_to_conditional_trigger_filters_false() {
        let (message_provider, parent_trigger, callback_listener) =
            setup_subscribed_conditional_trigger(ConditionType::IS_FALSE).await;

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
    async fn subscribe_to_conditional_trigger_filters_non_zero() {
        let (message_provider, parent_trigger, callback_listener) =
            setup_subscribed_conditional_trigger(ConditionType::IS_TRUE).await;

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
    async fn subscribe_to_conditional_trigger_filters_zero() {
        let (message_provider, parent_trigger, callback_listener) =
            setup_subscribed_conditional_trigger(ConditionType::IS_FALSE).await;

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
    async fn unsubscribe_not_subscribed_fails() {
        let trigger = create_valid_periodic_trigger();

        let err = trigger.unsubscribe(&TriggerSubscriber::MetricsConfigStart).await.err().unwrap();

        assert!(
            matches!(&err, TriggerUnsubscribeError::NotSubscribed { subscriber_name } if subscriber_name == &TriggerSubscriber::MetricsConfigStart.to_string()),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn unsubscribe_from_data_trigger_unsubscribe_returns_error_fails() {
        let mut publisher = create_valid_mock_publisher();
        publisher.set_unsubscribe_callback(|| {
            Some(PublisherUnsubscribeError::NotSubscribed {
                data_trigger_name: TRIGGER_NAME.to_owned(),
            })
        });
        let trigger = Arc::new(create_valid_data_trigger(Arc::new(publisher)));

        trigger
            .clone()
            .subscribe(&TriggerSubscriber::MetricsConfigStart, create_empty_fn_box())
            .await
            .unwrap();
        let err = trigger.unsubscribe(&TriggerSubscriber::MetricsConfigStart).await.err().unwrap();

        let TriggerUnsubscribeError::PublisherUnsubscribeError { error } = &err else {
            panic!("Unexpected error: {:?}", err);
        };
        assert!(
            matches!(error, PublisherUnsubscribeError::NotSubscribed { data_trigger_name } if data_trigger_name == TRIGGER_NAME),
            "Unexpected inner error: {:?}",
            error
        );
    }

    #[tokio::test]
    async fn unsubscribe_from_conditional_trigger_unsubscribe_returns_error_fails() {
        let mut parent_trigger = MockTrigger::new_for_subscriber(
            PARENT_TRIGGER_NAME.to_owned(),
            TriggerSubscriber::ConditionalTrigger { name: TRIGGER_NAME.to_owned() },
        );
        parent_trigger.set_unsubscribe_callback(|_| {
            Some(TriggerUnsubscribeError::NotSubscribed {
                subscriber_name: TriggerSubscriber::MetricsConfigEnd.to_string(),
            })
        });
        let trigger = Arc::new(TriggerImpl::new_conditional_trigger(
            TRIGGER_NAME.to_owned(),
            ConditionType::ALL_CHANGES,
            vec![Arc::new(parent_trigger)],
            Arc::new(Node::ConstantLeaf { value: NodeValue::None }),
        ));

        trigger
            .clone()
            .subscribe(&TriggerSubscriber::MetricsConfigStart, create_empty_fn_box())
            .await
            .unwrap();
        let err = trigger.unsubscribe(&TriggerSubscriber::MetricsConfigStart).await.err().unwrap();

        let TriggerUnsubscribeError::ParentTriggerUnsubscribeErrors { errors } = &err else {
            panic!("Unexpected error: {:?}", err);
        };
        // Verify that the error details are valid.
        assert_eq!(errors.len(), 1);
        let (name, error) = &errors[0];
        assert_eq!(name, PARENT_TRIGGER_NAME);
        assert!(
            matches!(error, TriggerUnsubscribeError::NotSubscribed { subscriber_name } if subscriber_name == &TriggerSubscriber::MetricsConfigEnd.to_string() ),
            "Unexpected inner error: {:?}",
            error
        );
    }
}
