// Copyright 2023 Google LLC. All Rights Reserved.

use async_trait::async_trait;
use protobuf::MessageDyn;
use std::{
    collections::HashMap,
    fmt,
    marker::{Send, Sync},
    sync::Arc,
};
use thiserror::Error;
use tokio::{sync::Mutex as AsyncMutex, time::Duration};

use crate::collection_extension::HashMapExtension;
use crate::expr::ast::Node;
use crate::metrics_config_orchestrator::{
    publisher::{PublisherSubscribeError, PublisherUnsubscribeError},
    Publisher,
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
        &self,
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
}

#[allow(dead_code)]
#[derive(Clone, Eq, Hash, PartialEq)]
pub(crate) enum TriggerSubscriber {
    AggregationPublisher { name: String },
    MetricsConfigStart,
    MetricsConfigEnd,
    MetricsConfigFinish,
    MetricsReportGenerator { name: String },
    PeriodicTrigger { name: String },
}

pub(crate) struct TriggerImpl {
    pub(super) name: String,
    pub(super) trigger_type: TriggerType,

    // The map of functions that need to be called when the trigger fires.
    // The first subscription to TriggerImpl creates a new map and activates underlying trigger
    // type, passing a reference the map. Last unsubscription removes the map and deactivates
    // underlying trigger type.
    callback_map:
        AsyncMutex<Arc<AsyncMutex<HashMap<TriggerSubscriber, Box<dyn FnMut() + Send + Sync>>>>>,
}

#[allow(dead_code)]
pub(crate) enum TriggerType {
    Data { publisher: Arc<dyn Publisher> },
    Periodic { interval_ms: u64 },
    Conditional { triggers: Vec<Arc<TriggerImpl>>, selector_expression: Arc<Node> },
}

impl fmt::Display for TriggerSubscriber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TriggerSubscriber::AggregationPublisher { name } => {
                write!(f, "Aggregation Publisher {}", name)
            }
            TriggerSubscriber::MetricsConfigStart => write!(f, "Metrics Config Start"),
            TriggerSubscriber::MetricsConfigEnd => write!(f, "Metrics Config End"),
            TriggerSubscriber::MetricsConfigFinish => write!(f, "Metrics Config Finish"),
            TriggerSubscriber::MetricsReportGenerator { name } => {
                write!(f, "Metrics Report Generator {}", name)
            }
            TriggerSubscriber::PeriodicTrigger { name } => write!(f, "Periodic Trigger {}", name),
        }
    }
}

#[async_trait]
impl Trigger for TriggerImpl {
    fn get_name(&self) -> &str {
        &self.name
    }

    async fn subscribe(
        &self,
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
    pub(super) fn new(name: String, trigger_type: TriggerType) -> Self {
        TriggerImpl {
            name,
            trigger_type,
            callback_map: AsyncMutex::new(Arc::new(AsyncMutex::new(HashMap::new()))),
        }
    }

    async fn activate(
        &self,
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
                            Self::on_publisher_new_message(&callbacks_container, msg)
                        }),
                    )
                    .await?;
            }
            TriggerType::Periodic { interval_ms } => {
                tokio::spawn(Self::call_periodically(*interval_ms, callbacks_container));
            }
            TriggerType::Conditional { .. } => unimplemented!(),
        };
        Ok(())
    }

    async fn deactivate(&self) -> Result<(), TriggerUnsubscribeError> {
        if let TriggerType::Data { publisher } = &self.trigger_type {
            publisher.unsubscribe(&self.name).await?;
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

    fn on_publisher_new_message(
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
}

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
mod tests {
    use protobuf::well_known_types::wrappers::Int32Value;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tokio::{sync::Notify, time};

    use super::*;
    use crate::expr::node_value::NodeValue;
    use crate::metrics_config_orchestrator::publisher::mock::MockPublisher;

    const INTERVAL_MS: u64 = 10;
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

    fn create_node() -> Node {
        Node::ConstantLeaf { value: NodeValue::Bool(false) }
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

    fn empty_fn() {}

    fn create_empty_fn_box() -> Box<dyn FnMut() + Send + Sync> {
        Box::new(empty_fn)
    }

    #[tokio::test]
    async fn subscribe_already_subscribed_fails() {
        let trigger = create_valid_periodic_trigger();

        trigger
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
        let trigger = create_valid_data_trigger(Arc::new(publisher));

        let err = trigger
            .subscribe(&TriggerSubscriber::MetricsConfigStart, create_empty_fn_box())
            .await
            .err()
            .unwrap();

        if let TriggerSubscribeError::PublisherSubscribeError { error } = &err {
            assert!(
                matches!(error, PublisherSubscribeError::DuplicateSubscription { data_trigger_name } if data_trigger_name == TRIGGER_NAME),
                "Unexpected inner error: {:?}",
                error
            );
        } else {
            panic!("Unexpected error: {:?}", err);
        }
    }

    #[tokio::test]
    async fn subscribe_to_data_trigger_subscribes_to_publisher() {
        let publisher = Arc::new(create_valid_mock_publisher());
        let trigger = create_valid_data_trigger(publisher.clone());
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

        callback_listener.callback_notify.notified().await;

        assert_eq!(callback_listener.get_fired_count(), 1);
    }

    #[tokio::test]
    async fn subscribe_to_periodic_trigger_immediately_unsubscribe_doesnt_fire() {
        let trigger = create_valid_periodic_trigger();
        let callback_listener = Arc::new(CallbackListener::new());
        let callback_listener_clone = callback_listener.clone();

        trigger
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
        let trigger = create_valid_periodic_trigger();
        let callback_listener = Arc::new(CallbackListener::new());
        let callback_listener_clone = callback_listener.clone();

        // Subscribe to trigger.
        trigger
            .subscribe(
                &TriggerSubscriber::MetricsConfigStart,
                Box::new(move || callback_listener_clone.on_callback_fired()),
            )
            .await
            .unwrap();

        // Wait for trigger to fire twice.
        callback_listener.callback_notify.notified().await;
        callback_listener.callback_notify.notified().await;

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
        let mut trigger = create_valid_periodic_trigger();
        let callback_listener1 = Arc::new(CallbackListener::new());
        let callback_listener1_clone = callback_listener1.clone();
        let callback_listener2 = Arc::new(CallbackListener::new());
        let callback_listener2_clone = callback_listener2.clone();

        // Subscribe to trigger.
        trigger
            .subscribe(
                &TriggerSubscriber::MetricsConfigStart,
                Box::new(move || callback_listener1_clone.on_callback_fired()),
            )
            .await
            .unwrap();

        // Increase interval so that new async loops won't generate messages any time soon.
        if let TriggerType::Periodic { interval_ms } = &mut trigger.trigger_type {
            *interval_ms = 100 * INTERVAL_MS;
        } else {
            panic!("Unexpected trigger type");
        }

        // Create another subscription.
        trigger
            .subscribe(
                &TriggerSubscriber::MetricsConfigEnd,
                Box::new(move || callback_listener2_clone.on_callback_fired()),
            )
            .await
            .unwrap();

        // Wait for trigger to fire once.
        callback_listener1.callback_notify.notified().await;
        callback_listener2.callback_notify.notified().await;

        // Verify that both subscriptions were notified.
        assert_eq!(callback_listener1.get_fired_count(), 1);
        assert_eq!(callback_listener2.get_fired_count(), 1);
    }

    #[should_panic]
    #[tokio::test]
    async fn subscribe_to_conditional_trigger_panics() {
        let trigger = TriggerImpl::new(
            TRIGGER_NAME.to_owned(),
            TriggerType::Conditional {
                triggers: vec![],
                selector_expression: Arc::new(create_node()),
            },
        );

        trigger
            .subscribe(&TriggerSubscriber::MetricsConfigStart, create_empty_fn_box())
            .await
            .unwrap();
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
        let trigger = create_valid_data_trigger(Arc::new(publisher));

        trigger
            .subscribe(&TriggerSubscriber::MetricsConfigStart, create_empty_fn_box())
            .await
            .unwrap();
        let err = trigger.unsubscribe(&TriggerSubscriber::MetricsConfigStart).await.err().unwrap();

        if let TriggerUnsubscribeError::PublisherUnsubscribeError { error } = &err {
            assert!(
                matches!(error, PublisherUnsubscribeError::NotSubscribed { data_trigger_name } if data_trigger_name == TRIGGER_NAME),
                "Unexpected inner error: {:?}",
                error
            );
        } else {
            panic!("Unexpected error: {:?}", err);
        }
    }
}
