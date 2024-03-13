// Copyright 2023 Google LLC. All Rights Reserved.

use async_trait::async_trait;
use log::error;
use protobuf::MessageDyn;
use sdv_telemetry_utils::collection_extension::HashMapExtension;
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::marker::{Send, Sync};
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::sync::Mutex as AsyncMutex;

use crate::metrics_config_orchestrator::message_builder::MessageBuilder;
use crate::metrics_config_orchestrator::service_adapter::{
    ServiceAdapter, ServiceAdapterSubscribeError, ServiceAdapterUnsubscribeError,
    ServiceSubscription,
};
use crate::metrics_config_orchestrator::trigger::{Trigger, TriggerSubscriber};

type CallbackFn = Box<dyn FnMut(&dyn MessageDyn) + Send + Sync>;

const PUBLISHER_STATE_INACTIVE: &str = "Inactive";
const PUBLISHER_STATE_ACTIVE: &str = "Active";

#[async_trait]
pub(crate) trait Publisher: Send + Sync {
    fn get_name(&self) -> &str;
    fn get_latest_message(&self) -> Option<Box<dyn MessageDyn>>;

    async fn activate(self: Arc<Self>) -> Result<(), PublisherActivateError>;
    async fn deactivate(&self) -> Result<(), PublisherDeactivateError>;

    async fn subscribe(&self, data_trigger_name: &str, callback: CallbackFn);
    async fn unsubscribe(&self, data_trigger_name: &str);
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum PublisherActivateError {
    #[error("Service adapter failed to process subscription request: {error}")]
    ServiceAdapterError {
        #[from]
        error: ServiceAdapterSubscribeError,
    },
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub(crate) enum PublisherDeactivateError {
    #[error("Service adapter failed to process unsubscription request: {error}")]
    ServiceAdapterError {
        #[from]
        error: ServiceAdapterUnsubscribeError,
    },
}

pub(crate) struct PublisherImpl {
    pub(super) name: String,
    pub(super) publisher_type: PublisherType,

    latest_message: Mutex<Option<Box<dyn MessageDyn>>>,
    state: AsyncMutex<PublisherState>,
}

pub(crate) enum PublisherState {
    Inactive,
    Active {
        // Map of unique data trigger names to the subscribed callbacks.
        callback_map: HashMap<String, CallbackFn>,
    },
}

#[allow(dead_code)]
pub(crate) enum PublisherType {
    Signal {
        signal_name: String,
    },
    Service {
        service_name: String,
        topic_name: String,
        service_adapter: Arc<dyn ServiceAdapter + Send + Sync>,
        service_subscription: Mutex<Option<ServiceSubscription>>,
    },
    Aggregation {
        message_builder: Box<dyn MessageBuilder>,
        reset_on_get: bool,
        trigger_subscriber: TriggerSubscriber,
        triggers: Vec<Arc<dyn Trigger>>,
    },
}

impl Display for PublisherState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            PublisherState::Inactive => f.write_str(PUBLISHER_STATE_INACTIVE),
            PublisherState::Active { .. } => f.write_str(PUBLISHER_STATE_ACTIVE),
        }
    }
}

#[async_trait]
impl Publisher for PublisherImpl {
    fn get_name(&self) -> &str {
        &self.name
    }

    fn get_latest_message(&self) -> Option<Box<dyn MessageDyn>> {
        self.latest_message.lock().unwrap().clone()
    }

    async fn activate(self: Arc<Self>) -> Result<(), PublisherActivateError> {
        // Lock state mutex and hold it until the state transition is complete.
        let mut state_lock = self.state.lock().await;
        assert!(
            matches!(*state_lock, PublisherState::Inactive),
            "Unable to activate publisher {}: it's in {} state",
            self.name,
            state_lock
        );

        // Perform activation logic specific to the publisher type.
        match &self.publisher_type {
            PublisherType::Aggregation { trigger_subscriber, triggers, .. } => {
                // Subscribe to parent triggers.
                for trigger in triggers {
                    let self_clone = self.clone();
                    trigger
                        .clone()
                        .subscribe(
                            trigger_subscriber,
                            Box::new(move || {
                                self_clone.on_aggregation_publisher_trigger_fired();
                            }),
                        )
                        .await;
                }
            }
            PublisherType::Service {
                service_name,
                topic_name,
                service_adapter,
                service_subscription,
            } => {
                assert!(service_subscription.lock().unwrap().is_none(), "Unable to activate publisher {} because it is in an inconsistent state: it's activating, but subscription is already set", self.name);

                // Subscribe to the service adapter.
                let self_clone = self.clone();
                let subscription = service_adapter
                    .subscribe(
                        service_name,
                        topic_name,
                        Box::new(move |msg| self_clone.on_service_publisher_new_message(msg)),
                    )
                    .await?;

                let mut subscription_container = service_subscription.lock().unwrap();
                assert!(subscription_container.is_none(), "Unable to activate publisher {} because it is in an inconsistent state: it's activating, but subscription is already set", self.name);
                subscription_container.replace(subscription);
            }
            _ => {}
        }

        // Set state to Active and release the mutex.
        *state_lock = PublisherState::Active { callback_map: HashMap::new() };

        Ok(())
    }

    async fn deactivate(&self) -> Result<(), PublisherDeactivateError> {
        // Lock state mutex and hold it until the state transition is complete.
        let mut state_lock = self.state.lock().await;
        if let PublisherState::Active { callback_map } = &*state_lock {
            assert!(
                callback_map.is_empty(),
                "Unable to deactivate publisher {} with active subscriptions",
                self.name
            );
        } else {
            panic!("Unable to deactivate publisher {}: it's in {} state", self.name, state_lock);
        }

        // Perform deactivation logic specific to the publisher type.
        match &self.publisher_type {
            PublisherType::Aggregation { triggers, trigger_subscriber, .. } => {
                for trigger in triggers {
                    trigger.unsubscribe(trigger_subscriber).await;
                }
            }
            PublisherType::Service { service_adapter, service_subscription, .. } => {
                let subscription_container = service_subscription.lock().unwrap().take();
                if let Some(subscription) = subscription_container {
                    service_adapter.unsubscribe(subscription).await?;
                } else {
                    panic!("Unable to deactivate publisher {} because it's in an inconsistent state: it's deactivating, but subscription is not set", self.name);
                }
            }
            _ => {}
        }

        // Reset latest message.
        // The publisher is unsubscribed from underlying data sources, so no callback will change it back to some stale message.
        *self.latest_message.lock().unwrap() = None;

        // Change state to Inactive and release the mutex.
        *state_lock = PublisherState::Inactive;

        Ok(())
    }

    async fn subscribe(
        &self,
        data_trigger_name: &str,
        callback: Box<dyn for<'a> FnMut(&'a dyn MessageDyn) + Send + Sync>,
    ) {
        let mut state_lock = self.state.lock().await;
        if let PublisherState::Active { callback_map } = &mut *state_lock {
            callback_map.try_insert_value(data_trigger_name.to_owned(), callback).unwrap_or_else(|_| panic!("Unable to subscribe to publisher {}: data trigger {} is already subscribed", self.name, data_trigger_name));
        } else {
            panic!("Unable to subscribe to publisher {}: it's in {} state", self.name, state_lock);
        }
    }

    async fn unsubscribe(&self, data_trigger_name: &str) {
        let mut state_lock = self.state.lock().await;
        if let PublisherState::Active { callback_map } = &mut *state_lock {
            let _ = callback_map.remove(data_trigger_name).unwrap_or_else(|| {
                panic!(
                    "Unable to unsubscribe from publisher {}: data trigger {} wasn't subscribed",
                    self.name, data_trigger_name
                )
            });
        } else {
            panic!(
                "Unable to unsubscribe from publisher {}: it's in {} state",
                self.name, state_lock
            );
        }
    }
}

impl PublisherImpl {
    pub(super) fn new(name: String, publisher_type: PublisherType) -> Self {
        Self {
            name,
            publisher_type,
            latest_message: None.into(),
            state: PublisherState::Inactive.into(),
        }
    }

    pub(super) fn new_aggregation_publisher(
        name: String,
        message_builder: Box<dyn MessageBuilder>,
        reset_on_get: bool,
        triggers: Vec<Arc<dyn Trigger>>,
    ) -> Self {
        let trigger_subscriber = TriggerSubscriber::AggregationPublisher { name: name.clone() };
        let publisher_type = PublisherType::Aggregation {
            reset_on_get,
            trigger_subscriber,
            triggers,
            message_builder,
        };
        Self::new(name, publisher_type)
    }

    pub(super) fn new_service_publisher(
        name: String,
        service_name: String,
        topic_name: String,
        service_adapter: Arc<dyn ServiceAdapter + Send + Sync>,
    ) -> Self {
        Self::new(
            name,
            PublisherType::Service {
                service_name,
                topic_name,
                service_adapter,
                service_subscription: None.into(),
            },
        )
    }

    async fn set_new_message(self: &Arc<Self>, message: &dyn MessageDyn) {
        // Lock state mutex and hold it to make sure the publisher is not deactivated.
        let mut state_lock = self.state.lock().await;

        let PublisherState::Active { callback_map } = &mut *state_lock else {
            panic!(
                "Publisher {} received callback when it was in the {} state",
                self.name, state_lock
            );
        };

        // Replace latest message.
        self.latest_message.lock().unwrap().replace(message.clone_box());

        // Notify subscribed data triggers.
        // TODO(b/294129114): Replace callback_map with just a single callback.
        for callback in callback_map.values_mut() {
            callback(message);
        }
    }

    fn on_aggregation_publisher_trigger_fired(self: &Arc<Self>) {
        let PublisherType::Aggregation { message_builder, .. } = &self.publisher_type else {
            panic!("Publisher {} that is not an Aggregation publisher received Aggregation publisher callback", self.name);
        };

        let msg = match message_builder.build() {
            Ok(msg) => msg,
            Err(err) => {
                error!(
                    "Error when trying to evaluate expression for publisher {}: {}",
                    self.name, err
                );
                return;
            }
        };

        let self_clone = self.clone();
        tokio::spawn(async move {
            self_clone.set_new_message(msg.as_ref()).await;
        });
    }

    fn on_service_publisher_new_message(self: &Arc<Self>, message: &dyn MessageDyn) {
        let self_clone = self.clone();
        let message_clone = message.clone_box();
        tokio::spawn(async move {
            self_clone.set_new_message(message_clone.as_ref()).await;
        });
    }
}

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
mod tests {
    use protobuf::well_known_types::wrappers::Int32Value;
    use sdv_telemetry_test_proto::test_message::TestMessage;
    use std::sync::Arc;
    use tokio::sync::oneshot;

    use super::*;
    use crate::metrics_config_orchestrator::{
        message_builder::mock::MockMessageBuilder, service_adapter::mock::MockServiceAdapter,
        trigger::mock::MockTrigger,
    };

    const DATA_TRIGGER_NAME: &str = "datatrigger";
    const PUBLISHER_NAME: &str = "publisher";
    const SERVICE_NAME: &str = "service";
    const TOPIC_NAME: &str = "topic";
    const TRIGGER_NAME: &str = "trigger";

    fn create_inactive_service_publisher(
        service_adapter: Option<Arc<MockServiceAdapter>>,
    ) -> Arc<PublisherImpl> {
        Arc::new(PublisherImpl::new_service_publisher(
            PUBLISHER_NAME.to_owned(),
            SERVICE_NAME.to_owned(),
            TOPIC_NAME.to_owned(),
            service_adapter.unwrap_or_else(|| Arc::new(MockServiceAdapter::new())),
        ))
    }

    fn create_inactive_aggregation_publisher(
        triggers: Vec<Arc<dyn Trigger>>,
    ) -> Arc<PublisherImpl> {
        Arc::new(PublisherImpl::new_aggregation_publisher(
            PUBLISHER_NAME.to_owned(),
            Box::new(MockMessageBuilder::new()),
            false,
            triggers,
        ))
    }

    async fn create_active_aggregation_publisher(
        triggers: Vec<Arc<dyn Trigger>>,
    ) -> Arc<PublisherImpl> {
        let publisher = create_inactive_aggregation_publisher(triggers);

        publisher.clone().activate().await.unwrap();
        publisher
    }

    async fn create_active_service_publisher(
        service_adapter: Option<Arc<MockServiceAdapter>>,
    ) -> Arc<PublisherImpl> {
        let publisher = create_inactive_service_publisher(service_adapter);

        publisher.clone().activate().await.unwrap();
        publisher
    }

    async fn create_service_publisher_with_subscription(
        service_adapter: Option<Arc<MockServiceAdapter>>,
    ) -> Arc<PublisherImpl> {
        let publisher = create_active_service_publisher(service_adapter).await;

        publisher.subscribe(DATA_TRIGGER_NAME, Box::new(|_| {})).await;
        publisher
    }

    #[tokio::test]
    #[should_panic]
    async fn activate_when_active_panics() {
        let publisher = create_active_service_publisher(None).await;

        publisher.activate().await.unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn deactivate_when_inactive_panics() {
        let publisher = create_inactive_service_publisher(None);

        publisher.deactivate().await.unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn deactivate_with_subscriptions_panics() {
        let publisher = create_service_publisher_with_subscription(None).await;

        publisher.deactivate().await.unwrap();
    }

    #[tokio::test]
    async fn deactivate_service_publisher_calls_unsubscribe() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        let publisher = create_active_service_publisher(Some(service_adapter.clone())).await;

        publisher.deactivate().await.unwrap();

        assert_eq!(service_adapter.get_num_unsubscribe_calls(), 1);
    }

    #[tokio::test]
    #[should_panic]
    async fn subscribe_when_inactive_panics() {
        let publisher = create_inactive_service_publisher(None);

        publisher.subscribe(DATA_TRIGGER_NAME, Box::new(|_| {})).await;
    }

    #[tokio::test]
    #[should_panic]
    async fn subscribe_already_subscribed_panics() {
        let publisher = create_service_publisher_with_subscription(None).await;

        publisher.subscribe(DATA_TRIGGER_NAME, Box::new(|_| {})).await;
    }

    #[tokio::test]
    async fn subscribe_service_publisher_new_message_notifies_subscriber() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        let publisher = create_active_service_publisher(Some(service_adapter.clone())).await;
        let (tx, rx) = oneshot::channel();
        let mut maybe_tx = Some(tx);
        let mut msg = Int32Value::new();
        msg.value = 42;

        publisher
            .subscribe(
                DATA_TRIGGER_NAME,
                Box::new(move |message| {
                    maybe_tx.take().unwrap().send(message.clone_box()).unwrap()
                }),
            )
            .await;
        service_adapter.publish_new_message(&msg).await;

        let expected = Box::new(msg) as Box<dyn MessageDyn>;
        assert_eq!(&rx.await.unwrap(), &expected);
        assert_eq!(publisher.get_latest_message().as_ref().unwrap(), &expected);
    }

    #[tokio::test]
    async fn subscribe_aggregation_publisher_triggered_notifies_subscriber() {
        let trigger = Arc::new(MockTrigger::new_for_subscriber(
            TRIGGER_NAME.to_owned(),
            TriggerSubscriber::AggregationPublisher { name: PUBLISHER_NAME.to_owned() },
        ));
        let publisher = create_active_aggregation_publisher(vec![trigger.clone()]).await;
        let (tx, rx) = oneshot::channel();
        let mut maybe_tx = Some(tx);

        publisher
            .subscribe(
                DATA_TRIGGER_NAME,
                Box::new(move |message| {
                    maybe_tx.take().unwrap().send(message.clone_box()).unwrap()
                }),
            )
            .await;
        trigger.fire().await;

        let test_message =
            <dyn MessageDyn>::downcast_box::<TestMessage>(rx.await.unwrap()).unwrap();
        assert_eq!(test_message.int_field, 1);
        let test_message =
            <dyn MessageDyn>::downcast_box::<TestMessage>(publisher.get_latest_message().unwrap())
                .unwrap();
        assert_eq!(test_message.int_field, 1);
    }

    #[tokio::test]
    #[should_panic]
    async fn unsubscribe_when_inactive_panics() {
        let publisher = create_inactive_service_publisher(None);

        publisher.unsubscribe(DATA_TRIGGER_NAME).await;
    }

    #[tokio::test]
    #[should_panic]
    async fn unsubscribe_not_subscribed_panics() {
        let publisher = create_active_service_publisher(None).await;

        publisher.unsubscribe(DATA_TRIGGER_NAME).await;
    }

    #[tokio::test]
    async fn unsubscribe_success() {
        let publisher = create_service_publisher_with_subscription(None).await;

        publisher.unsubscribe(DATA_TRIGGER_NAME).await;
    }
}
