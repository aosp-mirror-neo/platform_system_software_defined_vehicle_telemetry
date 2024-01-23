// Copyright 2023 Google LLC. All Rights Reserved.

use async_trait::async_trait;
use log::error;
use protobuf::MessageDyn;
use sdv_telemetry_utils::collection_extension::HashMapExtension;
use std::{
    collections::HashMap,
    fmt,
    fmt::{Debug, Display, Formatter},
    marker::{Send, Sync},
    sync::{Arc, Mutex, Weak},
};
use thiserror::Error;
use tokio::sync::Mutex as AsyncMutex;

use crate::metrics_config_orchestrator::message_builder::MessageBuilder;
use crate::metrics_config_orchestrator::service_adapter::{
    ServiceAdapter, ServiceAdapterSubscribeError, ServiceAdapterUnsubscribeError,
    ServiceSubscription,
};
use crate::metrics_config_orchestrator::trigger::{
    Trigger, TriggerSubscriber, TriggerUnsubscribeError,
};

const PUBLISHER_STATE_INACTIVE: &str = "Inactive";
const PUBLISHER_STATE_ACTIVATING: &str = "Activating";
const PUBLISHER_STATE_ACTIVE: &str = "Active";
const PUBLISHER_STATE_DEACTIVATING: &str = "Deactivating";

#[async_trait]
pub(crate) trait Publisher: Send + Sync {
    fn get_name(&self) -> &str;
    fn get_latest_message(&self) -> Option<Box<dyn MessageDyn>>;

    async fn activate(self: Arc<Self>) -> Result<(), PublisherActivateError>;
    async fn deactivate(&self) -> Result<(), PublisherDeactivateError>;

    async fn subscribe(
        &self,
        data_trigger_name: &str,
        callback: Box<dyn for<'a> FnMut(&'a dyn MessageDyn) + Send + Sync>,
    ) -> Result<(), PublisherSubscribeError>;
    async fn unsubscribe(&self, data_trigger_name: &str) -> Result<(), PublisherUnsubscribeError>;
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum PublisherActivateError {
    #[error("Unable to activate publisher: it's in {state} state")]
    InvalidState { state: String },
    #[error("Service adapter failed to process subscription request: {error}")]
    ServiceAdapterError {
        #[from]
        error: ServiceAdapterSubscribeError,
    },
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub(crate) enum PublisherDeactivateError {
    #[error("Unable to deactivate publisher with active subscriptions")]
    CannotDeactivateWithSubscriptions,
    #[error("Unable to deactivate publisher: it's in {state} state")]
    InvalidState { state: String },
    #[error("Service adapter failed to process unsubscription request: {error}")]
    ServiceAdapterError {
        #[from]
        error: ServiceAdapterUnsubscribeError,
    },
    #[error("Unable to fully unsubscribe from underlying triggers: {errors:?}")]
    TriggerUnsubscribeErrors { errors: Vec<(String, TriggerUnsubscribeError)> },
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum PublisherSubscribeError {
    #[error("Unable to subscribe to publisher when it's {state}")]
    PublisherNotActive { state: String },
    #[error("Data trigger {data_trigger_name} is already subscribed to the publisher. Duplicate subscriptions are not allowed.")]
    DuplicateSubscription { data_trigger_name: String },
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum PublisherUnsubscribeError {
    #[error("Unable to unsubscribe from publisher when it's {state}")]
    PublisherNotActive { state: String },
    #[error("Data trigger {data_trigger_name} wasn't subscribed to the publisher")]
    NotSubscribed { data_trigger_name: String },
}

pub(crate) struct PublisherImpl {
    pub(super) name: String,
    pub(super) publisher_type: PublisherType,

    latest_message: Mutex<Option<Box<dyn MessageDyn>>>,
    state: AsyncMutex<PublisherState>,
}

pub(crate) enum PublisherState {
    Inactive,
    Activating,
    Active {
        callback_map:
            Arc<AsyncMutex<HashMap<String, Box<dyn FnMut(&dyn MessageDyn) + Send + Sync>>>>,
    },
    Deactivating,
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
            PublisherState::Activating => f.write_str(PUBLISHER_STATE_ACTIVATING),
            PublisherState::Active { .. } => f.write_str(PUBLISHER_STATE_ACTIVE),
            PublisherState::Deactivating => f.write_str(PUBLISHER_STATE_DEACTIVATING),
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
        {
            let mut state = self.state.lock().await;
            if !matches!(*state, PublisherState::Inactive) {
                return Err(PublisherActivateError::InvalidState { state: state.to_string() });
            }
            *state = PublisherState::Activating;
        }

        let callback_map = Arc::new(AsyncMutex::new(HashMap::new()));
        match &self.publisher_type {
            PublisherType::Aggregation { trigger_subscriber, triggers, .. } => {
                for trigger in triggers {
                    let self_clone = self.clone();
                    let callback_map_reference = Arc::downgrade(&callback_map);
                    if let Err(err) = trigger
                        .clone()
                        .subscribe(
                            trigger_subscriber,
                            Box::new(move || {
                                self_clone
                                    .on_aggregation_publisher_trigger_fired(&callback_map_reference)
                            }),
                        )
                        .await
                    {
                        error!(
                            "Error when trying to activate trigger {} for aggregation publisher {}: {}.",
                            trigger.get_name(), self.name, err
                        );
                    }
                }
            }
            PublisherType::Service {
                service_name,
                topic_name,
                service_adapter,
                service_subscription,
            } => {
                if service_subscription.lock().unwrap().is_some() {
                    panic!("Unable to activate publisher {} because it is in an inconsistent state: it's activating, but subscription is already set", self.name);
                }

                let self_clone = self.clone();
                let callback_map_reference = Arc::downgrade(&callback_map);
                let subscription = service_adapter
                    .subscribe(
                        service_name,
                        topic_name,
                        Box::new(move |msg| {
                            self_clone
                                .on_service_publisher_new_message(&callback_map_reference, msg)
                        }),
                    )
                    .await?;

                let mut subscription_container = service_subscription.lock().unwrap();
                if subscription_container.is_some() {
                    panic!("Unable to activate publisher {} because it is in an inconsistent state: it's activating, but subscription is already set", self.name);
                }
                subscription_container.replace(subscription);
            }
            _ => {}
        }

        {
            let mut state = self.state.lock().await;
            if !matches!(*state, PublisherState::Activating) {
                panic!("Unable to mark publisher {} as active because it is in an inconsistent state: {} instead of Activating", self.name, state);
            }
            *state = PublisherState::Active { callback_map };
        }

        Ok(())
    }

    async fn deactivate(&self) -> Result<(), PublisherDeactivateError> {
        {
            let mut state = self.state.lock().await;
            if let PublisherState::Active { callback_map } = &*state {
                if !callback_map.lock().await.is_empty() {
                    return Err(PublisherDeactivateError::CannotDeactivateWithSubscriptions);
                }
            } else {
                return Err(PublisherDeactivateError::InvalidState { state: state.to_string() });
            }
            *state = PublisherState::Deactivating;
            *self.latest_message.lock().unwrap() = None;
        }

        match &self.publisher_type {
            PublisherType::Aggregation { triggers, trigger_subscriber, .. } => {
                let mut errors = Vec::new();

                for trigger in triggers {
                    if let Err(err) = trigger.unsubscribe(trigger_subscriber).await {
                        errors.push((trigger.get_name().to_owned(), err));
                    }
                }

                if !errors.is_empty() {
                    return Err(PublisherDeactivateError::TriggerUnsubscribeErrors { errors });
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

        {
            let mut state = self.state.lock().await;
            if !matches!(*state, PublisherState::Deactivating) {
                panic!("Unable to mark publisher {} as inactive because it is in an inconsistent state: {} instead of Deactivating", self.name, state);
            }
            *state = PublisherState::Inactive;
        }

        Ok(())
    }

    async fn subscribe(
        &self,
        data_trigger_name: &str,
        callback: Box<dyn for<'a> FnMut(&'a dyn MessageDyn) + Send + Sync>,
    ) -> Result<(), PublisherSubscribeError> {
        let state = self.state.lock().await;
        if let PublisherState::Active { callback_map } = &*state {
            callback_map
                .lock()
                .await
                .try_insert_value(data_trigger_name.to_owned(), callback)
                .map_err(|_| PublisherSubscribeError::DuplicateSubscription {
                    data_trigger_name: data_trigger_name.to_owned(),
                })?;
        } else {
            return Err(PublisherSubscribeError::PublisherNotActive { state: state.to_string() });
        }

        Ok(())
    }

    async fn unsubscribe(&self, data_trigger_name: &str) -> Result<(), PublisherUnsubscribeError> {
        let state = self.state.lock().await;
        if let PublisherState::Active { callback_map } = &*state {
            if callback_map.lock().await.remove(data_trigger_name).is_none() {
                Err(PublisherUnsubscribeError::NotSubscribed {
                    data_trigger_name: data_trigger_name.to_owned(),
                })
            } else {
                Ok(())
            }
        } else {
            Err(PublisherUnsubscribeError::PublisherNotActive { state: state.to_string() })
        }
    }
}

impl PublisherImpl {
    pub(super) fn new(name: String, publisher_type: PublisherType) -> Self {
        PublisherImpl {
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
        PublisherImpl::new(name, publisher_type)
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

    async fn set_new_message(
        self: &Arc<Self>,
        callback_map_reference: &Weak<
            AsyncMutex<HashMap<String, Box<dyn FnMut(&dyn MessageDyn) + Send + Sync>>>,
        >,
        message: &dyn MessageDyn,
    ) {
        // Return if the state has been changed to non-active while the task was pending
        // and the weak reference is no longer valid.
        let callback_map = match callback_map_reference.upgrade() {
            Some(callback_map) => callback_map,
            None => return,
        };

        // Lock map to make sure no subscriptions are added and publisher is not deactivated.
        let mut callback_map_guard = callback_map.lock().await;

        // Replace latest message.
        self.latest_message.lock().unwrap().replace(message.clone_box());

        // Notify subscribed data triggers.
        // TODO(b/294129114): Replace callback_map with just a single callback.
        for callback in callback_map_guard.values_mut() {
            callback(message);
        }
    }

    fn on_aggregation_publisher_trigger_fired(
        self: &Arc<Self>,
        callback_map_reference: &Weak<
            AsyncMutex<HashMap<String, Box<dyn FnMut(&dyn MessageDyn) + Send + Sync>>>,
        >,
    ) {
        let PublisherType::Aggregation { message_builder, .. } = &self.publisher_type else {
            panic!("Unexpected publisher type");
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
        let callback_map_reference = callback_map_reference.clone();
        tokio::spawn(async move {
            self_clone.set_new_message(&callback_map_reference, msg.as_ref()).await;
        });
    }

    fn on_service_publisher_new_message(
        self: &Arc<Self>,
        callback_map_reference: &Weak<
            AsyncMutex<HashMap<String, Box<dyn FnMut(&dyn MessageDyn) + Send + Sync>>>,
        >,
        message: &dyn MessageDyn,
    ) {
        let self_clone = self.clone();
        let callback_map_reference = callback_map_reference.clone();
        let message_clone = message.clone_box();
        tokio::spawn(async move {
            self_clone.set_new_message(&callback_map_reference, message_clone.as_ref()).await;
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

    fn create_activating_service_publisher() -> (Arc<PublisherImpl>, Arc<MockServiceAdapter>) {
        let mut service_adapter = MockServiceAdapter::new();
        service_adapter.block_subscribe = true;
        let service_adapter = Arc::new(service_adapter);
        let publisher = create_inactive_service_publisher(Some(service_adapter.clone()));

        // Spawn a task to start activation and leave it blocked in the Activating state.
        let publisher_clone = publisher.clone();
        tokio::spawn(async move {
            publisher_clone.activate().await.unwrap();
        });

        (publisher, service_adapter)
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

    async fn create_deactivating_service_publisher() -> (Arc<PublisherImpl>, Arc<MockServiceAdapter>)
    {
        let mut service_adapter = MockServiceAdapter::new();
        service_adapter.block_unsubscribe = true;
        let service_adapter = Arc::new(service_adapter);
        let publisher = create_active_service_publisher(Some(service_adapter.clone())).await;

        // Spawn a task to start deactivation and leave it blocked in the Deactivating state.
        let publisher_clone = publisher.clone();
        tokio::spawn(async move {
            publisher_clone.deactivate().await.unwrap();
        });

        (publisher, service_adapter)
    }

    async fn create_service_publisher_with_subscription(
        service_adapter: Option<Arc<MockServiceAdapter>>,
    ) -> Arc<PublisherImpl> {
        let publisher = create_active_service_publisher(service_adapter).await;

        publisher.subscribe(DATA_TRIGGER_NAME, Box::new(|_| {})).await.unwrap();
        publisher
    }

    #[tokio::test]
    async fn activate_when_activating_fails() {
        let (publisher, service_adapter) = create_activating_service_publisher();
        service_adapter.notify_subscribing.notified().await;

        let err = publisher.activate().await.err().unwrap();

        assert!(
            matches!(&err, PublisherActivateError::InvalidState { state } if state == PUBLISHER_STATE_ACTIVATING),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn activate_when_active_fails() {
        let publisher = create_active_service_publisher(None).await;

        let err = publisher.activate().await.err().unwrap();

        assert!(
            matches!(&err, PublisherActivateError::InvalidState { state } if state == PUBLISHER_STATE_ACTIVE),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn activate_when_deactivating_fails() {
        let (publisher, service_adapter) = create_deactivating_service_publisher().await;
        service_adapter.notify_unsubscribing.notified().await;

        let err = publisher.activate().await.err().unwrap();

        assert!(
            matches!(&err, PublisherActivateError::InvalidState { state } if state == PUBLISHER_STATE_DEACTIVATING),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn deactivate_when_inactive_fails() {
        let publisher = create_inactive_service_publisher(None);

        let err = publisher.deactivate().await.err().unwrap();

        assert!(
            matches!(&err, PublisherDeactivateError::InvalidState { state } if state == PUBLISHER_STATE_INACTIVE),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn deactivate_when_activating_fails() {
        let (publisher, service_adapter) = create_activating_service_publisher();
        service_adapter.notify_subscribing.notified().await;

        let err = publisher.deactivate().await.err().unwrap();

        assert!(
            matches!(&err, PublisherDeactivateError::InvalidState { state } if state == PUBLISHER_STATE_ACTIVATING),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn deactivate_when_deactivating_fails() {
        let (publisher, service_adapter) = create_deactivating_service_publisher().await;
        service_adapter.notify_unsubscribing.notified().await;

        let err = publisher.deactivate().await.err().unwrap();

        assert!(
            matches!(&err, PublisherDeactivateError::InvalidState { state } if state == PUBLISHER_STATE_DEACTIVATING),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn deactivate_with_subscriptions_fails() {
        let publisher = create_service_publisher_with_subscription(None).await;

        let err = publisher.deactivate().await.err().unwrap();

        assert!(
            matches!(&err, PublisherDeactivateError::CannotDeactivateWithSubscriptions),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn deactivate_service_publisher_calls_unsubscribe() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        let publisher = create_active_service_publisher(Some(service_adapter.clone())).await;

        publisher.deactivate().await.unwrap();

        assert_eq!(service_adapter.get_num_unsubscribe_calls(), 1);
    }

    #[tokio::test]
    async fn deactivate_aggregation_publisher_subscribe_error_fails() {
        let mut trigger1 = MockTrigger::new_for_subscriber(
            TRIGGER_NAME.to_owned(),
            TriggerSubscriber::AggregationPublisher { name: PUBLISHER_NAME.to_owned() },
        );
        trigger1.set_unsubscribe_callback(|_| {
            Some(TriggerUnsubscribeError::NotSubscribed {
                subscriber_name: DATA_TRIGGER_NAME.to_owned(),
            })
        });
        let mut trigger2 = MockTrigger::new_for_subscriber(
            DATA_TRIGGER_NAME.to_owned(),
            TriggerSubscriber::AggregationPublisher { name: PUBLISHER_NAME.to_owned() },
        );
        trigger2.set_unsubscribe_callback(|_| {
            Some(TriggerUnsubscribeError::PublisherUnsubscribeError {
                error: PublisherUnsubscribeError::NotSubscribed {
                    data_trigger_name: DATA_TRIGGER_NAME.to_owned(),
                },
            })
        });
        let publisher =
            create_active_aggregation_publisher(vec![Arc::new(trigger1), Arc::new(trigger2)]).await;

        let err = publisher.deactivate().await.err().unwrap();

        let PublisherDeactivateError::TriggerUnsubscribeErrors { errors } = &err else {
            panic!("Unexpected error: {}", err);
        };
        assert_eq!(errors.len(), 2);
        let (trigger_name, error) = &errors[0];
        assert_eq!(trigger_name, TRIGGER_NAME);
        assert!(
            matches!(error, TriggerUnsubscribeError::NotSubscribed { subscriber_name } if subscriber_name == DATA_TRIGGER_NAME)
        );
        let (trigger_name, error) = &errors[1];
        assert_eq!(trigger_name, DATA_TRIGGER_NAME);
        assert!(matches!(error, TriggerUnsubscribeError::PublisherUnsubscribeError { .. }));
    }

    #[tokio::test]
    async fn subscribe_when_inactive_fails() {
        let publisher = create_inactive_service_publisher(None);

        let err = publisher.subscribe(DATA_TRIGGER_NAME, Box::new(|_| {})).await.err().unwrap();

        assert!(
            matches!(&err, PublisherSubscribeError::PublisherNotActive { state } if state == PUBLISHER_STATE_INACTIVE),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn subscribe_when_activating_fails() {
        let (publisher, service_adapter) = create_activating_service_publisher();
        service_adapter.notify_subscribing.notified().await;

        let err = publisher.subscribe(DATA_TRIGGER_NAME, Box::new(|_| {})).await.err().unwrap();

        assert!(
            matches!(&err, PublisherSubscribeError::PublisherNotActive { state } if state == PUBLISHER_STATE_ACTIVATING),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn subscribe_when_deactivating_fails() {
        let (publisher, service_adapter) = create_deactivating_service_publisher().await;
        service_adapter.notify_unsubscribing.notified().await;

        let err = publisher.subscribe(DATA_TRIGGER_NAME, Box::new(|_| {})).await.err().unwrap();

        assert!(
            matches!(&err, PublisherSubscribeError::PublisherNotActive { state } if state == PUBLISHER_STATE_DEACTIVATING),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn subscribe_already_subscribed_fails() {
        let publisher = create_service_publisher_with_subscription(None).await;

        let err = publisher.subscribe(DATA_TRIGGER_NAME, Box::new(|_| {})).await.err().unwrap();

        assert!(
            matches!(&err, PublisherSubscribeError::DuplicateSubscription { data_trigger_name } if data_trigger_name == DATA_TRIGGER_NAME),
            "Unexpected error: {:?}",
            err
        );
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
            .await
            .unwrap();
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
            .await
            .unwrap();
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
    async fn unsubscribe_when_inactive_fails() {
        let publisher = create_inactive_service_publisher(None);

        let err = publisher.unsubscribe(DATA_TRIGGER_NAME).await.err().unwrap();

        assert!(
            matches!(&err, PublisherUnsubscribeError::PublisherNotActive { state } if state == PUBLISHER_STATE_INACTIVE),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn unsubscribe_when_activating_fails() {
        let (publisher, service_adapter) = create_activating_service_publisher();
        service_adapter.notify_subscribing.notified().await;

        let err = publisher.unsubscribe(DATA_TRIGGER_NAME).await.err().unwrap();

        assert!(
            matches!(&err, PublisherUnsubscribeError::PublisherNotActive { state } if state == PUBLISHER_STATE_ACTIVATING),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn unsubscribe_when_deactivating_fails() {
        let (publisher, service_adapter) = create_deactivating_service_publisher().await;
        service_adapter.notify_unsubscribing.notified().await;

        let err = publisher.unsubscribe(DATA_TRIGGER_NAME).await.err().unwrap();

        assert!(
            matches!(&err, PublisherUnsubscribeError::PublisherNotActive { state } if state == PUBLISHER_STATE_DEACTIVATING),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn unsubscribe_not_subscribed_fails() {
        let publisher = create_active_service_publisher(None).await;

        let err = publisher.unsubscribe(DATA_TRIGGER_NAME).await.err().unwrap();

        assert!(
            matches!(&err, PublisherUnsubscribeError::NotSubscribed { data_trigger_name } if data_trigger_name == DATA_TRIGGER_NAME),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn unsubscribe_success() {
        let publisher = create_service_publisher_with_subscription(None).await;

        publisher.unsubscribe(DATA_TRIGGER_NAME).await.unwrap();
    }
}
