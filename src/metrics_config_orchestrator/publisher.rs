// Copyright 2023 Google LLC. All Rights Reserved.

pub(super) mod service_adapter;

use async_trait::async_trait;
use log::error;
use protobuf::MessageDyn;
use std::{
    collections::HashMap,
    fmt,
    fmt::{Debug, Display, Formatter},
    marker::{Send, Sync},
    sync::{Arc, Mutex, Weak},
};
use thiserror::Error;
use tokio::sync::Mutex as AsyncMutex;

use crate::collection_extension::HashMapExtension;
use crate::metrics_config_orchestrator::{message_builder::MessageBuilder, TriggerImpl};
use service_adapter::{ServiceAdapter, ServiceAdapterError, ServiceSubscription};

const PUBLISHER_STATE_INACTIVE: &str = "Inactive";
const PUBLISHER_STATE_ACTIVATING: &str = "Activating";
const PUBLISHER_STATE_ACTIVE: &str = "Active";
const PUBLISHER_STATE_DEACTIVATING: &str = "Deactivating";

#[async_trait]
pub(crate) trait Publisher {
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
pub(crate) enum PublisherActivateError {
    #[error("Unable to activate publisher: it's in {state} state")]
    InvalidState { state: String },
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
        error: ServiceAdapterError,
    },
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
        service_adapter: Arc<dyn ServiceAdapter + Send + Sync>,
        service_subscription: Mutex<Option<ServiceSubscription>>,
    },
    Aggregation {
        message_builder: Box<dyn MessageBuilder>,
        reset_on_get: bool,
        triggers: Vec<Arc<TriggerImpl>>,
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

        if let PublisherType::Service { service_name, service_adapter, service_subscription } =
            &self.publisher_type
        {
            if service_subscription.lock().unwrap().is_some() {
                panic!("Unable to activate publisher {} because it is in an inconsistent state: it's activating, but subscription is already set", self.name);
            }

            let callback_map_reference = Arc::downgrade(&callback_map);
            let self_clone = self.clone();
            let subscription = service_adapter
                .subscribe(
                    service_name,
                    Box::new(move |msg| {
                        self_clone.on_service_publisher_new_message(&callback_map_reference, msg)
                    }),
                )
                .await;

            let mut subscription_container = service_subscription.lock().unwrap();
            if subscription_container.is_some() {
                panic!("Unable to activate publisher {} because it is in an inconsistent state: it's activating, but subscription is already set", self.name);
            }
            subscription_container.replace(subscription);
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

        if let PublisherType::Service { service_adapter, service_subscription, .. } =
            &self.publisher_type
        {
            let subscription_container = service_subscription.lock().unwrap().take();
            if let Some(subscription) = subscription_container {
                service_adapter.unsubscribe(subscription).await?;
            } else {
                panic!("Unable to deactivate publisher {} because it's in an inconsistent state: it's deactivating, but subscription is not set", self.name);
            }
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

    pub(super) fn new_service_publisher(
        name: String,
        service_name: String,
        service_adapter: Arc<dyn ServiceAdapter + Send + Sync>,
    ) -> Self {
        Self::new(
            name,
            PublisherType::Service {
                service_name,
                service_adapter,
                service_subscription: None.into(),
            },
        )
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
            // Return if the state has been changed to non-active while the task was pending
            // and the weak reference is no longer valid.
            let callback_map = match callback_map_reference.upgrade() {
                Some(callback_map) => callback_map,
                None => return,
            };

            // Lock map to make sure no subscriptions are added and publisher is not deactivated.
            let mut callback_map_guard = callback_map.lock().await;

            // Replace latest message.
            self_clone.latest_message.lock().unwrap().replace(message_clone.clone_box());

            // Notify subscribed data triggers.
            // TODO(b/294129114): Replace callback_map with just a single callback.
            for callback in callback_map_guard.values_mut() {
                callback(message_clone.as_ref());
            }
        });
    }
}

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
mod tests {
    use protobuf::well_known_types::wrappers::Int32Value;
    use std::{
        future,
        sync::{
            atomic::{AtomicU32, Ordering},
            Arc,
        },
    };
    use tokio::sync::{oneshot, Notify};

    use super::*;

    const DATA_TRIGGER_NAME: &str = "datatrigger";
    const PUBLISHER_NAME: &str = "publisher";
    const SERVICE_NAME: &str = "service";

    struct MockServiceAdapter {
        // Block subscribe call to keep publisher in the Subscribing state.
        block_subscribe: bool,
        // Block unsubscribe call to keep publisher in the Unsubscribing state.
        block_unsubscribe: bool,
        num_unsubscribe_calls: AtomicU32,
        new_message_callback:
            AsyncMutex<Option<Box<dyn for<'a> FnMut(&'a dyn MessageDyn) + Send + Sync>>>,
        notify_subscribing: Arc<Notify>,
        notify_unsubscribing: Arc<Notify>,
    }

    #[async_trait]
    impl ServiceAdapter for MockServiceAdapter {
        async fn subscribe(
            &self,
            _service_name: &str,
            new_message_callback: Box<dyn for<'a> FnMut(&'a dyn MessageDyn) + Send + Sync>,
        ) -> ServiceSubscription {
            self.new_message_callback.lock().await.replace(new_message_callback);
            if self.block_subscribe {
                self.notify_subscribing.notify_one();
                future::pending::<()>().await;
            }
            ServiceSubscription { service_name: String::default(), subscription_id: u32::MAX }
        }

        async fn unsubscribe(
            &self,
            _subscription: ServiceSubscription,
        ) -> Result<(), ServiceAdapterError> {
            self.num_unsubscribe_calls.fetch_add(1, Ordering::Release);
            if self.block_unsubscribe {
                self.notify_unsubscribing.notify_one();
                future::pending::<()>().await;
            }
            Ok(())
        }
    }

    impl MockServiceAdapter {
        fn new() -> Self {
            MockServiceAdapter {
                block_subscribe: false,
                block_unsubscribe: false,
                new_message_callback: None.into(),
                notify_subscribing: Notify::new().into(),
                notify_unsubscribing: Notify::new().into(),
                num_unsubscribe_calls: AtomicU32::new(0),
            }
        }

        async fn publish_new_message(&self, message: &dyn MessageDyn) {
            self.new_message_callback.lock().await.as_mut().unwrap()(message);
        }
    }

    fn create_inactive_service_publisher(
        service_adapter: Option<Arc<MockServiceAdapter>>,
    ) -> Arc<PublisherImpl> {
        Arc::new(PublisherImpl::new_service_publisher(
            PUBLISHER_NAME.to_owned(),
            SERVICE_NAME.to_owned(),
            service_adapter.unwrap_or_else(|| Arc::new(MockServiceAdapter::new())),
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

        assert_eq!(service_adapter.num_unsubscribe_calls.load(Ordering::Acquire), 1);
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
