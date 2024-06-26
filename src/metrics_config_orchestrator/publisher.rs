// Copyright 2023 Google LLC. All Rights Reserved.

use async_trait::async_trait;
use log::error;
use protobuf::{well_known_types::any::Any, MessageDyn};
use sdv_telemetry_utils::collection_extension::HashMapExtension;
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::marker::{Send, Sync};
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::sync::Mutex as AsyncMutex;

use crate::expr::ast::NodeEvaluationContext;
use crate::metrics_config_orchestrator::message_builder::MessageBuilder;
use crate::metrics_config_orchestrator::service_adapter::{
    ServiceAdapter, ServiceMessageProvider, ServiceSubscription,
};
use crate::metrics_config_orchestrator::trigger::{Trigger, TriggerSubscriber};

type CallbackFn = Box<dyn FnMut(&dyn MessageDyn, NodeEvaluationContext) + Send + Sync>;

const PUBLISHER_STATE_INACTIVE: &str = "Inactive";
const PUBLISHER_STATE_ACTIVE_GETTER: &str = "Active Getter";
const PUBLISHER_STATE_ACTIVE_SUBSCRIBED: &str = "Active Subscribed";

#[async_trait]
pub(crate) trait Publisher: Send + Sync {
    fn get_latest_message(&self) -> Option<Box<dyn MessageDyn>>;

    async fn activate(self: Arc<Self>) -> Result<(), PublisherActivateError>;
    async fn deactivate(&self) -> Result<(), PublisherDeactivateError>;

    async fn subscribe(&self, data_trigger_name: &str, callback: CallbackFn);
    async fn unsubscribe(&self, data_trigger_name: &str);
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum PublisherActivateError {
    #[error("Service adapter failed to create message provider: {error:#}")]
    ServiceAdapterGetMessageProviderError { error: anyhow::Error },
    #[error("Service adapter failed to process subscription request: {error:#}")]
    ServiceAdapterSubscribeError { error: anyhow::Error },
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub(crate) enum PublisherDeactivateError {
    #[error("Service adapter failed to process unsubscription request: {error:#}")]
    ServiceAdapterUnsubscribeError {
        #[from]
        error: anyhow::Error,
    },
}

pub(crate) struct PublisherImpl {
    pub(super) name: String,
    pub(super) publisher_type: PublisherType,

    latest_message: Mutex<Option<Box<dyn MessageDyn>>>,
    state: AsyncMutex<PublisherState>,
}

pub(super) enum PublisherState {
    /// The publisher is not subscribed to the underlying data source
    /// and doesn't provide the latest message.
    Inactive,
    /// The publisher is not subscribed to the underlying data source,
    /// but provides the latest message on demand.
    ActiveGetter {
        // Provider of the latest message.
        #[allow(dead_code)]
        message_provider: Box<dyn ServiceMessageProvider>,
    },
    /// The publisher is subscribed to the underlying data source
    /// and maintains the latest message.
    ActiveSubscribed {
        // Map of unique data trigger names to the subscribed callbacks.
        callback_map: HashMap<String, CallbackFn>,

        // Subscription to ServiceAdapter.
        // Set for Service publisher only.
        service_subscription: Option<ServiceSubscription>,
    },
}

pub(super) enum PublisherType {
    Service {
        configuration: Option<Any>,
        connection_type: ConnectionType,
        service_name: String,
        service_adapter: Arc<dyn ServiceAdapter + Send + Sync>,
    },
    Aggregation {
        message_builder: Box<dyn MessageBuilder>,
        #[allow(dead_code)]
        reset_on_get: bool,
        trigger_subscriber: TriggerSubscriber,
        triggers: Vec<Arc<dyn Trigger>>,
    },
}

pub(super) enum ConnectionType {
    Subscription,
    Getter,
}

impl Display for PublisherState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            PublisherState::Inactive => f.write_str(PUBLISHER_STATE_INACTIVE),
            PublisherState::ActiveGetter { .. } => f.write_str(PUBLISHER_STATE_ACTIVE_GETTER),
            PublisherState::ActiveSubscribed { .. } => {
                f.write_str(PUBLISHER_STATE_ACTIVE_SUBSCRIBED)
            }
        }
    }
}

#[async_trait]
impl Publisher for PublisherImpl {
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
        let new_state = match &self.publisher_type {
            PublisherType::Aggregation { trigger_subscriber, triggers, .. } => {
                // Subscribe to parent triggers.
                for trigger in triggers {
                    let self_clone = Arc::clone(&self);
                    Arc::clone(trigger)
                        .subscribe(
                            trigger_subscriber,
                            Box::new(move |context| {
                                self_clone.on_aggregation_publisher_trigger_fired(context);
                            }),
                        )
                        .await;
                }

                PublisherState::ActiveSubscribed {
                    callback_map: HashMap::new(),
                    service_subscription: None,
                }
            }
            PublisherType::Service {
                configuration,
                connection_type,
                service_name,
                service_adapter,
            } => {
                let maybe_message_provider = match connection_type {
                    ConnectionType::Getter => {
                        service_adapter.get_message_provider(service_name).map_err(|error| {
                            PublisherActivateError::ServiceAdapterGetMessageProviderError { error }
                        })?
                    }
                    ConnectionType::Subscription => None,
                };

                if let Some(message_provider) = maybe_message_provider {
                    // Don't subscribe to the service adapter and
                    // store message provider to get the latest message.
                    PublisherState::ActiveGetter { message_provider }
                } else {
                    // Subscribe to the service adapter.
                    let self_clone = Arc::clone(&self);
                    let subscription = service_adapter
                        .subscribe(
                            service_name,
                            configuration,
                            Box::new(move |msg| self_clone.on_service_publisher_new_message(msg)),
                        )
                        .await
                        .map_err(|error| PublisherActivateError::ServiceAdapterSubscribeError {
                            error,
                        })?;

                    PublisherState::ActiveSubscribed {
                        callback_map: HashMap::new(),
                        service_subscription: Some(subscription),
                    }
                }
            }
        };

        // Set the new state and release the mutex.
        *state_lock = new_state;

        Ok(())
    }

    async fn deactivate(&self) -> Result<(), PublisherDeactivateError> {
        // Lock state mutex and hold it until the state transition is complete.
        let mut state_lock = self.state.lock().await;
        match &*state_lock {
            PublisherState::ActiveGetter { .. } => {}
            PublisherState::ActiveSubscribed { callback_map, .. } => {
                assert!(
                    callback_map.is_empty(),
                    "Unable to deactivate publisher {} with active subscriptions",
                    self.name
                );
            }
            PublisherState::Inactive => {
                panic!("Unable to deactivate publisher {}: it's inactive", self.name)
            }
        }

        // Perform deactivation logic specific to the publisher type.
        match &self.publisher_type {
            PublisherType::Aggregation { triggers, trigger_subscriber, .. } => {
                // Verify that the publisher is in the subscribed state.
                assert!(
                    matches!(&*state_lock, PublisherState::ActiveSubscribed { .. }),
                    "Unable to deactivate Aggregation publisher {} because it's in {} state",
                    self.name,
                    state_lock
                );

                // Unsubscribe from parent triggers.
                for trigger in triggers {
                    trigger.unsubscribe(trigger_subscriber).await;
                }
            }
            PublisherType::Service { service_adapter, .. } => {
                // Unsubscribe from the service adapter if subscribed.
                if let PublisherState::ActiveSubscribed { service_subscription, .. } =
                    &mut *state_lock
                {
                    if let Some(subscription) = service_subscription.take() {
                        service_adapter.unsubscribe(subscription).await?;
                    } else {
                        panic!("Unable to deactivate publisher {} because it's in an inconsistent state: it's deactivating, but subscription is not set", self.name);
                    }
                }
            }
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
        callback: Box<dyn for<'a> FnMut(&'a dyn MessageDyn, NodeEvaluationContext) + Send + Sync>,
    ) {
        // Lock state mutex and hold it until operation is complete.
        let mut state_lock = self.state.lock().await;

        if let PublisherState::ActiveSubscribed { callback_map, .. } = &mut *state_lock {
            callback_map.try_insert_value(data_trigger_name.to_owned(), callback).unwrap_or_else(|_| panic!("Unable to subscribe to publisher {}: data trigger {} is already subscribed", self.name, data_trigger_name));
        } else {
            panic!("Unable to subscribe to publisher {}: it's in {} state", self.name, state_lock);
        }
    }

    async fn unsubscribe(&self, data_trigger_name: &str) {
        // Lock state mutex and hold it until operation is complete.
        let mut state_lock = self.state.lock().await;

        if let PublisherState::ActiveSubscribed { callback_map, .. } = &mut *state_lock {
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
        configuration: Option<Any>,
        connection_type: ConnectionType,
        service_adapter: Arc<dyn ServiceAdapter + Send + Sync>,
    ) -> Self {
        Self::new(
            name,
            PublisherType::Service {
                configuration,
                connection_type,
                service_name,
                service_adapter,
            },
        )
    }

    async fn set_new_message(
        self: &Arc<Self>,
        message: &dyn MessageDyn,
        context: NodeEvaluationContext,
    ) {
        // Lock state mutex and hold it to make sure the publisher is not deactivated.
        let mut state_lock = self.state.lock().await;

        let PublisherState::ActiveSubscribed { callback_map, .. } = &mut *state_lock else {
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
            callback(message, context.clone());
        }
    }

    fn on_aggregation_publisher_trigger_fired(self: &Arc<Self>, context: NodeEvaluationContext) {
        let PublisherType::Aggregation { message_builder, .. } = &self.publisher_type else {
            panic!("Publisher {} that is not an Aggregation publisher received Aggregation publisher callback", self.name);
        };

        let msg = match message_builder.build(&context) {
            Ok(msg) => msg,
            Err(err) => {
                error!(
                    "Error when trying to evaluate expression for publisher {}: {}",
                    self.name, err
                );
                return;
            }
        };

        let self_clone = Arc::clone(self);
        tokio::spawn(async move {
            self_clone.set_new_message(msg.as_ref(), context).await;
        });
    }

    fn on_service_publisher_new_message(self: &Arc<Self>, message: &dyn MessageDyn) {
        let self_clone = Arc::clone(self);
        let message_clone = message.clone_box();
        let context = NodeEvaluationContext::new();
        tokio::spawn(async move {
            self_clone.set_new_message(message_clone.as_ref(), context).await;
        });
    }
}

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use protobuf::well_known_types::{empty::Empty, wrappers::Int32Value};
    use protobuf::Message;
    use sdv_telemetry_test_proto::test_message::TestMessage;
    use std::sync::Arc;
    use tokio::sync::oneshot;

    use super::*;
    use crate::metrics_config_orchestrator::{
        message_builder::mock::MockMessageBuilder, service_adapter::mock::MockServiceAdapter,
        trigger::mock::MockTrigger,
    };

    const DATA_TRIGGER_NAME: &str = "datatrigger";
    const ERROR_MESSAGE: &str = "Error 42";
    const PUBLISHER_NAME: &str = "publisher";
    const SERVICE_NAME: &str = "service";
    const TRIGGER_NAME: &str = "trigger";

    fn create_inactive_aggregation_publisher(
        triggers: Vec<Arc<dyn Trigger>>,
        message_builder: Box<dyn MessageBuilder>,
    ) -> Arc<PublisherImpl> {
        Arc::new(PublisherImpl::new_aggregation_publisher(
            PUBLISHER_NAME.to_owned(),
            message_builder,
            false,
            triggers,
        ))
    }

    fn create_inactive_getter_service_publisher(
        service_adapter: Arc<MockServiceAdapter>,
    ) -> Arc<PublisherImpl> {
        Arc::new(PublisherImpl::new_service_publisher(
            PUBLISHER_NAME.to_owned(),
            SERVICE_NAME.to_owned(),
            None,
            ConnectionType::Getter,
            service_adapter,
        ))
    }

    fn create_inactive_subscription_service_publisher(
        service_adapter: Option<Arc<MockServiceAdapter>>,
    ) -> Arc<PublisherImpl> {
        Arc::new(PublisherImpl::new_service_publisher(
            PUBLISHER_NAME.to_owned(),
            SERVICE_NAME.to_owned(),
            None,
            ConnectionType::Subscription,
            service_adapter.unwrap_or_else(|| Arc::new(MockServiceAdapter::new())),
        ))
    }

    async fn create_active_aggregation_publisher(
        triggers: Vec<Arc<dyn Trigger>>,
    ) -> Arc<PublisherImpl> {
        let publisher =
            create_inactive_aggregation_publisher(triggers, Box::new(MockMessageBuilder::new()));

        Arc::clone(&publisher).activate().await.unwrap();
        publisher
    }

    async fn create_active_subscribed_service_publisher(
        service_adapter: Option<Arc<MockServiceAdapter>>,
    ) -> Arc<PublisherImpl> {
        let publisher = create_inactive_subscription_service_publisher(service_adapter);

        Arc::clone(&publisher).activate().await.unwrap();
        publisher
    }

    async fn create_service_publisher_with_subscription(
        service_adapter: Option<Arc<MockServiceAdapter>>,
    ) -> Arc<PublisherImpl> {
        let publisher = create_active_subscribed_service_publisher(service_adapter).await;

        publisher.subscribe(DATA_TRIGGER_NAME, Box::new(|_, _| {})).await;
        publisher
    }

    #[tokio::test]
    async fn activate_getter_service_publisher_propagates_adapter_error() {
        let service_adapter =
            Arc::new(MockServiceAdapter::new().with_next_error(anyhow!(ERROR_MESSAGE)));
        let publisher = create_inactive_getter_service_publisher(Arc::clone(&service_adapter));

        let err = publisher.activate().await.unwrap_err();

        assert!(
            matches!(&err, PublisherActivateError::ServiceAdapterGetMessageProviderError { error } if error.to_string() == ERROR_MESSAGE),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn activate_getter_service_publisher_success() {
        let service_adapter = Arc::new(MockServiceAdapter::new().with_next_message_provider());
        let publisher = Arc::new(PublisherImpl::new_service_publisher(
            PUBLISHER_NAME.to_owned(),
            SERVICE_NAME.to_owned(),
            Some(Any::pack(Empty::default_instance()).unwrap()),
            ConnectionType::Getter,
            Arc::<MockServiceAdapter>::clone(&service_adapter),
        ));

        publisher.activate().await.unwrap();

        assert_eq!(service_adapter.get_num_get_message_provider_calls(), 1);
        assert_eq!(service_adapter.get_num_subscribe_calls(), 0);
    }

    #[tokio::test]
    async fn activate_getter_service_publisher_falls_back_to_subscription() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        let publisher = Arc::new(PublisherImpl::new_service_publisher(
            PUBLISHER_NAME.to_owned(),
            SERVICE_NAME.to_owned(),
            Some(Any::pack(Empty::default_instance()).unwrap()),
            ConnectionType::Getter,
            Arc::<MockServiceAdapter>::clone(&service_adapter),
        ));

        publisher.activate().await.unwrap();

        assert_eq!(service_adapter.get_num_get_message_provider_calls(), 1);
        assert_eq!(service_adapter.get_num_subscribe_calls(), 1);
    }

    #[tokio::test]
    async fn activate_subscription_service_publisher_passes_configuration() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        let publisher = Arc::new(PublisherImpl::new_service_publisher(
            PUBLISHER_NAME.to_owned(),
            SERVICE_NAME.to_owned(),
            Some(Any::pack(Empty::default_instance()).unwrap()),
            ConnectionType::Subscription,
            Arc::<MockServiceAdapter>::clone(&service_adapter),
        ));

        publisher.activate().await.unwrap();

        let latest_configuration = service_adapter
            .latest_configuration
            .lock()
            .unwrap()
            .as_ref()
            .expect("Option should be Some")
            .unpack::<Empty>()
            .unwrap()
            .expect("The value of Any should be an Empty");
        assert_eq!(&latest_configuration, Empty::default_instance());
    }

    #[tokio::test]
    #[should_panic]
    async fn activate_when_active_panics() {
        let publisher = create_active_subscribed_service_publisher(None).await;

        publisher.activate().await.unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn deactivate_when_inactive_panics() {
        let publisher = create_inactive_subscription_service_publisher(None);

        publisher.deactivate().await.unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn deactivate_with_subscriptions_panics() {
        let publisher = create_service_publisher_with_subscription(None).await;

        publisher.deactivate().await.unwrap();
    }

    #[tokio::test]
    async fn deactivate_subscribed_service_publisher_calls_unsubscribe() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        let publisher =
            create_active_subscribed_service_publisher(Some(Arc::clone(&service_adapter))).await;

        publisher.deactivate().await.unwrap();

        assert_eq!(service_adapter.get_num_unsubscribe_calls(), 1);
    }

    #[tokio::test]
    #[should_panic]
    async fn subscribe_when_inactive_panics() {
        let publisher = create_inactive_subscription_service_publisher(None);

        publisher.subscribe(DATA_TRIGGER_NAME, Box::new(|_, _| {})).await;
    }

    #[tokio::test]
    #[should_panic]
    async fn subscribe_already_subscribed_panics() {
        let publisher = create_service_publisher_with_subscription(None).await;

        publisher.subscribe(DATA_TRIGGER_NAME, Box::new(|_, _| {})).await;
    }

    #[tokio::test]
    async fn subscribe_service_publisher_new_message_notifies_subscriber() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        let publisher =
            create_active_subscribed_service_publisher(Some(Arc::clone(&service_adapter))).await;
        let (tx, rx) = oneshot::channel();
        let mut maybe_tx = Some(tx);
        let mut msg = Int32Value::new();
        msg.value = 42;

        publisher
            .subscribe(
                DATA_TRIGGER_NAME,
                Box::new(move |message, _context| {
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
        let publisher =
            create_active_aggregation_publisher(vec![Arc::<MockTrigger>::clone(&trigger)]).await;
        let (tx, rx) = oneshot::channel();
        let mut maybe_tx = Some(tx);

        publisher
            .subscribe(
                DATA_TRIGGER_NAME,
                Box::new(move |message, _context| {
                    maybe_tx.take().unwrap().send(message.clone_box()).unwrap()
                }),
            )
            .await;
        trigger.fire().await;

        let messages = [rx.await.unwrap(), publisher.get_latest_message().unwrap()];
        for message in messages {
            let test_message = <dyn MessageDyn>::downcast_box::<TestMessage>(message).unwrap();
            assert_eq!(test_message.int_field, 1);
        }
    }

    #[tokio::test]
    async fn aggregation_publisher_notifies_all_subscriber_with_trigger_context() {
        let trigger = Arc::new(MockTrigger::new_for_subscriber(
            TRIGGER_NAME.to_owned(),
            TriggerSubscriber::AggregationPublisher { name: PUBLISHER_NAME.to_owned() },
        ));
        let publisher =
            create_active_aggregation_publisher(vec![Arc::<MockTrigger>::clone(&trigger)]).await;
        let (context_tx, context_rx) = oneshot::channel();
        let mut context_tx = Some(context_tx);

        publisher
            .subscribe(
                DATA_TRIGGER_NAME,
                Box::new(move |_message, context| {
                    context_tx.take().unwrap().send(context).unwrap()
                }),
            )
            .await;
        let trigger_context = NodeEvaluationContext::new();
        trigger.fire_with_context(trigger_context.clone()).await;

        let context = context_rx.await.unwrap();

        assert_eq!(
            trigger_context, context,
            "Node evaluation context should be equal to the trigger context."
        );
    }

    #[tokio::test]
    #[should_panic]
    async fn unsubscribe_when_inactive_panics() {
        let publisher = create_inactive_subscription_service_publisher(None);

        publisher.unsubscribe(DATA_TRIGGER_NAME).await;
    }

    #[tokio::test]
    #[should_panic]
    async fn unsubscribe_not_subscribed_panics() {
        let publisher = create_active_subscribed_service_publisher(None).await;

        publisher.unsubscribe(DATA_TRIGGER_NAME).await;
    }

    #[tokio::test]
    async fn unsubscribe_success() {
        let publisher = create_service_publisher_with_subscription(None).await;

        publisher.unsubscribe(DATA_TRIGGER_NAME).await;
    }
}
