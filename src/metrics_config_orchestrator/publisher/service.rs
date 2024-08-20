// Copyright 2024 Google LLC

use anyhow::anyhow;
use async_trait::async_trait;
use futures::FutureExt;
use protobuf::{well_known_types::any::Any, MessageDyn};
use sdv_telemetry_utils::{
    collection_extension::HashMapExtension, operation_result::OperationResult,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex as AsyncMutex;

use super::{
    base::BasePublisher, CallbackFn, Publisher, PublisherActivateError, PublisherDeactivateError,
};
use crate::{
    expr::node::NodeEvaluationContext,
    metrics_config_orchestrator::service_adapter::{
        ServiceAdapter, ServiceMessageProvider, ServiceSubscription,
    },
};

pub(crate) enum ConnectionType {
    Subscription,
    Getter,
}

pub(crate) struct ServicePublisher {
    base: BasePublisher,
    state: AsyncMutex<ServicePublisherState>,

    pub(crate) configuration: Option<Any>,
    pub(crate) connection_type: ConnectionType,
    pub(crate) service_name: String,
    service_adapter: Arc<dyn ServiceAdapter + Send + Sync>,
}

#[derive(strum_macros::Display)]
#[strum(serialize_all = "title_case")]
enum ServicePublisherState {
    /// The publisher is not subscribed to the underlying data source
    /// and doesn't provide the latest message.
    Inactive,
    /// The publisher is not subscribed to the underlying data source,
    /// but provides the latest message on demand.
    ActiveGetter {
        // Provider of the latest message.
        message_provider: Arc<dyn ServiceMessageProvider>,
    },
    /// The publisher is subscribed to the underlying data source
    /// and maintains the latest message.
    ActiveSubscribed {
        // Map of unique data trigger names to the subscribed callbacks.
        callback_map: HashMap<String, CallbackFn>,

        // Subscription to ServiceAdapter.
        service_subscription: Option<ServiceSubscription>,
    },
}

#[async_trait]
impl Publisher for ServicePublisher {
    fn get_name(&self) -> &str {
        self.base.get_name()
    }

    fn is_subscribable(&self) -> bool {
        match self.connection_type {
            ConnectionType::Getter => false,
            ConnectionType::Subscription => true,
        }
    }

    fn get_latest_message(
        self: Arc<Self>,
    ) -> OperationResult<'static, Option<Arc<dyn MessageDyn>>, anyhow::Error> {
        if let Some(message) = self.base.get_latest_message() {
            return Ok(Some(message)).into();
        }

        async move {
            let message_provider = match &*self.state.lock().await {
                ServicePublisherState::Inactive => panic!(
                    "Publisher {} is inactive and cannot provide latest message",
                    self.get_name()
                ),
                ServicePublisherState::ActiveGetter { message_provider } => {
                    Arc::clone(message_provider)
                }
                ServicePublisherState::ActiveSubscribed { .. } => return Ok(None),
            };
            Ok(message_provider
                .get_latest_message(self.configuration.as_ref())
                .await?
                .map(|msg| msg.into()))
        }
        .boxed()
        .into()
    }

    async fn activate(self: Arc<Self>) -> Result<(), PublisherActivateError> {
        // Lock state mutex and hold it until the state transition is complete.
        let mut state_lock = self.state.lock().await;
        assert!(
            matches!(*state_lock, ServicePublisherState::Inactive),
            "Unable to activate publisher {}: it's in {} state",
            self.get_name(),
            state_lock
        );

        let maybe_message_provider = match self.connection_type {
            ConnectionType::Getter => {
                self.service_adapter.get_message_provider(&self.service_name).map_err(|error| {
                    PublisherActivateError::ServiceAdapterGetMessageProviderError { error }
                })?
            }
            ConnectionType::Subscription => None,
        };

        let new_state = if let Some(message_provider) = maybe_message_provider {
            // Don't subscribe to the service adapter and store message provider to get the latest
            // message.
            ServicePublisherState::ActiveGetter { message_provider: message_provider.into() }
        } else {
            // Subscribe to the service adapter.
            let self_clone = Arc::clone(&self);
            let maybe_subscription = self
                .service_adapter
                .subscribe(
                    &self.service_name,
                    self.configuration.as_ref(),
                    Box::new(move |msg| self_clone.on_new_message(msg)),
                )
                .await
                .map_err(|error| PublisherActivateError::ServiceAdapterSubscribeError { error })?;

            let Some(subscription) = maybe_subscription else {
                // TODO(b/333138446): Don't fail MetricsConfig activation if publisher is not available.
                return Err(PublisherActivateError::ServiceAdapterSubscribeError {
                    error: anyhow!("Service with name {} is not available", self.service_name),
                });
            };

            ServicePublisherState::ActiveSubscribed {
                callback_map: HashMap::new(),
                service_subscription: Some(subscription),
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
            ServicePublisherState::ActiveGetter { .. } => {}
            ServicePublisherState::ActiveSubscribed { callback_map, .. } => {
                assert!(
                    callback_map.is_empty(),
                    "Unable to deactivate publisher {} with active subscriptions",
                    self.get_name()
                );
            }
            ServicePublisherState::Inactive => {
                panic!("Unable to deactivate publisher {}: it's inactive", self.get_name())
            }
        }

        // Perform deactivation logic specific to the publisher type.
        // Unsubscribe from the service adapter if subscribed.
        if let ServicePublisherState::ActiveSubscribed { service_subscription, .. } =
            &mut *state_lock
        {
            if let Some(subscription) = service_subscription.take() {
                self.service_adapter.unsubscribe(subscription).await?;
            } else {
                panic!("Unable to deactivate publisher {} because it's in an inconsistent state: it's deactivating, but subscription is not set", self.get_name());
            }
        }

        // Reset latest message.
        // The publisher is unsubscribed from underlying data sources, so no callback will change it back to some stale message.
        self.base.reset_latest_message();

        // Change state to Inactive and release the mutex.
        *state_lock = ServicePublisherState::Inactive;

        Ok(())
    }

    async fn subscribe(
        &self,
        data_trigger_name: &str,
        callback: Box<
            dyn for<'a> FnMut(&'a dyn MessageDyn, Arc<NodeEvaluationContext>) + Send + Sync,
        >,
    ) {
        // Lock state mutex and hold it until operation is complete.
        let mut state_lock = self.state.lock().await;

        if let ServicePublisherState::ActiveSubscribed { callback_map, .. } = &mut *state_lock {
            callback_map.try_insert_value(data_trigger_name.to_owned(), callback).unwrap_or_else(|_| panic!("Unable to subscribe to publisher {}: data trigger {} is already subscribed", self.get_name(), data_trigger_name));
        } else {
            panic!(
                "Unable to subscribe to publisher {}: it's in {} state",
                self.get_name(),
                state_lock
            );
        }
    }

    async fn unsubscribe(&self, data_trigger_name: &str) {
        // Lock state mutex and hold it until operation is complete.
        let mut state_lock = self.state.lock().await;

        if let ServicePublisherState::ActiveSubscribed { callback_map, .. } = &mut *state_lock {
            let _ = callback_map.remove(data_trigger_name).unwrap_or_else(|| {
                panic!(
                    "Unable to unsubscribe from publisher {}: data trigger {} wasn't subscribed",
                    self.get_name(),
                    data_trigger_name
                )
            });
        } else {
            panic!(
                "Unable to unsubscribe from publisher {}: it's in {} state",
                self.get_name(),
                state_lock
            );
        }
    }
}

impl ServicePublisher {
    pub(crate) fn new(
        name: String,
        service_name: String,
        configuration: Option<Any>,
        connection_type: ConnectionType,
        service_adapter: Arc<dyn ServiceAdapter + Send + Sync>,
    ) -> Self {
        Self {
            base: BasePublisher::new(name),
            state: ServicePublisherState::Inactive.into(),

            service_name,
            configuration,
            connection_type,
            service_adapter,
        }
    }

    fn on_new_message(self: &Arc<Self>, message: &dyn MessageDyn) {
        // Create context outside of the task, so that the date/time is set as fast as possible,
        // and not just at the moment when this async task should start running.
        let context = Arc::new(NodeEvaluationContext::new());

        let self_clone = Arc::clone(self);
        let message_clone = message.clone_box().into();
        tokio::spawn(async move {
            // Lock state mutex and hold it to make sure the publisher is not deactivated.
            let mut state_lock = self_clone.state.lock().await;

            let ServicePublisherState::ActiveSubscribed { callback_map, .. } = &mut *state_lock
            else {
                panic!(
                    "Publisher {} received callback when it was in the {} state",
                    self_clone.get_name(),
                    state_lock
                );
            };

            self_clone.base.set_new_message(message_clone, &context, callback_map);
        });
    }
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use protobuf::reflect::ReflectEqMode;
    use protobuf::well_known_types::{empty::Empty, wrappers::Int32Value};
    use protobuf::Message;
    use std::sync::Arc;
    use tokio::sync::oneshot;

    use super::*;
    use crate::metrics_config_orchestrator::service_adapter::mock::{
        MockServiceAdapter, MockServiceMessageProvider,
    };

    const DATA_TRIGGER_NAME: &str = "datatrigger";
    const ERROR_MESSAGE: &str = "Error 42";
    const PUBLISHER_NAME: &str = "publisher";
    const SERVICE_NAME: &str = "service";

    fn create_inactive_getter_service_publisher(
        service_adapter: Arc<MockServiceAdapter>,
    ) -> Arc<dyn Publisher> {
        Arc::new(ServicePublisher::new(
            PUBLISHER_NAME.to_owned(),
            SERVICE_NAME.to_owned(),
            None,
            ConnectionType::Getter,
            service_adapter,
        ))
    }

    fn create_inactive_subscription_service_publisher(
        service_adapter: Option<Arc<MockServiceAdapter>>,
    ) -> Arc<dyn Publisher> {
        Arc::new(ServicePublisher::new(
            PUBLISHER_NAME.to_owned(),
            SERVICE_NAME.to_owned(),
            None,
            ConnectionType::Subscription,
            service_adapter.unwrap_or_else(|| Arc::new(MockServiceAdapter::new())),
        ))
    }

    async fn create_active_getter_service_publisher(
        service_adapter: Arc<MockServiceAdapter>,
    ) -> Arc<dyn Publisher> {
        let publisher = create_inactive_getter_service_publisher(service_adapter);

        Arc::clone(&publisher).activate().await.unwrap();
        publisher
    }

    async fn create_active_subscribed_service_publisher(
        service_adapter: Option<Arc<MockServiceAdapter>>,
    ) -> Arc<dyn Publisher> {
        let publisher = create_inactive_subscription_service_publisher(service_adapter);

        Arc::clone(&publisher).activate().await.unwrap();
        publisher
    }

    async fn create_service_publisher_with_subscription(
        service_adapter: Option<Arc<MockServiceAdapter>>,
    ) -> Arc<dyn Publisher> {
        let publisher = create_active_subscribed_service_publisher(service_adapter).await;

        publisher.subscribe(DATA_TRIGGER_NAME, Box::new(|_, _| {})).await;
        publisher
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_getter_propagates_adapter_error() {
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

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_getter_success() {
        let service_adapter = Arc::new(
            MockServiceAdapter::new().with_next_message_provider(MockServiceMessageProvider::new()),
        );
        let publisher = Arc::new(ServicePublisher::new(
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

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_getter_falls_back_to_subscription() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        let publisher = Arc::new(ServicePublisher::new(
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

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_subscription_passes_configuration() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        let publisher = Arc::new(ServicePublisher::new(
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
            .as_ref()
            .expect("Option should be Some")
            .unpack::<Empty>()
            .unwrap()
            .expect("The value of Any should be an Empty");
        assert_eq!(&latest_configuration, Empty::default_instance());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    #[should_panic]
    async fn activate_when_active_panics() {
        let publisher = create_active_subscribed_service_publisher(None).await;

        publisher.activate().await.unwrap();
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    #[should_panic]
    async fn deactivate_when_inactive_panics() {
        let publisher = create_inactive_subscription_service_publisher(None);

        publisher.deactivate().await.unwrap();
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    #[should_panic]
    async fn deactivate_with_subscriptions_panics() {
        let publisher = create_service_publisher_with_subscription(None).await;

        publisher.deactivate().await.unwrap();
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn deactivate_subscribed_calls_unsubscribe() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        let publisher =
            create_active_subscribed_service_publisher(Some(Arc::clone(&service_adapter))).await;

        publisher.deactivate().await.unwrap();

        assert_eq!(service_adapter.get_num_unsubscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    #[should_panic]
    async fn subscribe_when_inactive_panics() {
        let publisher = create_inactive_subscription_service_publisher(None);

        publisher.subscribe(DATA_TRIGGER_NAME, Box::new(|_, _| {})).await;
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    #[should_panic]
    async fn subscribe_already_subscribed_panics() {
        let publisher = create_service_publisher_with_subscription(None).await;

        publisher.subscribe(DATA_TRIGGER_NAME, Box::new(|_, _| {})).await;
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn subscribe_new_message_notifies_subscriber() {
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

        let received_message = rx.await.unwrap();
        let latest_message = publisher.get_latest_message().unwrap_sync().unwrap().unwrap();
        let expected_message = Box::new(msg) as Box<dyn MessageDyn>;
        assert_eq!(&received_message, &expected_message);
        assert!(
            latest_message.reflect_eq_dyn(expected_message.as_ref(), &ReflectEqMode::default()),
            "Expected {latest_message:?} to equal {expected_message:?}"
        );
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_latest_message_calls_adapter() {
        let message = Box::new(Int32Value { value: 42, ..Default::default() });
        let mut message_provider = MockServiceMessageProvider::new();
        let message_clone = message.clone();
        message_provider.set_message_provider(move || message_clone.clone());
        let service_adapter =
            Arc::new(MockServiceAdapter::new().with_next_message_provider(message_provider));
        let publisher = create_active_getter_service_publisher(service_adapter).await;

        let latest_message =
            Arc::clone(&publisher).get_latest_message().unwrap_async().await.unwrap().unwrap();

        assert!(
            latest_message.reflect_eq_dyn(message.as_ref(), &ReflectEqMode::default()),
            "Expected {latest_message:?} to equal {message:?}"
        );
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_latest_message_propagates_provider_error() {
        const ERROR_STRING: &str = "Error 42";
        let message_provider =
            MockServiceMessageProvider::new().with_next_error(anyhow!(ERROR_STRING));
        let service_adapter =
            Arc::new(MockServiceAdapter::new().with_next_message_provider(message_provider));
        let publisher = create_active_getter_service_publisher(service_adapter).await;

        let err = publisher.get_latest_message().unwrap_async().await.unwrap_err();

        assert!(err.to_string().contains(ERROR_STRING), "Unexpected error: {:?}", err);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    #[should_panic]
    async fn unsubscribe_when_inactive_panics() {
        let publisher = create_inactive_subscription_service_publisher(None);

        publisher.unsubscribe(DATA_TRIGGER_NAME).await;
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    #[should_panic]
    async fn unsubscribe_not_subscribed_panics() {
        let publisher = create_active_subscribed_service_publisher(None).await;

        publisher.unsubscribe(DATA_TRIGGER_NAME).await;
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn unsubscribe_success() {
        let publisher = create_service_publisher_with_subscription(None).await;

        publisher.unsubscribe(DATA_TRIGGER_NAME).await;
    }
}
