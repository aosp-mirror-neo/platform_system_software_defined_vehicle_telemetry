// Copyright 2024 Google LLC

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use either::Either;
use futures::FutureExt;
use log::{debug, info, warn};
use parking_lot::Mutex;
use protobuf::{well_known_types::any::Any, MessageDyn};
use sdv_telemetry_utils::{
    collection_extension::HashMapExtension, operation_result::OperationResult,
};
use std::sync::{Arc, Weak};
use std::{collections::HashMap, mem};
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;

use super::{base::BasePublisher, CallbackFn, Publisher};
use crate::expr::node_evaluation_context::NodeEvaluationContext;
use crate::metrics_config_orchestrator::async_event_processor::{
    AsyncEventHandler, AsyncEventProcessor, AsyncReceiver,
};
use crate::metrics_config_orchestrator::service_adapter::{
    GetMessageProviderOutcome, ServiceAdapter, ServiceMessageProvider, ServiceSubscription,
};
use crate::metrics_config_runtime_error::{
    ServicePublisherRuntimeError, ServicePublisherRuntimeErrorKind,
};
use crate::service_publisher_accessor::{
    AvailabilityCallbackRegistration, CreateSubscriptionError, GetLatestMessageError,
    GetMessageProviderError,
};
use crate::sub_sampling_interval::SubSamplingInterval;

pub(crate) enum ConnectionType {
    Subscription,
    Getter,
}

pub(crate) struct ServicePublisher {
    pub(crate) configuration: Option<Any>,
    pub(crate) sub_sampling_interval: SubSamplingInterval,
    pub(crate) connection_type: ConnectionType,
    pub(crate) service_name: String,

    base: BasePublisher,
    service_adapter: Arc<dyn ServiceAdapter + Send + Sync>,
    state: AsyncMutex<ServicePublisherState>,
    error_callback: Arc<dyn Fn(ServicePublisherRuntimeError) + Send + Sync>,
}

#[derive(strum_macros::Display)]
#[strum(serialize_all = "title_case")]
enum ServicePublisherState {
    /// The publisher is not subscribed to the underlying data source
    /// and doesn't provide the latest message.
    Inactive,
    /// The publisher attempted activation, but service wasn't available.
    /// It will retry activation after timeout.
    Pending {
        // Registration to service availability notifications.
        availability_registration: Arc<Mutex<Option<Box<dyn AvailabilityCallbackRegistration>>>>,

        // Map of unique data trigger names to the subscribed callbacks.
        callback_map: HashMap<String, CallbackFn>,
    },
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

        // Processor of incoming messages.
        _event_processor: AsyncEventProcessor,

        // Subscription to ServiceAdapter.
        service_subscription: ServiceSubscription,
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
    ) -> OperationResult<'static, Option<Arc<dyn MessageDyn>>, GetLatestMessageError> {
        if let Some(message) = self.base.get_latest_message() {
            return Ok(Some(message)).into();
        }

        async move {
            let message_provider = match &*self.state.lock().await {
                ServicePublisherState::Inactive => panic!(
                    "Publisher {} is inactive and cannot provide latest message",
                    self.get_name()
                ),
                ServicePublisherState::Pending { .. } => return Ok(None),
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

    async fn activate(self: Arc<Self>) -> anyhow::Result<()> {
        // Lock state mutex and hold it until the state transition is complete.
        let mut state_lock = self.state.lock().await;
        assert!(
            matches!(*state_lock, ServicePublisherState::Inactive),
            "Unable to activate publisher {}: it's in {} state",
            self.get_name(),
            state_lock
        );

        let new_state = if let Some(state) = self.try_activate().await? {
            state
        } else {
            info!(
                "Publisher {} activation wasn't able to discover the service. \
                Subscribing to availability notifications",
                self.get_name()
            );

            let registration_container = Arc::new(Mutex::new(None));
            let registration_container_clone = Arc::clone(&registration_container);
            let self_clone = Arc::clone(&self);
            let maybe_registration = self
                .service_adapter
                .register_availability_callback(
                    &self.service_name,
                    Box::new(move || {
                        let registration_container_clone =
                            Arc::clone(&registration_container_clone);
                        let self_clone = Arc::clone(&self_clone);
                        tokio::spawn(async move {
                            self_clone.retry_activation(registration_container_clone).await;
                        });
                    }),
                )
                .await
                .with_context(|| "Service adapter failed to subscribe to service availability")?;
            let Some(registration) = maybe_registration else {
                return Err(anyhow!(
                    "Service with name {} is not supported by underlying data providers",
                    self.service_name
                ));
            };

            assert!(registration_container.lock().replace(registration).is_none());

            ServicePublisherState::Pending {
                availability_registration: registration_container,
                callback_map: HashMap::new(),
            }
        };

        // Set the new state and release the mutex.
        *state_lock = new_state;
        Ok(())
    }

    async fn deactivate(&self) -> anyhow::Result<()> {
        // Lock state mutex and hold it until the state transition is complete.
        let mut state_lock = self.state.lock().await;
        let old_state = mem::replace(&mut *state_lock, ServicePublisherState::Inactive);

        match old_state {
            ServicePublisherState::Inactive => {
                panic!("Unable to deactivate publisher {}: it's inactive", self.get_name())
            }
            ServicePublisherState::Pending { availability_registration, .. } => {
                let registration = availability_registration
                    .lock()
                    .take()
                    .expect("Valid pending state should contain availability registration");
                registration.unregister().await
            }
            ServicePublisherState::ActiveGetter { .. } => {}
            ServicePublisherState::ActiveSubscribed {
                callback_map, service_subscription, ..
            } => {
                assert!(
                    callback_map.is_empty(),
                    "Unable to deactivate publisher {} with active subscriptions",
                    self.get_name()
                );

                self.service_adapter
                    .unsubscribe(service_subscription)
                    .await
                    .with_context(|| "Service adapter failed to process unsubscription request")?;
            }
        }

        // Reset latest message.
        // The publisher is unsubscribed from underlying data sources, so no callback will change it back to some stale message.
        self.base.reset_latest_message();

        Ok(())
    }

    async fn subscribe(&self, data_trigger_name: &str, callback: CallbackFn) {
        if !self.is_subscribable() {
            panic!("Unable to subscribe to publisher {}: it's not subscribable", self.get_name());
        }

        // Lock state mutex and hold it until operation is complete.
        let mut state_lock = self.state.lock().await;

        let callback_map = match &mut *state_lock {
            ServicePublisherState::Inactive => {
                panic!("Unable to subscribe to publisher {}: it's inactive", self.get_name())
            }
            ServicePublisherState::Pending { callback_map, .. }
            | ServicePublisherState::ActiveSubscribed { callback_map, .. } => callback_map,
            ServicePublisherState::ActiveGetter { .. } => {
                panic!("Getter publisher {} doesn't support subscription.", self.get_name())
            }
        };

        callback_map.try_insert_value(data_trigger_name.to_owned(), callback).unwrap_or_else(
            |_| {
                panic!(
                    "Unable to subscribe to publisher {}: data trigger {} is already subscribed",
                    self.get_name(),
                    data_trigger_name
                )
            },
        );
    }

    async fn unsubscribe(&self, data_trigger_name: &str) {
        if !self.is_subscribable() {
            panic!(
                "Unable to unsubscribe from publisher {}: it's not subscribable",
                self.get_name()
            );
        }

        // Lock state mutex and hold it until operation is complete.
        let mut state_lock = self.state.lock().await;

        let callback_map = match &mut *state_lock {
            ServicePublisherState::Inactive => {
                panic!("Unable to unsubscribe from publisher {}: it's inactive", self.get_name())
            }
            ServicePublisherState::Pending { callback_map, .. }
            | ServicePublisherState::ActiveSubscribed { callback_map, .. } => callback_map,
            ServicePublisherState::ActiveGetter { .. } => {
                panic!("Getter publisher {} doesn't support unsubscription.", self.get_name())
            }
        };

        let _ = callback_map.remove(data_trigger_name).unwrap_or_else(|| {
            panic!(
                "Unable to unsubscribe from publisher {}: data trigger {} wasn't subscribed",
                self.get_name(),
                data_trigger_name
            )
        });
    }
}

impl ServicePublisher {
    pub(crate) fn new(
        name: String,
        service_name: String,
        configuration: Option<Any>,
        sub_sampling_interval: SubSamplingInterval,
        connection_type: ConnectionType,
        service_adapter: Arc<dyn ServiceAdapter + Send + Sync>,
        error_callback: Arc<dyn Fn(ServicePublisherRuntimeError) + Send + Sync>,
    ) -> Self {
        Self {
            base: BasePublisher::new(name),
            configuration,
            sub_sampling_interval,
            connection_type,
            service_adapter,
            service_name,
            state: ServicePublisherState::Inactive.into(),
            error_callback,
        }
    }

    /// Moves callbacks from pending state callback map if the new state after Pending supports subscriptions.
    fn maybe_swap_callback_map(
        new_state: &mut ServicePublisherState,
        pending_callback_map: &mut HashMap<String, CallbackFn>,
    ) {
        match new_state {
            ServicePublisherState::Inactive => {
                panic!("Unexpected change of status from Pending to Inactive")
            }
            ServicePublisherState::Pending { .. } => {
                panic!("Unexpected change of status from Pending to Pending")
            }
            ServicePublisherState::ActiveGetter { .. } => {}
            ServicePublisherState::ActiveSubscribed { callback_map, .. } => {
                mem::swap(callback_map, pending_callback_map)
            }
        }
    }

    /// Retries publisher activation.
    async fn retry_activation(
        self: &Arc<Self>,
        availability_registration: Arc<Mutex<Option<Box<dyn AvailabilityCallbackRegistration>>>>,
    ) {
        // Lock state mutex and hold it until operation is complete.
        let mut state_lock = self.state.lock().await;

        let Some(registration) = availability_registration.lock().take() else {
            // Abort retry operation because either retry has already been processed,
            // or Publisher state is no longer Pending.
            return;
        };

        match &mut *state_lock {
            ServicePublisherState::Inactive => {
                panic!("Publisher {} state is Inactive, but registration wasn't taken", self.get_name());
            },
            ServicePublisherState::Pending { callback_map, .. } => {
                match self.try_activate().await {
                    Ok(Some(mut new_state)) => {
                        registration.unregister().await;
                        Self::maybe_swap_callback_map(&mut new_state, callback_map);
                        debug!(
                            "Publisher {} activation retry was successful and it's now {}",
                            self.get_name(), new_state
                        );
                        *state_lock = new_state;
                        return;
                    },
                    Ok(None) => {
                        warn!(
                            "Publisher {} activation retry wasn't able to discover the service,\
                            despite receiving availability notification",
                            self.get_name()
                        );
                    }
                    Err(err) => {
                        (self.error_callback)(ServicePublisherRuntimeError {
                            publisher_name: self.get_name().to_owned(),
                            error: ServicePublisherRuntimeErrorKind::RetryActivationFailed(err)
                        });
                    }
                }
            },
            ServicePublisherState::ActiveGetter { .. } =>
                panic!("Unable to retry activation of publisher {} due to invalid state: getter publisher is already active",
                    self.get_name()),
            ServicePublisherState::ActiveSubscribed { .. } =>
                panic!("Unable to retry activation of publisher {} due to invalid state: publisher is already subscribed",
                    self.get_name()),
        }

        // Activation retry was unsuccessful, so keep availability registration active.
        assert!(availability_registration.lock().replace(registration).is_none());
    }

    /// Attempts to activate service publisher and returns the new state if activation was successful.
    async fn try_activate(
        self: &Arc<Self>,
    ) -> Result<
        Option<ServicePublisherState>,
        Either<GetMessageProviderError, CreateSubscriptionError>,
    > {
        let outcome = match self.connection_type {
            ConnectionType::Getter => self
                .service_adapter
                .get_message_provider(&self.service_name)
                .await
                .map_err(Either::Left)?,
            ConnectionType::Subscription => GetMessageProviderOutcome::GetNotSupported,
        };

        let new_state = match outcome {
            GetMessageProviderOutcome::Success(message_provider) => {
                // Don't subscribe to the service adapter and store message provider to get the
                // latest message.
                Some(ServicePublisherState::ActiveGetter {
                    message_provider: message_provider.into(),
                })
            }
            GetMessageProviderOutcome::NotRegistered => None,
            GetMessageProviderOutcome::GetNotSupported => {
                // Subscribe to the service adapter.
                let (sender, receiver) = mpsc::unbounded_channel();
                let maybe_subscription = self
                    .service_adapter
                    .subscribe(
                        &self.service_name,
                        self.configuration.as_ref(),
                        &self.sub_sampling_interval,
                        Box::new(move |msg| {
                            sender
                                .send(Arc::clone(msg))
                                .expect("Callback should only receive messages when subscribed")
                        }),
                        Box::new(|| {}),
                    )
                    .await
                    .map_err(Either::Right)?;

                struct ServicePublisherHandler {
                    service_publisher: Weak<ServicePublisher>,
                }

                #[async_trait]
                impl AsyncEventHandler<Arc<dyn MessageDyn>> for ServicePublisherHandler {
                    async fn handle(&mut self, message: Arc<dyn MessageDyn>) {
                        let Some(service_publisher) = self.service_publisher.upgrade() else {
                            return;
                        };
                        service_publisher.on_new_message(message).await;
                    }
                }

                let event_processor = AsyncEventProcessor::new(
                    AsyncReceiver::Unbounded(receiver),
                    ServicePublisherHandler { service_publisher: Arc::downgrade(self) },
                );
                maybe_subscription.map(|service_subscription| {
                    ServicePublisherState::ActiveSubscribed {
                        callback_map: HashMap::new(),
                        _event_processor: event_processor,
                        service_subscription,
                    }
                })
            }
        };
        Ok(new_state)
    }

    async fn on_new_message(&self, message: Arc<dyn MessageDyn>) {
        // Create context outside of the lock, so that the date/time is set as fast as possible,
        // and not just at the moment when the lock is obtained.
        let context = Arc::new(NodeEvaluationContext::with_publisher_message(
            self.get_name().to_owned(),
            Arc::clone(&message),
        ));

        // Lock state mutex and hold it to make sure the publisher is not deactivated.
        let mut state_lock = self.state.lock().await;

        let ServicePublisherState::ActiveSubscribed { callback_map, .. } = &mut *state_lock else {
            return;
        };

        self.base.set_new_message(message, &context, callback_map);
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
    ) -> Arc<ServicePublisher> {
        Arc::new(ServicePublisher::new(
            PUBLISHER_NAME.to_owned(),
            SERVICE_NAME.to_owned(),
            None,
            SubSamplingInterval::ZERO,
            ConnectionType::Getter,
            service_adapter,
            Arc::new(|err| panic!("Unexpected error: {err:?}")),
        ))
    }

    fn create_inactive_subscription_service_publisher(
        service_adapter: Option<Arc<MockServiceAdapter>>,
    ) -> Arc<ServicePublisher> {
        Arc::new(ServicePublisher::new(
            PUBLISHER_NAME.to_owned(),
            SERVICE_NAME.to_owned(),
            None,
            SubSamplingInterval::ZERO,
            ConnectionType::Subscription,
            service_adapter.unwrap_or_else(|| Arc::new(MockServiceAdapter::new())),
            Arc::new(|err| panic!("Unexpected error: {err:?}")),
        ))
    }

    async fn create_active_getter_service_publisher(
        service_adapter: Arc<MockServiceAdapter>,
    ) -> Arc<ServicePublisher> {
        let publisher = create_inactive_getter_service_publisher(service_adapter);

        Arc::clone(&publisher).activate().await.unwrap();
        publisher
    }

    async fn create_active_subscribed_service_publisher(
        service_adapter: Option<Arc<MockServiceAdapter>>,
    ) -> Arc<ServicePublisher> {
        let service_adapter = service_adapter.unwrap_or_else(|| {
            let service_adapter = Arc::new(MockServiceAdapter::new());
            service_adapter.set_next_subscribe_successful();
            service_adapter
        });
        let publisher = create_inactive_subscription_service_publisher(Some(service_adapter));

        Arc::clone(&publisher).activate().await.unwrap();
        publisher
    }

    async fn create_service_publisher_with_subscription(
        service_adapter: Option<Arc<MockServiceAdapter>>,
    ) -> Arc<ServicePublisher> {
        let publisher = create_active_subscribed_service_publisher(service_adapter).await;

        publisher.subscribe(DATA_TRIGGER_NAME, create_empty_fn_box()).await;
        publisher
    }

    /// Creates a Box with a callback that accepts context and does nothing.
    pub fn create_empty_fn_box() -> CallbackFn {
        Box::new(|_| {})
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_getter_propagates_adapter_error() {
        let service_adapter =
            Arc::new(MockServiceAdapter::new().with_next_error(anyhow!(ERROR_MESSAGE)));
        let publisher = create_inactive_getter_service_publisher(Arc::clone(&service_adapter));

        let err = publisher.activate().await.unwrap_err();

        assert_eq!(
            err.chain().map(|err| err.to_string()).collect::<Vec<_>>(),
            vec![format!("failed to parse metadata: {ERROR_MESSAGE}")]
        );
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_getter_success() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        service_adapter.set_next_message_provider(MockServiceMessageProvider::new());
        let publisher = create_inactive_getter_service_publisher(Arc::clone(&service_adapter));

        Arc::clone(&publisher).activate().await.unwrap();

        assert!(matches!(
            *publisher.state.lock().await,
            ServicePublisherState::ActiveGetter { .. }
        ));
        assert_eq!(service_adapter.get_num_get_message_provider_calls(), 1);
        assert_eq!(service_adapter.get_num_subscribe_calls(), 0);
        assert_eq!(service_adapter.get_num_register_availability_callback_calls(), 0);
        assert_eq!(service_adapter.get_num_unregister_availability_callback_calls(), 0);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_getter_falls_back_to_subscription() {
        let service_adapter =
            Arc::new(MockServiceAdapter::new().with_next_message_provider_get_not_supported());
        service_adapter.set_next_subscribe_successful();
        let publisher = create_inactive_getter_service_publisher(Arc::clone(&service_adapter));

        Arc::clone(&publisher).activate().await.unwrap();

        let state = publisher.state.lock().await;
        assert!(
            matches!(*state, ServicePublisherState::ActiveSubscribed { .. }),
            "Unexpected state: {}",
            *state
        );
        assert_eq!(service_adapter.get_num_get_message_provider_calls(), 1);
        assert_eq!(service_adapter.get_num_subscribe_calls(), 1);
        assert_eq!(service_adapter.get_num_register_availability_callback_calls(), 0);
        assert_eq!(service_adapter.get_num_unregister_availability_callback_calls(), 0);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_getter_pending_if_not_found() {
        let service_adapter = Arc::new(MockServiceAdapter::new().with_availability_callback());
        let publisher = create_inactive_getter_service_publisher(Arc::clone(&service_adapter));

        Arc::clone(&publisher).activate().await.unwrap();

        let state = publisher.state.lock().await;
        assert!(
            matches!(*state, ServicePublisherState::Pending { .. }),
            "Unexpected state: {}",
            *state
        );
        assert_eq!(service_adapter.get_num_get_message_provider_calls(), 1);
        assert_eq!(service_adapter.get_num_subscribe_calls(), 0);
        assert_eq!(service_adapter.get_num_register_availability_callback_calls(), 1);
        assert_eq!(service_adapter.get_num_unregister_availability_callback_calls(), 0);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_getter_retries_if_not_found() {
        let service_adapter = Arc::new(MockServiceAdapter::new().with_availability_callback());
        let publisher = Arc::new(ServicePublisher::new(
            PUBLISHER_NAME.to_owned(),
            SERVICE_NAME.to_owned(),
            None,
            SubSamplingInterval::ZERO,
            ConnectionType::Getter,
            Arc::<MockServiceAdapter>::clone(&service_adapter),
            Arc::new(|err| panic!("Unexpected error: {err:?}")),
        ));

        Arc::clone(&publisher).activate().await.unwrap();

        // Fire availability callback when service is still not available.
        service_adapter.call_availability_callback();
        tokio::task::yield_now().await;

        // Fire availability callback when service is available.
        let call_receiver =
            service_adapter.set_next_message_provider(MockServiceMessageProvider::new());
        service_adapter.call_availability_callback();
        call_receiver.await.unwrap();

        let state = publisher.state.lock().await;
        assert!(
            matches!(*state, ServicePublisherState::ActiveGetter { .. }),
            "Unexpected state: {}",
            *state
        );
        assert!(
            service_adapter.get_num_get_message_provider_calls() > 1,
            "Unexpected number of get_message_provider calls: {}",
            service_adapter.get_num_get_message_provider_calls()
        );
        assert_eq!(service_adapter.get_num_subscribe_calls(), 0);
        assert_eq!(service_adapter.get_num_register_availability_callback_calls(), 1);
        assert_eq!(service_adapter.get_num_unregister_availability_callback_calls(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_getter_with_retries_unsubscribes_on_deactivate() {
        let service_adapter = Arc::new(MockServiceAdapter::new().with_availability_callback());
        let publisher = Arc::new(ServicePublisher::new(
            PUBLISHER_NAME.to_owned(),
            SERVICE_NAME.to_owned(),
            None,
            SubSamplingInterval::ZERO,
            ConnectionType::Getter,
            Arc::<MockServiceAdapter>::clone(&service_adapter),
            Arc::new(|err| panic!("Unexpected error: {err:?}")),
        ));

        Arc::clone(&publisher).activate().await.unwrap();
        publisher.deactivate().await.unwrap();

        // Ensure availability callback is ignored.
        service_adapter.call_availability_callback();
        tokio::task::yield_now().await;

        let state = publisher.state.lock().await;
        assert!(matches!(*state, ServicePublisherState::Inactive), "Unexpected state: {}", *state);
        assert_eq!(service_adapter.get_num_get_message_provider_calls(), 1);
        assert_eq!(service_adapter.get_num_subscribe_calls(), 0);
        assert_eq!(service_adapter.get_num_register_availability_callback_calls(), 1);
        assert_eq!(service_adapter.get_num_unregister_availability_callback_calls(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_subscription_success() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        service_adapter.set_next_subscribe_successful();
        let publisher = Arc::new(ServicePublisher::new(
            PUBLISHER_NAME.to_owned(),
            SERVICE_NAME.to_owned(),
            Some(Any::pack(Empty::default_instance()).unwrap()),
            SubSamplingInterval::ZERO,
            ConnectionType::Subscription,
            Arc::<MockServiceAdapter>::clone(&service_adapter),
            Arc::new(|err| panic!("Unexpected error: {err:?}")),
        ));

        Arc::clone(&publisher).activate().await.unwrap();

        let state = publisher.state.lock().await;
        assert!(
            matches!(*state, ServicePublisherState::ActiveSubscribed { .. }),
            "Unexpected state: {}",
            *state
        );
        let latest_configuration = service_adapter
            .latest_configuration
            .lock()
            .as_ref()
            .expect("Option should be Some")
            .unpack::<Empty>()
            .unwrap()
            .expect("The value of Any should be an Empty");
        assert_eq!(&latest_configuration, Empty::default_instance());
        assert_eq!(service_adapter.get_num_get_message_provider_calls(), 0);
        assert_eq!(service_adapter.get_num_subscribe_calls(), 1);
        assert_eq!(service_adapter.get_num_register_availability_callback_calls(), 0);
        assert_eq!(service_adapter.get_num_unregister_availability_callback_calls(), 0);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_subscription_pending_if_not_found() {
        let service_adapter = Arc::new(MockServiceAdapter::new().with_availability_callback());
        let publisher =
            create_inactive_subscription_service_publisher(Some(Arc::clone(&service_adapter)));

        Arc::clone(&publisher).activate().await.unwrap();

        let state = publisher.state.lock().await;
        assert!(
            matches!(*state, ServicePublisherState::Pending { .. }),
            "Unexpected state: {}",
            *state
        );
        assert_eq!(service_adapter.get_num_get_message_provider_calls(), 0);
        assert_eq!(service_adapter.get_num_subscribe_calls(), 1);
        assert_eq!(service_adapter.get_num_register_availability_callback_calls(), 1);
        assert_eq!(service_adapter.get_num_unregister_availability_callback_calls(), 0);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_subscription_retries_if_not_found() {
        let service_adapter = Arc::new(MockServiceAdapter::new().with_availability_callback());
        let publisher = Arc::new(ServicePublisher::new(
            PUBLISHER_NAME.to_owned(),
            SERVICE_NAME.to_owned(),
            None,
            SubSamplingInterval::ZERO,
            ConnectionType::Subscription,
            Arc::<MockServiceAdapter>::clone(&service_adapter),
            Arc::new(|err| panic!("Unexpected error: {err:?}")),
        ));

        Arc::clone(&publisher).activate().await.unwrap();

        let (tx, rx) = oneshot::channel();
        let mut maybe_tx = Some(tx);
        let msg = Arc::new(Int32Value { value: 42, ..Default::default() }) as Arc<dyn MessageDyn>;
        publisher
            .subscribe(
                DATA_TRIGGER_NAME,
                Box::new(move |context| {
                    let msg = context
                        .get_message(PUBLISHER_NAME)
                        .expect("Publisher message should be present in the context");
                    maybe_tx.take().unwrap().send(Arc::clone(msg)).unwrap()
                }),
            )
            .await;

        let subscribe_receiver = service_adapter.set_next_subscribe_successful();
        service_adapter.call_availability_callback();
        subscribe_receiver.await.unwrap();

        service_adapter.publish_new_message(&msg).await;
        let received_message = rx.await.unwrap();

        assert!(
            Arc::ptr_eq(&received_message, &msg),
            "Expected {received_message:?} to equal {msg:?}"
        );
        let state = publisher.state.lock().await;
        assert!(
            matches!(*state, ServicePublisherState::ActiveSubscribed { .. }),
            "Unexpected state: {}",
            *state
        );
        assert_eq!(service_adapter.get_num_get_message_provider_calls(), 0);
        assert!(
            service_adapter.get_num_subscribe_calls() > 1,
            "Unexpected number of subscribe calls: {}",
            service_adapter.get_num_subscribe_calls()
        );
        assert_eq!(service_adapter.get_num_register_availability_callback_calls(), 1);
        assert_eq!(service_adapter.get_num_unregister_availability_callback_calls(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_subscription_no_availability_callback_fails() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        let publisher = Arc::new(ServicePublisher::new(
            PUBLISHER_NAME.to_owned(),
            SERVICE_NAME.to_owned(),
            None,
            SubSamplingInterval::ZERO,
            ConnectionType::Subscription,
            Arc::<MockServiceAdapter>::clone(&service_adapter),
            Arc::new(|err| panic!("Unexpected error: {err:?}")),
        ));

        let err = Arc::clone(&publisher).activate().await.err().unwrap();

        assert!(
            err.to_string().contains("is not supported by underlying data providers"),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(service_adapter.get_num_register_availability_callback_calls(), 1);
        assert_eq!(service_adapter.get_num_unregister_availability_callback_calls(), 0);
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
    async fn deactivate_pending_success() {
        let service_adapter = Arc::new(MockServiceAdapter::new().with_availability_callback());
        let publisher =
            create_active_subscribed_service_publisher(Some(Arc::clone(&service_adapter))).await;

        publisher.deactivate().await.unwrap();

        let state = publisher.state.lock().await;
        assert!(matches!(*state, ServicePublisherState::Inactive), "Unexpected state: {}", *state);
        assert_eq!(service_adapter.get_num_unsubscribe_calls(), 0);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn deactivate_subscribed_calls_unsubscribe() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        service_adapter.set_next_subscribe_successful();
        let publisher =
            create_active_subscribed_service_publisher(Some(Arc::clone(&service_adapter))).await;

        publisher.deactivate().await.unwrap();

        let state = publisher.state.lock().await;
        assert!(matches!(*state, ServicePublisherState::Inactive), "Unexpected state: {}", *state);
        assert_eq!(service_adapter.get_num_unsubscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    #[should_panic]
    async fn subscribe_when_inactive_panics() {
        let publisher = create_inactive_subscription_service_publisher(None);

        publisher.subscribe(DATA_TRIGGER_NAME, create_empty_fn_box()).await;
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    #[should_panic]
    async fn subscribe_already_subscribed_panics() {
        let publisher = create_service_publisher_with_subscription(None).await;

        publisher.subscribe(DATA_TRIGGER_NAME, create_empty_fn_box()).await;
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn subscribe_new_message_notifies_subscriber() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        service_adapter.set_next_subscribe_successful();
        let publisher =
            create_active_subscribed_service_publisher(Some(Arc::clone(&service_adapter))).await;

        let (tx, rx) = oneshot::channel();
        let mut maybe_tx = Some(tx);
        let msg = Arc::new(Int32Value { value: 42, ..Default::default() }) as Arc<dyn MessageDyn>;
        publisher
            .subscribe(
                DATA_TRIGGER_NAME,
                Box::new(move |context| {
                    let msg = context
                        .get_message(PUBLISHER_NAME)
                        .expect("Publisher message should be present in the context");
                    maybe_tx.take().unwrap().send(Arc::clone(msg)).unwrap()
                }),
            )
            .await;
        service_adapter.publish_new_message(&msg).await;

        let received_message = rx.await.unwrap();
        let latest_message = publisher.get_latest_message().unwrap_sync().unwrap().unwrap();
        assert!(
            Arc::ptr_eq(&received_message, &msg),
            "Expected {received_message:?} to equal {msg:?}"
        );
        assert!(Arc::ptr_eq(&latest_message, &msg), "Expected {latest_message:?} to equal {msg:?}");
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_latest_message_calls_adapter() {
        let message = Box::new(Int32Value { value: 42, ..Default::default() });
        let mut message_provider = MockServiceMessageProvider::new();
        let message_clone = message.clone();
        message_provider.set_message_provider(move || message_clone.clone());
        let service_adapter = Arc::new(MockServiceAdapter::new());
        service_adapter.set_next_message_provider(message_provider);
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
        let service_adapter = Arc::new(MockServiceAdapter::new());
        service_adapter.set_next_message_provider(message_provider);
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
