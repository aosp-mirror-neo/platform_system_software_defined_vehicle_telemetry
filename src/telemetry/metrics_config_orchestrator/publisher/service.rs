// Copyright 2024 Google LLC

use async_trait::async_trait;
use futures::{future::BoxFuture, FutureExt};
use log::{info, warn};
use parking_lot::Mutex;
use protobuf::{well_known_types::any::Any, MessageDyn};
use sdv_telemetry_expr::dynamic_message::DynamicMessage;
use sdv_telemetry_service_publisher_accessor::{
    AvailabilityCallbackRegistration, ServicePublisherAccessorError, SubSamplingInterval,
};
use sdv_telemetry_utils::{
    collection_extension::HashMapExtension, operation_result::OperationResult,
};
use std::sync::{Arc, Weak};
use std::time::Duration;
use std::{collections::HashMap, mem};
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::{CancellationToken, DropGuard};

use super::{base::BasePublisher, CallbackFn, Publisher};
use crate::metrics_config_orchestrator::async_event_processor::{
    AsyncEventHandler, AsyncEventProcessor,
};
use crate::metrics_config_orchestrator::error_status_codes::ErrorStatusCodes;
use crate::metrics_config_orchestrator::node_evaluation_context_factory::NodeEvaluationContextFactory;
use crate::metrics_config_orchestrator::protobuf_dynamic_message::ProtobufDynamicMessage;
use crate::metrics_config_orchestrator::service_adapter::{
    GetMessageProviderOutcome, ServiceAdapter, ServiceMessageProvider, ServiceSubscription,
};
use crate::status_reporter::StatusReporter;

use google_sdv_telemetry::aidl::google::sdv::telemetry::status::{
    StatusDetails::ServicePublisherErrorDetails::ServicePublisherErrorDetails,
    StatusDetails::StatusDetails,
};

const RETRY_INTERVAL: Duration = Duration::from_secs(5 * 60);

pub(crate) enum ConnectionType {
    Subscription,
    Getter,
}

pub(crate) struct ServicePublisher {
    base: BasePublisher,
    configuration: Option<Any>,
    connection_type: ConnectionType,
    fetch_last_message: bool,
    node_evaluation_context_factory: Arc<dyn NodeEvaluationContextFactory>,
    service_adapter: Arc<dyn ServiceAdapter + Send + Sync>,
    service_name: String,
    state: AsyncMutex<ServicePublisherState>,
    status_reporter: Arc<dyn StatusReporter>,
    sub_sampling_interval: SubSamplingInterval,
    activation_retry_interval: Duration,
}

#[derive(strum_macros::Display)]
#[strum(serialize_all = "title_case")]
enum ServicePublisherState {
    /// The publisher is not subscribed to the underlying data source
    /// and doesn't provide the latest message.
    Inactive,
    /// The publisher attempted activation, but service wasn't available.
    /// Publisher wasn't able to subscribe to availability notifications.
    ActivationNotPossible,
    /// The publisher failed to attempt activation due to external reasons.
    /// It will retry activation after timeout.
    PendingActivation {
        // Map of unique data trigger names to the subscribed callbacks.
        callback_map: HashMap<String, CallbackFn>,

        // Cancellation token drop guard that is flagged to cancel retries of pending activation.
        _cancellation_drop_guard: DropGuard,
    },
    /// The publisher attempted activation, but service wasn't available.
    /// It will retry activation after receiving availability notification.
    PendingService {
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

    fn get_metrics_config_uuid(&self) -> &str {
        self.base.get_metrics_config_uuid()
    }

    fn is_subscribable(&self) -> bool {
        match self.connection_type {
            ConnectionType::Getter => false,
            ConnectionType::Subscription => true,
        }
    }

    fn get_latest_message(
        self: Arc<Self>,
    ) -> OperationResult<'static, Option<Arc<dyn DynamicMessage>>, ServicePublisherAccessorError>
    {
        if let Some(message) = self.base.get_latest_message() {
            return Ok(Some(message)).into();
        }

        async move {
            let message_provider = match &*self.state.lock().await {
                ServicePublisherState::Inactive
                | ServicePublisherState::ActivationNotPossible
                | ServicePublisherState::PendingActivation { .. }
                | ServicePublisherState::PendingService { .. } => return Ok(None),
                ServicePublisherState::ActiveGetter { message_provider } => {
                    Arc::clone(message_provider)
                }
                ServicePublisherState::ActiveSubscribed { .. } => return Ok(None),
            };
            Ok(message_provider.get_latest_message(self.configuration.as_ref()).await?.map(|msg| {
                Arc::new(ProtobufDynamicMessage::new(msg.into())) as Arc<dyn DynamicMessage>
            }))
        }
        .boxed()
        .into()
    }

    async fn activate(self: Arc<Self>) {
        // Lock state mutex and hold it until the state transition is complete.
        let state_lock = self.state.lock().await;
        assert!(
            matches!(*state_lock, ServicePublisherState::Inactive),
            "Unable to activate publisher {}: it's in {} state",
            self.get_name(),
            state_lock
        );

        self.activate_impl(state_lock).await
    }

    async fn deactivate(&self) {
        // Lock state mutex and hold it until the state transition is complete.
        let mut state_lock = self.state.lock().await;
        let old_state = mem::replace(&mut *state_lock, ServicePublisherState::Inactive);

        match old_state {
            ServicePublisherState::Inactive => {
                panic!("Unable to deactivate publisher {}: it's inactive", self.get_name())
            }
            ServicePublisherState::ActivationNotPossible
            | ServicePublisherState::PendingActivation { .. } => {}
            ServicePublisherState::PendingService { availability_registration, .. } => {
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

                self.service_adapter.unsubscribe(service_subscription).await;
            }
        }

        // Reset latest message.
        // The publisher is unsubscribed from underlying data sources, so no callback will change it back to some stale message.
        self.base.reset_latest_message();
    }

    async fn subscribe(&self, data_trigger_name: &str, callback: CallbackFn) {
        if !self.is_subscribable() {
            panic!("Unable to subscribe to publisher {}: it's not subscribable", self.get_name());
        }

        // Lock state mutex and hold it until operation is complete.
        let mut state_lock = self.state.lock().await;
        match &mut *state_lock {
            ServicePublisherState::Inactive => {
                panic!("Unable to subscribe to publisher {}: it's inactive", self.get_name())
            }
            ServicePublisherState::ActivationNotPossible => {}
            ServicePublisherState::PendingActivation { callback_map, .. }
            | ServicePublisherState::PendingService { callback_map, .. }
            | ServicePublisherState::ActiveSubscribed { callback_map, .. } => {
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
            ServicePublisherState::ActiveGetter { .. } => {
                panic!("Getter publisher {} doesn't support subscription.", self.get_name())
            }
        };
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
        match &mut *state_lock {
            ServicePublisherState::Inactive => {
                panic!("Unable to unsubscribe from publisher {}: it's inactive", self.get_name())
            }
            ServicePublisherState::ActivationNotPossible => {}
            ServicePublisherState::PendingActivation { callback_map, .. }
            | ServicePublisherState::PendingService { callback_map, .. }
            | ServicePublisherState::ActiveSubscribed { callback_map, .. } => {
                let _ = callback_map.remove(data_trigger_name).unwrap_or_else(|| {
                    panic!(
                        "Unable to unsubscribe from publisher {}: data trigger {} wasn't subscribed",
                        self.get_name(),
                        data_trigger_name
                    )
                });
            }
            ServicePublisherState::ActiveGetter { .. } => {
                panic!("Getter publisher {} doesn't support unsubscription.", self.get_name())
            }
        };
    }
}

impl ServicePublisher {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new(
        name: String,
        configuration: Option<Any>,
        connection_type: ConnectionType,
        fetch_last_message: bool,
        metrics_config_uuid: String,
        service_name: String,
        sub_sampling_interval: SubSamplingInterval,

        // Dependencies
        node_evaluation_context_factory: Arc<dyn NodeEvaluationContextFactory>,
        service_adapter: Arc<dyn ServiceAdapter + Send + Sync>,
        status_reporter: Arc<dyn StatusReporter>,
    ) -> Self {
        Self {
            base: BasePublisher::new(name, metrics_config_uuid),
            configuration,
            connection_type,
            fetch_last_message,
            node_evaluation_context_factory,
            service_adapter,
            service_name,
            state: ServicePublisherState::Inactive.into(),
            status_reporter,
            sub_sampling_interval,
            activation_retry_interval: RETRY_INTERVAL,
        }
    }

    /// Attempts Service Publisher activation, subscribes to availability callback if activation
    /// didn't succeed and sets the new publisher status using provided state lock.
    async fn activate_impl(
        self: &Arc<Self>,
        mut state_lock: AsyncMutexGuard<'_, ServicePublisherState>,
    ) {
        let new_state = match self.try_activate().await {
            Ok(Some(state)) => state,
            Ok(None) => self
                .subscribe_to_availability_notifications(false)
                .await
                .expect("Activation attempt should not be performed from PendingActivation state"),
            Err(err) => {
                self.report_activation_error(err, "failed to activate (retries are scheduled)");
                self.schedule_activation_retries()
            }
        };

        // Set the new state and release the mutex.
        *state_lock = new_state;
    }

    fn subscribe_to_availability_notifications(
        self: &Arc<Self>,
        from_pending_activation_state: bool,
    ) -> BoxFuture<Option<ServicePublisherState>> {
        async move {
            info!(
                "Publisher {} activation wasn't able to discover the service. \
                Subscribing to availability notifications",
                self.get_name()
            );

            let registration_container = Arc::new(Mutex::new(None));
            let registration_container_clone = Arc::clone(&registration_container);
            let self_clone = Arc::clone(self);
            let maybe_registration = self
                .service_adapter
                .register_availability_callback(
                    &self.service_name,
                    Box::new(move || {
                        let registration_container_clone =
                            Arc::clone(&registration_container_clone);
                        let self_clone = Arc::clone(&self_clone);
                        tokio::spawn(async move {
                            if registration_container_clone.lock().is_none() {
                                // Abort retry operation because either retry has already been
                                // processed, or Publisher state is no longer PendingService.
                                return;
                            }
                            let state_lock = self_clone.state.lock().await;
                            self_clone.retry_activation(state_lock).await;
                        });
                    }),
                )
                .await;

            match maybe_registration {
                Ok(Some(registration)) => {
                    assert!(registration_container.lock().replace(registration).is_none());
                    Some(ServicePublisherState::PendingService {
                        availability_registration: registration_container,
                        callback_map: HashMap::new(),
                    })
                }
                Ok(None) => {
                    // There are no compatible accessors.
                    // Receiving availability notifications is not possible
                    Some(ServicePublisherState::ActivationNotPossible)
                }
                Err(_) => {
                    if !from_pending_activation_state {
                        // Set state to PendingActivation and schedule activation retries.
                        let new_state = self.schedule_activation_retries();
                        Some(new_state)
                    } else {
                        // State is already PendingActivation, and retries are set up.
                        // Not scheduling new retries.
                        None
                    }
                }
            }
        }
        .boxed()
    }

    fn report_activation_error(
        self: &Arc<Self>,
        err: ServicePublisherAccessorError,
        message: &str,
    ) {
        let publisher_name = self.get_name().to_owned();
        let metrics_config_uuid = self.get_metrics_config_uuid().to_owned();
        self.status_reporter.report(
            err.get_status_code(),
            format!("Metrics config {metrics_config_uuid} service publisher {publisher_name} {message}: {err}"),
            StatusDetails::ServicePublisherErrorDetails(
                ServicePublisherErrorDetails {
                    metricsConfigUUID: metrics_config_uuid,
                    publisherName: publisher_name,
            }));
    }

    fn schedule_activation_retries(self: &Arc<Self>) -> ServicePublisherState {
        let cancellation_token = CancellationToken::new();
        let cancellation_token_clone = cancellation_token.clone();
        let new_state = ServicePublisherState::PendingActivation {
            callback_map: HashMap::new(),
            _cancellation_drop_guard: cancellation_token.drop_guard(),
        };

        let self_clone = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                let state_lock = select! {
                    biased;
                    _ = cancellation_token_clone.cancelled() => break,
                    state_lock = async {
                        tokio::time::sleep(self_clone.activation_retry_interval).await;
                        self_clone.state.lock().await
                    } => state_lock,
                };
                if matches!(*state_lock, ServicePublisherState::PendingActivation { .. }) {
                    self_clone.retry_activation(state_lock).await;
                }
            }
        });

        new_state
    }

    /// Moves callbacks from a pending state callback map if the new state supports
    /// subscriptions or it is another allowed pending state.
    fn maybe_swap_callback_map(
        state: &mut ServicePublisherState,
        new_state: &mut ServicePublisherState,
    ) {
        match (state, new_state) {
            (
                ServicePublisherState::PendingActivation { .. },
                ServicePublisherState::ActivationNotPossible
                | ServicePublisherState::ActiveGetter { .. },
            )
            | (
                ServicePublisherState::PendingService { .. },
                ServicePublisherState::ActiveGetter { .. },
            ) => {}
            (
                ServicePublisherState::PendingActivation {
                    callback_map: pending_callback_map, ..
                },
                ServicePublisherState::PendingService { callback_map, .. },
            )
            | (
                ServicePublisherState::PendingActivation {
                    callback_map: pending_callback_map, ..
                },
                ServicePublisherState::ActiveSubscribed { callback_map, .. },
            )
            | (
                ServicePublisherState::PendingService {
                    callback_map: pending_callback_map, ..
                },
                ServicePublisherState::ActiveSubscribed { callback_map, .. },
            ) => mem::swap(callback_map, pending_callback_map),
            (state, new_state) => {
                panic!("Unexpected state change from {state} to {new_state}")
            }
        }
    }

    /// Retries publisher activation.
    async fn retry_activation(
        self: &Arc<Self>,
        mut state_lock: AsyncMutexGuard<'_, ServicePublisherState>,
    ) {
        let warn_service_unavailable = |state| {
            warn!(
                "Publisher {} was able to retry activation from {} state \
                but wasn't able to discover the service",
                self.get_name(),
                state,
            );
        };
        let report_activation_error = |err, state| {
            let message = format!("failed to retry activation from {state} state");
            self.report_activation_error(err, &message);
        };

        match &mut *state_lock {
            ServicePublisherState::Inactive
            | ServicePublisherState::ActivationNotPossible
            | ServicePublisherState::ActiveGetter { .. }
            | ServicePublisherState::ActiveSubscribed { .. } => {
                panic!(
                    "Publisher {} state is {}, but activation retry is expected from a pending state",
                    self.get_name(),
                    *state_lock,
                );
            }
            // Previous activation attempt failed, current retry was scheduled by a timer
            ServicePublisherState::PendingActivation { .. } => match self.try_activate().await {
                Ok(Some(mut new_state)) => {
                    // Service became available. Assign new state
                    Self::maybe_swap_callback_map(&mut state_lock, &mut new_state);
                    *state_lock = new_state;
                }
                Ok(None) => {
                    // Current activation retry didn't fail, but service is unavailable.
                    // Try subscribing to availability notifications and assign corresponding state
                    warn_service_unavailable(&state_lock);

                    let maybe_new_state = self.subscribe_to_availability_notifications(true).await;
                    if let Some(mut new_state) = maybe_new_state {
                        Self::maybe_swap_callback_map(&mut state_lock, &mut new_state);
                        *state_lock = new_state;
                    }
                }
                Err(err) => report_activation_error(err, &state_lock),
            },
            // Previous activation attempt didn't fail but service was not available.
            // Retrying due to receiving service availability notification
            ServicePublisherState::PendingService { availability_registration, .. } => {
                match self.try_activate().await {
                    Ok(Some(mut new_state)) => {
                        // Service became available.
                        // Assign new state and unsubscribe from availability notifications
                        let registration = availability_registration.lock().take().expect(
                            "Registration should be Some when activation retry is requested",
                        );
                        registration.unregister().await;
                        Self::maybe_swap_callback_map(&mut state_lock, &mut new_state);
                        *state_lock = new_state;
                    }
                    Ok(None) => {
                        // Service is not available despite receiving availability notification.
                        // Publisher state remains PendingService
                        warn_service_unavailable(&state_lock);
                    }
                    Err(err) => report_activation_error(err, &state_lock),
                }
            }
        }
    }

    /// Attempts to activate service publisher and returns the new state if activation was successful.
    async fn try_activate(
        self: &Arc<Self>,
    ) -> Result<Option<ServicePublisherState>, ServicePublisherAccessorError> {
        let outcome = match self.connection_type {
            ConnectionType::Getter => {
                self.service_adapter.get_message_provider(&self.service_name).await?
            }
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
                let self_clone = Arc::clone(self);
                let maybe_subscription = Arc::clone(&self.service_adapter)
                    .subscribe(
                        &self.service_name,
                        self.configuration.as_ref(),
                        self.fetch_last_message,
                        &self.sub_sampling_interval,
                        Box::new(move |msg| {
                            sender
                                .send(Arc::clone(msg))
                                .expect("Callback should only receive messages when subscribed")
                        }),
                        Box::new(move || {
                            info!("Service Publisher {} received subscription cancellation notification", self_clone.base.get_name());
                            self_clone.on_subscription_cancelled();
                        }),
                    )
                    .await?;

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
                    UnboundedReceiverStream::new(receiver),
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
        let message = Arc::new(ProtobufDynamicMessage::new(message));

        // Create context outside of the lock, so that the date/time is set as fast as possible,
        // and not just at the moment when the lock is obtained.
        let context = Arc::new(self.node_evaluation_context_factory.create_with_publisher_message(
            self.get_name().to_owned(),
            Arc::<ProtobufDynamicMessage>::clone(&message),
        ));

        // Lock state mutex and hold it to make sure the publisher is not deactivated.
        let mut state_lock = self.state.lock().await;

        let ServicePublisherState::ActiveSubscribed { callback_map, .. } = &mut *state_lock else {
            return;
        };

        self.base.set_new_message(message, &context, callback_map);
    }

    fn on_subscription_cancelled(self: &Arc<Self>) {
        let self_clone = Arc::clone(self);
        tokio::spawn(async move {
            // Lock state mutex and hold it until the state transition is complete.
            let state_lock = self_clone.state.lock().await;

            if !matches!(*state_lock, ServicePublisherState::ActiveSubscribed { .. }) {
                return;
            }

            self_clone.activate_impl(state_lock).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use anyhow::anyhow;
    use google_sdv_telemetry::aidl::google::sdv::telemetry::status::{
        TelemetryServiceStatus::TelemetryServiceStatus,
        TelemetryServiceStatusCodes::TelemetryServiceStatusCodes as StatusCodes,
    };
    use googletest::assert_that;
    use protobuf::reflect::ReflectEqMode;
    use protobuf::well_known_types::{empty::Empty, wrappers::Int32Value};
    use protobuf::Message;
    use sdv_telemetry_platform_abstraction_layer::new_stub_pal;
    use sdv_telemetry_test_utils::{msg_dyn_eq, oneshot_callback};
    use std::sync::Arc;

    use crate::metrics_config_orchestrator::node_evaluation_context_factory::NodeEvaluationContextFactoryImpl;
    use crate::metrics_config_orchestrator::service_adapter::mock::{
        MockServiceAdapter, MockServiceMessageProvider,
    };
    use crate::StatusReporterImpl;

    const CONFIG_UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";
    const DATA_TRIGGER_NAME: &str = "datatrigger";
    const ERROR_MESSAGE: &str = "Error 42";
    const PUBLISHER_NAME: &str = "publisher";
    const SERVICE_NAME: &str = "service";

    struct ServicePublisherBuilder {
        name: String,
        configuration: Option<Any>,
        connection_type: ConnectionType,
        fetch_last_message: bool,
        metrics_config_uuid: String,
        service_name: String,
        sub_sampling_interval: SubSamplingInterval,
        node_evaluation_context_factory: Arc<dyn NodeEvaluationContextFactory>,
        service_adapter: Arc<dyn ServiceAdapter + Send + Sync>,
        status_reporter: Arc<dyn StatusReporter>,
        activation_retry_interval: Option<Duration>,
    }

    impl ServicePublisherBuilder {
        fn new(service_adapter: Arc<MockServiceAdapter>) -> Self {
            Self {
                name: PUBLISHER_NAME.to_owned(),
                configuration: None,
                connection_type: ConnectionType::Getter,
                fetch_last_message: false,
                metrics_config_uuid: CONFIG_UUID.to_owned(),
                service_name: SERVICE_NAME.to_owned(),
                sub_sampling_interval: SubSamplingInterval::ZERO,
                node_evaluation_context_factory: Arc::new(NodeEvaluationContextFactoryImpl::new(
                    new_stub_pal(),
                )),
                service_adapter,
                status_reporter: Arc::new(StatusReporterImpl::new(|err| {
                    panic!("Unexpected error: {err:?}")
                })),
                activation_retry_interval: None,
            }
        }

        fn with_connection_type(mut self, connection_type: ConnectionType) -> Self {
            self.connection_type = connection_type;
            self
        }

        fn with_status_reporter(mut self, status_reporter: Arc<dyn StatusReporter>) -> Self {
            self.status_reporter = status_reporter;
            self
        }

        fn with_activation_retry_interval(mut self, interval: Duration) -> Self {
            self.activation_retry_interval = Some(interval);
            self
        }

        fn with_configuration(mut self, config: Any) -> Self {
            self.configuration = Some(config);
            self
        }

        fn build(self) -> Arc<ServicePublisher> {
            let mut publisher = ServicePublisher::new(
                self.name,
                self.configuration,
                self.connection_type,
                self.fetch_last_message,
                self.metrics_config_uuid,
                self.service_name,
                self.sub_sampling_interval,
                self.node_evaluation_context_factory,
                self.service_adapter,
                self.status_reporter,
            );
            if let Some(interval) = self.activation_retry_interval {
                publisher.activation_retry_interval = interval;
            }
            Arc::new(publisher)
        }

        async fn build_and_activate(self) -> Arc<ServicePublisher> {
            let publisher = self.build();
            Arc::clone(&publisher).activate().await;
            publisher
        }
    }

    /// Creates a Box with a callback that accepts context and does nothing.
    pub fn create_empty_fn_box() -> CallbackFn {
        Box::new(|_| {})
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_getter_success() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        service_adapter.set_next_message_provider(MockServiceMessageProvider::new());
        let publisher =
            ServicePublisherBuilder::new(Arc::clone(&service_adapter)).build_and_activate().await;

        assert!(matches!(
            *publisher.state.lock().await,
            ServicePublisherState::ActiveGetter { .. }
        ));
        assert_eq!(service_adapter.get_num_get_message_provider_calls(), 1);
        assert_eq!(service_adapter.get_num_subscribe_calls(), 0);
        assert_eq!(service_adapter.get_num_register_availability_callback_calls(), 0);
        assert_eq!(service_adapter.get_num_unregister_availability_callback_calls(), 0);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_getter_retries_on_adapter_error() {
        // Retrieving message provider fails and causes retries. On retry, provider is found which
        // activates the publisher
        let service_adapter =
            Arc::new(MockServiceAdapter::new().with_next_error(anyhow!(ERROR_MESSAGE)));
        let publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_status_reporter(Arc::new(StatusReporterImpl::new(|_| {})))
            .with_activation_retry_interval(Duration::from_millis(3))
            .build_and_activate()
            .await;

        // First request of message_provider happens immediately
        service_adapter.wait_message_provider_returned().await;
        assert!(
            matches!(
                *publisher.state.lock().await,
                ServicePublisherState::PendingActivation { .. }
            ),
            "Unexpected state: {}",
            *publisher.state.lock().await
        );

        // Set message provider
        service_adapter.set_next_message_provider(MockServiceMessageProvider::new());
        // Wait until retry happens by waiting for the next request of message_provider
        service_adapter.wait_message_provider_returned().await;
        assert!(
            matches!(*publisher.state.lock().await, ServicePublisherState::ActiveGetter { .. }),
            "Unexpected state: {}",
            *publisher.state.lock().await
        );

        assert_eq!(service_adapter.get_num_get_message_provider_calls(), 2);
        assert_eq!(service_adapter.get_num_subscribe_calls(), 0);
        assert_eq!(service_adapter.get_num_register_availability_callback_calls(), 0);
        assert_eq!(service_adapter.get_num_unregister_availability_callback_calls(), 0);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_getter_on_deactivate_cancels_retries() {
        let service_adapter =
            Arc::new(MockServiceAdapter::new().with_next_error(anyhow!(ERROR_MESSAGE)));
        let publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_status_reporter(Arc::new(StatusReporterImpl::new(|_| {})))
            .with_activation_retry_interval(Duration::from_millis(3))
            .build_and_activate()
            .await;

        assert!(
            matches!(
                *publisher.state.lock().await,
                ServicePublisherState::PendingActivation { .. }
            ),
            "Unexpected state: {}",
            *publisher.state.lock().await
        );

        Arc::clone(&publisher).deactivate().await;
        assert!(
            matches!(*publisher.state.lock().await, ServicePublisherState::Inactive),
            "Unexpected state: {}",
            *publisher.state.lock().await
        );

        assert_eq!(service_adapter.get_num_get_message_provider_calls(), 1);
        assert_eq!(service_adapter.get_num_subscribe_calls(), 0);
        assert_eq!(service_adapter.get_num_register_availability_callback_calls(), 0);
        assert_eq!(service_adapter.get_num_unregister_availability_callback_calls(), 0);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_getter_falls_back_to_subscription() {
        let service_adapter =
            Arc::new(MockServiceAdapter::new().with_next_message_provider_get_not_supported());
        service_adapter.set_next_subscribe_successful();
        let publisher =
            ServicePublisherBuilder::new(Arc::clone(&service_adapter)).build_and_activate().await;

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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_getter_pending_if_not_found() {
        let service_adapter = Arc::new(MockServiceAdapter::new().with_availability_callback());
        let publisher =
            ServicePublisherBuilder::new(Arc::clone(&service_adapter)).build_and_activate().await;

        let state = publisher.state.lock().await;
        assert!(
            matches!(*state, ServicePublisherState::PendingService { .. }),
            "Unexpected state: {}",
            *state
        );
        assert_eq!(service_adapter.get_num_get_message_provider_calls(), 1);
        assert_eq!(service_adapter.get_num_subscribe_calls(), 0);
        assert_eq!(service_adapter.get_num_register_availability_callback_calls(), 1);
        assert_eq!(service_adapter.get_num_unregister_availability_callback_calls(), 0);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_subscription_no_availability_callback_success() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        let publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_connection_type(ConnectionType::Subscription)
            .build_and_activate()
            .await;

        assert!(
            matches!(*publisher.state.lock().await, ServicePublisherState::ActivationNotPossible),
            "Unexpected state: {}",
            *publisher.state.lock().await
        );

        Arc::clone(&publisher).deactivate().await;
        assert!(matches!(*publisher.state.lock().await, ServicePublisherState::Inactive));

        assert_eq!(service_adapter.get_num_register_availability_callback_calls(), 1);
        assert_eq!(service_adapter.get_num_unregister_availability_callback_calls(), 0);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_getter_activates_on_receiving_availabilty_notification() {
        // No message provided is found. Reactivating publisher after receiving availability
        // notification
        let service_adapter = Arc::new(MockServiceAdapter::new().with_availability_callback());
        let publisher =
            ServicePublisherBuilder::new(Arc::clone(&service_adapter)).build_and_activate().await;

        // First request of message_provider happens immediately
        service_adapter.wait_message_provider_returned().await;

        // Fire availability callback when service is still not available.
        service_adapter.call_availability_callback();
        // Next activation attempt happens when availability callback fired
        service_adapter.wait_message_provider_returned().await;

        tokio::task::yield_now().await;

        // Fire availability callback when service is available.
        service_adapter.set_next_message_provider(MockServiceMessageProvider::new());
        service_adapter.call_availability_callback();
        // Wait until publisher state is updated by waiting for the next request of message_provider
        service_adapter.wait_message_provider_returned().await;

        let state = publisher.state.lock().await;
        assert!(
            matches!(*state, ServicePublisherState::ActiveGetter { .. }),
            "Unexpected state: {}",
            *state
        );
        assert_eq!(service_adapter.get_num_get_message_provider_calls(), 3);
        assert_eq!(service_adapter.get_num_subscribe_calls(), 0);
        assert_eq!(service_adapter.get_num_register_availability_callback_calls(), 1);
        assert_eq!(service_adapter.get_num_unregister_availability_callback_calls(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_getter_changes_pending_state() {
        // Retrieving message provider fails and causes retries. On retry, no provider is found.
        // Publisher successfully registers for availability notifications.
        let service_adapter = Arc::new(
            MockServiceAdapter::new()
                .with_availability_callback()
                .with_next_error(anyhow!(ERROR_MESSAGE)),
        );
        let publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_status_reporter(Arc::new(StatusReporterImpl::new(|_| {})))
            .with_activation_retry_interval(Duration::from_millis(3))
            .build_and_activate()
            .await;

        // First request of message_provider happens immediately
        service_adapter.wait_message_provider_returned().await;
        assert!(
            matches!(
                *publisher.state.lock().await,
                ServicePublisherState::PendingActivation { .. }
            ),
            "Unexpected state: {}",
            *publisher.state.lock().await
        );

        // Wait until retry happens by waiting for the next request of message_provider
        service_adapter.wait_message_provider_returned().await;
        assert!(
            matches!(*publisher.state.lock().await, ServicePublisherState::PendingService { .. }),
            "Unexpected state: {}",
            *publisher.state.lock().await
        );

        assert_eq!(service_adapter.get_num_get_message_provider_calls(), 2);
        assert_eq!(service_adapter.get_num_subscribe_calls(), 0);
        assert_eq!(service_adapter.get_num_register_availability_callback_calls(), 1);
        assert_eq!(service_adapter.get_num_unregister_availability_callback_calls(), 0);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_getter_activation_not_possible_after_retry_success() {
        // Retrieving message provider fails and causes retries. On retry, no provider is found.
        // Publisher attempts registering availability callback but no registration is found.
        let service_adapter =
            Arc::new(MockServiceAdapter::new().with_next_error(anyhow!(ERROR_MESSAGE)));
        let publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_status_reporter(Arc::new(StatusReporterImpl::new(|_| {})))
            .with_activation_retry_interval(Duration::from_millis(3))
            .build_and_activate()
            .await;

        // First request of message_provider happens immediately
        service_adapter.wait_message_provider_returned().await;
        assert!(
            matches!(
                *publisher.state.lock().await,
                ServicePublisherState::PendingActivation { .. }
            ),
            "Unexpected state: {}",
            *publisher.state.lock().await
        );

        // Wait until retry happens by waiting message_provider being requested for the second time
        service_adapter.wait_message_provider_returned().await;
        assert!(
            matches!(*publisher.state.lock().await, ServicePublisherState::ActivationNotPossible),
            "Unexpected state: {}",
            *publisher.state.lock().await
        );

        assert_eq!(service_adapter.get_num_get_message_provider_calls(), 2);
        assert_eq!(service_adapter.get_num_subscribe_calls(), 0);
        assert_eq!(service_adapter.get_num_register_availability_callback_calls(), 1);
        assert_eq!(service_adapter.get_num_unregister_availability_callback_calls(), 0);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_getter_failed_register_availability_callback_retries() {
        // No message provider is set. Publisher fails registering availability callback causing
        // retries
        let service_adapter = Arc::new(
            MockServiceAdapter::new()
                .with_next_register_availability_callback_error(anyhow!(ERROR_MESSAGE)),
        );

        let publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_status_reporter(Arc::new(StatusReporterImpl::new(|_| {})))
            .with_activation_retry_interval(Duration::from_millis(3))
            .build_and_activate()
            .await;

        assert!(
            matches!(
                *publisher.state.lock().await,
                ServicePublisherState::PendingActivation { .. }
            ),
            "Unexpected state: {}",
            *publisher.state.lock().await
        );

        assert_eq!(service_adapter.get_num_get_message_provider_calls(), 1);
        assert_eq!(service_adapter.get_num_subscribe_calls(), 0);
        assert_eq!(service_adapter.get_num_register_availability_callback_calls(), 0);
        assert_eq!(service_adapter.get_num_unregister_availability_callback_calls(), 0);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_getter_failed_register_availability_callback_keeps_retries() {
        // Retrieving message provider fails and causes retries. On retry, no provider is found,
        // and publisher fails registering availability callback, which keeps retries
        let service_adapter = Arc::new(
            MockServiceAdapter::new()
                .with_next_error(anyhow!(ERROR_MESSAGE))
                .with_next_register_availability_callback_error(anyhow!(ERROR_MESSAGE)),
        );

        let publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_status_reporter(Arc::new(StatusReporterImpl::new(|_| {})))
            .with_activation_retry_interval(Duration::from_millis(3))
            .build_and_activate()
            .await;

        // First request of message_provider happens immediately
        service_adapter.wait_message_provider_returned().await;
        assert!(
            matches!(
                *publisher.state.lock().await,
                ServicePublisherState::PendingActivation { .. }
            ),
            "Unexpected state: {}",
            *publisher.state.lock().await
        );

        // Wait until retry happens by waiting for the next request of message_provider
        service_adapter.wait_message_provider_returned().await;
        assert!(
            matches!(
                *publisher.state.lock().await,
                ServicePublisherState::PendingActivation { .. }
            ),
            "Unexpected state: {}",
            *publisher.state.lock().await
        );

        assert_eq!(service_adapter.get_num_get_message_provider_calls(), 2);
        assert_eq!(service_adapter.get_num_subscribe_calls(), 0);
        assert_eq!(service_adapter.get_num_register_availability_callback_calls(), 0);
        assert_eq!(service_adapter.get_num_unregister_availability_callback_calls(), 0);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_getter_activation_fails_twice() {
        // Retrieving message provider fails twice and causes retries on the first failure,
        // reporting errors to StatusReporter.
        let service_adapter =
            Arc::new(MockServiceAdapter::new().with_next_error(anyhow!(ERROR_MESSAGE)));

        let (tx, mut rx) = mpsc::unbounded_channel();
        let _publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_status_reporter(Arc::new(StatusReporterImpl::new(Box::new(move |status| {
                tx.send(status).unwrap();
            }))))
            .with_activation_retry_interval(Duration::from_millis(3))
            .build_and_activate()
            .await;

        // Receive first error status
        rx.recv().await.unwrap();

        // Set error to fail activation retry attempt
        service_adapter.set_next_error(anyhow!(ERROR_MESSAGE));

        // Receive second error status
        rx.recv().await.unwrap();

        assert_eq!(service_adapter.get_num_get_message_provider_calls(), 2);
        assert_eq!(service_adapter.get_num_subscribe_calls(), 0);
        assert_eq!(service_adapter.get_num_register_availability_callback_calls(), 0);
        assert_eq!(service_adapter.get_num_unregister_availability_callback_calls(), 0);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_getter_retry_reports_errors() {
        let service_adapter = Arc::new(MockServiceAdapter::new().with_availability_callback());
        let (tx, rx) = oneshot_callback();
        let _publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_status_reporter(Arc::new(StatusReporterImpl::new(tx)))
            .build_and_activate()
            .await;

        // Fire availability callback when service is still not available.
        service_adapter.call_availability_callback();
        tokio::task::yield_now().await;

        // Service adapter returns an error
        service_adapter.set_next_error(anyhow!(ERROR_MESSAGE));

        // Fire availability callback when service is available.
        service_adapter.call_availability_callback();

        // Wait for error status to be delivered
        let status = rx.await;
        assert!(
            matches!(&status, TelemetryServiceStatus {
                id: 0,
                code: StatusCodes::METRICS_CONFIG_RUNTIME_SERVICE_METADATA_PARSING_ERROR,
                message,
                details: StatusDetails::ServicePublisherErrorDetails(
                    ServicePublisherErrorDetails { metricsConfigUUID, publisherName }
                ),
            }
            if message.contains("failed to retry activation") && message.contains(ERROR_MESSAGE) && metricsConfigUUID == CONFIG_UUID && publisherName == PUBLISHER_NAME),
            "Unexpected status: {status:?}"
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_getter_with_retries_unsubscribes_on_deactivate() {
        let service_adapter = Arc::new(MockServiceAdapter::new().with_availability_callback());
        let publisher =
            ServicePublisherBuilder::new(Arc::clone(&service_adapter)).build_and_activate().await;

        publisher.deactivate().await;

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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_subscription_success() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        service_adapter.set_next_subscribe_successful();
        let publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_connection_type(ConnectionType::Subscription)
            .with_configuration(Any::pack(Empty::default_instance()).unwrap())
            .build_and_activate()
            .await;

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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_subscription_pending_if_not_found() {
        let service_adapter = Arc::new(MockServiceAdapter::new().with_availability_callback());
        let publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_connection_type(ConnectionType::Subscription)
            .build_and_activate()
            .await;

        let state = publisher.state.lock().await;
        assert!(
            matches!(*state, ServicePublisherState::PendingService { .. }),
            "Unexpected state: {}",
            *state
        );
        assert_eq!(service_adapter.get_num_get_message_provider_calls(), 0);
        assert_eq!(service_adapter.get_num_subscribe_calls(), 1);
        assert_eq!(service_adapter.get_num_register_availability_callback_calls(), 1);
        assert_eq!(service_adapter.get_num_unregister_availability_callback_calls(), 0);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_subscription_retries_if_not_found() {
        let service_adapter = Arc::new(MockServiceAdapter::new().with_availability_callback());
        let publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_connection_type(ConnectionType::Subscription)
            .build_and_activate()
            .await;

        let (tx, rx) = oneshot_callback();
        let msg = Arc::new(Int32Value { value: 42, ..Default::default() }) as Arc<dyn MessageDyn>;
        publisher
            .subscribe(
                DATA_TRIGGER_NAME,
                Box::new(move |context| {
                    let msg = context
                        .get_message(PUBLISHER_NAME)
                        .expect("Publisher message should be present in the context");
                    tx(Arc::clone(msg));
                }),
            )
            .await;

        let subscribe_receiver = service_adapter.set_next_subscribe_successful();
        service_adapter.call_availability_callback();
        subscribe_receiver.await.unwrap();

        service_adapter.publish_new_message(&msg).await;
        let received_message = rx.await.to_protobuf().unwrap();

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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_subscription_resubscribes_on_cancelled() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        service_adapter.set_next_subscribe_successful();
        let publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_connection_type(ConnectionType::Subscription)
            .build_and_activate()
            .await;

        let rx = service_adapter.set_next_subscribe_successful();
        service_adapter.notify_cancelled();
        rx.await.unwrap();

        let state = publisher.state.lock().await;
        assert!(
            matches!(*state, ServicePublisherState::ActiveSubscribed { .. }),
            "Unexpected state: {}",
            *state
        );
        assert_eq!(service_adapter.get_num_get_message_provider_calls(), 0);
        assert_eq!(service_adapter.get_num_subscribe_calls(), 2);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_subscription_on_cancelled_reports_errors() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        service_adapter.set_next_subscribe_successful();

        let (tx, rx) = oneshot_callback();
        let _publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_connection_type(ConnectionType::Subscription)
            .with_status_reporter(Arc::new(StatusReporterImpl::new(tx)))
            .build_and_activate()
            .await;

        service_adapter.set_next_error(anyhow!(ERROR_MESSAGE));
        service_adapter.notify_cancelled();

        // Wait for error status to be delivered.
        let status = rx.await;
        assert!(
            matches!(&status, TelemetryServiceStatus {
                id: 0,
                code: StatusCodes::METRICS_CONFIG_RUNTIME_SERVICE_METADATA_PARSING_ERROR,
                message,
                details: StatusDetails::ServicePublisherErrorDetails(
                    ServicePublisherErrorDetails { metricsConfigUUID, publisherName }
                ),
            }
            if message.contains(ERROR_MESSAGE) && metricsConfigUUID == CONFIG_UUID && publisherName == PUBLISHER_NAME),
            "Unexpected status: {status:?}"
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_subscription_not_resubscribes_when_inactive() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        service_adapter.set_next_subscribe_successful();
        let publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_connection_type(ConnectionType::Subscription)
            .build_and_activate()
            .await;

        publisher.deactivate().await;
        service_adapter.notify_cancelled();
        tokio::task::yield_now().await;

        let state = publisher.state.lock().await;
        assert!(matches!(*state, ServicePublisherState::Inactive), "Unexpected state: {}", *state);
        assert_eq!(service_adapter.get_num_get_message_provider_calls(), 0);
        assert_eq!(service_adapter.get_num_subscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn activate_when_active_panics() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        service_adapter.set_next_subscribe_successful();
        let publisher = ServicePublisherBuilder::new(service_adapter)
            .with_connection_type(ConnectionType::Subscription)
            .build_and_activate()
            .await;

        publisher.activate().await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn deactivate_when_inactive_panics() {
        let publisher = ServicePublisherBuilder::new(Arc::new(MockServiceAdapter::new()))
            .with_connection_type(ConnectionType::Subscription)
            .build();

        publisher.deactivate().await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn deactivate_activation_not_possible_success() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        let publisher =
            ServicePublisherBuilder::new(Arc::clone(&service_adapter)).build_and_activate().await;

        publisher.deactivate().await;

        let state = publisher.state.lock().await;
        assert!(matches!(*state, ServicePublisherState::Inactive), "Unexpected state: {}", *state);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn deactivate_pending_activation_success() {
        let service_adapter =
            Arc::new(MockServiceAdapter::new().with_next_error(anyhow!(ERROR_MESSAGE)));
        let publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_status_reporter(Arc::new(StatusReporterImpl::new(|_| {})))
            .build_and_activate()
            .await;

        publisher.deactivate().await;

        let state = publisher.state.lock().await;
        assert!(matches!(*state, ServicePublisherState::Inactive), "Unexpected state: {}", *state);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn deactivate_pending_service_success() {
        let service_adapter = Arc::new(MockServiceAdapter::new().with_availability_callback());
        let publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_connection_type(ConnectionType::Subscription)
            .build_and_activate()
            .await;

        publisher.deactivate().await;

        let state = publisher.state.lock().await;
        assert!(matches!(*state, ServicePublisherState::Inactive), "Unexpected state: {}", *state);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn deactivate_getter_success() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        service_adapter.set_next_message_provider(MockServiceMessageProvider::new());
        let publisher =
            ServicePublisherBuilder::new(Arc::clone(&service_adapter)).build_and_activate().await;

        publisher.deactivate().await;

        let state = publisher.state.lock().await;
        assert!(matches!(*state, ServicePublisherState::Inactive), "Unexpected state: {}", *state);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn deactivate_with_subscriptions_panics() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        service_adapter.set_next_subscribe_successful();
        let publisher = ServicePublisherBuilder::new(service_adapter)
            .with_connection_type(ConnectionType::Subscription)
            .build_and_activate()
            .await;
        publisher.subscribe(DATA_TRIGGER_NAME, create_empty_fn_box()).await;

        publisher.deactivate().await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn deactivate_subscribed_success() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        service_adapter.set_next_subscribe_successful();
        let publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_connection_type(ConnectionType::Subscription)
            .build_and_activate()
            .await;

        publisher.deactivate().await;

        let state = publisher.state.lock().await;
        assert!(matches!(*state, ServicePublisherState::Inactive), "Unexpected state: {}", *state);
        assert_eq!(service_adapter.get_num_unsubscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn deactivate_subscribed_calls_unsubscribe() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        service_adapter.set_next_subscribe_successful();
        let publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_connection_type(ConnectionType::Subscription)
            .build_and_activate()
            .await;

        publisher.deactivate().await;

        let state = publisher.state.lock().await;
        assert!(matches!(*state, ServicePublisherState::Inactive), "Unexpected state: {}", *state);
        assert_eq!(service_adapter.get_num_unsubscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn subscribe_when_inactive_panics() {
        let publisher = ServicePublisherBuilder::new(Arc::new(MockServiceAdapter::new()))
            .with_connection_type(ConnectionType::Subscription)
            .build();

        publisher.subscribe(DATA_TRIGGER_NAME, create_empty_fn_box()).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribe_activation_not_possible_noop() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        let publisher = ServicePublisherBuilder::new(service_adapter)
            .with_connection_type(ConnectionType::Subscription)
            .build_and_activate()
            .await;

        publisher.subscribe(DATA_TRIGGER_NAME, create_empty_fn_box()).await;

        let state = publisher.state.lock().await;
        assert!(
            matches!(*state, ServicePublisherState::ActivationNotPossible),
            "Unexpected state: {}",
            *state
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn subscribe_active_getter_panics() {
        let publisher = ServicePublisherBuilder::new(Arc::new(MockServiceAdapter::new()))
            .build_and_activate()
            .await;

        publisher.subscribe(DATA_TRIGGER_NAME, create_empty_fn_box()).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn subscribe_already_subscribed_panics() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        service_adapter.set_next_subscribe_successful();
        let publisher = ServicePublisherBuilder::new(service_adapter)
            .with_connection_type(ConnectionType::Subscription)
            .build_and_activate()
            .await;
        publisher.subscribe(DATA_TRIGGER_NAME, create_empty_fn_box()).await;

        publisher.subscribe(DATA_TRIGGER_NAME, create_empty_fn_box()).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribe_new_message_notifies_subscriber() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        service_adapter.set_next_subscribe_successful();
        let publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_connection_type(ConnectionType::Subscription)
            .build_and_activate()
            .await;

        let (tx, rx) = oneshot_callback();
        let msg = Arc::new(Int32Value { value: 42, ..Default::default() }) as Arc<dyn MessageDyn>;
        publisher
            .subscribe(
                DATA_TRIGGER_NAME,
                Box::new(move |context| {
                    let msg = context
                        .get_message(PUBLISHER_NAME)
                        .expect("Publisher message should be present in the context");
                    tx(Arc::clone(msg));
                }),
            )
            .await;
        service_adapter.publish_new_message(&msg).await;

        let received_message = rx.await.to_protobuf().unwrap();
        let latest_message =
            publisher.get_latest_message().unwrap_sync().unwrap().unwrap().to_protobuf().unwrap();
        assert!(
            Arc::ptr_eq(&received_message, &msg),
            "Expected {received_message:?} to equal {msg:?}"
        );
        assert!(Arc::ptr_eq(&latest_message, &msg), "Expected {latest_message:?} to equal {msg:?}");
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribe_pending_activation_retries_and_notifier_subscriber() {
        // Publisher attempts activation and receives an error, leading to the retry attempts
        let service_adapter =
            Arc::new(MockServiceAdapter::new().with_next_error(anyhow!(ERROR_MESSAGE)));
        let publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_connection_type(ConnectionType::Subscription)
            .with_status_reporter(Arc::new(StatusReporterImpl::new(|_| {})))
            .with_activation_retry_interval(Duration::from_millis(3))
            .build_and_activate()
            .await;

        let (tx, rx) = oneshot_callback();
        let msg = Arc::new(Int32Value { value: 42, ..Default::default() }) as Arc<dyn MessageDyn>;
        publisher
            .subscribe(
                DATA_TRIGGER_NAME,
                Box::new(move |context| {
                    let msg = context
                        .get_message(PUBLISHER_NAME)
                        .expect("Publisher message should be present in the context");
                    tx(Arc::clone(msg));
                }),
            )
            .await;

        // Make the second activation attempt successful
        let subscribe_receiver = service_adapter.set_next_subscribe_successful();
        // Wait for the attempt to happen
        subscribe_receiver.await.unwrap();

        service_adapter.publish_new_message(&msg).await;

        let received_message = rx.await.to_protobuf().unwrap();
        let latest_message =
            publisher.get_latest_message().unwrap_sync().unwrap().unwrap().to_protobuf().unwrap();
        assert!(
            Arc::ptr_eq(&received_message, &msg),
            "Expected {received_message:?} to equal {msg:?}"
        );
        assert!(Arc::ptr_eq(&latest_message, &msg), "Expected {latest_message:?} to equal {msg:?}");
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_latest_message_calls_adapter() {
        let message = Box::new(Int32Value { value: 42, ..Default::default() });
        let mut message_provider = MockServiceMessageProvider::new();
        let message_clone = message.clone();
        message_provider.set_message_provider(move || message_clone.clone());
        let service_adapter = Arc::new(MockServiceAdapter::new());
        service_adapter.set_next_message_provider(message_provider);
        let publisher = ServicePublisherBuilder::new(service_adapter).build_and_activate().await;

        let latest_message = Arc::clone(&publisher)
            .get_latest_message()
            .unwrap_async()
            .await
            .unwrap()
            .unwrap()
            .to_protobuf()
            .unwrap();

        assert_that!(latest_message, msg_dyn_eq(message.as_ref(), &ReflectEqMode::default()));
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_latest_message_propagates_provider_error() {
        const ERROR_STRING: &str = "Error 42";
        let message_provider =
            MockServiceMessageProvider::new().with_next_error(anyhow!(ERROR_STRING));
        let service_adapter = Arc::new(MockServiceAdapter::new());
        service_adapter.set_next_message_provider(message_provider);
        let publisher = ServicePublisherBuilder::new(service_adapter).build_and_activate().await;

        let err = publisher.get_latest_message().unwrap_async().await.unwrap_err();

        assert!(err.to_string().contains(ERROR_STRING), "Unexpected error: {err:?}");
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_latest_message_inactive_returns_none() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        let publisher = ServicePublisherBuilder::new(service_adapter)
            .with_connection_type(ConnectionType::Subscription)
            .build();

        let latest_message =
            Arc::clone(&publisher).get_latest_message().unwrap_async().await.unwrap();

        assert!(latest_message.is_none());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_latest_message_activation_not_possible_returns_none() {
        let service_adapter = Arc::new(MockServiceAdapter::new());

        let publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_connection_type(ConnectionType::Subscription)
            .build_and_activate()
            .await;

        let latest_message =
            Arc::clone(&publisher).get_latest_message().unwrap_async().await.unwrap();

        assert!(latest_message.is_none());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_latest_message_pending_service_returns_none() {
        let service_adapter = Arc::new(MockServiceAdapter::new().with_availability_callback());
        let publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_connection_type(ConnectionType::Subscription)
            .build_and_activate()
            .await;

        let latest_message =
            Arc::clone(&publisher).get_latest_message().unwrap_async().await.unwrap();

        assert!(latest_message.is_none());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_latest_message_pending_activation_returns_none() {
        let service_adapter =
            Arc::new(MockServiceAdapter::new().with_next_error(anyhow!(ERROR_MESSAGE)));
        let publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_connection_type(ConnectionType::Subscription)
            .with_status_reporter(Arc::new(StatusReporterImpl::new(|_| {})))
            .build_and_activate()
            .await;

        let latest_message =
            Arc::clone(&publisher).get_latest_message().unwrap_async().await.unwrap();

        assert!(latest_message.is_none());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_latest_message_subscribed_no_messages_returns_none() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        service_adapter.set_next_subscribe_successful();

        let publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_connection_type(ConnectionType::Subscription)
            .build_and_activate()
            .await;

        let latest_message =
            Arc::clone(&publisher).get_latest_message().unwrap_async().await.unwrap();

        assert!(latest_message.is_none());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn unsubscribe_when_inactive_panics() {
        let publisher = ServicePublisherBuilder::new(Arc::new(MockServiceAdapter::new()))
            .with_connection_type(ConnectionType::Subscription)
            .build();

        publisher.unsubscribe(DATA_TRIGGER_NAME).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn unsubscribe_activation_not_possible_noop() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        let publisher = ServicePublisherBuilder::new(service_adapter)
            .with_connection_type(ConnectionType::Subscription)
            .build_and_activate()
            .await;

        publisher.subscribe(DATA_TRIGGER_NAME, create_empty_fn_box()).await;
        publisher.unsubscribe(DATA_TRIGGER_NAME).await;

        let state = publisher.state.lock().await;
        assert!(
            matches!(*state, ServicePublisherState::ActivationNotPossible),
            "Unexpected state: {}",
            *state
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn unsubscribe_pending_activation_success() {
        let service_adapter =
            Arc::new(MockServiceAdapter::new().with_next_error(anyhow!(ERROR_MESSAGE)));
        let publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_connection_type(ConnectionType::Subscription)
            .with_status_reporter(Arc::new(StatusReporterImpl::new(|_| {})))
            .with_activation_retry_interval(Duration::from_millis(3))
            .build_and_activate()
            .await;

        publisher.subscribe(DATA_TRIGGER_NAME, create_empty_fn_box()).await;
        publisher.unsubscribe(DATA_TRIGGER_NAME).await;

        let state = publisher.state.lock().await;
        assert!(
            matches!(*state, ServicePublisherState::PendingActivation { ref callback_map, .. }
                if callback_map.is_empty()),
            "Unexpected state: {}",
            *state
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn unsubscribe_pending_service_success() {
        let service_adapter = Arc::new(MockServiceAdapter::new().with_availability_callback());
        let publisher = ServicePublisherBuilder::new(Arc::clone(&service_adapter))
            .with_connection_type(ConnectionType::Subscription)
            .build_and_activate()
            .await;

        publisher.subscribe(DATA_TRIGGER_NAME, create_empty_fn_box()).await;
        publisher.unsubscribe(DATA_TRIGGER_NAME).await;

        let state = publisher.state.lock().await;
        assert!(
            matches!(*state, ServicePublisherState::PendingService { ref callback_map, .. }
                if callback_map.is_empty()),
            "Unexpected state: {}",
            *state
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn unsubscribe_active_getter_panics() {
        let publisher = ServicePublisherBuilder::new(Arc::new(MockServiceAdapter::new()))
            .build_and_activate()
            .await;

        publisher.unsubscribe(DATA_TRIGGER_NAME).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn unsubscribe_not_subscribed_panics() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        service_adapter.set_next_subscribe_successful();
        let publisher = ServicePublisherBuilder::new(service_adapter)
            .with_connection_type(ConnectionType::Subscription)
            .build_and_activate()
            .await;

        publisher.unsubscribe(DATA_TRIGGER_NAME).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn unsubscribe_success() {
        let service_adapter = Arc::new(MockServiceAdapter::new());
        service_adapter.set_next_subscribe_successful();
        let publisher = ServicePublisherBuilder::new(service_adapter)
            .with_connection_type(ConnectionType::Subscription)
            .build_and_activate()
            .await;
        publisher.subscribe(DATA_TRIGGER_NAME, create_empty_fn_box()).await;

        publisher.unsubscribe(DATA_TRIGGER_NAME).await;
    }
}
