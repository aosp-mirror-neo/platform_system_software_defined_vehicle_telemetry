// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;
use log::error;
use protobuf::{well_known_types::any::Any, MessageDyn};
use sdv_telemetry_service_publisher_accessor::{
    AvailabilityCallbackRegistration, CreateSubscriptionOutcome, FetchLastMessageOutcome,
    GetMessageProviderOutcome as AccessorGetMessageProviderOutcome, ServicePublisherAccessor,
    ServicePublisherAccessorError, ServicePublisherMessageProvider, ServicePublisherSubscription,
    SubSamplingInterval,
};
use sdv_telemetry_utils::atomic::{SequenceNumber, SequenceNumberType};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};
use tokio::task;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::metrics_config_orchestrator::async_event_processor::{
    AsyncEventHandler, AsyncEventProcessor,
};
use crate::metrics_config_orchestrator::error_status_codes::ErrorStatusCodes;
use crate::status_reporter::StatusReporter;

use google_sdv_telemetry::aidl::google::sdv::telemetry::status::{
    StatusDetails::ServiceErrorDetails::ServiceErrorDetails, StatusDetails::StatusDetails,
};

type CallbackContainer = Arc<
    AsyncMutex<(
        HashMap<
            SequenceNumberType,
            (
                Box<dyn for<'a> FnMut(&'a Arc<dyn MessageDyn>) + Send + Sync>,
                Box<dyn FnOnce() + Send + Sync>,
            ),
        >,
        // Latest message that was sent to the callbacks.
        Option<Arc<dyn MessageDyn>>,
    )>,
>;

/// Provides an interface to subscribe to messages published by services.
#[async_trait]
pub(crate) trait ServiceAdapter: Send + Sync {
    /// Returns provider of messages from the publisher specified by the service name.
    async fn get_message_provider(
        &self,
        service_name: &str,
    ) -> Result<GetMessageProviderOutcome, ServicePublisherAccessorError>;

    /// Subscribe to new messages from the service with the specified name
    /// using specified configuration.
    /// The callback mustn't block because it's executed on the adapter thread.
    async fn subscribe(
        self: Arc<Self>,
        service_name: &str,
        configuration: Option<&Any>,
        fetch_last_message: bool,
        sub_sampling_interval: &SubSamplingInterval,
        new_message_callback: Box<dyn for<'a> FnMut(&'a Arc<dyn MessageDyn>) + Send + Sync>,
        cancellation_callback: Box<dyn FnOnce() + Send + Sync>,
    ) -> Result<Option<ServiceSubscription>, ServicePublisherAccessorError>;

    /// Deactivate specified subscription.
    /// After asynchronous function is complete, the callback is guaranteed not to be called.
    async fn unsubscribe(&self, subscription: ServiceSubscription);

    /// Registers a callback that will be called when a service with the specified string
    /// service_name becomes available.
    async fn register_availability_callback(
        &self,
        service_name: &str,
        callback: Box<dyn Fn() + Send + Sync>,
    ) -> anyhow::Result<Option<Box<dyn AvailabilityCallbackRegistration>>>;
}

#[async_trait]
pub(crate) trait ServiceMessageProvider: Send + Sync {
    async fn get_latest_message(
        &self,
        configuration: Option<&Any>,
    ) -> Result<Option<Box<dyn MessageDyn>>, ServicePublisherAccessorError>;
}

/// Stores details about a service subscription.
pub(crate) struct ServiceSubscription {
    subscription_type: ServiceSubscriptionType,
}

enum ServiceSubscriptionType {
    /// Subscription with configuration that can't be shared.
    Configured {
        /// Underlying accessor subscription.
        accessor_subscription: Box<dyn ServicePublisherSubscription>,
    },
    /// Subscription without configuration that can be shared.
    Shared {
        /// The name of the service of the shared subscription.
        service_name: String,
        /// The sub-sampling interval of the shared subscription.
        sub_sampling_interval: SubSamplingInterval,
        /// The fetch last message outcome of the shared subscription.
        fetch_last_message_outcome: FetchLastMessageOutcome,
        /// Unique service subscription identifier.
        subscription_id: SequenceNumberType,
    },
}

/// An implementation of ServiceAdapter that publishes incrementing i32 messages at random interval.
pub(crate) struct ServiceAdapterImpl {
    // Id that will be used for the next subscription.
    next_subscription_id: SequenceNumber,

    // The registry of available service publishers.
    service_publisher_accessor: Arc<dyn ServicePublisherAccessor>,

    // The map with service name, sub-sampling interval, and fetch last message outcome as a key and
    // a tuple as a value that consists of:
    // 1. Underlying ServicePublisherSubscription.
    // 2. Async event processor.
    // 3. The map of functions that need to be called when a new message is available.
    //
    // The first subscription to service creates a new function map and passes it to the callback
    // that handles new messages. Last unsubscription to service clears the map signaling that the
    // callback shouldn't process messages.
    service_subscriptions: AsyncMutex<
        HashMap<
            (String, SubSamplingInterval, FetchLastMessageOutcome),
            (
                // Underlying accessor subscription.
                Box<dyn ServicePublisherSubscription>,
                // Async event processor that handles new messages.
                AsyncEventProcessor,
                // Map of new message and cancellation callbacks.
                CallbackContainer,
            ),
        >,
    >,
    status_reporter: Arc<dyn StatusReporter>,
}

impl ServiceSubscriptionType {
    fn new_configured(
        accessor_subscription: Box<dyn ServicePublisherSubscription>,
    ) -> ServiceSubscriptionType {
        ServiceSubscriptionType::Configured { accessor_subscription }
    }

    fn new_shared(
        service_name: String,
        subscription_id: SequenceNumberType,
        sub_sampling_interval: SubSamplingInterval,
        fetch_last_message_outcome: FetchLastMessageOutcome,
    ) -> ServiceSubscriptionType {
        ServiceSubscriptionType::Shared {
            service_name,
            subscription_id,
            sub_sampling_interval,
            fetch_last_message_outcome,
        }
    }
}

#[async_trait]
impl ServiceAdapter for ServiceAdapterImpl {
    async fn get_message_provider(
        &self,
        service_name: &str,
    ) -> Result<GetMessageProviderOutcome, ServicePublisherAccessorError> {
        struct ServiceMessageProviderImpl {
            service_publisher_message_provider: Box<dyn ServicePublisherMessageProvider>,
        }

        #[async_trait::async_trait]
        impl ServiceMessageProvider for ServiceMessageProviderImpl {
            async fn get_latest_message(
                &self,
                configuration: Option<&Any>,
            ) -> Result<Option<Box<dyn MessageDyn>>, ServicePublisherAccessorError> {
                self.service_publisher_message_provider.get_latest_message(configuration).await
            }
        }

        Ok(match self.service_publisher_accessor.get_message_provider(service_name).await? {
            AccessorGetMessageProviderOutcome::NotRegistered => {
                GetMessageProviderOutcome::NotRegistered
            }
            AccessorGetMessageProviderOutcome::GetNotSupported => {
                GetMessageProviderOutcome::GetNotSupported
            }
            AccessorGetMessageProviderOutcome::Success(service_publisher_message_provider) => {
                GetMessageProviderOutcome::Success(Box::new(ServiceMessageProviderImpl {
                    service_publisher_message_provider,
                })
                    as Box<dyn ServiceMessageProvider>)
            }
        })
    }

    async fn subscribe(
        self: Arc<Self>,
        service_name: &str,
        configuration: Option<&Any>,
        fetch_last_message: bool,
        sub_sampling_interval: &SubSamplingInterval,
        mut new_message_callback: Box<dyn for<'a> FnMut(&'a Arc<dyn MessageDyn>) + Send + Sync>,
        cancellation_callback: Box<dyn FnOnce() + Send + Sync>,
    ) -> Result<Option<ServiceSubscription>, ServicePublisherAccessorError> {
        let maybe_subscription_type = if configuration.is_some() {
            let status_reporter = Arc::clone(&self.status_reporter);
            let service_name_clone = service_name.to_owned();
            // Subscription has configuration, so it can't be shared.
            let create_subscription_outcome = self
                .service_publisher_accessor
                .create_subscription(
                    service_name,
                    configuration,
                    fetch_last_message,
                    sub_sampling_interval,
                    Box::new(move |msg| match msg {
                        Ok(msg) => new_message_callback(&msg.into()),
                        Err(err) => {
                            status_reporter.report(
                                err.get_status_code(),
                                format!(
                                    "Service runtime error for service '{service_name_clone}': {err}'"
                                ),
                                StatusDetails::ServiceErrorDetails(ServiceErrorDetails {
                                    serviceName: service_name_clone.clone(),
                                }));
                        }
                    }),
                    cancellation_callback,
                )
                .await?;

            match create_subscription_outcome {
                CreateSubscriptionOutcome::Subscribed {
                    subscription,
                    fetch_last_message_outcome: _,
                } => Some(ServiceSubscriptionType::new_configured(subscription)),
                CreateSubscriptionOutcome::NotRegistered { .. } => None,
            }
        } else {
            // Subscription doesn't have configuration, so it can be shared.
            let subscription_id = self.next_subscription_id.next();

            let mut service_subscriptions = self.service_subscriptions.lock().await;

            // First, try to find an existing subscription that supports `fetch_last_message` for
            // the `service_name`, `sub_sampling_interval`, and requested `fetch_last_message`.
            let existing_subscription = service_subscriptions
                .get_key_value(&(
                    service_name.to_owned(),
                    sub_sampling_interval.to_owned(),
                    FetchLastMessageOutcome::new_supported(fetch_last_message),
                ))
                .or_else(|| {
                    // If no existing subscription that supports `fetch_last_message` is found, try
                    // to fall back to an existing subscription that doesn't support
                    // `fetch_last_message`.
                    service_subscriptions.get_key_value(&(
                        service_name.to_owned(),
                        sub_sampling_interval.to_owned(),
                        FetchLastMessageOutcome::NotSupported,
                    ))
                });
            if let Some(((_, _, fetch_last_message_outcome), (_, _, callback_container))) =
                existing_subscription
            {
                let mut callback_container = callback_container.lock().await;
                let (ref mut callback_container, ref latest_message) = *callback_container;

                // If a subscription already exists, and the subscribe() call requests
                // `fetch_last_message`, and the existing subscription has `fetch_last_message`
                // supported and enabled, then manually call `new_message_callback` to ensure the
                // same behavior as with a fresh new subscription.
                if fetch_last_message
                    && matches!(fetch_last_message_outcome, FetchLastMessageOutcome::Enabled)
                {
                    if let Some(latest_message) = latest_message {
                        new_message_callback(latest_message);
                    }
                }

                // If there's already an active subscription, add callback to its map.
                callback_container
                    .insert(subscription_id, (new_message_callback, cancellation_callback));

                Some(ServiceSubscriptionType::new_shared(
                    service_name.to_owned(),
                    subscription_id,
                    sub_sampling_interval.to_owned(),
                    *fetch_last_message_outcome,
                ))
            } else {
                // If there is no active subscription, create a new one and store it.
                let service_name_clone = service_name.to_owned();
                let status_reporter = Arc::clone(&self.status_reporter);
                let (new_message_tx, new_message_rx) = mpsc::unbounded_channel();
                let (cancel_tx, cancel_rx) = oneshot::channel();
                let create_subscription_outcome = self
                .service_publisher_accessor
                .create_subscription(
                    service_name,
                    None,
                    fetch_last_message,
                    sub_sampling_interval,
                    Box::new(move |msg| match msg {
                        Ok(msg) => {
                            if new_message_tx.send(msg).is_err() {
                                // This means that `receiver` was dropped - which should never
                                // happen, since it is only dropped when unsubscribing.
                                error!("Received message for service {service_name_clone}, but receiver has already been dropped!");
                            }
                        }
                        Err(err) => {
                            status_reporter.report(
                                err.get_status_code(),
                                format!("Service runtime error for service '{service_name_clone}': {err}'"),
                                StatusDetails::ServiceErrorDetails(ServiceErrorDetails {
                                    serviceName: service_name_clone.clone(),
                                }));
                        }
                    }),
                    Box::new(move || {
                        // An error here would mean that the receiver has already been dropped, in
                        // which case we are no longer interested in the cancellation.
                        let _ = cancel_tx.send(());
                    }),
                )
                .await?;

                match create_subscription_outcome {
                    CreateSubscriptionOutcome::Subscribed {
                        subscription,
                        fetch_last_message_outcome,
                    } => {
                        let self_clone = Arc::clone(&self);
                        let service_name_clone = service_name.to_owned();
                        let sub_sampling_interval_clone = sub_sampling_interval.to_owned();
                        // We cannot directly call `on_shared_subscription_cancelled` inside of the
                        // cancellation callback argument passed to `create_subscription`, because
                        // we need to know the `fetch_last_message_outcome` to perform cancellation,
                        // which we only know after we have subscribed successfully.
                        task::spawn(async move {
                            let Ok(()) = cancel_rx.await else {
                                // An error here means that the sender was dropped, which means that
                                // cancellation can no longer occur.
                                return;
                            };
                            self_clone.on_shared_subscription_cancelled(
                                &service_name_clone,
                                &sub_sampling_interval_clone,
                                fetch_last_message_outcome,
                            )
                        });

                        struct ServiceAdapterHandler {
                            callback_container: CallbackContainer,
                        }

                        #[async_trait]
                        impl AsyncEventHandler<Box<dyn MessageDyn>> for ServiceAdapterHandler {
                            async fn handle(&mut self, message: Box<dyn MessageDyn>) {
                                let mut callback_container = self.callback_container.lock().await;
                                let (ref mut callbacks, ref mut latest_message) =
                                    *callback_container;

                                // Synchronously invoke callbacks. The loop needs to wait before
                                // they finish to release the mutex lock when they're done because
                                // they're allowed to mutate themselves.
                                let msg = message.into();
                                for (new_message_callback, _) in callbacks.values_mut() {
                                    new_message_callback(&msg);
                                }
                                *latest_message = Some(msg);
                            }
                        }

                        let callbacks = HashMap::from([(
                            subscription_id,
                            (new_message_callback, cancellation_callback),
                        )]);
                        let callback_container = Arc::new(AsyncMutex::new((callbacks, None)));
                        let callback_container_clone = Arc::clone(&callback_container);
                        let event_queue = AsyncEventProcessor::new(
                            UnboundedReceiverStream::new(new_message_rx),
                            ServiceAdapterHandler { callback_container: callback_container_clone },
                        );
                        service_subscriptions.insert(
                            (
                                service_name.to_owned(),
                                sub_sampling_interval.to_owned(),
                                fetch_last_message_outcome,
                            ),
                            (subscription, event_queue, callback_container),
                        );
                        Some(ServiceSubscriptionType::new_shared(
                            service_name.to_owned(),
                            subscription_id,
                            sub_sampling_interval.to_owned(),
                            fetch_last_message_outcome,
                        ))
                    }
                    CreateSubscriptionOutcome::NotRegistered { .. } => None,
                }
            }
        };

        Ok(maybe_subscription_type
            .map(|subscription_type| ServiceSubscription { subscription_type }))
    }

    async fn unsubscribe(&self, subscription: ServiceSubscription) {
        match subscription.subscription_type {
            ServiceSubscriptionType::Configured { accessor_subscription } => {
                accessor_subscription.unsubscribe().await;
            }
            ServiceSubscriptionType::Shared {
                service_name,
                subscription_id,
                sub_sampling_interval,
                fetch_last_message_outcome,
            } => {
                match self.service_subscriptions.lock().await.entry((
                    service_name,
                    sub_sampling_interval,
                    fetch_last_message_outcome,
                )) {
                    Entry::Occupied(o) => {
                        let (_, _, callback_container) = o.get();
                        let is_last_callback = {
                            let mut callback_container = callback_container.lock().await;
                            let (ref mut callbacks, ref _latest_message) = *callback_container;
                            let _ = callbacks
                                .remove(&subscription_id)
                                .expect("Subscription with the specified ID should exist");
                            callbacks.is_empty()
                        };

                        if is_last_callback {
                            // Remove entry and unsubscribe from the underlying subscription
                            // if the last callback was removed.
                            let (_, (underlying_subscription, _, _)) = o.remove_entry();
                            underlying_subscription.unsubscribe().await;
                        }
                    }
                    Entry::Vacant(v) => {
                        panic!("No entry found for service name {:?}", v.key());
                    }
                }
            }
        }
    }

    async fn register_availability_callback(
        &self,
        service_name: &str,
        callback: Box<dyn Fn() + Send + Sync>,
    ) -> anyhow::Result<Option<Box<dyn AvailabilityCallbackRegistration>>> {
        self.service_publisher_accessor
            .register_availability_callback(service_name, &callback.into())
            .await
    }
}

impl ServiceAdapterImpl {
    pub(super) fn new(
        service_publisher_manager: Arc<dyn ServicePublisherAccessor>,
        status_reporter: Arc<dyn StatusReporter>,
    ) -> Self {
        Self {
            next_subscription_id: SequenceNumber::new(),
            service_publisher_accessor: service_publisher_manager,
            service_subscriptions: AsyncMutex::new(HashMap::new()),
            status_reporter,
        }
    }

    fn on_shared_subscription_cancelled(
        self: &Arc<Self>,
        service_name: &str,
        sub_sampling_interval: &SubSamplingInterval,
        fetch_last_message_outcome: FetchLastMessageOutcome,
    ) {
        let self_clone = Arc::clone(self);
        let service_name_clone = service_name.to_owned();
        let sub_sampling_interval_clone = sub_sampling_interval.to_owned();
        tokio::spawn(async move {
            let Some((_, _, callback_container)) =
                self_clone.service_subscriptions.lock().await.remove(&(
                    service_name_clone,
                    sub_sampling_interval_clone,
                    fetch_last_message_outcome,
                ))
            else {
                return;
            };

            let mut callback_container = callback_container.lock().await;
            let (ref mut callbacks, ref _latest_message) = *callback_container;
            for (_, (_, cancellation_callback)) in callbacks.drain() {
                cancellation_callback();
            }
        });
    }
}

/// Successful result of getting a message provider.
pub(crate) enum GetMessageProviderOutcome {
    /// Service is available and supports Get.
    Success(Box<dyn ServiceMessageProvider>),
    /// Service is available, but does not support Get.
    GetNotSupported,
    /// Service is not available.
    NotRegistered,
}

#[cfg(test)]
impl std::fmt::Debug for GetMessageProviderOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Success(_) => "Success(<provider>)",
                Self::GetNotSupported => "GetNotSupported",
                Self::NotRegistered => "NotRegistered",
            }
        )
    }
}

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    use anyhow::anyhow;
    use google_sdv_telemetry::aidl::google::sdv::telemetry::status::{
        TelemetryServiceStatus::TelemetryServiceStatus,
        TelemetryServiceStatusCodes::TelemetryServiceStatusCodes as StatusCodes,
    };
    use lazy_static::lazy_static;
    use protobuf::reflect::FieldDescriptor;
    use protobuf::well_known_types::wrappers::Int32Value;
    use protobuf::MessageFull;
    use sdv_telemetry_test_utils::oneshot_callback;
    use tokio::sync::mpsc;

    use crate::mock_service_publisher_accessor::MockServicePublisherAccessor;
    use crate::StatusReporterImpl;

    const ERROR_MESSAGE: &str = "Error 42";
    const SERVICE_NAME: &str = "service";
    const SERVICE_NAME_2: &str = "service2";

    lazy_static! {
        static ref VALUE_FIELD_DESCRIPTOR: FieldDescriptor =
            Int32Value::descriptor().field_by_name("value").unwrap();
    }

    fn create_empty_fn_box() -> Box<dyn FnOnce() + Send + Sync> {
        Box::new(|| unreachable!())
    }

    fn create_empty_message_handler(
    ) -> Box<dyn for<'a> FnMut(&'a Arc<dyn MessageDyn>) + Send + Sync> {
        Box::new(|_| unreachable!())
    }

    fn create_service_adapter(
        accessor: Arc<MockServicePublisherAccessor>,
    ) -> Arc<ServiceAdapterImpl> {
        Arc::new(ServiceAdapterImpl::new(
            accessor,
            Arc::new(StatusReporterImpl::new(|err| {
                panic!("Unexpected error: {err:?}");
            })),
        ))
    }

    fn create_invalid_subscription() -> ServiceSubscription {
        ServiceSubscription {
            subscription_type: ServiceSubscriptionType::Shared {
                service_name: String::default(),
                subscription_id: SequenceNumberType::MAX,
                sub_sampling_interval: SubSamplingInterval::MAX,
                fetch_last_message_outcome: FetchLastMessageOutcome::NotSupported,
            },
        }
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_message_provider_accessor_returns_error_fails() {
        let accessor =
            Arc::new(MockServicePublisherAccessor::new().with_next_error(anyhow!(ERROR_MESSAGE)));
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        let err = service_adapter.get_message_provider(SERVICE_NAME).await.err().unwrap();

        assert!(err.to_string().contains(ERROR_MESSAGE), "Unexpected error: {err:?}");
        assert_eq!(accessor.get_num_get_message_provider_calls(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_message_provider_get_not_supported_success() {
        let accessor = Arc::new(MockServicePublisherAccessor::new().with_next_get_not_supported());
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        let outcome = service_adapter.get_message_provider(SERVICE_NAME).await;

        assert!(
            matches!(outcome, Ok(GetMessageProviderOutcome::GetNotSupported)),
            "Unexpected outcome: {outcome:?}"
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_message_provider_accessor_latest_message_returns_error_fails() {
        let accessor = Arc::new(MockServicePublisherAccessor::new());
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        let GetMessageProviderOutcome::Success(provider) =
            service_adapter.get_message_provider(SERVICE_NAME).await.unwrap()
        else {
            panic!("Provider should be available");
        };
        accessor.set_next_error(anyhow!(ERROR_MESSAGE));
        let err = provider.get_latest_message(None).await.err().unwrap();

        assert!(err.to_string().contains(ERROR_MESSAGE), "Unexpected error: {err:?}");
        assert_eq!(accessor.get_num_get_message_provider_calls(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_message_provider_get_latest_message_success() {
        let mut message = Box::new(Int32Value::new());
        message.value = 42;
        let accessor =
            Arc::new(MockServicePublisherAccessor::new().with_latest_message(message.clone()));
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        let outcome = service_adapter.get_message_provider(SERVICE_NAME).await;
        let Ok(GetMessageProviderOutcome::Success(message_provider)) = outcome else {
            panic!("Unexpected outcome: {outcome:?}");
        };

        let latest_message = message_provider
            .get_latest_message(None)
            .await
            .unwrap()
            .expect("Message should be available");
        assert_eq!(&latest_message, &(message as Box<dyn MessageDyn>));
        assert_eq!(accessor.get_num_get_message_provider_calls(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribe_configured_accessor_returns_error_fails() {
        let accessor =
            Arc::new(MockServicePublisherAccessor::new().with_next_error(anyhow!(ERROR_MESSAGE)));
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        let err = service_adapter
            .subscribe(
                SERVICE_NAME,
                Some(&Any::new()),
                false,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .err()
            .unwrap();

        assert!(err.to_string().contains(ERROR_MESSAGE), "Unexpected error: {err:?}");
        assert_eq!(accessor.get_num_subscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribe_configured_accessor_returns_not_available_success() {
        let accessor = Arc::new(MockServicePublisherAccessor::new().with_next_not_available());
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        let res = service_adapter
            .subscribe(
                SERVICE_NAME,
                Some(&Any::new()),
                false,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .unwrap();

        assert!(res.is_none());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribe_configured_success() {
        let accessor = Arc::new(MockServicePublisherAccessor::new());
        let service_adapter = create_service_adapter(Arc::clone(&accessor));
        let (tx, mut rx) = mpsc::channel(1);
        let mut msg = Box::new(Int32Value::new());
        msg.value = 1;

        service_adapter
            .subscribe(
                SERVICE_NAME,
                Some(&Any::new()),
                false,
                &SubSamplingInterval::ZERO,
                Box::new(move |msg| tx.try_send(msg.clone_box()).unwrap()),
                create_empty_fn_box(),
            )
            .await
            .unwrap();

        // Notify about a new message and receive the message.
        accessor.notify_subscriber(SERVICE_NAME, &SubSamplingInterval::ZERO, Ok(msg.clone()));
        let msg_received = rx.recv().await.unwrap();

        // Verify that the received message is the same as the one sent.
        assert_eq!(&msg_received, &(msg as Box<dyn MessageDyn>));

        // Verify the number of registry subscribe calls.
        assert_eq!(accessor.get_num_subscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribe_shared_accessor_returns_error_fails() {
        let accessor =
            Arc::new(MockServicePublisherAccessor::new().with_next_error(anyhow!(ERROR_MESSAGE)));
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        let err = service_adapter
            .subscribe(
                SERVICE_NAME,
                None,
                false,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .err()
            .unwrap();

        assert!(err.to_string().contains(ERROR_MESSAGE), "Unexpected error: {err:?}");
        assert_eq!(accessor.get_num_subscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribe_shared_accessor_returns_not_available_success() {
        let accessor = Arc::new(MockServicePublisherAccessor::new().with_next_not_available());
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        let res = service_adapter
            .subscribe(
                SERVICE_NAME,
                None,
                false,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .unwrap();

        assert!(res.is_none());
        assert_eq!(accessor.get_num_subscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribe_shared_different_services_subscribes_to_registry() {
        let accessor = Arc::new(MockServicePublisherAccessor::new());
        let service_adapter = create_service_adapter(Arc::clone(&accessor));
        let (tx, mut rx) = mpsc::channel(1);
        let mut msg1 = Box::new(Int32Value::new());
        msg1.value = 1;
        let mut msg2 = Box::new(Int32Value::new());
        msg2.value = 2;

        // Create two subscriptions to different services.
        let tx_clone = tx.clone();
        Arc::clone(&service_adapter)
            .subscribe(
                SERVICE_NAME,
                None,
                false,
                &SubSamplingInterval::ZERO,
                Box::new(move |msg| tx_clone.try_send(msg.clone_box()).unwrap()),
                create_empty_fn_box(),
            )
            .await
            .unwrap();
        Arc::clone(&service_adapter)
            .subscribe(
                SERVICE_NAME_2,
                None,
                false,
                &SubSamplingInterval::ZERO,
                Box::new(move |msg| tx.try_send(msg.clone_box()).unwrap()),
                create_empty_fn_box(),
            )
            .await
            .unwrap();

        // Notify about a new message from each service and receive the message.
        accessor.notify_subscriber(SERVICE_NAME, &SubSamplingInterval::ZERO, Ok(msg1.clone()));
        let msg1_received = rx.recv().await.unwrap();

        accessor.notify_subscriber(SERVICE_NAME_2, &SubSamplingInterval::ZERO, Ok(msg2.clone()));
        let msg2_received = rx.recv().await.unwrap();

        // Verify that the received messages are the same as the ones sent.
        assert_eq!(&msg1_received, &(msg1 as Box<dyn MessageDyn>));
        assert_eq!(&msg2_received, &(msg2 as Box<dyn MessageDyn>));

        // Verify the number of registry subscribe calls.
        assert_eq!(accessor.get_num_subscribe_calls(), 2);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribe_shared_same_service_subscribes_to_registry() {
        let accessor = Arc::new(MockServicePublisherAccessor::new());
        let service_adapter = create_service_adapter(Arc::clone(&accessor));
        let (tx, mut rx) = mpsc::channel(2);
        let mut msg = Box::new(Int32Value::new());
        msg.value = 1;

        // Create two subscriptions to the same service.
        let tx_clone = tx.clone();
        Arc::clone(&service_adapter)
            .subscribe(
                SERVICE_NAME,
                None,
                false,
                &SubSamplingInterval::ZERO,
                Box::new(move |msg| tx_clone.try_send(msg.clone_box()).unwrap()),
                create_empty_fn_box(),
            )
            .await
            .unwrap();
        Arc::clone(&service_adapter)
            .subscribe(
                SERVICE_NAME,
                None,
                false,
                &SubSamplingInterval::ZERO,
                Box::new(move |msg| tx.try_send(msg.clone_box()).unwrap()),
                create_empty_fn_box(),
            )
            .await
            .unwrap();

        // Notify about a new message from the service
        // and receive messages from the two subscriptions.
        accessor.notify_subscriber(SERVICE_NAME, &SubSamplingInterval::ZERO, Ok(msg.clone()));
        let msg1_received = rx.recv().await.unwrap();
        let msg2_received = rx.recv().await.unwrap();

        // Verify that the received messages are the same as the one sent.
        let msg = msg as Box<dyn MessageDyn>;
        assert_eq!(&msg1_received, &msg);
        assert_eq!(&msg2_received, &msg);

        // Verify the number of registry subscribe calls.
        assert_eq!(accessor.get_num_subscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribe_shared_same_service_different_sub_sampling_interval_subscribes_twice() {
        let accessor = Arc::new(MockServicePublisherAccessor::new());
        let service_adapter = create_service_adapter(Arc::clone(&accessor));
        let (tx, mut rx) = mpsc::channel(2);
        let mut msg = Box::new(Int32Value::new());
        msg.value = 1;

        // Create two subscriptions to the same service.
        let tx_clone = tx.clone();
        Arc::clone(&service_adapter)
            .subscribe(
                SERVICE_NAME,
                None,
                false,
                &SubSamplingInterval::ZERO,
                Box::new(move |msg| tx_clone.try_send(msg.clone_box()).unwrap()),
                create_empty_fn_box(),
            )
            .await
            .unwrap();
        Arc::clone(&service_adapter)
            .subscribe(
                SERVICE_NAME,
                None,
                false,
                &SubSamplingInterval::MAX,
                Box::new(move |msg| tx.try_send(msg.clone_box()).unwrap()),
                create_empty_fn_box(),
            )
            .await
            .unwrap();

        // Notify about a new message from the service
        // and receive messages from the two subscriptions.
        accessor.notify_subscriber(SERVICE_NAME, &SubSamplingInterval::ZERO, Ok(msg.clone()));
        accessor.notify_subscriber(SERVICE_NAME, &SubSamplingInterval::MAX, Ok(msg.clone()));
        let msg1_received = rx.recv().await.unwrap();
        let msg2_received = rx.recv().await.unwrap();

        // Verify that the received messages are the same as the one sent.
        let msg = msg as Box<dyn MessageDyn>;
        assert_eq!(&msg1_received, &msg);
        assert_eq!(&msg2_received, &msg);

        // Verify the number of registry subscribe calls.
        assert_eq!(accessor.get_num_subscribe_calls(), 2);
    }

    // This test must run on a multi-threaded executor, because only then does the code under test
    // exhibit the problematic behavior.
    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn subscribe_shared_same_service_receives_messages_in_order() {
        const RANGE: std::ops::RangeInclusive<i32> = 1..=10;

        // Run this multiple times, because the erroneous behavior does not always manifest.
        for _ in 1..=100 {
            let accessor = Arc::new(MockServicePublisherAccessor::new());
            let service_adapter = Arc::new(ServiceAdapterImpl::new(
                Arc::<MockServicePublisherAccessor>::clone(&accessor),
                Arc::new(StatusReporterImpl::new(|err| {
                    panic!("Unexpected error: {err:?}");
                })),
            ));
            let (tx, mut rx) = mpsc::unbounded_channel();

            Arc::clone(&service_adapter)
                .subscribe(
                    SERVICE_NAME,
                    None,
                    false,
                    &SubSamplingInterval::ZERO,
                    Box::new(move |msg| tx.send(msg.clone_box()).unwrap()),
                    create_empty_fn_box(),
                )
                .await
                .unwrap();

            for i in RANGE {
                accessor.notify_subscriber(
                    SERVICE_NAME,
                    &SubSamplingInterval::ZERO,
                    Ok(Box::new(Int32Value { value: i, ..Default::default() })),
                );
            }
            let mut values = vec![];
            for _ in RANGE {
                values.push(rx.recv().await.unwrap().downcast_ref::<Int32Value>().unwrap().value);
            }

            assert_eq!(values, RANGE.collect::<Vec<_>>());
        }
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribe_shared_calls_cancellation_callbacks() {
        let accessor = Arc::new(MockServicePublisherAccessor::new());
        let service_adapter = create_service_adapter(Arc::clone(&accessor));
        let (tx1, rx1) = oneshot_callback();
        let (tx2, rx2) = oneshot_callback();

        let _subscription = Arc::clone(&service_adapter)
            .subscribe(
                SERVICE_NAME,
                None,
                false,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                Box::new(move || tx1(())),
            )
            .await
            .unwrap()
            .expect("Subscription should be available");

        let _subscription2 = Arc::clone(&service_adapter)
            .subscribe(
                SERVICE_NAME,
                None,
                false,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                Box::new(move || tx2(())),
            )
            .await
            .unwrap()
            .expect("Subscription should be available");

        accessor.notify_cancellation();

        rx1.await;
        rx2.await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn unsubscribe_not_subscribed_panics() {
        let service_adapter = create_service_adapter(Arc::new(MockServicePublisherAccessor::new()));

        // Create subscription for a different service.
        Arc::clone(&service_adapter)
            .subscribe(
                SERVICE_NAME,
                None,
                false,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .unwrap();

        // Try unsubscribing using invalid subscription.
        service_adapter.unsubscribe(create_invalid_subscription()).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn unsubscribe_configured_success() {
        let accessor = Arc::new(MockServicePublisherAccessor::new());
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        let subscription = Arc::clone(&service_adapter)
            .subscribe(
                SERVICE_NAME,
                Some(&Any::new()),
                false,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .unwrap()
            .expect("Subscription should be available");
        service_adapter.unsubscribe(subscription).await;

        // Verify the number of registry unsubscribe calls.
        assert_eq!(accessor.get_num_unsubscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn unsubscribe_shared_success() {
        let accessor = Arc::new(MockServicePublisherAccessor::new());
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        let subscription = Arc::clone(&service_adapter)
            .subscribe(
                SERVICE_NAME,
                None,
                false,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .unwrap()
            .expect("Subscription should be available");
        service_adapter.unsubscribe(subscription).await;

        // Verify the number of registry unsubscribe calls.
        assert_eq!(accessor.get_num_unsubscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn unsubscribe_shared_not_called_unless_last_subscriber_unsubscribes() {
        let accessor = Arc::new(MockServicePublisherAccessor::new());
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        let subscription = Arc::clone(&service_adapter)
            .subscribe(
                SERVICE_NAME,
                None,
                false,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .unwrap()
            .expect("Subscription should be available");

        let _subscription2 = Arc::clone(&service_adapter)
            .subscribe(
                SERVICE_NAME,
                None,
                false,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .unwrap()
            .expect("Subscription should be available");

        service_adapter.unsubscribe(subscription).await;

        // Verify the number of registry unsubscribe calls.
        assert_eq!(accessor.get_num_unsubscribe_calls(), 0);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn unsubscribe_shared_called_after_last_subscriber_unsubscribes() {
        let accessor = Arc::new(MockServicePublisherAccessor::new());
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        let subscription = Arc::clone(&service_adapter)
            .subscribe(
                SERVICE_NAME,
                None,
                false,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .unwrap()
            .expect("Subscription should be available");

        let subscription2 = Arc::clone(&service_adapter)
            .subscribe(
                SERVICE_NAME,
                None,
                false,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .unwrap()
            .expect("Subscription should be available");

        service_adapter.unsubscribe(subscription).await;
        service_adapter.unsubscribe(subscription2).await;

        // Verify the number of registry unsubscribe calls.
        assert_eq!(accessor.get_num_unsubscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn register_availability_callback_success() {
        let accessor = Arc::new(MockServicePublisherAccessor::new().with_availability_callback());
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        service_adapter
            .register_availability_callback(SERVICE_NAME, Box::new(|| ()))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(accessor.get_num_register_availability_callback_calls(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn register_availability_callback_propagates_error() {
        let accessor = Arc::new(
            MockServicePublisherAccessor::new().with_next_error(anyhow!(ERROR_MESSAGE.to_owned())),
        );
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        let err = service_adapter
            .register_availability_callback(SERVICE_NAME, Box::new(|| ()))
            .await
            .err()
            .unwrap();

        assert_eq!(err.to_string(), ERROR_MESSAGE);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribe_configured_reports_errors() {
        let accessor = Arc::new(MockServicePublisherAccessor::new());

        let (tx, rx) = oneshot_callback();
        let service_adapter = Arc::new(ServiceAdapterImpl::new(
            Arc::clone(&accessor) as Arc<dyn ServicePublisherAccessor>,
            Arc::new(StatusReporterImpl::new(tx)),
        ));

        service_adapter
            .subscribe(
                SERVICE_NAME,
                Some(&Any::new()),
                false,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .unwrap();

        accessor.notify_subscriber(
            SERVICE_NAME,
            &SubSamplingInterval::ZERO,
            Err(ServicePublisherAccessorError::MessageParsingFailed(anyhow!(
                ERROR_MESSAGE.to_owned()
            ))),
        );

        // Wait for error status to be delivered
        let status = rx.await;
        assert!(
            matches!(&status, TelemetryServiceStatus {
                id: 0,
                code: StatusCodes::METRICS_CONFIG_RUNTIME_SERVICE_MESSAGE_PARSING_ERROR,
                message,
                details: StatusDetails::ServiceErrorDetails(ServiceErrorDetails { serviceName } ),
            }
            if message.contains("Service runtime error for service") && message.contains(ERROR_MESSAGE) && serviceName == SERVICE_NAME),
            "Unexpected status: {status:?}"
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribe_shared_reports_errors() {
        let accessor = Arc::new(MockServicePublisherAccessor::new());

        let (tx, rx) = oneshot_callback();
        let service_adapter = Arc::new(ServiceAdapterImpl::new(
            Arc::clone(&accessor) as Arc<dyn ServicePublisherAccessor>,
            Arc::new(StatusReporterImpl::new(tx)),
        ));

        service_adapter
            .subscribe(
                SERVICE_NAME,
                None,
                false,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .unwrap();

        accessor.notify_subscriber(
            SERVICE_NAME,
            &SubSamplingInterval::ZERO,
            Err(ServicePublisherAccessorError::MessageParsingFailed(anyhow!(
                ERROR_MESSAGE.to_owned()
            ))),
        );

        // Wait for error status to be delivered
        let status = rx.await;
        assert!(
            matches!(&status, TelemetryServiceStatus {
                id: 0,
                code: StatusCodes::METRICS_CONFIG_RUNTIME_SERVICE_MESSAGE_PARSING_ERROR,
                message,
                details: StatusDetails::ServiceErrorDetails(
                    ServiceErrorDetails { serviceName }
                ),
            }
            if message.contains("Service runtime error for service") && message.contains(ERROR_MESSAGE) && serviceName == SERVICE_NAME),
            "Unexpected status: {status:?}"
        );
    }
}

#[cfg(test)]
mod tests_with_mockall {
    use super::*;

    use futures::{stream, StreamExt};
    use googletest::expect_that;
    use googletest::matchers::{container_eq, eq};
    use mockall::{predicate as m, Sequence};
    use protobuf::well_known_types::wrappers::Int32Value;
    use rand::random_bool;
    use sdv_telemetry_service_publisher_accessor::{
        MockServicePublisherAccessor, MockServicePublisherSubscription,
    };
    use sdv_telemetry_test_utils::oneshot_callback;
    use sdv_telemetry_test_utils::parameterized_test;
    use std::{sync::LazyLock, time::Duration};
    use tokio::time::sleep;

    use crate::StatusReporterImpl;

    const SERVICE_NAME: &str = "service";

    static CONFIG_1: LazyLock<Option<Any>> =
        LazyLock::new(|| Some(Any::pack(&Int32Value { value: 1, ..Default::default() }).unwrap()));
    static CONFIG_2: LazyLock<Option<Any>> =
        LazyLock::new(|| Some(Any::pack(&Int32Value { value: 2, ..Default::default() }).unwrap()));

    fn create_service_adapter(
        accessor: Arc<MockServicePublisherAccessor>,
    ) -> Arc<ServiceAdapterImpl> {
        Arc::new(ServiceAdapterImpl::new(
            accessor,
            Arc::new(StatusReporterImpl::new(|status| {
                panic!("Unexpected status: {status:?}");
            })),
        ))
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribe_with_config_does_not_share_subscription() {
        // All of these subscribe requests should create individual subscriptions, since all of them
        // specify a configuration.
        let steps = [
            (CONFIG_1.as_ref(), false, FetchLastMessageOutcome::Disabled),
            (CONFIG_1.as_ref(), false, FetchLastMessageOutcome::Disabled),
            (CONFIG_1.as_ref(), false, FetchLastMessageOutcome::NotSupported),
            (CONFIG_1.as_ref(), true, FetchLastMessageOutcome::NotSupported),
            (CONFIG_1.as_ref(), true, FetchLastMessageOutcome::NotSupported),
            (CONFIG_1.as_ref(), true, FetchLastMessageOutcome::Enabled),
            (CONFIG_1.as_ref(), true, FetchLastMessageOutcome::Enabled),
            (CONFIG_2.as_ref(), false, FetchLastMessageOutcome::Disabled),
            (CONFIG_2.as_ref(), false, FetchLastMessageOutcome::Disabled),
            (CONFIG_2.as_ref(), false, FetchLastMessageOutcome::NotSupported),
            (CONFIG_2.as_ref(), false, FetchLastMessageOutcome::NotSupported),
            (CONFIG_2.as_ref(), false, FetchLastMessageOutcome::Enabled),
            (CONFIG_2.as_ref(), true, FetchLastMessageOutcome::Enabled),
            (CONFIG_2.as_ref(), true, FetchLastMessageOutcome::Disabled),
            (CONFIG_2.as_ref(), true, FetchLastMessageOutcome::Disabled),
        ];

        let mut seq = Sequence::new();
        let mut accessor = MockServicePublisherAccessor::new();
        for (expected_config, expected_fetch_last_message, expected_fetch_last_message_outcome) in
            steps.iter().cloned()
        {
            accessor
                .expect_create_subscription()
                .once()
                .in_sequence(&mut seq)
                .withf(
                    move |service_name,
                          configuration,
                          fetch_last_message,
                          sub_sampling_interval,
                          _new_message_callback,
                          _cancellation_callback| {
                        service_name == SERVICE_NAME
                            && *configuration == expected_config
                            && *fetch_last_message == expected_fetch_last_message
                            && *sub_sampling_interval == SubSamplingInterval::ZERO
                    },
                )
                .return_once(
                    move |_service_name,
                          _configuration,
                          _fetch_last_message,
                          _sub_sampling_interval,
                          _new_message_callback,
                          _cancellation_callback| {
                        Ok(CreateSubscriptionOutcome::Subscribed {
                            subscription: Box::new(MockServicePublisherSubscription::new()),
                            fetch_last_message_outcome: expected_fetch_last_message_outcome,
                        })
                    },
                );
        }
        let accessor = Arc::new(accessor);
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        for (config, fetch_last_message, _) in steps.into_iter() {
            let outcome = Arc::clone(&service_adapter)
                .subscribe(
                    SERVICE_NAME,
                    config,
                    fetch_last_message,
                    &SubSamplingInterval::ZERO,
                    Box::new(move |msg| panic!("Unexpected message: {msg:?}")),
                    Box::new(move || panic!("Unexpected cancellation")),
                )
                .await
                .unwrap();

            assert!(matches!(
                outcome,
                Some(ServiceSubscription {
                    subscription_type: ServiceSubscriptionType::Configured {
                        accessor_subscription: _
                    }
                })
            ));
        }
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribe_without_config_and_unsupported_fetch_last_message_shares_subscription() {
        let fetch_last_message_values = [
            random_bool(0.5),
            random_bool(0.5),
            random_bool(0.5),
            random_bool(0.5),
            random_bool(0.5),
            random_bool(0.5),
            random_bool(0.5),
            random_bool(0.5),
            random_bool(0.5),
            random_bool(0.5),
        ];
        eprintln!("fetch_last_message_values: {fetch_last_message_values:?}");

        let mut accessor = MockServicePublisherAccessor::new();
        accessor
            .expect_create_subscription()
            .once()
            .withf(
                move |service_name,
                      configuration,
                      fetch_last_message,
                      sub_sampling_interval,
                      _new_message_callback,
                      _cancellation_callback| {
                    service_name == SERVICE_NAME
                        && configuration.is_none()
                        && *fetch_last_message == fetch_last_message_values[0]
                        && *sub_sampling_interval == SubSamplingInterval::ZERO
                },
            )
            .return_once(
                move |_service_name,
                      _configuration,
                      _fetch_last_message,
                      _sub_sampling_interval,
                      _new_message_callback,
                      _cancellation_callback| {
                    Ok(CreateSubscriptionOutcome::Subscribed {
                        subscription: Box::new(MockServicePublisherSubscription::new()),
                        fetch_last_message_outcome: FetchLastMessageOutcome::NotSupported,
                    })
                },
            );
        let accessor = Arc::new(accessor);
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        for fetch_last_message in fetch_last_message_values.into_iter() {
            let outcome = Arc::clone(&service_adapter)
                .subscribe(
                    SERVICE_NAME,
                    None,
                    fetch_last_message,
                    &SubSamplingInterval::ZERO,
                    Box::new(move |msg| panic!("Unexpected message: {msg:?}")),
                    Box::new(move || panic!("Unexpected cancellation")),
                )
                .await
                .unwrap();

            assert!(matches!(
                outcome,
                Some(ServiceSubscription {
                    subscription_type: ServiceSubscriptionType::Shared {
                        fetch_last_message_outcome: FetchLastMessageOutcome::NotSupported,
                        ..
                    }
                })
            ));
        }
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribe_without_config_and_supported_fetch_last_message_shares_subscription_for_same_fetch_last_message_value(
    ) {
        let fetch_last_message_values = [
            random_bool(0.5),
            random_bool(0.5),
            random_bool(0.5),
            random_bool(0.5),
            random_bool(0.5),
            random_bool(0.5),
            random_bool(0.5),
            random_bool(0.5),
            random_bool(0.5),
            random_bool(0.5),
            // Ensure that at least one value is true and one value is false.
            true,
            false,
        ];
        eprintln!("fetch_last_message_values: {fetch_last_message_values:?}");

        let mut accessor = MockServicePublisherAccessor::new();
        for want_fetch_last_message in [true, false] {
            accessor
                .expect_create_subscription()
                .once()
                .withf(
                    move |service_name,
                          configuration,
                          fetch_last_message,
                          sub_sampling_interval,
                          _new_message_callback,
                          _cancellation_callback| {
                        service_name == SERVICE_NAME
                            && configuration.is_none()
                            && *fetch_last_message == want_fetch_last_message
                            && *sub_sampling_interval == SubSamplingInterval::ZERO
                    },
                )
                .return_once(
                    move |_service_name,
                          _configuration,
                          _fetch_last_message,
                          _sub_sampling_interval,
                          _new_message_callback,
                          _cancellation_callback| {
                        Ok(CreateSubscriptionOutcome::Subscribed {
                            subscription: Box::new(MockServicePublisherSubscription::new()),
                            fetch_last_message_outcome: FetchLastMessageOutcome::new_supported(
                                want_fetch_last_message,
                            ),
                        })
                    },
                );
        }
        let accessor = Arc::new(accessor);
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        for fetch_last_message in fetch_last_message_values.into_iter() {
            let subscription = Arc::clone(&service_adapter)
                .subscribe(
                    SERVICE_NAME,
                    None,
                    fetch_last_message,
                    &SubSamplingInterval::ZERO,
                    Box::new(move |msg| panic!("Unexpected message: {msg:?}")),
                    Box::new(move || panic!("Unexpected cancellation")),
                )
                .await
                .unwrap()
                .unwrap();

            let ServiceSubscription {
                subscription_type:
                    ServiceSubscriptionType::Shared { fetch_last_message_outcome, .. },
            } = subscription
            else {
                panic!("Unexpected subscription");
            };

            assert!(if fetch_last_message {
                matches!(fetch_last_message_outcome, FetchLastMessageOutcome::Enabled)
            } else {
                matches!(fetch_last_message_outcome, FetchLastMessageOutcome::Disabled)
            });
        }
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribing_twice_with_fetch_last_message_but_there_is_no_last_message() {
        let mut accessor = MockServicePublisherAccessor::new();
        accessor
            .expect_create_subscription()
            .once()
            .withf(
                move |service_name,
                      configuration,
                      fetch_last_message,
                      sub_sampling_interval,
                      _new_message_callback,
                      _cancellation_callback| {
                    service_name == SERVICE_NAME
                        && configuration.is_none()
                        && *fetch_last_message
                        && *sub_sampling_interval == SubSamplingInterval::ZERO
                },
            )
            .return_once(
                move |_service_name,
                      _configuration,
                      _fetch_last_message,
                      _sub_sampling_interval,
                      _new_message_callback,
                      _cancellation_callback| {
                    Ok(CreateSubscriptionOutcome::Subscribed {
                        subscription: Box::new(MockServicePublisherSubscription::new()),
                        fetch_last_message_outcome: FetchLastMessageOutcome::new_supported(true),
                    })
                },
            );
        let accessor = Arc::new(accessor);
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        for _ in 0..2 {
            let subscription = Arc::clone(&service_adapter)
                .subscribe(
                    SERVICE_NAME,
                    None,
                    true,
                    &SubSamplingInterval::ZERO,
                    Box::new(move |msg| panic!("Unexpected message: {msg:?}")),
                    Box::new(move || panic!("Unexpected cancellation")),
                )
                .await
                .unwrap()
                .unwrap();

            let ServiceSubscription {
                subscription_type:
                    ServiceSubscriptionType::Shared { fetch_last_message_outcome, .. },
            } = subscription
            else {
                panic!("Unexpected subscription");
            };

            assert!(matches!(fetch_last_message_outcome, FetchLastMessageOutcome::Enabled));
        }
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribing_twice_with_fetch_last_message_and_there_is_a_last_message() {
        let mut accessor = MockServicePublisherAccessor::new();
        accessor
            .expect_create_subscription()
            .once()
            .withf(
                move |service_name,
                      configuration,
                      fetch_last_message,
                      sub_sampling_interval,
                      _new_message_callback,
                      _cancellation_callback| {
                    service_name == SERVICE_NAME
                        && configuration.is_none()
                        && *fetch_last_message
                        && *sub_sampling_interval == SubSamplingInterval::ZERO
                },
            )
            .return_once(
                move |_service_name,
                      _configuration,
                      _fetch_last_message,
                      _sub_sampling_interval,
                      mut new_message_callback,
                      _cancellation_callback| {
                    new_message_callback(Ok(Box::new(Int32Value {
                        value: 123,
                        ..Default::default()
                    })));

                    Ok(CreateSubscriptionOutcome::Subscribed {
                        subscription: Box::new(MockServicePublisherSubscription::new()),
                        fetch_last_message_outcome: FetchLastMessageOutcome::new_supported(true),
                    })
                },
            );
        let accessor = Arc::new(accessor);
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        for _ in 0..2 {
            let (tx, rx) = oneshot_callback();
            let subscription = Arc::clone(&service_adapter)
                .subscribe(
                    SERVICE_NAME,
                    None,
                    true,
                    &SubSamplingInterval::ZERO,
                    Box::new(move |msg| tx(msg.to_owned())),
                    Box::new(move || panic!("Unexpected cancellation")),
                )
                .await
                .unwrap()
                .unwrap();

            let ServiceSubscription {
                subscription_type:
                    ServiceSubscriptionType::Shared { fetch_last_message_outcome, .. },
            } = subscription
            else {
                panic!("Unexpected subscription");
            };

            assert!(matches!(fetch_last_message_outcome, FetchLastMessageOutcome::Enabled));

            let msg = rx.await.downcast_ref::<Int32Value>().unwrap().to_owned();
            assert_eq!(msg.value, 123);
        }
    }

    parameterized_test!(
        #[googletest::test]
        #[sdv_telemetry_test_utils::timeout]
        #[tokio::test]
        subscribing_thrice_times_with_fetch_last_message_and_there_are_multiple_messages,

        |expected_fetch_last_message| async move {
            let (new_message_tx, new_message_rx) = mpsc::unbounded_channel();
            let mut new_message_rx = UnboundedReceiverStream::new(new_message_rx);

            let mut accessor = MockServicePublisherAccessor::new();
            accessor
                .expect_create_subscription()
                .once()
                .withf(
                    move |service_name,
                        configuration,
                        fetch_last_message,
                        sub_sampling_interval,
                        _new_message_callback,
                        _cancellation_callback| {
                        service_name == SERVICE_NAME
                            && configuration.is_none()
                            && expected_fetch_last_message == *fetch_last_message
                            && *sub_sampling_interval == SubSamplingInterval::ZERO
                    },
                )
                .return_once(
                    move |_service_name,
                        _configuration,
                        _fetch_last_message,
                        _sub_sampling_interval,
                        mut new_message_callback,
                        _cancellation_callback| {
                        task::spawn(async move {
                            while let Some(value) = new_message_rx.next().await {
                                new_message_callback(Ok(Box::new(Int32Value {
                                    value,
                                    ..Default::default()
                                })));
                            }
                        });

                        let mut subscription = MockServicePublisherSubscription::new();
                        subscription.expect_unsubscribe().once().return_const(());

                        Ok(CreateSubscriptionOutcome::Subscribed {
                            subscription: Box::new(subscription),
                            fetch_last_message_outcome: FetchLastMessageOutcome::new_supported(expected_fetch_last_message),
                        })
                    },
                );

            let accessor = Arc::new(accessor);
            let service_adapter = create_service_adapter(Arc::clone(&accessor));

            let (subscriber1_tx, subscriber1_rx) = mpsc::unbounded_channel();
            let (subscriber2_tx, subscriber2_rx) = mpsc::unbounded_channel();
            let (subscriber3_tx, subscriber3_rx) = mpsc::unbounded_channel();
            let mut subscriptions = vec![];
            for (i, subscriber_tx) in [subscriber1_tx, subscriber2_tx, subscriber3_tx].into_iter().enumerate() {
                let subscription = Arc::clone(&service_adapter)
                    .subscribe(
                        SERVICE_NAME,
                        None,
                        expected_fetch_last_message,
                        &SubSamplingInterval::ZERO,
                        Box::new(move |msg| subscriber_tx.send(msg.to_owned()).unwrap()),
                        Box::new(move || panic!("Unexpected cancellation")),
                    )
                    .await
                    .unwrap()
                    .unwrap();
                assert!(if expected_fetch_last_message {
                    matches!(
                        subscription,
                        ServiceSubscription {
                            subscription_type: ServiceSubscriptionType::Shared {
                                fetch_last_message_outcome: FetchLastMessageOutcome::Enabled,
                                ..
                            },
                        }
                    )
                } else {
                    matches!(
                        subscription,
                        ServiceSubscription {
                            subscription_type: ServiceSubscriptionType::Shared {
                                fetch_last_message_outcome: FetchLastMessageOutcome::Disabled,
                                ..
                            },
                        }
                    )
                });
                subscriptions.push(subscription);

                // Publish a few messages after creating the first subscription, but before the second
                // and third subscription.
                if i == 0 {
                    new_message_tx.send(1).unwrap();
                    new_message_tx.send(2).unwrap();
                    new_message_tx.send(3).unwrap();

                    // Give the code some time to process the messages before creating the next
                    // subscription.
                    sleep(Duration::from_millis(200)).await;
                }
            }

            // Publish a few more messages after all three subscriptions are created.
            new_message_tx.send(4).unwrap();
            new_message_tx.send(5).unwrap();
            new_message_tx.send(6).unwrap();

            // Give the code some time to process the messages before unsubscribing.
            sleep(Duration::from_millis(200)).await;
            for subscription in subscriptions.into_iter() {
                service_adapter.unsubscribe(subscription).await;
            }

            let [messages1, messages2, messages3] =
                stream::iter([subscriber1_rx, subscriber2_rx, subscriber3_rx])
                    .then(|subscriber_rx| {
                        UnboundedReceiverStream::new(subscriber_rx)
                            .map(|msg| msg.downcast_ref::<Int32Value>().unwrap().value)
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>()
                    .await
                    .try_into()
                    .unwrap();

            expect_that!(messages1, container_eq([1, 2, 3, 4, 5, 6]));
            if expected_fetch_last_message {
                expect_that!(messages2, container_eq([3, 4, 5, 6]));
                expect_that!(messages3, container_eq([3, 4, 5, 6]));
            } else {
                expect_that!(messages2, container_eq([4, 5, 6]));
                expect_that!(messages3, container_eq([4, 5, 6]));
            }
        },

        flm_true: (true),
        flm_false: (false),
    );

    parameterized_test!(
        #[googletest::test]
        #[sdv_telemetry_test_utils::timeout]
        #[tokio::test]
        fetch_last_message_works,

        |fetch_last_message_values: Vec<bool>| async move {
            run_fetch_last_message_works(fetch_last_message_values).await;
        },

        r#false: (vec![false]),
        r#true:  (vec![true]),

        false_false: (vec![false, false]),
        false_true:  (vec![false, true]),
        true_false:  (vec![true,  false]),
        true_true:   (vec![true,  true]),

        false_false_false: (vec![false, false, false]),
        false_false_true:  (vec![false, false, true]),
        false_true_false:  (vec![false, true,  false]),
        false_true_true:   (vec![false, true,  true]),
        true_false_false:  (vec![true,  false, false]),
        true_false_true:   (vec![true,  false, true]),
        true_true_false:   (vec![true,  true,  false]),
        true_true_true:    (vec![true,  true,  true]),
    );

    async fn run_fetch_last_message_works(fetch_last_message_values: Vec<bool>) {
        let mut accessor = MockServicePublisherAccessor::new();

        if fetch_last_message_values.iter().any(|fetch_last_message| !*fetch_last_message) {
            accessor
                .expect_create_subscription()
                .once()
                .with(
                    /* service_name */ m::eq(SERVICE_NAME),
                    /* configuration */ m::always(),
                    /* fetch_last_message */ m::eq(false),
                    /* sub_sampling_interval */ m::eq(SubSamplingInterval::ZERO),
                    /* new_message_callback */ m::always(),
                    /* cancellation_callback */ m::always(),
                )
                .returning(
                    move |_service_name,
                          _configuration,
                          _fetch_last_message,
                          _sub_sampling_interval,
                          _new_message_callback,
                          _cancellation_callback| {
                        Ok(CreateSubscriptionOutcome::Subscribed {
                            subscription: Box::new(MockServicePublisherSubscription::new()),
                            fetch_last_message_outcome: FetchLastMessageOutcome::Disabled,
                        })
                    },
                );
        }

        if fetch_last_message_values.iter().any(|fetch_last_message| *fetch_last_message) {
            accessor
                .expect_create_subscription()
                .once()
                .with(
                    /* service_name */ m::eq(SERVICE_NAME),
                    /* configuration */ m::always(),
                    /* fetch_last_message */ m::eq(true),
                    /* sub_sampling_interval */ m::eq(SubSamplingInterval::ZERO),
                    /* new_message_callback */ m::always(),
                    /* cancellation_callback */ m::always(),
                )
                .returning(
                    move |_service_name,
                          _configuration,
                          _fetch_last_message,
                          _sub_sampling_interval,
                          mut new_message_callback,
                          _cancellation_callback| {
                        new_message_callback(Ok(Box::new(Int32Value {
                            value: 123,
                            ..Default::default()
                        })));

                        Ok(CreateSubscriptionOutcome::Subscribed {
                            subscription: Box::new(MockServicePublisherSubscription::new()),
                            fetch_last_message_outcome: FetchLastMessageOutcome::Enabled,
                        })
                    },
                );
        }

        let accessor = Arc::new(accessor);
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        let messages = stream::iter(fetch_last_message_values.into_iter())
            .then(|fetch_last_message| {
                let service_adapter = Arc::clone(&service_adapter);
                async move {
                    let (message_tx, message_rx) = oneshot_callback();

                    let subscription = service_adapter
                        .subscribe(
                            SERVICE_NAME,
                            None,
                            fetch_last_message,
                            &SubSamplingInterval::ZERO,
                            Box::new(move |msg| {
                                if fetch_last_message {
                                    message_tx(msg.to_owned());
                                } else {
                                    panic!("Unexpected message: {msg:?}")
                                }
                            }),
                            Box::new(|| panic!("Unexpected cancellation")),
                        )
                        .await
                        .unwrap()
                        .unwrap();
                    assert!(if fetch_last_message {
                        matches!(
                            subscription,
                            ServiceSubscription {
                                subscription_type: ServiceSubscriptionType::Shared {
                                    fetch_last_message_outcome: FetchLastMessageOutcome::Enabled,
                                    ..
                                },
                            }
                        )
                    } else {
                        matches!(
                            subscription,
                            ServiceSubscription {
                                subscription_type: ServiceSubscriptionType::Shared {
                                    fetch_last_message_outcome: FetchLastMessageOutcome::Disabled,
                                    ..
                                },
                            }
                        )
                    });
                    (fetch_last_message, message_rx)
                }
            })
            .collect::<Vec<_>>()
            .await;

        // Give the code some time to process the messages.
        sleep(Duration::from_millis(200)).await;

        for (fetch_last_message, message_rx) in messages {
            if fetch_last_message {
                let msg = message_rx.await.downcast_ref::<Int32Value>().unwrap().to_owned();
                expect_that!(msg.value, eq(123));
            }
        }
    }
}
