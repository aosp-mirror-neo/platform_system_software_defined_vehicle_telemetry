// Copyright 2023 Google LLC

use async_trait::async_trait;
use log::error;
use protobuf::{well_known_types::any::Any, MessageDyn};
use sdv_telemetry_utils::atomic::{SequenceNumber, SequenceNumberType};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex as AsyncMutex};

use crate::metrics_config_orchestrator::async_event_processor::{
    AsyncEventHandler, AsyncEventProcessor, AsyncReceiver,
};
use crate::metrics_config_runtime_error::MetricsConfigRuntimeError;
use crate::service_publisher_accessor::{
    AvailabilityCallbackRegistration, CreateSubscriptionError, CreateSubscriptionOutcome,
    GetMessageProviderError, GetMessageProviderOutcome as AccessorGetMessageProviderOutcome,
    ServicePublisherAccessor, ServicePublisherMessageProvider, ServicePublisherSubscription,
};
use crate::sub_sampling_interval::SubSamplingInterval;

/// Provides an interface to subscribe to messages published by services.
#[async_trait]
pub(crate) trait ServiceAdapter {
    /// Returns provider of messages from the publisher specified by the service name.
    async fn get_message_provider(
        &self,
        service_name: &str,
    ) -> Result<GetMessageProviderOutcome, GetMessageProviderError>;

    /// Subscribe to new messages from the service with the specified name
    /// using specified configuration.
    /// The callback mustn't block because it's executed on the adapter thread.
    async fn subscribe(
        &self,
        service_name: &str,
        configuration: Option<&Any>,
        sub_sampling_interval: &SubSamplingInterval,
        new_message_callback: Box<dyn for<'a> FnMut(&'a Arc<dyn MessageDyn>) + Send + Sync>,
    ) -> Result<Option<ServiceSubscription>, CreateSubscriptionError>;

    /// Deactivate specified subscription.
    /// After asynchronous function is complete, the callback is guaranteed not to be called.
    async fn unsubscribe(&self, subscription: ServiceSubscription) -> anyhow::Result<()>;

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
    ) -> anyhow::Result<Option<Box<dyn MessageDyn>>>;
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

    // The map with service name and sub-sampling interval as a key and a tuple as a value that
    // consists of:
    // 1. Underlying ServicePublisherSubscription.
    // 2. The map of functions that need to be called when a new message is available.
    //
    // The first subscription to service creates a new function map and passes it to the callback
    // that handles new messages. Last unsubscription to service clears the map signaling that the
    // callback shouldn't process messages.
    service_subscriptions: AsyncMutex<
        HashMap<
            (String, SubSamplingInterval),
            (
                Box<dyn ServicePublisherSubscription>,
                AsyncEventProcessor,
                Arc<
                    AsyncMutex<
                        HashMap<
                            SequenceNumberType,
                            Box<dyn for<'a> FnMut(&'a Arc<dyn MessageDyn>) + Send + Sync>,
                        >,
                    >,
                >,
            ),
        >,
    >,
    error_callback: Arc<dyn Fn(MetricsConfigRuntimeError) + Send + Sync>,
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
    ) -> ServiceSubscriptionType {
        ServiceSubscriptionType::Shared { service_name, subscription_id, sub_sampling_interval }
    }
}

#[async_trait]
impl ServiceAdapter for ServiceAdapterImpl {
    async fn get_message_provider(
        &self,
        service_name: &str,
    ) -> Result<GetMessageProviderOutcome, GetMessageProviderError> {
        struct ServiceMessageProviderImpl {
            service_publisher_message_provider: Box<dyn ServicePublisherMessageProvider>,
        }

        #[async_trait::async_trait]
        impl ServiceMessageProvider for ServiceMessageProviderImpl {
            async fn get_latest_message(
                &self,
                configuration: Option<&Any>,
            ) -> anyhow::Result<Option<Box<dyn MessageDyn>>> {
                self.service_publisher_message_provider.get_latest_message(configuration).await
            }
        }

        let (message_provider, service_discovery_errors) =
            self.service_publisher_accessor.get_message_provider(service_name).await;
        for service_discovery_error in service_discovery_errors {
            (self.error_callback)(MetricsConfigRuntimeError::ServiceDiscovery {
                metrics_config_uuid: None,
                service_name: service_name.to_owned(),
                error: service_discovery_error,
            })
        }
        Ok(match message_provider? {
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
        &self,
        service_name: &str,
        configuration: Option<&Any>,
        sub_sampling_interval: &SubSamplingInterval,
        mut new_message_callback: Box<dyn for<'a> FnMut(&'a Arc<dyn MessageDyn>) + Send + Sync>,
    ) -> Result<Option<ServiceSubscription>, CreateSubscriptionError> {
        let maybe_subscription_type = if configuration.is_some() {
            let error_callback = Arc::clone(&self.error_callback);
            let service_name_clone = service_name.to_owned();
            // Subscription has configuration, so it can't be shared.
            let (create_subscription_outcome, service_discovery_errors) = self
                .service_publisher_accessor
                .create_subscription(
                    service_name,
                    configuration,
                    sub_sampling_interval,
                    Box::new(move |msg| match msg {
                        Ok(msg) => new_message_callback(&msg.into()),
                        Err(err) => {
                            (error_callback)(MetricsConfigRuntimeError::Service {
                                service_name: service_name_clone.clone(),
                                error: err,
                            });
                        }
                    }),
                )
                .await;
            for service_discovery_error in service_discovery_errors {
                (self.error_callback)(MetricsConfigRuntimeError::ServiceDiscovery {
                    metrics_config_uuid: None,
                    service_name: service_name.to_owned(),
                    error: service_discovery_error,
                })
            }

            match create_subscription_outcome? {
                CreateSubscriptionOutcome::Subscribed(subscription) => {
                    Some(ServiceSubscriptionType::new_configured(subscription))
                }
                CreateSubscriptionOutcome::NotRegistered(_) => None,
            }
        } else {
            // Subscription doesn't have configuration, so it can be shared.
            let subscription_id = self.next_subscription_id.next();

            match self
                .service_subscriptions
                .lock()
                .await
                .entry((service_name.to_owned(), sub_sampling_interval.to_owned()))
            {
                Entry::Occupied(o) => {
                    // If there's already an active subscription, add callback to its map.
                    let (_, _, callback_container) = o.get();
                    callback_container.lock().await.insert(subscription_id, new_message_callback);

                    Some(ServiceSubscriptionType::new_shared(
                        service_name.to_owned(),
                        subscription_id,
                        sub_sampling_interval.to_owned(),
                    ))
                }
                Entry::Vacant(v) => {
                    // If there is no active subscription, create a new one and store it.
                    let error_callback = Arc::clone(&self.error_callback);
                    let (sender, receiver) = mpsc::unbounded_channel();
                    let service_name_clone = service_name.to_owned();
                    let (create_subscription_outcome, service_discovery_errors) = self
                    .service_publisher_accessor
                        .create_subscription(
                            service_name,
                            None,
                            sub_sampling_interval,
                            Box::new(move |msg| match msg {
                                Ok(msg) => {
                                    if sender.send(msg).is_err() {
                                        // This means that `receiver` was dropped - which should
                                        // never happen, since it is only dropped when unsubscribing.
                                        error!("Received message for service {service_name_clone}, but receiver has already been dropped!");
                                    }
                                }
                                Err(err) => {
                                    (error_callback)(MetricsConfigRuntimeError::Service {
                                        service_name: service_name_clone.clone(),
                                        error: err,
                                    });
                                }
                            }),
                        )
                        .await;
                    for service_discovery_error in service_discovery_errors {
                        (self.error_callback)(MetricsConfigRuntimeError::ServiceDiscovery {
                            metrics_config_uuid: None,
                            service_name: service_name.to_owned(),
                            error: service_discovery_error,
                        })
                    }

                    match create_subscription_outcome? {
                        CreateSubscriptionOutcome::Subscribed(subscription) => {
                            struct ServiceAdapterHandler {
                                callback_container: Arc<
                                    AsyncMutex<
                                        HashMap<
                                            SequenceNumberType,
                                            Box<
                                                dyn for<'a> FnMut(&'a Arc<dyn MessageDyn>)
                                                    + Send
                                                    + Sync,
                                            >,
                                        >,
                                    >,
                                >,
                            }

                            #[async_trait]
                            impl AsyncEventHandler<Box<dyn MessageDyn>> for ServiceAdapterHandler {
                                async fn handle(&mut self, message: Box<dyn MessageDyn>) {
                                    let mut subscriptions = self.callback_container.lock().await;

                                    // Synchronously invoke callbacks. The loop needs to wait before
                                    // they finish to release the mutex lock when they're done
                                    // because they're allowed to mutate themselves.
                                    let msg = message.into();
                                    for subscription in subscriptions.values_mut() {
                                        subscription(&msg);
                                    }
                                }
                            }

                            let callbacks =
                                HashMap::from([(subscription_id, new_message_callback)]);
                            let callback_container = Arc::new(AsyncMutex::new(callbacks));
                            let callback_container_clone = Arc::clone(&callback_container);
                            let event_queue = AsyncEventProcessor::new(
                                AsyncReceiver::Unbounded(receiver),
                                ServiceAdapterHandler {
                                    callback_container: callback_container_clone,
                                },
                            );
                            v.insert((subscription, event_queue, callback_container));
                            Some(ServiceSubscriptionType::new_shared(
                                service_name.to_owned(),
                                subscription_id,
                                sub_sampling_interval.to_owned(),
                            ))
                        }
                        CreateSubscriptionOutcome::NotRegistered(_) => None,
                    }
                }
            }
        };

        Ok(maybe_subscription_type
            .map(|subscription_type| ServiceSubscription { subscription_type }))
    }

    async fn unsubscribe(&self, subscription: ServiceSubscription) -> anyhow::Result<()> {
        match subscription.subscription_type {
            ServiceSubscriptionType::Configured { accessor_subscription } => {
                accessor_subscription.unsubscribe().await?;
            }
            ServiceSubscriptionType::Shared {
                service_name,
                subscription_id,
                sub_sampling_interval,
            } => {
                match self
                    .service_subscriptions
                    .lock()
                    .await
                    .entry((service_name, sub_sampling_interval))
                {
                    Entry::Occupied(o) => {
                        let (_, _, callback_container) = o.get();
                        let is_last_callback = {
                            let mut callbacks = callback_container.lock().await;
                            let _ = callbacks
                                .remove(&subscription_id)
                                .expect("Subscription with the specified ID should exist");
                            callbacks.is_empty()
                        };

                        if is_last_callback {
                            // Remove entry and unsubscribe from the underlying subscription
                            // if the last callback was removed.
                            let (_, (underlying_subscription, _, _)) = o.remove_entry();
                            underlying_subscription.unsubscribe().await?;
                        }
                    }
                    Entry::Vacant(v) => {
                        panic!("No entry found for service name {:?}", v.key());
                    }
                }
            }
        }

        Ok(())
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
        error_callback: Arc<dyn Fn(MetricsConfigRuntimeError) + Send + Sync>,
    ) -> Self {
        Self {
            next_subscription_id: SequenceNumber::new(),
            service_publisher_accessor: service_publisher_manager,
            service_subscriptions: AsyncMutex::new(HashMap::new()),
            error_callback,
        }
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
    use lazy_static::lazy_static;
    use protobuf::reflect::FieldDescriptor;
    use protobuf::well_known_types::wrappers::Int32Value;
    use protobuf::MessageFull;
    use tokio::sync::mpsc;

    use crate::service_publisher_accessor::mock::MockServicePublisherAccessor;

    const ERROR_MESSAGE: &str = "Error 42";
    const SERVICE_NAME: &str = "service";
    const SERVICE_NAME_2: &str = "service2";

    lazy_static! {
        static ref VALUE_FIELD_DESCRIPTOR: FieldDescriptor =
            Int32Value::descriptor().field_by_name("value").unwrap();
    }

    fn create_empty_fn_box() -> Box<dyn for<'a> FnMut(&'a Arc<dyn MessageDyn>) + Send + Sync> {
        Box::new(|_| {})
    }

    fn create_service_adapter(accessor: Arc<MockServicePublisherAccessor>) -> ServiceAdapterImpl {
        ServiceAdapterImpl::new(
            accessor,
            Arc::new(|err| {
                panic!("Unexpected error: {err:?}");
            }),
        )
    }

    fn create_invalid_subscription() -> ServiceSubscription {
        ServiceSubscription {
            subscription_type: ServiceSubscriptionType::Shared {
                service_name: String::default(),
                subscription_id: SequenceNumberType::MAX,
                sub_sampling_interval: SubSamplingInterval::MAX,
            },
        }
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_message_provider_accessor_returns_error_fails() {
        let accessor =
            Arc::new(MockServicePublisherAccessor::new().with_next_error(anyhow!(ERROR_MESSAGE)));
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        let err = service_adapter.get_message_provider(SERVICE_NAME).await.err().unwrap();

        assert!(err.to_string().contains(ERROR_MESSAGE), "Unexpected error: {:?}", err);
        assert_eq!(accessor.get_num_get_message_provider_calls(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
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

    #[sdv_telemetry_test_utils_macros::timeout]
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

        assert!(err.to_string().contains(ERROR_MESSAGE), "Unexpected error: {:?}", err);
        assert_eq!(accessor.get_num_get_message_provider_calls(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
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

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn subscribe_configured_accessor_returns_error_fails() {
        let accessor =
            Arc::new(MockServicePublisherAccessor::new().with_next_error(anyhow!(ERROR_MESSAGE)));
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        let err = service_adapter
            .subscribe(
                SERVICE_NAME,
                Some(&Any::new()),
                &SubSamplingInterval::ZERO,
                create_empty_fn_box(),
            )
            .await
            .err()
            .unwrap();

        assert!(err.to_string().contains(ERROR_MESSAGE), "Unexpected error: {:?}", err);
        assert_eq!(accessor.get_num_subscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn subscribe_configured_accessor_returns_not_available_success() {
        let accessor = Arc::new(MockServicePublisherAccessor::new().with_next_not_available());
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        let res = service_adapter
            .subscribe(
                SERVICE_NAME,
                Some(&Any::new()),
                &SubSamplingInterval::ZERO,
                create_empty_fn_box(),
            )
            .await
            .unwrap();

        assert!(res.is_none());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
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
                &SubSamplingInterval::ZERO,
                Box::new(move |msg| tx.try_send(msg.clone_box()).unwrap()),
            )
            .await
            .unwrap();

        // Notify about a new message and receive the message.
        accessor.notify_subscriber(SERVICE_NAME, &SubSamplingInterval::ZERO, msg.clone());
        let msg_received = rx.recv().await.unwrap();

        // Verify that the received message is the same as the one sent.
        assert_eq!(&msg_received, &(msg as Box<dyn MessageDyn>));

        // Verify the number of registry subscribe calls.
        assert_eq!(accessor.get_num_subscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn subscribe_shared_accessor_returns_error_fails() {
        let accessor =
            Arc::new(MockServicePublisherAccessor::new().with_next_error(anyhow!(ERROR_MESSAGE)));
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        let err = service_adapter
            .subscribe(SERVICE_NAME, None, &SubSamplingInterval::ZERO, create_empty_fn_box())
            .await
            .err()
            .unwrap();

        assert!(err.to_string().contains(ERROR_MESSAGE), "Unexpected error: {:?}", err);
        assert_eq!(accessor.get_num_subscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn subscribe_shared_accessor_returns_not_available_success() {
        let accessor = Arc::new(MockServicePublisherAccessor::new().with_next_not_available());
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        let res = service_adapter
            .subscribe(SERVICE_NAME, None, &SubSamplingInterval::ZERO, create_empty_fn_box())
            .await
            .unwrap();

        assert!(res.is_none());
        assert_eq!(accessor.get_num_subscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
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
        service_adapter
            .subscribe(
                SERVICE_NAME,
                None,
                &SubSamplingInterval::ZERO,
                Box::new(move |msg| tx_clone.try_send(msg.clone_box()).unwrap()),
            )
            .await
            .unwrap();
        service_adapter
            .subscribe(
                SERVICE_NAME_2,
                None,
                &SubSamplingInterval::ZERO,
                Box::new(move |msg| tx.try_send(msg.clone_box()).unwrap()),
            )
            .await
            .unwrap();

        // Notify about a new message from each service and receive the message.
        accessor.notify_subscriber(SERVICE_NAME, &SubSamplingInterval::ZERO, msg1.clone());
        let msg1_received = rx.recv().await.unwrap();

        accessor.notify_subscriber(SERVICE_NAME_2, &SubSamplingInterval::ZERO, msg2.clone());
        let msg2_received = rx.recv().await.unwrap();

        // Verify that the received messages are the same as the ones sent.
        assert_eq!(&msg1_received, &(msg1 as Box<dyn MessageDyn>));
        assert_eq!(&msg2_received, &(msg2 as Box<dyn MessageDyn>));

        // Verify the number of registry subscribe calls.
        assert_eq!(accessor.get_num_subscribe_calls(), 2);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn subscribe_shared_same_service_subscribes_to_registry() {
        let accessor = Arc::new(MockServicePublisherAccessor::new());
        let service_adapter = create_service_adapter(Arc::clone(&accessor));
        let (tx, mut rx) = mpsc::channel(2);
        let mut msg = Box::new(Int32Value::new());
        msg.value = 1;

        // Create two subscriptions to the same service.
        let tx_clone = tx.clone();
        service_adapter
            .subscribe(
                SERVICE_NAME,
                None,
                &SubSamplingInterval::ZERO,
                Box::new(move |msg| tx_clone.try_send(msg.clone_box()).unwrap()),
            )
            .await
            .unwrap();
        service_adapter
            .subscribe(
                SERVICE_NAME,
                None,
                &SubSamplingInterval::ZERO,
                Box::new(move |msg| tx.try_send(msg.clone_box()).unwrap()),
            )
            .await
            .unwrap();

        // Notify about a new message from the service
        // and receive messages from the two subscriptions.
        accessor.notify_subscriber(SERVICE_NAME, &SubSamplingInterval::ZERO, msg.clone());
        let msg1_received = rx.recv().await.unwrap();
        let msg2_received = rx.recv().await.unwrap();

        // Verify that the received messages are the same as the one sent.
        let msg = msg as Box<dyn MessageDyn>;
        assert_eq!(&msg1_received, &msg);
        assert_eq!(&msg2_received, &msg);

        // Verify the number of registry subscribe calls.
        assert_eq!(accessor.get_num_subscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn subscribe_shared_same_service_different_sub_sampling_interval_subscribes_twice() {
        let accessor = Arc::new(MockServicePublisherAccessor::new());
        let service_adapter = create_service_adapter(Arc::clone(&accessor));
        let (tx, mut rx) = mpsc::channel(2);
        let mut msg = Box::new(Int32Value::new());
        msg.value = 1;

        // Create two subscriptions to the same service.
        let tx_clone = tx.clone();
        service_adapter
            .subscribe(
                SERVICE_NAME,
                None,
                &SubSamplingInterval::ZERO,
                Box::new(move |msg| tx_clone.try_send(msg.clone_box()).unwrap()),
            )
            .await
            .unwrap();
        service_adapter
            .subscribe(
                SERVICE_NAME,
                None,
                &SubSamplingInterval::MAX,
                Box::new(move |msg| tx.try_send(msg.clone_box()).unwrap()),
            )
            .await
            .unwrap();

        // Notify about a new message from the service
        // and receive messages from the two subscriptions.
        accessor.notify_subscriber(SERVICE_NAME, &SubSamplingInterval::ZERO, msg.clone());
        accessor.notify_subscriber(SERVICE_NAME, &SubSamplingInterval::MAX, msg.clone());
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
    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn subscribe_shared_same_service_receives_messages_in_order() {
        const RANGE: std::ops::RangeInclusive<i32> = 1..=10;

        // Run this multiple times, because the erroneous behavior does not always manifest.
        for _ in 1..=100 {
            let accessor = Arc::new(MockServicePublisherAccessor::new());
            let service_adapter = ServiceAdapterImpl::new(
                Arc::<MockServicePublisherAccessor>::clone(&accessor),
                Arc::new(|err| {
                    panic!("Unexpected error: {err:?}");
                }),
            );
            let (tx, mut rx) = mpsc::unbounded_channel();

            service_adapter
                .subscribe(
                    SERVICE_NAME,
                    None,
                    &SubSamplingInterval::ZERO,
                    Box::new(move |msg| tx.send(msg.clone_box()).unwrap()),
                )
                .await
                .unwrap();

            for i in RANGE {
                accessor.notify_subscriber(
                    SERVICE_NAME,
                    &SubSamplingInterval::ZERO,
                    Box::new(Int32Value { value: i, ..Default::default() }),
                );
            }
            let mut values = vec![];
            for _ in RANGE {
                values.push(rx.recv().await.unwrap().downcast_ref::<Int32Value>().unwrap().value);
            }

            assert_eq!(values, RANGE.collect::<Vec<_>>());
        }
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    #[should_panic]
    async fn unsubscribe_not_subscribed_panics() {
        let service_adapter = create_service_adapter(Arc::new(MockServicePublisherAccessor::new()));

        // Create subscription for a different service.
        service_adapter
            .subscribe(SERVICE_NAME, None, &SubSamplingInterval::ZERO, create_empty_fn_box())
            .await
            .unwrap();

        // Try unsubscribing using invalid subscription.
        service_adapter.unsubscribe(create_invalid_subscription()).await.unwrap();
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn unsubscribe_configured_accessor_returns_error_fails() {
        let accessor = Arc::new(MockServicePublisherAccessor::new());
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        let subscription = service_adapter
            .subscribe(
                SERVICE_NAME,
                Some(&Any::new()),
                &SubSamplingInterval::ZERO,
                create_empty_fn_box(),
            )
            .await
            .unwrap()
            .expect("Subscription should be available");
        accessor.set_next_error(anyhow!(ERROR_MESSAGE));
        let err = service_adapter.unsubscribe(subscription).await.unwrap_err();

        assert!(err.to_string().contains(ERROR_MESSAGE), "Unexpected error: {:?}", err);
        assert_eq!(accessor.get_num_unsubscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn unsubscribe_configured_success() {
        let accessor = Arc::new(MockServicePublisherAccessor::new());
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        let subscription = service_adapter
            .subscribe(
                SERVICE_NAME,
                Some(&Any::new()),
                &SubSamplingInterval::ZERO,
                create_empty_fn_box(),
            )
            .await
            .unwrap()
            .expect("Subscription should be available");
        service_adapter.unsubscribe(subscription).await.unwrap();

        // Verify the number of registry unsubscribe calls.
        assert_eq!(accessor.get_num_unsubscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn unsubscribe_shared_accessor_returns_error_fails() {
        let accessor = Arc::new(MockServicePublisherAccessor::new());
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        let subscription = service_adapter
            .subscribe(SERVICE_NAME, None, &SubSamplingInterval::ZERO, create_empty_fn_box())
            .await
            .unwrap()
            .expect("Subscription should be available");
        accessor.set_next_error(anyhow!(ERROR_MESSAGE));
        let err = service_adapter.unsubscribe(subscription).await.unwrap_err();

        assert!(err.to_string().contains(ERROR_MESSAGE), "Unexpected error: {:?}", err);
        assert_eq!(accessor.get_num_unsubscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn unsubscribe_shared_success() {
        let accessor = Arc::new(MockServicePublisherAccessor::new());
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        let subscription = service_adapter
            .subscribe(SERVICE_NAME, None, &SubSamplingInterval::ZERO, create_empty_fn_box())
            .await
            .unwrap()
            .expect("Subscription should be available");
        service_adapter.unsubscribe(subscription).await.unwrap();

        // Verify the number of registry unsubscribe calls.
        assert_eq!(accessor.get_num_unsubscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn unsubscribe_shared_not_called_unless_last_subscriber_unsubscribes() {
        let accessor = Arc::new(MockServicePublisherAccessor::new());
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        let subscription = service_adapter
            .subscribe(SERVICE_NAME, None, &SubSamplingInterval::ZERO, create_empty_fn_box())
            .await
            .unwrap()
            .expect("Subscription should be available");

        let _subscription2 = service_adapter
            .subscribe(SERVICE_NAME, None, &SubSamplingInterval::ZERO, create_empty_fn_box())
            .await
            .unwrap()
            .expect("Subscription should be available");

        service_adapter.unsubscribe(subscription).await.unwrap();

        // Verify the number of registry unsubscribe calls.
        assert_eq!(accessor.get_num_unsubscribe_calls(), 0);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn unsubscribe_shared_called_after_last_subscriber_unsubscribes() {
        let accessor = Arc::new(MockServicePublisherAccessor::new());
        let service_adapter = create_service_adapter(Arc::clone(&accessor));

        let subscription = service_adapter
            .subscribe(SERVICE_NAME, None, &SubSamplingInterval::ZERO, create_empty_fn_box())
            .await
            .unwrap()
            .expect("Subscription should be available");

        let subscription2 = service_adapter
            .subscribe(SERVICE_NAME, None, &SubSamplingInterval::ZERO, create_empty_fn_box())
            .await
            .unwrap()
            .expect("Subscription should be available");

        service_adapter.unsubscribe(subscription).await.unwrap();
        service_adapter.unsubscribe(subscription2).await.unwrap();

        // Verify the number of registry unsubscribe calls.
        assert_eq!(accessor.get_num_unsubscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
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

    #[sdv_telemetry_test_utils_macros::timeout]
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
}
