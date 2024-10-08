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
    AsyncEventHandler, AsyncEventProcessor,
};
use crate::service_publisher_accessor::{
    CreateSubscriptionOutcome, ServicePublisherAccessor, ServicePublisherMessageProvider,
    ServicePublisherSubscription,
};

/// Provides an interface to subscribe to messages published by services.
#[async_trait]
pub(crate) trait ServiceAdapter {
    /// Returns provider of messages from the publisher specified by the service name.
    fn get_message_provider(
        &self,
        service_name: &str,
    ) -> anyhow::Result<Option<Box<dyn ServiceMessageProvider>>>;

    /// Subscribe to new messages from the service with the specified name
    /// using specified configuration.
    /// The callback mustn't block because it's executed on the adapter thread.
    async fn subscribe(
        &self,
        service_name: &str,
        configuration: Option<&Any>,
        new_message_callback: Box<dyn for<'a> FnMut(&'a Arc<dyn MessageDyn>) + Send + Sync>,
    ) -> anyhow::Result<Option<ServiceSubscription>>;

    /// Deactivate specified subscription.
    /// After asynchronous function is complete, the callback is guaranteed not to be called.
    async fn unsubscribe(&self, subscription: ServiceSubscription) -> anyhow::Result<()>;
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

    // The map with service name as a key and a tuple as a value that consists of:
    // 1. Underlying ServicePublisherSubscription.
    // 2. The map of functions that need to be called when a new message is available.
    // The first subscription to service creates a new function map and passes it to the callback
    // that handles new messages. Last unsubscription to service clears the map signaling that
    // the callback shouldn't process messages.
    service_subscriptions: AsyncMutex<
        HashMap<
            String,
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
    ) -> ServiceSubscriptionType {
        ServiceSubscriptionType::Shared { service_name, subscription_id }
    }
}

#[async_trait]
impl ServiceAdapter for ServiceAdapterImpl {
    fn get_message_provider(
        &self,
        service_name: &str,
    ) -> anyhow::Result<Option<Box<dyn ServiceMessageProvider>>> {
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

        let res = self.service_publisher_accessor.get_message_provider(service_name)?.map(
            |service_publisher_message_provider| {
                Box::new(ServiceMessageProviderImpl { service_publisher_message_provider })
                    as Box<dyn ServiceMessageProvider>
            },
        );
        Ok(res)
    }

    async fn subscribe(
        &self,
        service_name: &str,
        configuration: Option<&Any>,
        mut new_message_callback: Box<dyn for<'a> FnMut(&'a Arc<dyn MessageDyn>) + Send + Sync>,
    ) -> anyhow::Result<Option<ServiceSubscription>> {
        let maybe_subscription_type = if configuration.is_some() {
            // Subscription has configuration, so it can't be shared.
            let create_subscription_outcome = self
                .service_publisher_accessor
                .create_subscription(
                    service_name,
                    configuration,
                    Box::new(move |msg| new_message_callback(&msg.into())),
                )
                .await?;

            match create_subscription_outcome {
                CreateSubscriptionOutcome::Subscribed(subscription) => {
                    Some(ServiceSubscriptionType::new_configured(subscription))
                }
                CreateSubscriptionOutcome::NotRegistered(_) => None,
            }
        } else {
            // Subscription doesn't have configuration, so it can be shared.
            let subscription_id = self.next_subscription_id.next();

            match self.service_subscriptions.lock().await.entry(service_name.to_owned()) {
                Entry::Occupied(o) => {
                    // If there's already an active subscription, add callback to its map.
                    let (_, _, callback_container) = o.get();
                    callback_container.lock().await.insert(subscription_id, new_message_callback);

                    Some(ServiceSubscriptionType::new_shared(
                        service_name.to_owned(),
                        subscription_id,
                    ))
                }
                Entry::Vacant(v) => {
                    // If there is no active subscription, create a new one and store it.
                    let (sender, receiver) = mpsc::unbounded_channel();
                    let service_name_clone = service_name.to_owned();
                    let create_subscription_outcome = self
                        .service_publisher_accessor
                        .create_subscription(
                            service_name,
                            None,
                            Box::new(move |msg| {
                                if sender.send(msg).is_err() {
                                    // This means that `receiver` was dropped - which should
                                    // never happen, since it is only dropped when unsubscribing.
                                    error!("Received message for service {service_name_clone}, but receiver has already been dropped!");
                                }
                            }),
                        )
                        .await?;

                    match create_subscription_outcome {
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
                                receiver,
                                ServiceAdapterHandler {
                                    callback_container: callback_container_clone,
                                },
                            );
                            v.insert((subscription, event_queue, callback_container));
                            Some(ServiceSubscriptionType::new_shared(
                                service_name.to_owned(),
                                subscription_id,
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
            ServiceSubscriptionType::Shared { service_name, subscription_id } => {
                match self.service_subscriptions.lock().await.entry(service_name) {
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
                        panic!("No entry found for service name {}", v.key());
                    }
                }
            }
        }

        Ok(())
    }
}

impl ServiceAdapterImpl {
    pub(super) fn new(service_publisher_manager: Arc<dyn ServicePublisherAccessor>) -> Self {
        Self {
            next_subscription_id: SequenceNumber::new(),
            service_publisher_accessor: service_publisher_manager,
            service_subscriptions: AsyncMutex::new(HashMap::new()),
        }
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

    fn create_default_service_adapter() -> ServiceAdapterImpl {
        ServiceAdapterImpl::new(Arc::new(MockServicePublisherAccessor::new()))
    }

    fn create_invalid_subscription() -> ServiceSubscription {
        ServiceSubscription {
            subscription_type: ServiceSubscriptionType::Shared {
                service_name: String::default(),
                subscription_id: SequenceNumberType::MAX,
            },
        }
    }

    #[test]
    fn get_message_provider_accessor_returns_error_fails() {
        let accessor =
            Arc::new(MockServicePublisherAccessor::new().with_next_error(anyhow!(ERROR_MESSAGE)));
        let service_adapter =
            ServiceAdapterImpl::new(Arc::<MockServicePublisherAccessor>::clone(&accessor));

        let err = service_adapter.get_message_provider(SERVICE_NAME).err().unwrap();

        assert!(err.to_string().contains(ERROR_MESSAGE), "Unexpected error: {:?}", err);
        assert_eq!(accessor.get_num_get_message_provider_calls(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_message_provider_accessor_latest_message_returns_error_fails() {
        let accessor = Arc::new(MockServicePublisherAccessor::new());
        let service_adapter =
            ServiceAdapterImpl::new(Arc::<MockServicePublisherAccessor>::clone(&accessor));

        let provider = service_adapter
            .get_message_provider(SERVICE_NAME)
            .unwrap()
            .expect("Provider should be available");
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
        let service_adapter =
            ServiceAdapterImpl::new(Arc::<MockServicePublisherAccessor>::clone(&accessor));

        let message_provider = service_adapter
            .get_message_provider(SERVICE_NAME)
            .unwrap()
            .expect("Provider should be available");

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
        let service_adapter =
            ServiceAdapterImpl::new(Arc::<MockServicePublisherAccessor>::clone(&accessor));

        let err = service_adapter
            .subscribe(SERVICE_NAME, Some(&Any::new()), create_empty_fn_box())
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
        let service_adapter =
            ServiceAdapterImpl::new(Arc::<MockServicePublisherAccessor>::clone(&accessor));

        let res = service_adapter
            .subscribe(SERVICE_NAME, Some(&Any::new()), create_empty_fn_box())
            .await
            .unwrap();

        assert!(res.is_none());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn subscribe_configured_success() {
        let accessor = Arc::new(MockServicePublisherAccessor::new());
        let service_adapter =
            ServiceAdapterImpl::new(Arc::<MockServicePublisherAccessor>::clone(&accessor));
        let (tx, mut rx) = mpsc::channel(1);
        let mut msg = Box::new(Int32Value::new());
        msg.value = 1;

        service_adapter
            .subscribe(
                SERVICE_NAME,
                Some(&Any::new()),
                Box::new(move |msg| tx.try_send(msg.clone_box()).unwrap()),
            )
            .await
            .unwrap();

        // Notify about a new message and receive the message.
        accessor.notify_subscriber(SERVICE_NAME, msg.clone());
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
        let service_adapter =
            ServiceAdapterImpl::new(Arc::<MockServicePublisherAccessor>::clone(&accessor));

        let err = service_adapter
            .subscribe(SERVICE_NAME, None, create_empty_fn_box())
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
        let service_adapter =
            ServiceAdapterImpl::new(Arc::<MockServicePublisherAccessor>::clone(&accessor));

        let res =
            service_adapter.subscribe(SERVICE_NAME, None, create_empty_fn_box()).await.unwrap();

        assert!(res.is_none());
        assert_eq!(accessor.get_num_subscribe_calls(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn subscribe_shared_different_services_subscribes_to_registry() {
        let accessor = Arc::new(MockServicePublisherAccessor::new());
        let service_adapter =
            ServiceAdapterImpl::new(Arc::<MockServicePublisherAccessor>::clone(&accessor));
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
                Box::new(move |msg| tx_clone.try_send(msg.clone_box()).unwrap()),
            )
            .await
            .unwrap();
        service_adapter
            .subscribe(
                SERVICE_NAME_2,
                None,
                Box::new(move |msg| tx.try_send(msg.clone_box()).unwrap()),
            )
            .await
            .unwrap();

        // Notify about a new message from each service and receive the message.
        accessor.notify_subscriber(SERVICE_NAME, msg1.clone());
        let msg1_received = rx.recv().await.unwrap();

        accessor.notify_subscriber(SERVICE_NAME_2, msg2.clone());
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
        let service_adapter =
            ServiceAdapterImpl::new(Arc::<MockServicePublisherAccessor>::clone(&accessor));
        let (tx, mut rx) = mpsc::channel(2);
        let mut msg = Box::new(Int32Value::new());
        msg.value = 1;

        // Create two subscriptions to the same service.
        let tx_clone = tx.clone();
        service_adapter
            .subscribe(
                SERVICE_NAME,
                None,
                Box::new(move |msg| tx_clone.try_send(msg.clone_box()).unwrap()),
            )
            .await
            .unwrap();
        service_adapter
            .subscribe(
                SERVICE_NAME,
                None,
                Box::new(move |msg| tx.try_send(msg.clone_box()).unwrap()),
            )
            .await
            .unwrap();

        // Notify about a new message from the service
        // and receive messages from the two subscriptions.
        accessor.notify_subscriber(SERVICE_NAME, msg.clone());
        let msg1_received = rx.recv().await.unwrap();
        let msg2_received = rx.recv().await.unwrap();

        // Verify that the received messages are the same as the one sent.
        let msg = msg as Box<dyn MessageDyn>;
        assert_eq!(&msg1_received, &msg);
        assert_eq!(&msg2_received, &msg);

        // Verify the number of registry subscribe calls.
        assert_eq!(accessor.get_num_subscribe_calls(), 1);
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
            let service_adapter =
                ServiceAdapterImpl::new(Arc::<MockServicePublisherAccessor>::clone(&accessor));
            let (tx, mut rx) = mpsc::unbounded_channel();

            service_adapter
                .subscribe(
                    SERVICE_NAME,
                    None,
                    Box::new(move |msg| tx.send(msg.clone_box()).unwrap()),
                )
                .await
                .unwrap();

            for i in RANGE {
                accessor.notify_subscriber(
                    SERVICE_NAME,
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
        let service_adapter = create_default_service_adapter();

        // Create subscription for a different service.
        service_adapter.subscribe(SERVICE_NAME, None, create_empty_fn_box()).await.unwrap();

        // Try unsubscribing using invalid subscription.
        service_adapter.unsubscribe(create_invalid_subscription()).await.unwrap();
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn unsubscribe_configured_accessor_returns_error_fails() {
        let accessor = Arc::new(MockServicePublisherAccessor::new());
        let service_adapter =
            ServiceAdapterImpl::new(Arc::<MockServicePublisherAccessor>::clone(&accessor));

        let subscription = service_adapter
            .subscribe(SERVICE_NAME, Some(&Any::new()), create_empty_fn_box())
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
        let service_adapter =
            ServiceAdapterImpl::new(Arc::<MockServicePublisherAccessor>::clone(&accessor));

        let subscription = service_adapter
            .subscribe(SERVICE_NAME, Some(&Any::new()), create_empty_fn_box())
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
        let service_adapter =
            ServiceAdapterImpl::new(Arc::<MockServicePublisherAccessor>::clone(&accessor));

        let subscription = service_adapter
            .subscribe(SERVICE_NAME, None, create_empty_fn_box())
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
        let service_adapter =
            ServiceAdapterImpl::new(Arc::<MockServicePublisherAccessor>::clone(&accessor));

        let subscription = service_adapter
            .subscribe(SERVICE_NAME, None, create_empty_fn_box())
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
        let service_adapter =
            ServiceAdapterImpl::new(Arc::<MockServicePublisherAccessor>::clone(&accessor));

        let subscription = service_adapter
            .subscribe(SERVICE_NAME, None, create_empty_fn_box())
            .await
            .unwrap()
            .expect("Subscription should be available");

        let _subscription2 = service_adapter
            .subscribe(SERVICE_NAME, None, create_empty_fn_box())
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
        let service_adapter =
            ServiceAdapterImpl::new(Arc::<MockServicePublisherAccessor>::clone(&accessor));

        let subscription = service_adapter
            .subscribe(SERVICE_NAME, None, create_empty_fn_box())
            .await
            .unwrap()
            .expect("Subscription should be available");

        let subscription2 = service_adapter
            .subscribe(SERVICE_NAME, None, create_empty_fn_box())
            .await
            .unwrap()
            .expect("Subscription should be available");

        service_adapter.unsubscribe(subscription).await.unwrap();
        service_adapter.unsubscribe(subscription2).await.unwrap();

        // Verify the number of registry unsubscribe calls.
        assert_eq!(accessor.get_num_unsubscribe_calls(), 1);
    }
}
