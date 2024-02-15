// Copyright 2023 Google LLC. All Rights Reserved.

use async_trait::async_trait;
use protobuf::MessageDyn;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex as AsyncMutex;

use crate::service_publisher_manager::{
    ServicePublisherManager, ServicePublisherManagerSubscribeError,
};

/// Provides an interface to subscribe to messages published by services.
#[async_trait]
pub(crate) trait ServiceAdapter {
    /// Subscribe to new messages from the specified topic published by the service
    /// with the specified name.
    /// The callback mustn't block because it's executed on the adapter thread.
    async fn subscribe(
        &self,
        service_name: &str,
        topic_name: &str,
        new_message_callback: Box<dyn for<'a> FnMut(&'a dyn MessageDyn) + Send + Sync>,
    ) -> Result<ServiceSubscription, ServiceAdapterSubscribeError>;

    /// Deactivate specified subscription.
    /// After asynchronous function is complete, the callback is guaranteed not to be called.
    async fn unsubscribe(
        &self,
        subscription: ServiceSubscription,
    ) -> Result<(), ServiceAdapterUnsubscribeError>;
}

pub(crate) struct ServiceSubscription {
    service_name: String,
    topic_name: String,
    subscription_id: u32,
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ServiceAdapterSubscribeError {
    #[error("Unable to perform subscription: {error}")]
    SubscriptionFailed {
        #[from]
        error: ServicePublisherManagerSubscribeError,
    },
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub(crate) enum ServiceAdapterUnsubscribeError {
    #[error("Service subscription is invalid")]
    InvalidServiceSubscription,
    #[error("Unable to unsubscribe: {error}")]
    UnsubscribeFailed {
        #[from]
        error: ServicePublisherManagerSubscribeError,
    },
}

/// An implementation of ServiceAdapter that publishes incrementing i32 messages at random interval.
pub(crate) struct ServiceAdapterImpl {
    // Id that will be used for the next subscription.
    next_subscription_id: AtomicU32,

    // The registry of available service publishers.
    service_publisher_manager: Arc<dyn ServicePublisherManager>,

    // The map of functions that need to be called when a new message is available.
    // The first subscription to service creates a new map and passes it to the callback that
    // handles new messages. Last unsubscription to service clears the map signaling that
    // the callback shouldn't process messages.
    service_subscriptions: AsyncMutex<
        HashMap<
            (String, String),
            Arc<AsyncMutex<HashMap<u32, Box<dyn for<'a> FnMut(&'a dyn MessageDyn) + Send + Sync>>>>,
        >,
    >,
}

#[async_trait]
impl ServiceAdapter for ServiceAdapterImpl {
    async fn subscribe(
        &self,
        service_name: &str,
        topic_name: &str,
        new_message_callback: Box<dyn for<'a> FnMut(&'a dyn MessageDyn) + Send + Sync>,
    ) -> Result<ServiceSubscription, ServiceAdapterSubscribeError> {
        let subscription_id = self.next_subscription_id.fetch_add(1, Ordering::SeqCst);

        match self
            .service_subscriptions
            .lock()
            .await
            .entry((service_name.to_owned(), topic_name.to_owned()))
        {
            Entry::Occupied(o) => {
                // If there's already an active subscription, add callback to its map.
                o.get().lock().await.insert(subscription_id, new_message_callback);
            }
            Entry::Vacant(v) => {
                // If there is no active subscription, create a new map.
                let mut subscriptions = HashMap::new();
                subscriptions.insert(subscription_id, new_message_callback);
                let subscriptions_container = Arc::new(AsyncMutex::new(subscriptions));
                v.insert(subscriptions_container.clone());

                self.service_publisher_manager
                    .try_subscribe(
                        service_name,
                        topic_name,
                        Arc::new(move |msg| Self::on_new_message(msg, &subscriptions_container)),
                        None,
                    )
                    .await?;
            }
        };

        Ok(ServiceSubscription {
            service_name: service_name.to_owned(),
            subscription_id,
            topic_name: topic_name.to_owned(),
        })
    }

    async fn unsubscribe(
        &self,
        subscription: ServiceSubscription,
    ) -> Result<(), ServiceAdapterUnsubscribeError> {
        match self
            .service_subscriptions
            .lock()
            .await
            .entry((subscription.service_name.clone(), subscription.topic_name.clone()))
        {
            Entry::Occupied(o) => {
                let is_last_subscription = {
                    let mut subscriptions = o.get().lock().await;

                    if subscriptions.remove(&subscription.subscription_id).is_none() {
                        return Err(ServiceAdapterUnsubscribeError::InvalidServiceSubscription);
                    }

                    subscriptions.is_empty()
                };

                if is_last_subscription {
                    self.service_publisher_manager
                        .unsubscribe(&subscription.service_name, &subscription.topic_name, None)
                        .await?;

                    o.remove_entry();
                }
            }
            Entry::Vacant(_) => {
                return Err(ServiceAdapterUnsubscribeError::InvalidServiceSubscription)
            }
        }

        Ok(())
    }
}

impl ServiceAdapterImpl {
    pub(super) fn new(service_publisher_manager: Arc<dyn ServicePublisherManager>) -> Self {
        ServiceAdapterImpl {
            next_subscription_id: AtomicU32::new(1),
            service_publisher_manager,
            service_subscriptions: AsyncMutex::new(HashMap::new()),
        }
    }

    // Handles notification about new messages.
    fn on_new_message(
        message: Box<dyn MessageDyn>,
        subscriptions_container: &Arc<
            AsyncMutex<HashMap<u32, Box<dyn for<'a> FnMut(&'a dyn MessageDyn) + Send + Sync>>>,
        >,
    ) {
        let subscriptions_container_clone = subscriptions_container.clone();
        tokio::spawn(async move {
            // Retrieve subscriptions.
            let mut subscriptions = subscriptions_container_clone.lock().await;

            // Verify that the map hasn't been cleared.
            if subscriptions.is_empty() {
                return;
            }

            // Synchronously invoke callbacks.
            // The loop needs to wait before they finish to release the mutex lock when they're
            // done because they're allowed to mutate themselves.
            for subscription in subscriptions.values_mut() {
                subscription(message.as_ref());
            }
        });
    }
}

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
pub(super) mod tests {
    use lazy_static::lazy_static;
    use protobuf::reflect::FieldDescriptor;
    use protobuf::well_known_types::wrappers::Int32Value;
    use protobuf::MessageFull;
    use sdv_telemetry_utils::test_utils::with_timeout;
    use tokio::sync::mpsc;

    use super::*;
    use crate::service_publisher_manager::mock::MockServicePublisherManager;

    const SERVICE_NAME: &str = "service";
    const TOPIC_NAME: &str = "topic";
    const TOPIC_NAME_2: &str = "topic2";

    lazy_static! {
        static ref VALUE_FIELD_DESCRIPTOR: FieldDescriptor =
            Int32Value::descriptor().field_by_name("value").unwrap();
    }

    fn create_empty_fn_box() -> Box<dyn for<'a> FnMut(&'a dyn MessageDyn) + Send + Sync> {
        Box::new(|_| {})
    }

    fn create_default_service_adapter() -> ServiceAdapterImpl {
        ServiceAdapterImpl::new(Arc::new(MockServicePublisherManager::default()))
    }

    fn create_invalid_subscription() -> ServiceSubscription {
        ServiceSubscription {
            service_name: String::default(),
            subscription_id: u32::MAX,
            topic_name: String::default(),
        }
    }

    #[tokio::test]
    async fn subscribe_registry_returns_error_fails() {
        let registry = MockServicePublisherManager::default();
        registry.next_error.lock().unwrap().replace(
            ServicePublisherManagerSubscribeError::TopicNotFound {
                service_name: SERVICE_NAME.to_owned(),
                topic_name: TOPIC_NAME.to_owned(),
            },
        );
        let registry = Arc::new(registry);
        let service_adapter = ServiceAdapterImpl::new(registry.clone());

        let err = service_adapter
            .subscribe(SERVICE_NAME, TOPIC_NAME, create_empty_fn_box())
            .await
            .err()
            .unwrap();

        let ServiceAdapterSubscribeError::SubscriptionFailed { error } = &err;
        assert!(
            matches!(error, ServicePublisherManagerSubscribeError::TopicNotFound { service_name, topic_name } if service_name == SERVICE_NAME && topic_name == TOPIC_NAME ),
            "Unexpected inner error: {:?}",
            error
        );

        assert_eq!(registry.get_num_subscribe_calls(), 1);
    }

    #[tokio::test]
    async fn subscribe_different_topics_subscribes_to_registry() {
        let registry = Arc::new(MockServicePublisherManager::default());
        let service_adapter = ServiceAdapterImpl::new(registry.clone());
        let (tx, mut rx) = mpsc::channel(1);
        let mut msg1 = Box::new(Int32Value::new());
        msg1.value = 1;
        let mut msg2 = Box::new(Int32Value::new());
        msg2.value = 2;

        // Create two subscriptions to different topics.
        let tx_clone = tx.clone();
        service_adapter
            .subscribe(
                SERVICE_NAME,
                TOPIC_NAME,
                Box::new(move |msg| tx_clone.try_send(msg.clone_box()).unwrap()),
            )
            .await
            .unwrap();
        service_adapter
            .subscribe(
                SERVICE_NAME,
                TOPIC_NAME_2,
                Box::new(move |msg| tx.try_send(msg.clone_box()).unwrap()),
            )
            .await
            .unwrap();

        // Notify about a new message in each topic and receive the message.
        registry.notify_subscriber(SERVICE_NAME.to_owned(), TOPIC_NAME.to_owned(), msg1.clone());
        let msg1_received = with_timeout(rx.recv()).await.unwrap();

        registry.notify_subscriber(SERVICE_NAME.to_owned(), TOPIC_NAME_2.to_owned(), msg2.clone());
        let msg2_received = with_timeout(rx.recv()).await.unwrap();

        // Verify that the received messages are the same as the ones sent.
        assert_eq!(&msg1_received, &(msg1 as Box<dyn MessageDyn>));
        assert_eq!(&msg2_received, &(msg2 as Box<dyn MessageDyn>));

        // Verify the number of registry subscribe calls.
        assert_eq!(registry.get_num_subscribe_calls(), 2);
    }

    #[tokio::test]
    async fn subscribe_same_topic_subscribes_to_registry() {
        let registry = Arc::new(MockServicePublisherManager::default());
        let service_adapter = ServiceAdapterImpl::new(registry.clone());
        let (tx, mut rx) = mpsc::channel(2);
        let mut msg = Box::new(Int32Value::new());
        msg.value = 1;

        // Create two subscriptions to the same topic.
        let tx_clone = tx.clone();
        service_adapter
            .subscribe(
                SERVICE_NAME,
                TOPIC_NAME,
                Box::new(move |msg| tx_clone.try_send(msg.clone_box()).unwrap()),
            )
            .await
            .unwrap();
        service_adapter
            .subscribe(
                SERVICE_NAME,
                TOPIC_NAME,
                Box::new(move |msg| tx.try_send(msg.clone_box()).unwrap()),
            )
            .await
            .unwrap();

        // Notify about a new message in the topic and receive messages from the two subscriptions.
        registry.notify_subscriber(SERVICE_NAME.to_owned(), TOPIC_NAME.to_owned(), msg.clone());
        let msg1_received = with_timeout(rx.recv()).await.unwrap();
        let msg2_received = with_timeout(rx.recv()).await.unwrap();

        // Verify that the received messages are the same as the one sent.
        let msg = msg as Box<dyn MessageDyn>;
        assert_eq!(&msg1_received, &msg);
        assert_eq!(&msg2_received, &msg);

        // Verify the number of registry subscribe calls.
        assert_eq!(registry.get_num_subscribe_calls(), 1);
    }

    #[tokio::test]
    async fn unsubscribe_not_subscribed_fails() {
        let service_adapter = create_default_service_adapter();

        // Create subscription for a different service.
        service_adapter.subscribe(SERVICE_NAME, TOPIC_NAME, create_empty_fn_box()).await.unwrap();

        // Try unsubscribing using invalid subscription.
        let err = service_adapter.unsubscribe(create_invalid_subscription()).await.err().unwrap();

        assert!(
            matches!(&err, ServiceAdapterUnsubscribeError::InvalidServiceSubscription),
            "Unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn unsubscribe_success() {
        let registry = MockServicePublisherManager::default();
        let registry = Arc::new(registry);
        let service_adapter = ServiceAdapterImpl::new(registry.clone());

        let subscription = service_adapter
            .subscribe(SERVICE_NAME, TOPIC_NAME, create_empty_fn_box())
            .await
            .unwrap();
        service_adapter.unsubscribe(subscription).await.unwrap();

        // Verify the number of registry unsubscribe calls.
        assert_eq!(registry.get_num_unsubscribe_calls(), 1);
    }

    #[tokio::test]
    async fn unsubscribe_not_called_unless_last_subscriber_unsubscribes() {
        let registry = MockServicePublisherManager::default();
        let registry = Arc::new(registry);
        let service_adapter = ServiceAdapterImpl::new(registry.clone());

        let subscription = service_adapter
            .subscribe(SERVICE_NAME, TOPIC_NAME, create_empty_fn_box())
            .await
            .unwrap();

        let _subscription2 = service_adapter
            .subscribe(SERVICE_NAME, TOPIC_NAME, create_empty_fn_box())
            .await
            .unwrap();

        service_adapter.unsubscribe(subscription).await.unwrap();

        // Verify the number of registry unsubscribe calls.
        assert_eq!(registry.get_num_unsubscribe_calls(), 0);
    }

    #[tokio::test]
    async fn unsubscribe_called_after_last_subscriber_unsubscribes() {
        let registry = MockServicePublisherManager::default();
        let registry = Arc::new(registry);
        let service_adapter = ServiceAdapterImpl::new(registry.clone());

        let subscription = service_adapter
            .subscribe(SERVICE_NAME, TOPIC_NAME, create_empty_fn_box())
            .await
            .unwrap();

        let subscription2 = service_adapter
            .subscribe(SERVICE_NAME, TOPIC_NAME, create_empty_fn_box())
            .await
            .unwrap();

        service_adapter.unsubscribe(subscription).await.unwrap();
        service_adapter.unsubscribe(subscription2).await.unwrap();

        // Verify the number of registry unsubscribe calls.
        assert_eq!(registry.get_num_unsubscribe_calls(), 1);
    }
}
