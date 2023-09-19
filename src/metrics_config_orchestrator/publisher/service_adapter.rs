// Copyright 2023 Google LLC. All Rights Reserved.

use async_trait::async_trait;
use protobuf::{well_known_types::wrappers::Int32Value, MessageDyn};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};
use thiserror::Error;
use tokio::{sync::Mutex as AsyncMutex, time::Duration};

/// Provides an interface to subscribe to messages published by services.
#[async_trait]
pub(crate) trait ServiceAdapter {
    /// Subscribe to new messages from the service specified with the specified name.
    /// The callback mustn't block because it's executed on the adapter thread.
    async fn subscribe(
        &self,
        service_name: &str,
        new_message_callback: Box<dyn for<'a> FnMut(&'a dyn MessageDyn) + Send + Sync>,
    ) -> ServiceSubscription;

    /// Deactivate specified subscription.
    /// After asynchronous function is complete, the callback is guaranteed not to be called.
    async fn unsubscribe(
        &self,
        subscription: ServiceSubscription,
    ) -> Result<(), ServiceAdapterError>;
}

pub(crate) struct ServiceSubscription {
    pub(super) service_name: String,
    pub(super) subscription_id: u32,
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub(crate) enum ServiceAdapterError {
    #[error("Service subscription is invalid")]
    InvalidServiceSubscription,
}

/// An implementation of ServiceAdapter that publishes incrementing i32 messages at random interval.
pub(crate) struct ServiceAdapterImpl {
    /// Minimum interval between published messages.
    min_interval_ms: u64,

    /// Maximum interval between published messages.
    max_interval_ms: u64,

    // The map of functions that need to be called when a new message is available.
    // The first subscription to service creates a new map and passes it to an async loop
    // that generates the data. Last unsubscription to service clears the map signaling that
    // the loop should return.
    service_subscriptions: AsyncMutex<
        HashMap<
            String,
            Arc<AsyncMutex<HashMap<u32, Box<dyn for<'a> FnMut(&'a dyn MessageDyn) + Send + Sync>>>>,
        >,
    >,

    next_service_id: AtomicU32,
}

#[async_trait]
impl ServiceAdapter for ServiceAdapterImpl {
    async fn subscribe(
        &self,
        service_name: &str,
        new_message_callback: Box<dyn for<'a> FnMut(&'a dyn MessageDyn) + Send + Sync>,
    ) -> ServiceSubscription {
        let subscription_id = self.next_service_id.fetch_add(1, Ordering::SeqCst);
        let mut subscriptions_map = self.service_subscriptions.lock().await;

        if let Some(subscriptions_container) = subscriptions_map.get(service_name) {
            // If there's already an async loop that generates the data, add callback to its map.
            subscriptions_container.lock().await.insert(subscription_id, new_message_callback);
        } else {
            // If there no async loop that generates the data, create a new map.
            let mut subscriptions = HashMap::new();
            subscriptions.insert(subscription_id, new_message_callback);
            let subscriptions_container = Arc::new(AsyncMutex::new(subscriptions));
            subscriptions_map.insert(service_name.to_owned(), subscriptions_container.clone());

            // Spawn a new async loop for the new map.
            tokio::spawn(Self::publish_data_randomly(
                self.min_interval_ms,
                self.max_interval_ms,
                subscriptions_container,
            ));
        }

        ServiceSubscription { service_name: service_name.to_owned(), subscription_id }
    }

    async fn unsubscribe(
        &self,
        subscription: ServiceSubscription,
    ) -> Result<(), ServiceAdapterError> {
        let mut subscriptions_map = self.service_subscriptions.lock().await;

        let is_last_subscription = if let Some(subscriptions_container) =
            subscriptions_map.get(&subscription.service_name)
        {
            let mut subscriptions = subscriptions_container.lock().await;

            if subscriptions.remove(&subscription.subscription_id).is_none() {
                return Err(ServiceAdapterError::InvalidServiceSubscription);
            }

            subscriptions.is_empty()
        } else {
            return Err(ServiceAdapterError::InvalidServiceSubscription);
        };

        if is_last_subscription {
            // Remove the map since related async loop will end when it finds out
            // that the map is empty.
            subscriptions_map.remove(&subscription.service_name);
        }

        Ok(())
    }
}

impl ServiceAdapterImpl {
    pub(crate) fn new() -> Self {
        ServiceAdapterImpl {
            min_interval_ms: 500,
            max_interval_ms: 5000,
            service_subscriptions: AsyncMutex::new(HashMap::new()),
            next_service_id: AtomicU32::new(1),
        }
    }

    // Publishes messages at random intervals until the map is cleared.
    async fn publish_data_randomly(
        min_interval_ms: u64,
        max_interval_ms: u64,
        subscriptions_container: Arc<
            AsyncMutex<HashMap<u32, Box<dyn for<'a> FnMut(&'a dyn MessageDyn) + Send + Sync>>>,
        >,
    ) {
        let mut small_rng = SmallRng::from_entropy();
        let mut num = 1;

        loop {
            // Verify that the map hasn't been cleared.
            if subscriptions_container.lock().await.is_empty() {
                return;
            }

            // If there is an interval, sleep randomly in within bounds, otherwise sleep fixed duration.
            tokio::time::sleep(Duration::from_millis(if min_interval_ms != max_interval_ms {
                small_rng.gen_range(min_interval_ms..max_interval_ms)
            } else {
                min_interval_ms
            }))
            .await;

            // Retrieve subscriptions.
            let mut subscriptions = subscriptions_container.lock().await;

            // Verify that the map hasn't been cleared.
            if subscriptions.is_empty() {
                return;
            }

            // Generate a message and notify subscribers.
            let mut message = Int32Value::new();
            message.value = num;
            num += 1;

            // Synchronously invoke callbacks.
            // The loop needs to wait before they finish to release the mutex lock when they're
            // done because they're allowed to mutate themselves.
            for subscription in subscriptions.values_mut() {
                subscription(&message);
            }
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use lazy_static::lazy_static;
    use protobuf::{reflect::FieldDescriptor, MessageFull};
    use std::{fmt::Debug, sync::Mutex};
    use tokio::time;

    use super::*;

    const INTERVAL_MS: u64 = 10;
    const SERVICE_NAME: &str = "test_service";
    const SERVICE_NAME2: &str = "test_service2";

    lazy_static! {
        static ref VALUE_FIELD_DESCRIPTOR: FieldDescriptor =
            Int32Value::descriptor().field_by_name("value").unwrap();
    }

    fn assert_vec_eq<T: Debug + PartialEq>(expected: &[T], got: &[T]) {
        assert!(
            vec_eq(expected, got),
            "Vectors are not equal: expected {:?}, got {:?}",
            expected,
            got
        );
    }

    pub(super) fn create_invalid_subscription() -> ServiceSubscription {
        ServiceSubscription { service_name: String::default(), subscription_id: u32::MAX }
    }

    fn create_service_adapter() -> ServiceAdapterImpl {
        let mut adapter = ServiceAdapterImpl::new();
        adapter.min_interval_ms = INTERVAL_MS;
        adapter.max_interval_ms = INTERVAL_MS;
        adapter
    }

    fn get_msg_value(msg: &dyn MessageDyn) -> i32 {
        VALUE_FIELD_DESCRIPTOR.get_singular(msg).unwrap().to_i32().unwrap()
    }

    fn vec_eq<T: Debug + PartialEq>(left: &[T], right: &[T]) -> bool {
        if left.len() != right.len() {
            return false;
        }
        for (left, right) in left.iter().zip(right.iter()) {
            if left != right {
                return false;
            }
        }
        true
    }

    #[tokio::test]
    async fn subscribe_two_services_success() {
        let mut service_adapter = create_service_adapter();
        let msg_nums = Arc::new(Mutex::new(Vec::new()));
        let msg_nums_clone = msg_nums.clone();
        let msg_nums_clone2 = msg_nums.clone();

        service_adapter
            .subscribe(
                SERVICE_NAME,
                Box::new(move |msg| {
                    msg_nums_clone.lock().unwrap().push((SERVICE_NAME, get_msg_value(msg)))
                }),
            )
            .await;
        time::sleep(Duration::from_millis(1)).await;
        service_adapter.min_interval_ms = 2 * INTERVAL_MS;
        service_adapter.max_interval_ms = service_adapter.min_interval_ms;
        service_adapter
            .subscribe(
                SERVICE_NAME2,
                Box::new(move |msg| {
                    msg_nums_clone2.lock().unwrap().push((SERVICE_NAME2, get_msg_value(msg)))
                }),
            )
            .await;
        // Sleep for 3 intervals to let adapter generate 3 messages.
        // TODO(b/300703055): Find a way to remove sleeps.
        time::sleep(Duration::from_millis(3 * INTERVAL_MS)).await;

        let received = msg_nums.lock().unwrap().iter().take(3).cloned().collect::<Vec<_>>();
        let expected = vec![(SERVICE_NAME, 1), (SERVICE_NAME, 2), (SERVICE_NAME2, 1)];
        assert_vec_eq(&expected, &received);
    }

    #[tokio::test]
    async fn subscribe_same_service_one_async_loop() {
        let mut service_adapter = create_service_adapter();
        let subscription1_got_data = Arc::new(Mutex::new(false));
        let subscription2_got_data = Arc::new(Mutex::new(false));
        let subscription1_got_data_clone = subscription1_got_data.clone();
        let subscription2_got_data_clone = subscription2_got_data.clone();

        service_adapter
            .subscribe(
                SERVICE_NAME,
                Box::new(move |_| *subscription1_got_data_clone.lock().unwrap() = true),
            )
            .await;
        // Increase interval so that new async loops won't generate messages any time soon.
        service_adapter.min_interval_ms = 100 * INTERVAL_MS;
        service_adapter.max_interval_ms = service_adapter.min_interval_ms;
        service_adapter
            .subscribe(
                SERVICE_NAME,
                Box::new(move |_| *subscription2_got_data_clone.lock().unwrap() = true),
            )
            .await;
        // Sleep for 2 intervals to let adapter generate messages.
        // TODO(b/300703055): Find a way to remove sleeps.
        time::sleep(Duration::from_millis(2 * INTERVAL_MS)).await;

        // Verify that both subscriptions were notified.
        assert!(*subscription1_got_data.lock().unwrap());
        assert!(*subscription2_got_data.lock().unwrap());
    }

    #[tokio::test]
    async fn unsubscribe_not_subscribed_fails() {
        let service_adapter = create_service_adapter();

        let err = service_adapter.unsubscribe(create_invalid_subscription()).await.err().unwrap();

        assert!(
            matches!(&err, ServiceAdapterError::InvalidServiceSubscription),
            "Unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn unsubscribe_not_subscribed_service_exists_fails() {
        let service_adapter = create_service_adapter();

        service_adapter.subscribe(SERVICE_NAME, Box::new(|_| {})).await;
        let err = service_adapter.unsubscribe(create_invalid_subscription()).await.err().unwrap();

        assert!(
            matches!(&err, ServiceAdapterError::InvalidServiceSubscription),
            "Unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn unsubscribe_stops_firing() {
        let service_adapter = create_service_adapter();
        let msg_nums = Arc::new(Mutex::new(Vec::new()));
        let msg_nums_clone = msg_nums.clone();

        let subscription = service_adapter
            .subscribe(
                SERVICE_NAME,
                Box::new(move |msg| msg_nums_clone.lock().unwrap().push(get_msg_value(msg))),
            )
            .await;
        service_adapter.unsubscribe(subscription).await.unwrap();
        // Sleep for 1.5 interval to ensure the trigger won't fire.
        // TODO(b/300703055): Find a way to remove sleeps.
        time::sleep(Duration::from_millis(INTERVAL_MS * 3 / 2)).await;

        assert_eq!(msg_nums.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn unsubscribe_from_callback_success() {
        let service_adapter = Arc::new(create_service_adapter());
        let mut service_adapter_clone = Some(service_adapter.clone());
        let subscription_holder = Arc::new(Mutex::new(None));
        let subscription_holder_clone = subscription_holder.clone();

        let subscription = service_adapter
            .subscribe(
                SERVICE_NAME,
                Box::new(move |_| {
                    // Take and unwrap to make sure the adapter and the holder are not empty
                    // and the callback fails if called twice.
                    let subscription = subscription_holder_clone.lock().unwrap().take().unwrap();
                    let service_adapter_clone = service_adapter_clone.take().unwrap();
                    tokio::spawn(async move {
                        service_adapter_clone.unsubscribe(subscription).await.unwrap();
                    });
                }),
            )
            .await;
        subscription_holder.lock().unwrap().replace(subscription);
        // Sleep for 2.5 intervals to let adapter generate 2 messages.
        // TODO(b/300703055): Find a way to remove sleeps.
        time::sleep(Duration::from_millis(5 * INTERVAL_MS / 2)).await;

        // Verify that subscription has been consumed by the unsubscribe call.
        assert!(subscription_holder.lock().unwrap().is_none())
    }
}
