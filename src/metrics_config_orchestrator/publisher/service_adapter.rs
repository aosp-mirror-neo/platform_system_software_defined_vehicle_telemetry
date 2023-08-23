// Copyright 2023 Google LLC. All Rights Reserved.

use protobuf::{well_known_types::wrappers::Int32Value, MessageDyn};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex,
    },
};
use thiserror::Error;
use tokio::time::Duration;

/// Provides an interface to subscribe to messages published by services.
pub(crate) trait ServiceAdapter {
    fn subscribe(
        &self,
        service_name: &str,
        new_message_callback: Box<dyn Fn(&dyn MessageDyn) + Send + Sync>,
    ) -> ServiceSubscription;
    fn unsubscribe(&self, subscription: ServiceSubscription) -> Result<(), ServiceAdapterError>;
}

pub(crate) struct ServiceSubscription {
    pub(super) service_name: String,
    pub(super) subscription_id: u32,
}

#[derive(Error, Debug)]
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
    service_subscriptions: Mutex<
        HashMap<String, Arc<Mutex<HashMap<u32, Box<dyn Fn(&dyn MessageDyn) + Send + Sync>>>>>,
    >,

    next_service_id: AtomicU32,
}

impl ServiceAdapterImpl {
    pub(crate) fn new() -> Self {
        ServiceAdapterImpl {
            min_interval_ms: 500,
            max_interval_ms: 5000,
            service_subscriptions: Mutex::new(HashMap::new()),
            next_service_id: AtomicU32::new(1),
        }
    }

    // Publishes messages at random intervals until the map is cleared.
    async fn publish_data_randomly(
        min_interval_ms: u64,
        max_interval_ms: u64,
        subscriptions_container: Arc<
            Mutex<HashMap<u32, Box<dyn Fn(&dyn MessageDyn) + Send + Sync>>>,
        >,
    ) {
        let mut small_rng = SmallRng::from_entropy();
        let mut num = 1;

        loop {
            // Verify that the map hasn't been cleared.
            if subscriptions_container.lock().unwrap().is_empty() {
                return;
            }

            // If there is an interval, sleep randomly in within bounds, otherwise sleep fixed duration.
            tokio::time::sleep(Duration::from_millis(if min_interval_ms != max_interval_ms {
                small_rng.gen_range(min_interval_ms..max_interval_ms)
            } else {
                min_interval_ms
            }))
            .await;

            let subscriptions = subscriptions_container.lock().unwrap();

            // Verify that the map hasn't been cleared.
            if subscriptions.is_empty() {
                return;
            }

            // Generate a message and notify subscribers.
            let mut message = Int32Value::new();
            message.value = num;
            num += 1;

            for subscription in subscriptions.values() {
                subscription(&message);
            }
        }
    }
}

impl ServiceAdapter for ServiceAdapterImpl {
    fn subscribe(
        &self,
        service_name: &str,
        new_message_callback: Box<dyn Fn(&dyn MessageDyn) + Send + Sync>,
    ) -> ServiceSubscription {
        let subscription_id = self.next_service_id.fetch_add(1, Ordering::SeqCst);
        let mut subscriptions_map = self.service_subscriptions.lock().unwrap();

        if let Some(subscriptions_container) = subscriptions_map.get(service_name) {
            // If there's already an async loop that generates the data, add callback to its map.
            subscriptions_container.lock().unwrap().insert(subscription_id, new_message_callback);
        } else {
            // If there no async loop that generates the data, create a new map.
            let mut subscriptions = HashMap::new();
            subscriptions.insert(subscription_id, new_message_callback);
            let subscriptions_container = Arc::new(Mutex::new(subscriptions));
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

    fn unsubscribe(&self, subscription: ServiceSubscription) -> Result<(), ServiceAdapterError> {
        let mut subscriptions_map = self.service_subscriptions.lock().unwrap();

        let is_last_subscription = if let Some(subscriptions_container) =
            subscriptions_map.get(&subscription.service_name)
        {
            let mut subscriptions = subscriptions_container.lock().unwrap();

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

#[cfg(test)]
pub(super) mod tests {
    use lazy_static::lazy_static;
    use protobuf::{reflect::FieldDescriptor, MessageFull};
    use std::fmt::Debug;
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

        service_adapter.subscribe(
            SERVICE_NAME,
            Box::new(move |msg| {
                msg_nums_clone.lock().unwrap().push((SERVICE_NAME, get_msg_value(msg)))
            }),
        );
        time::sleep(Duration::from_millis(1)).await;
        service_adapter.min_interval_ms = 2 * INTERVAL_MS;
        service_adapter.max_interval_ms = service_adapter.min_interval_ms;
        service_adapter.subscribe(
            SERVICE_NAME2,
            Box::new(move |msg| {
                msg_nums_clone2.lock().unwrap().push((SERVICE_NAME2, get_msg_value(msg)))
            }),
        );
        // Sleep for 2.5 intervals to let adapter generate 3 messages.
        time::sleep(Duration::from_millis(5 * INTERVAL_MS / 2)).await;

        let received = msg_nums.lock().unwrap();
        let expected = vec![(SERVICE_NAME, 1), (SERVICE_NAME, 2), (SERVICE_NAME2, 1)];
        assert_vec_eq(&expected, &received);
    }

    #[tokio::test]
    async fn subscribe_same_service_one_async_loop() {
        let mut service_adapter = create_service_adapter();
        let msg_nums = Arc::new(Mutex::new(Vec::new()));
        let msg_nums_clone = msg_nums.clone();
        let msg_nums_clone2 = msg_nums.clone();

        service_adapter.subscribe(
            SERVICE_NAME,
            Box::new(move |msg| msg_nums_clone.lock().unwrap().push((1, get_msg_value(msg)))),
        );
        time::sleep(Duration::from_millis(INTERVAL_MS / 4)).await;
        // Increase interval so that new async loops won't generate messages any time soon.
        service_adapter.min_interval_ms = 100 * INTERVAL_MS;
        service_adapter.max_interval_ms = service_adapter.min_interval_ms;
        service_adapter.subscribe(
            SERVICE_NAME,
            Box::new(move |msg| msg_nums_clone2.lock().unwrap().push((2, get_msg_value(msg)))),
        );
        // Sleep for 2.5 intervals to let adapter generate 4 messages.
        time::sleep(Duration::from_millis(5 * INTERVAL_MS / 2)).await;

        assert_eq!(msg_nums.lock().unwrap().len(), 4);
    }

    #[test]
    fn unsubscribe_not_subscribed_fails() {
        let service_adapter = create_service_adapter();

        let err = service_adapter.unsubscribe(create_invalid_subscription()).err().unwrap();

        assert!(
            matches!(&err, ServiceAdapterError::InvalidServiceSubscription),
            "Unexpected error: {}",
            err
        );
    }

    #[tokio::test]
    async fn unsubscribe_not_subscribed_service_exists_fails() {
        let service_adapter = create_service_adapter();
        service_adapter.subscribe(SERVICE_NAME, Box::new(|_| {}));

        let err = service_adapter.unsubscribe(create_invalid_subscription()).err().unwrap();

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
        let subscription = service_adapter.subscribe(
            SERVICE_NAME,
            Box::new(move |msg| msg_nums_clone.lock().unwrap().push(get_msg_value(msg))),
        );

        service_adapter.unsubscribe(subscription).unwrap();
        // Sleep for 1.5 interval to ensure the trigger won't fire.
        time::sleep(Duration::from_millis(INTERVAL_MS * 3 / 2)).await;

        assert_eq!(msg_nums.lock().unwrap().len(), 0);
    }
}
