// Copyright 2023 Google LLC. All Rights Reserved.

//! The subscription_manager module provides the SubscriptionManager trait and its implementation,
//! which is responsible for managing subscriptions to specific types of events. The
//! SubscriptionManager allows for the addition and removal of subscriptions, and
//! automatically activates or deactivates an activatable object as needed. The activatable
//! object is responsible for performing the actual work associated with the subscription.

use async_trait::async_trait;
use sdv_telemetry_utils::collection_extension::HashMapExtension;
use std::sync::{Arc, Weak};
use std::{collections::HashMap, fmt::Display, hash::Hash};
use tokio::sync::Mutex as AsyncMutex;

pub(crate) type CallbackFn = Box<dyn FnMut() + Send + Sync>;

#[async_trait]
pub(crate) trait SubscriptionManager<TKey: Display + Eq + Hash + Send + Sync>:
    Send + Sync
{
    /// Adds a new subscription that is associated with the specified unique key
    /// and activates underlying activatable if it's not active.
    async fn subscribe(&self, key: TKey, callback: CallbackFn);

    /// Removes a subscription that is associated with the specified key
    /// and deactivates underlying activatable if this was the last subscription.
    async fn unsubscribe(&self, key: &TKey);
}

/// Trait of an object that can be activated.
#[async_trait]
pub trait Activatable<TKey: Eq + Hash>: Send + Sync {
    /// Activates the object.
    /// The function will only be called for an inactive object
    /// that is not going through state transition.
    async fn activate(self: Arc<Self>, callback_map: Arc<AsyncMutex<HashMap<TKey, CallbackFn>>>);

    /// Deactivates the object.
    /// The function will only be called for an active object
    /// that is not going through state transition.
    async fn deactivate(&self);
}

/// Manages subscriptions to a specific type of event.
pub struct SubscriptionManagerImpl<TKey: Display + Eq + Hash + Send + Sync> {
    /// Reference to the underlying activatable object.
    activatable_reference: Weak<dyn Activatable<TKey>>,

    /// The callback map stores the callbacks associated with the unique subscription keys.
    /// It is initialized with a new Arc when the first subscription is added.
    /// It is cleared and set to None when the last subscription is removed.
    maybe_callback_map: AsyncMutex<Option<Arc<AsyncMutex<HashMap<TKey, CallbackFn>>>>>,
}

#[async_trait]
impl<TKey: Display + Eq + Hash + Send + Sync> SubscriptionManager<TKey>
    for SubscriptionManagerImpl<TKey>
{
    async fn subscribe(&self, key: TKey, callback: CallbackFn) {
        let mut maybe_callback_map_lock = self.maybe_callback_map.lock().await;

        // Retrieve the callback map, creating a new one if necessary.
        if let Some(callback_map) = maybe_callback_map_lock.as_ref() {
            // Underlying activatable is already activated.
            // Add the callback to the callback map.
            callback_map.lock().await.try_insert_value(key, callback).unwrap_or_else(|entry| {
                panic!("Key {} is already present in the callback map.", entry.key())
            });
        } else {
            // Underlying activatable is not activated yet.
            // Create a new callback map and perform activation.
            let activatable = self
                .activatable_reference
                .upgrade()
                .expect("Subscribe shouldn't be called after activatable is freed");
            let callback_map = Arc::new(AsyncMutex::new(HashMap::from([(key, callback)])));
            activatable.clone().activate(callback_map.clone()).await;
            maybe_callback_map_lock.replace(callback_map);
        };
    }

    async fn unsubscribe(&self, key: &TKey) {
        let mut maybe_callback_map_lock = self.maybe_callback_map.lock().await;

        let is_last_subscription = {
            // Remove the callback from the callback map.
            let callback_map = maybe_callback_map_lock
                .as_ref()
                .expect("Unsubscribe shouldn't be called when there are no subscriptions.");
            let mut callback_map_lock = callback_map.lock().await;
            assert!(
                callback_map_lock.remove(key).is_some(),
                "Key {} isn't present in the callback map.",
                key
            );
            callback_map_lock.is_empty()
        };

        if is_last_subscription {
            // Last subscription removed, perform deactivation and remove the callback map.
            let activatable = self
                .activatable_reference
                .upgrade()
                .expect("Subscribe shouldn't be called after activatable is freed");
            activatable.deactivate().await;
            maybe_callback_map_lock.take();
        }
    }
}

impl<TKey: Display + Eq + Hash + Send + Sync> SubscriptionManagerImpl<TKey> {
    /// Creates a new instance of the callback manager that activates the specified activatable
    /// on first subscription and deactivates it on last unsubscription.
    pub fn new(activatable_reference: Weak<dyn Activatable<TKey>>) -> Self {
        SubscriptionManagerImpl { activatable_reference, maybe_callback_map: None.into() }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Mutex;

    use super::*;

    const KEY: &str = "test_key";
    const KEY2: &str = "test_key2";

    struct MockActivatable {
        callback_map: Mutex<Option<Arc<AsyncMutex<HashMap<String, CallbackFn>>>>>,
        num_activate_calls: AtomicU32,
        num_deactivate_calls: AtomicU32,
    }

    impl MockActivatable {
        fn new() -> Self {
            MockActivatable {
                callback_map: None.into(),
                num_activate_calls: 0.into(),
                num_deactivate_calls: 0.into(),
            }
        }

        fn get_num_activate_calls(&self) -> u32 {
            self.num_activate_calls.load(Ordering::SeqCst)
        }

        fn get_num_deactivate_calls(&self) -> u32 {
            self.num_deactivate_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl Activatable<String> for MockActivatable {
        async fn activate(
            self: Arc<Self>,
            callback_map: Arc<AsyncMutex<HashMap<String, CallbackFn>>>,
        ) {
            self.num_activate_calls.fetch_add(1, Ordering::SeqCst);
            self.callback_map.lock().unwrap().replace(callback_map);
        }

        async fn deactivate(&self) {
            self.num_deactivate_calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn create_subscription_manager() -> (SubscriptionManagerImpl<String>, Arc<MockActivatable>) {
        let activatable = Arc::new(MockActivatable::new());
        (SubscriptionManagerImpl::new(Arc::<MockActivatable>::downgrade(&activatable)), activatable)
    }

    fn create_empty_fn_box() -> CallbackFn {
        Box::new(|| {})
    }

    #[tokio::test]
    #[should_panic]
    async fn subscribe_already_subscribed_panics() {
        let (manager, _) = create_subscription_manager();

        // Subscribe twice with the same key.
        manager.subscribe(KEY.to_owned(), create_empty_fn_box()).await;
        manager.subscribe(KEY.to_owned(), create_empty_fn_box()).await;
    }

    #[tokio::test]
    async fn subscribe_twice_activates_once() {
        let (manager, activatable) = create_subscription_manager();

        // Subscribe twice with different keys.
        manager.subscribe(KEY.to_owned(), create_empty_fn_box()).await;
        manager.subscribe(KEY2.to_owned(), create_empty_fn_box()).await;

        // Verify that activate was called once and deactivate wasn't called.
        assert_eq!(activatable.get_num_activate_calls(), 1);
        assert_eq!(activatable.get_num_deactivate_calls(), 0);
    }

    #[tokio::test]
    async fn subscribe_adds_callback() {
        let (manager, activatable) = create_subscription_manager();

        manager.subscribe(KEY.to_owned(), create_empty_fn_box()).await;

        // Verify that the map contains a callback for the specified key.
        let callback_map = activatable.callback_map.lock().unwrap().take().unwrap();
        let callback_map_lock = callback_map.lock().await;
        assert!(callback_map_lock.contains_key(KEY));
    }

    #[tokio::test]
    #[should_panic]
    async fn unsubscribe_not_subscribed_panics() {
        let (manager, _) = create_subscription_manager();

        // Try to unsubscribe using a key that wasn't subscribed.
        manager.unsubscribe(&KEY.to_owned()).await;
    }

    #[tokio::test]
    async fn unsubscribe_twice_deactivates_once() {
        let (manager, activatable) = create_subscription_manager();

        // Subscribe twice with different keys.
        manager.subscribe(KEY.to_owned(), create_empty_fn_box()).await;
        manager.subscribe(KEY2.to_owned(), create_empty_fn_box()).await;

        // Unsubscribe from these keys.
        manager.unsubscribe(&KEY.to_owned()).await;
        manager.unsubscribe(&KEY2.to_owned()).await;

        // Verify that deactivate was called once and activate wasn't called.
        assert_eq!(activatable.get_num_deactivate_calls(), 1);
        assert_eq!(activatable.get_num_activate_calls(), 1);
    }

    #[tokio::test]
    async fn unsubscribe_removes_callback() {
        let (manager, activatable) = create_subscription_manager();

        // Subscribe and then immediately unsubscribe.
        manager.subscribe(KEY.to_owned(), create_empty_fn_box()).await;
        manager.unsubscribe(&KEY.to_owned()).await;

        // Verify that the callback was removed from the map.
        let callback_map = activatable.callback_map.lock().unwrap().take().unwrap();
        let callback_map_lock = callback_map.lock().await;
        assert!(!callback_map_lock.contains_key(KEY));
    }
}
