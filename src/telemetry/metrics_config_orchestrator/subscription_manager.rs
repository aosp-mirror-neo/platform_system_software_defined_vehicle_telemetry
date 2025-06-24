// Copyright 2023 Google LLC

//! The subscription_manager module provides the SubscriptionManager trait and its implementation,
//! which is responsible for managing subscriptions to specific types of events. The
//! SubscriptionManager allows for the addition and removal of subscriptions, and
//! automatically activates or deactivates an activatable object as needed. The activatable
//! object is responsible for performing the actual work associated with the subscription.

use async_trait::async_trait;
use sdv_telemetry_utils::atomic::{SequenceNumber, SequenceNumberType};
use sdv_telemetry_utils::collection_extension::HashMapExtension;
use sdv_telemetry_utils::countdown_latch::{Level, MultiLevelCountdownLatch};
use std::sync::{Arc, Weak};
use std::{collections::HashMap, fmt::Display, hash::Hash};
use tokio::sync::Mutex as AsyncMutex;

pub(crate) type CallbackFn<TCallbackArg> =
    Box<dyn FnMut(TCallbackArg, Level, MultiLevelCountdownLatch) + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ActivationId(SequenceNumberType);

#[cfg(test)]
impl ActivationId {
    pub const fn new(n: SequenceNumberType) -> Self {
        Self(n)
    }
}

#[async_trait]
pub(crate) trait SubscriptionManager<
    TKey: Display + Eq + Hash + Send + Sync,
    TCallbackArg: Clone + Send,
>: Send + Sync
{
    /// Adds a new subscription that is associated with the specified unique key
    /// and activates underlying activatable if it's not active.
    async fn subscribe(&self, key: TKey, callback: CallbackFn<TCallbackArg>);

    /// Removes a subscription that is associated with the specified key
    /// and deactivates underlying activatable if this was the last subscription.
    async fn unsubscribe(&self, key: &TKey);

    /// Notifies all subscribers with the provided callback argument, if the provided
    /// `activation_id` matches that of the most recent activation. Otherwise does nothing.
    async fn notify(&self, activation_id: ActivationId, arg: TCallbackArg);
}

/// Trait of an object that can be activated.
#[async_trait]
pub(crate) trait Activatable<TKey: Eq + Hash, TCallbackArg>: Send + Sync {
    /// Activates the object. The function will only be called for an inactive object that is not
    /// going through state transition. The `activation_id` must be passed to (async) calls to
    /// `SubscriptionManager::notify` triggered by this activation.
    async fn activate(self: Arc<Self>, activation_id: ActivationId);

    /// Deactivates the object.
    /// The function will only be called for an active object
    /// that is not going through state transition.
    async fn deactivate(&self);
}

pub(crate) trait ComputeOrder: Send + Sync {
    fn compute_order(&self) -> u32;
}

/// Manages subscriptions to a specific type of event.
pub(crate) struct SubscriptionManagerImpl<TKey: Display + Eq + Hash + Send + Sync, TCallbackArg> {
    /// Generator for the next activation ID.
    activation_id_generator: SequenceNumber,

    /// Reference to the underlying activatable object.
    activatable_reference: Weak<dyn Activatable<TKey, TCallbackArg>>,

    /// The callback map stores the callbacks associated with the unique subscription keys.
    /// It is initialized with a new activation id and hashmap when the first subscription is added.
    /// It is cleared and set to None when the last subscription is removed.
    maybe_callback_map: AsyncMutex<Option<(ActivationId, HashMap<TKey, CallbackFn<TCallbackArg>>)>>,
}

#[async_trait]
impl<TKey: ComputeOrder + Display + Eq + Hash + Send + Sync, TCallbackArg: Clone + Send>
    SubscriptionManager<TKey, TCallbackArg> for SubscriptionManagerImpl<TKey, TCallbackArg>
{
    async fn subscribe(&self, key: TKey, callback: CallbackFn<TCallbackArg>) {
        let mut maybe_callback_map_lock = self.maybe_callback_map.lock().await;

        // Retrieve the callback map, creating a new one if necessary.
        if let Some((_, callback_map)) = maybe_callback_map_lock.as_mut() {
            // Underlying activatable is already activated.
            // Add the callback to the callback map.
            callback_map.try_insert_value(key, callback).unwrap_or_else(|entry| {
                panic!("Key {} is already present in the callback map.", entry.key())
            });
        } else {
            // Underlying activatable is not activated yet.
            // Create a new callback map and perform activation.
            let activatable = self
                .activatable_reference
                .upgrade()
                .expect("Subscribe shouldn't be called after activatable is freed");
            let activation_id = ActivationId(self.activation_id_generator.next());
            maybe_callback_map_lock.replace((activation_id, HashMap::from([(key, callback)])));
            Arc::clone(&activatable).activate(activation_id).await;
        };
    }

    async fn unsubscribe(&self, key: &TKey) {
        let mut maybe_callback_map_lock = self.maybe_callback_map.lock().await;
        let is_last_subscription = {
            // Remove the callback from the callback map.
            let (_, callback_map) = maybe_callback_map_lock
                .as_mut()
                .expect("Unsubscribe shouldn't be called when there are no subscriptions.");
            assert!(
                callback_map.remove(key).is_some(),
                "Key {} isn't present in the callback map.",
                key
            );
            callback_map.is_empty()
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

    async fn notify(&self, activation_id: ActivationId, arg: TCallbackArg) {
        let mut maybe_callback_map_lock = self.maybe_callback_map.lock().await;
        let Some((stored_activation_id, callback_map)) = maybe_callback_map_lock.as_mut() else {
            return;
        };
        if stored_activation_id != &activation_id {
            return;
        }

        let countdown_latch = callback_map
            .keys()
            .map(|key| Level(key.compute_order()))
            .fold(HashMap::new(), |mut count_by_level, level| {
                *count_by_level.entry(level).or_default() += 1;
                count_by_level
            })
            .into_iter()
            .fold(MultiLevelCountdownLatch::builder(), |builder, (level, count)| {
                builder.with_level(level, count)
            })
            .build();

        for (key, callback) in callback_map {
            callback(arg.clone(), Level(key.compute_order()), countdown_latch.clone());
        }
    }
}

impl<TKey: ComputeOrder + Display + Eq + Hash + Send + Sync, TCallbackArg>
    SubscriptionManagerImpl<TKey, TCallbackArg>
{
    /// Creates a new instance of the callback manager that activates the specified activatable
    /// on first subscription and deactivates it on last unsubscription.
    pub(crate) fn new(activatable_reference: Weak<dyn Activatable<TKey, TCallbackArg>>) -> Self {
        SubscriptionManagerImpl {
            activation_id_generator: SequenceNumber::new(),
            activatable_reference,
            maybe_callback_map: None.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use mockall::predicate::eq;
    use tokio::sync::mpsc;

    const KEY: &str = "test_key";
    const KEY2: &str = "test_key2";

    mockall::mock! {
        ActivatableImpl {}

        #[async_trait]
        impl Activatable<String, u64> for ActivatableImpl {
            async fn activate(self: Arc<Self>, activation_id: ActivationId);
            async fn deactivate(&self);
        }
    }

    impl ComputeOrder for String {
        fn compute_order(&self) -> u32 {
            if self == KEY2 {
                1
            } else {
                0
            }
        }
    }

    fn create_subscription_manager(
        mock_setup: impl FnOnce(&mut MockActivatableImpl),
    ) -> (SubscriptionManagerImpl<String, u64>, Arc<MockActivatableImpl>) {
        let mut activatable = MockActivatableImpl::new();
        mock_setup(&mut activatable);
        let activatable = Arc::new(activatable);
        (
            SubscriptionManagerImpl::new(Arc::<MockActivatableImpl>::downgrade(&activatable)),
            // Return the `Arc` so that it is not dropped.
            activatable,
        )
    }

    fn create_empty_fn_box() -> CallbackFn<u64> {
        Box::new(|_, _, _| {})
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn subscribe_already_subscribed_panics() {
        let (manager, _activatable) = create_subscription_manager(|activatable| {
            activatable.expect_activate().once().return_const(());
        });

        // Subscribe twice with the same key.
        manager.subscribe(KEY.to_owned(), create_empty_fn_box()).await;
        manager.subscribe(KEY.to_owned(), create_empty_fn_box()).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribe_twice_activates_once() {
        let (manager, _activatable) = create_subscription_manager(|activatable| {
            activatable.expect_activate().with(eq(ActivationId(0))).once().return_const(());
        });

        // Subscribe twice with different keys.
        manager.subscribe(KEY.to_owned(), create_empty_fn_box()).await;
        manager.subscribe(KEY2.to_owned(), create_empty_fn_box()).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribe_adds_callback() {
        let (manager, _activatable) = create_subscription_manager(|activatable| {
            activatable.expect_activate().once().return_const(());
        });

        manager.subscribe(KEY.to_owned(), create_empty_fn_box()).await;

        // Verify that the map contains a callback for the specified key.
        let maybe_callback_map_lock = manager.maybe_callback_map.lock().await;
        let (_, callback_map) = maybe_callback_map_lock.as_ref().unwrap();
        assert!(callback_map.contains_key(KEY));
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn unsubscribe_not_subscribed_panics() {
        let (manager, _activatable) = create_subscription_manager(|_activatable| {});

        // Try to unsubscribe using a key that wasn't subscribed.
        manager.unsubscribe(&KEY.to_owned()).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn unsubscribe_twice_deactivates_once() {
        let (manager, _activatable) = create_subscription_manager(|activatable| {
            let mut seq = mockall::Sequence::new();
            activatable
                .expect_activate()
                .with(eq(ActivationId(0)))
                .once()
                .in_sequence(&mut seq)
                .return_const(());
            activatable.expect_deactivate().once().in_sequence(&mut seq).return_const(());
        });

        // Subscribe twice with different keys.
        manager.subscribe(KEY.to_owned(), create_empty_fn_box()).await;
        manager.subscribe(KEY2.to_owned(), create_empty_fn_box()).await;

        // Unsubscribe from these keys.
        manager.unsubscribe(&KEY.to_owned()).await;
        manager.unsubscribe(&KEY2.to_owned()).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn unsubscribe_removes_callback() {
        let (manager, _activatable) = create_subscription_manager(|activatable| {
            let mut seq = mockall::Sequence::new();
            activatable.expect_activate().once().in_sequence(&mut seq).return_const(());
            activatable.expect_deactivate().once().in_sequence(&mut seq).return_const(());
        });

        // Subscribe and then immediately unsubscribe.
        manager.subscribe(KEY.to_owned(), create_empty_fn_box()).await;
        manager.unsubscribe(&KEY.to_owned()).await;

        // Verify that the callback map was cleared.
        let maybe_callback_map_lock = manager.maybe_callback_map.lock().await;
        assert!(maybe_callback_map_lock.is_none());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribe_unsubscribe_subscribe_unsubscribe_increments_activation_id() {
        let (manager, _activatable) = create_subscription_manager(|activatable| {
            let mut seq = mockall::Sequence::new();
            activatable
                .expect_activate()
                .with(eq(ActivationId(0)))
                .once()
                .in_sequence(&mut seq)
                .return_const(());
            activatable.expect_deactivate().once().in_sequence(&mut seq).return_const(());
            activatable
                .expect_activate()
                .with(eq(ActivationId(1)))
                .once()
                .in_sequence(&mut seq)
                .return_const(());
            activatable.expect_deactivate().once().in_sequence(&mut seq).return_const(());
        });

        manager.subscribe(KEY.to_owned(), create_empty_fn_box()).await;
        manager.unsubscribe(&KEY.to_owned()).await;
        manager.subscribe(KEY.to_owned(), create_empty_fn_box()).await;
        manager.unsubscribe(&KEY.to_owned()).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn notify_notifies_only_callback_with_current_activation_id() {
        let (manager, _activatable) = create_subscription_manager(|activatable| {
            let mut seq = mockall::Sequence::new();
            activatable
                .expect_activate()
                .with(eq(ActivationId(0)))
                .once()
                .in_sequence(&mut seq)
                .return_const(());
            activatable.expect_deactivate().once().in_sequence(&mut seq).return_const(());
            activatable
                .expect_activate()
                .with(eq(ActivationId(1)))
                .once()
                .in_sequence(&mut seq)
                .return_const(());
        });

        manager
            .subscribe(
                KEY.to_owned(),
                Box::new(|_, _, _| {
                    panic!("Callback set in previous activation should not be called.")
                }),
            )
            .await;
        manager.unsubscribe(&KEY.to_owned()).await;

        let (tx, mut rx) = mpsc::unbounded_channel();
        manager.subscribe(KEY.to_owned(), Box::new(move |arg, _, _| tx.send(arg).unwrap())).await;

        manager.notify(ActivationId(1), 123).await;

        assert_eq!(123, rx.recv().await.unwrap());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn notify_can_be_called_without_subscribers() {
        let (manager, _activatable) = create_subscription_manager(|_activatable| {});

        manager.notify(ActivationId(0), 123).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn notify_callbacks_depend_on_countdown_latch() {
        let (manager, _activatable) = create_subscription_manager(|activatable| {
            let mut seq = mockall::Sequence::new();
            activatable.expect_activate().once().in_sequence(&mut seq).return_const(());
            activatable.expect_deactivate().once().in_sequence(&mut seq).return_const(());
            activatable.expect_activate().once().in_sequence(&mut seq).return_const(());
        });

        let (tx, mut rx) = mpsc::unbounded_channel();

        let tx_clone = tx.clone();
        // KEY compute_order is 0. This callback notifies KEY2 callback after completion
        let key_callback = Box::new(move |_, _, latch: MultiLevelCountdownLatch| {
            let tx_clone = tx_clone.clone();
            tokio::spawn(async move {
                latch.count_down(Level(0)).await;
                tx_clone.send(KEY).unwrap();
            });
        });

        // KEY2 compute_order is 1. It should wait for the KEY callback
        let key2_callback = Box::new(move |_, _, latch: MultiLevelCountdownLatch| {
            let tx_clone = tx.clone();
            tokio::spawn(async move {
                latch.wait(Level(0)).await;
                tx_clone.send(KEY2).unwrap();
            });
        });

        manager.subscribe(KEY.to_owned(), key_callback.clone()).await;
        manager.subscribe(KEY2.to_owned(), key2_callback.clone()).await;

        manager.notify(ActivationId::new(0), 42).await;

        // Verify the execution order.
        assert_eq!(
            rx.recv().await.unwrap(),
            KEY.to_owned(),
            "KEY (compute_order=0) should be called first"
        );
        assert_eq!(
            rx.recv().await.unwrap(),
            KEY2.to_owned(),
            "KEY2 (compute_order=1) should be called after latch.wait"
        );

        // unsubscribe to reset manager
        manager.unsubscribe(&KEY.to_owned()).await;
        manager.unsubscribe(&KEY2.to_owned()).await;

        // subscribe again in reversed order to make sure the order of calling is deterministic
        manager.subscribe(KEY2.to_owned(), key2_callback).await;
        manager.subscribe(KEY.to_owned(), key_callback).await;

        manager.notify(ActivationId::new(1), 42).await;

        // Verify the execution order.
        assert_eq!(
            rx.recv().await.unwrap(),
            KEY.to_owned(),
            "KEY (compute_order=0) should be called first"
        );
        assert_eq!(
            rx.recv().await.unwrap(),
            KEY2.to_owned(),
            "KEY2 (compute_order=1) should be called after latch.wait()"
        );
    }
}
