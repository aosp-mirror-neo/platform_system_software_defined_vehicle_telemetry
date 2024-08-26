// Copyright 2024 Google LLC

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use tokio::{sync::Mutex as AsyncMutex, time::Duration};

use super::base::BaseTrigger;
use super::Trigger;
use crate::expr::node::NodeEvaluationContext;
use crate::metrics_config_orchestrator::subscription_manager::Activatable;
use crate::metrics_config_orchestrator::trigger::{TriggerCallbackFn, TriggerSubscriber};

pub(crate) struct PeriodicTrigger {
    base: BaseTrigger,

    pub(crate) interval: Duration,
}

impl PeriodicTrigger {
    pub(crate) fn new(name: String, interval: Duration) -> Arc<Self> {
        Arc::new_cyclic(|me: &Weak<Self>| Self {
            base: BaseTrigger::new(name, Weak::clone(me) as Weak<dyn Activatable<_, _>>),
            interval,
        })
    }

    /// Periodically invokes callbacks until the weak reference is no longer valid.
    async fn call_periodically(
        trigger_name: &str,
        interval: Duration,
        callback_map: Arc<AsyncMutex<HashMap<TriggerSubscriber, TriggerCallbackFn>>>,
    ) {
        let mut timer = tokio::time::interval(interval);

        // Trigger first tick that returns immediately.
        timer.tick().await;

        loop {
            // Verify that the map hasn't been cleared.
            if callback_map.lock().await.is_empty() {
                return;
            }

            // Sleep until next interval tick.
            timer.tick().await;

            // Fire trigger.
            let context = Arc::new(NodeEvaluationContext::new());
            BaseTrigger::fire_trigger(trigger_name, &callback_map, &context).await;
        }
    }
}

#[async_trait]
impl Trigger for PeriodicTrigger {
    fn get_name(&self) -> &str {
        self.base.get_name()
    }

    async fn subscribe(
        self: Arc<Self>,
        subscriber: &TriggerSubscriber,
        on_fired_callback: TriggerCallbackFn,
    ) {
        self.base.subscription_manager.subscribe(subscriber.clone(), on_fired_callback).await;
    }

    async fn unsubscribe(&self, subscriber: &TriggerSubscriber) {
        self.base.subscription_manager.unsubscribe(subscriber).await;
    }
}

#[async_trait]
impl Activatable<TriggerSubscriber, Arc<NodeEvaluationContext>> for PeriodicTrigger {
    async fn activate(
        self: Arc<Self>,
        callback_map: Arc<AsyncMutex<HashMap<TriggerSubscriber, TriggerCallbackFn>>>,
    ) {
        let trigger_name = self.get_name().to_owned();
        let interval = self.interval;
        tokio::spawn(async move {
            Self::call_periodically(&trigger_name, interval, callback_map).await;
        });
    }

    async fn deactivate(&self) {}
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;
    use tokio::sync::oneshot;
    use tokio::sync::Mutex as AsyncMutex;

    use super::*;
    use crate::metrics_config_orchestrator::subscription_manager::Activatable;
    use crate::metrics_config_orchestrator::trigger::mock::{
        MockCallbackListener, MockSubscriptionManager,
    };

    const TRIGGER_NAME: &str = "trigger";
    const INTERVAL: Duration = Duration::from_millis(10);

    fn create_valid_periodic_trigger() -> (Arc<PeriodicTrigger>, MockSubscriptionManager) {
        let trigger = PeriodicTrigger::new(TRIGGER_NAME.to_owned(), INTERVAL);

        let mut trigger = Arc::into_inner(trigger).unwrap();
        let subscription_manager = MockSubscriptionManager::new();
        trigger.base.subscription_manager = Box::new(subscription_manager.clone());

        (Arc::new(trigger), subscription_manager)
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn subscribe_calls_subscription_manager() {
        let (trigger, subscription_manager) = create_valid_periodic_trigger();

        Arc::clone(&trigger)
            .subscribe(&TriggerSubscriber::MetricsConfigStart, Box::new(|_| {}))
            .await;

        // Verify that the key and the callback are set.
        let subscription_key = subscription_manager.key.lock().take().unwrap();
        assert!(
            subscription_key == TriggerSubscriber::MetricsConfigStart,
            "{} != {}",
            subscription_key,
            TriggerSubscriber::MetricsConfigStart
        );
        assert!(subscription_manager.callback.lock().is_some());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn unsubscribe_calls_subscription_manager() {
        let (trigger, subscription_manager) = create_valid_periodic_trigger();

        trigger.unsubscribe(&TriggerSubscriber::MetricsConfigStart).await;

        // Verify that the key is set and the callback is empty.
        let subscription_key = subscription_manager.key.lock().take().unwrap();
        assert!(
            subscription_key == TriggerSubscriber::MetricsConfigStart,
            "{} != {}",
            subscription_key,
            TriggerSubscriber::MetricsConfigStart
        );
        assert!(subscription_manager.callback.lock().is_none());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_periodic_trigger_fires_twice() {
        let (trigger, _) = create_valid_periodic_trigger();
        let callback_listener = Arc::new(MockCallbackListener::new());

        // Subscribe to trigger.
        Arc::clone(&trigger).activate(callback_listener.create_callback_map()).await;

        // Wait for trigger to fire twice.
        let context1 = callback_listener.wait_for_callback_fired().await;
        let context2 = callback_listener.wait_for_callback_fired().await;

        // Unsubscribe from trigger.
        trigger.deactivate().await;

        // Verify that the trigger fired twice.
        assert_eq!(callback_listener.get_fired_count(), 2);
        assert!(context1.root_id() < context2.root_id());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn periodic_trigger_shares_context_across_subscribers() {
        let (trigger, _) = create_valid_periodic_trigger();
        let callback_listener = Arc::new(MockCallbackListener::new());
        let callback_map = callback_listener.create_callback_map();

        // Add a second callback to `callback_map`.
        let (context1_tx, context1_rx) = oneshot::channel();
        let mut context1_tx = Some(context1_tx);
        callback_map.lock().await.insert(
            TriggerSubscriber::AggregationPublisher { name: "publisher-name".to_owned() },
            Box::new(move |context| context1_tx.take().unwrap().send(context).unwrap()),
        );
        // Subscribe to trigger.
        Arc::clone(&trigger).activate(callback_map).await;

        // Wait for trigger to fire and both callbacks to run.
        let context1 = context1_rx.await.unwrap();
        let context2 = callback_listener.wait_for_callback_fired().await;
        trigger.deactivate().await;

        assert!(
            Arc::ptr_eq(&context1, &context2),
            "All contexts of a single firing of a periodic trigger should be equal."
        );
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_periodic_trigger_deactivate_from_callback_success() {
        let (trigger, _) = create_valid_periodic_trigger();
        let mut trigger_clone = Some(Arc::clone(&trigger));
        let callback: TriggerCallbackFn = Box::new(move |_| {
            let trigger_clone = trigger_clone.take().unwrap();
            tokio::spawn(async move {
                trigger_clone.deactivate().await;
            });
        });
        let callback_map = Arc::new(AsyncMutex::new(HashMap::from([(
            TriggerSubscriber::MetricsConfigStart,
            callback,
        )])));

        // Verify that there's no deadlock.
        trigger.activate(callback_map).await;
    }
}
