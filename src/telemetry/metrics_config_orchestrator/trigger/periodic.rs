// Copyright 2024 Google LLC

use async_trait::async_trait;
use sdv_telemetry_expr::node_evaluation_context::NodeEvaluationContext;
use std::sync::{Arc, Weak};
use tokio::sync::mpsc;
use tokio::time::{Duration, MissedTickBehavior};
use tokio_stream::wrappers::ReceiverStream;

use super::base::BaseTrigger;
use super::Trigger;
use crate::metrics_config_orchestrator::async_event_processor::{
    AsyncEventHandler, AsyncEventProcessor,
};
use crate::metrics_config_orchestrator::node_evaluation_context_factory::NodeEvaluationContextFactory;
use crate::metrics_config_orchestrator::subscription_manager::{Activatable, ActivationId};
use crate::metrics_config_orchestrator::trigger::{
    base::TriggerState, TriggerCallbackFn, TriggerSubscriber,
};

pub(crate) struct PeriodicTrigger {
    base: BaseTrigger,
    node_evaluation_context_factory: Arc<dyn NodeEvaluationContextFactory>,
    interval: Duration,
}

impl PeriodicTrigger {
    pub(crate) fn new(
        name: String,
        metrics_config_uuid: String,
        interval: Duration,
        node_evaluation_context_factory: Arc<dyn NodeEvaluationContextFactory>,
    ) -> Arc<Self> {
        Arc::new_cyclic(|me: &Weak<Self>| Self {
            base: BaseTrigger::new(
                name,
                metrics_config_uuid,
                Weak::clone(me) as Weak<dyn Activatable<_, _>>,
            ),
            node_evaluation_context_factory,
            interval,
        })
    }
}

#[async_trait]
impl Trigger for PeriodicTrigger {
    fn get_name(&self) -> &str {
        self.base.get_name()
    }

    fn get_metrics_config_uuid(&self) -> &str {
        self.base.get_metrics_config_uuid()
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
    async fn activate(self: Arc<Self>, activation_id: ActivationId) {
        // Lock state mutex and hold it until the state transition is complete.
        let mut state_lock = self.base.state.lock().await;
        assert!(
            matches!(*state_lock, TriggerState::Inactive),
            "Unable to activate trigger {}: it's in {} state",
            self.get_name(),
            state_lock
        );

        let interval = self.interval;
        let node_evaluation_context_factory = Arc::clone(&self.node_evaluation_context_factory);
        // There is no point in buffering more than a single trigger message, because
        // `tokio::time::interval` already has built-in missed-tick handling.
        let (sender, receiver) = mpsc::channel(1);
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval);
            timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

            // Trigger first tick that returns immediately.
            timer.tick().await;

            loop {
                // Sleep until next interval tick.
                timer.tick().await;
                let context = Arc::new(node_evaluation_context_factory.create());
                if sender.send(context).await.is_err() {
                    // Receiver dropped, so we can stop here.
                    return;
                }
            }
        });

        struct PeriodicTriggerHandler {
            activation_id: ActivationId,
            trigger: Weak<PeriodicTrigger>,
        }

        #[async_trait]
        impl AsyncEventHandler<Arc<NodeEvaluationContext>> for PeriodicTriggerHandler {
            async fn handle(&mut self, context: Arc<NodeEvaluationContext>) {
                let Some(trigger) = self.trigger.upgrade() else {
                    return;
                };
                trigger.base.fire_trigger(self.activation_id, context).await;
            }
        }

        let event_processor = AsyncEventProcessor::new(
            ReceiverStream::new(receiver),
            PeriodicTriggerHandler { activation_id, trigger: Arc::downgrade(&self) },
        );

        // Set the new state and release the mutex.
        *state_lock = TriggerState::Active { _event_processor: event_processor };
    }

    async fn deactivate(&self) {
        // Lock state mutex and hold it until the state transition is complete.
        let mut state_lock = self.base.state.lock().await;
        assert!(
            matches!(*state_lock, TriggerState::Active { .. }),
            "Unable to deactivate trigger {}: it's in {} state",
            self.get_name(),
            state_lock
        );

        // Set the new state and release the mutex.
        *state_lock = TriggerState::Inactive;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use sdv_telemetry_platform_abstraction_layer::new_stub_pal;
    use std::time::Duration;
    use tokio::sync::{mpsc, Notify};

    use crate::metrics_config_orchestrator::node_evaluation_context_factory::NodeEvaluationContextFactoryImpl;
    use crate::metrics_config_orchestrator::subscription_manager::{
        Activatable, SubscriptionManager,
    };
    use crate::metrics_config_orchestrator::trigger::mock::{
        MockCallbackListener, MockSubscriptionManager,
    };

    const CONFIG_UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";
    const TRIGGER_NAME: &str = "trigger";
    const INTERVAL: Duration = Duration::from_millis(300);
    const ACTIVATION_ID: ActivationId = ActivationId::new(3);

    fn create_valid_periodic_trigger() -> Arc<PeriodicTrigger> {
        PeriodicTrigger::new(
            TRIGGER_NAME.to_owned(),
            CONFIG_UUID.to_owned(),
            INTERVAL,
            Arc::new(NodeEvaluationContextFactoryImpl::new(new_stub_pal())),
        )
    }

    fn create_valid_periodic_trigger_with_mock_subscription_manager(
    ) -> (Arc<PeriodicTrigger>, MockSubscriptionManager) {
        let mut trigger = Arc::into_inner(create_valid_periodic_trigger()).unwrap();
        let subscription_manager = MockSubscriptionManager::new();
        trigger.base.subscription_manager = Box::new(subscription_manager.clone());

        (Arc::new(trigger), subscription_manager)
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscribe_calls_subscription_manager() {
        let (trigger, subscription_manager) =
            create_valid_periodic_trigger_with_mock_subscription_manager();

        Arc::clone(&trigger)
            .subscribe(&TriggerSubscriber::MetricsConfigStart, Box::new(|_, _, _| {}))
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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn unsubscribe_calls_subscription_manager() {
        let (trigger, subscription_manager) =
            create_valid_periodic_trigger_with_mock_subscription_manager();

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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_periodic_trigger_fires_twice() {
        let (trigger, subscription_manager) =
            create_valid_periodic_trigger_with_mock_subscription_manager();

        let (tx, mut rx) = mpsc::unbounded_channel();
        subscription_manager
            .subscribe_for_test(
                TriggerSubscriber::MetricsConfigStart,
                Box::new(move |arg, _, _| tx.send(arg).unwrap()),
            )
            .await;

        // Activate trigger.
        Arc::clone(&trigger).activate(ACTIVATION_ID).await;

        // Wait for trigger to fire twice.
        let (activation_id1, context1) = rx.recv().await.unwrap();
        let (activation_id2, context2) = rx.recv().await.unwrap();

        // Unsubscribe from trigger.
        trigger.deactivate().await;

        assert!(context1.root_id() < context2.root_id());
        assert_eq!(activation_id1, ACTIVATION_ID);
        assert_eq!(activation_id2, ACTIVATION_ID);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn shares_context_across_subscribers() {
        let trigger = create_valid_periodic_trigger();

        let callback_listener1 = Arc::new(MockCallbackListener::new());
        let callback_listener2 = Arc::new(MockCallbackListener::new());

        Arc::clone(&trigger)
            .subscribe(&TriggerSubscriber::MetricsConfigStart, callback_listener1.create_callback())
            .await;
        Arc::clone(&trigger)
            .subscribe(&TriggerSubscriber::MetricsConfigEnd, callback_listener2.create_callback())
            .await;

        // Wait for trigger to fire and both callbacks to run.
        let context1 = callback_listener1.wait_for_callback_fired().await;
        let context2 = callback_listener2.wait_for_callback_fired().await;

        trigger.deactivate().await;

        assert!(
            Arc::ptr_eq(&context1, &context2),
            "All contexts of a single firing of a periodic trigger should be equal."
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_periodic_trigger_deactivate_from_callback_success() {
        let (trigger, subscription_manager) =
            create_valid_periodic_trigger_with_mock_subscription_manager();
        let mut trigger_clone = Some(Arc::clone(&trigger));

        let notify = Arc::new(Notify::new());
        let notify_clone = Arc::clone(&notify);
        subscription_manager
            .subscribe(
                TriggerSubscriber::MetricsConfigStart,
                Box::new(move |_, _, _| {
                    let trigger_clone = trigger_clone.take().unwrap();
                    let notify_clone = Arc::clone(&notify_clone);
                    tokio::spawn(async move {
                        trigger_clone.deactivate().await;
                        notify_clone.notify_one();
                    });
                }),
            )
            .await;

        trigger.activate(ACTIVATION_ID).await;

        // Verify that there's no deadlock.
        notify.notified().await;
    }
}
