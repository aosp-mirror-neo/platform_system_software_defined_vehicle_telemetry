// Copyright 2024 Google LLC

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use tokio::sync::{mpsc, Mutex as AsyncMutex};

use super::base::{BaseTrigger, TriggerState};
use super::Trigger;
use crate::expr::node_evaluation_context::NodeEvaluationContext;
use crate::metrics_config_orchestrator::async_event_processor::{
    AsyncEventHandler, AsyncEventProcessor,
};
use crate::metrics_config_orchestrator::subscription_manager::Activatable;
use crate::metrics_config_orchestrator::trigger::{TriggerCallbackFn, TriggerSubscriber};
use crate::metrics_config_orchestrator::Publisher;

pub(crate) struct DataTrigger {
    base: BaseTrigger,

    pub(crate) publisher: Arc<dyn Publisher>,
}

#[async_trait]
impl Trigger for DataTrigger {
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
impl Activatable<TriggerSubscriber, Arc<NodeEvaluationContext>> for DataTrigger {
    async fn activate(
        self: Arc<Self>,
        callback_map: Arc<AsyncMutex<HashMap<TriggerSubscriber, TriggerCallbackFn>>>,
    ) {
        // Lock state mutex and hold it until the state transition is complete.
        let mut state_lock = self.base.state.lock().await;
        assert!(
            matches!(*state_lock, TriggerState::Inactive),
            "Unable to activate trigger {}: it's in {} state",
            self.get_name(),
            state_lock
        );

        let (sender, receiver) = mpsc::unbounded_channel();
        let trigger_name = self.get_name().to_owned();
        self.publisher
            .subscribe(
                self.get_name(),
                Box::new(move |context| {
                    sender
                        .send((trigger_name.clone(), context))
                        .expect("Callback should only be called when subscribed");
                }),
            )
            .await;

        struct DataTriggerHandler {
            callback_map: Arc<AsyncMutex<HashMap<TriggerSubscriber, TriggerCallbackFn>>>,
        }

        #[async_trait]
        impl AsyncEventHandler<(String, Arc<NodeEvaluationContext>)> for DataTriggerHandler {
            async fn handle(
                &mut self,
                (trigger_name, context): (String, Arc<NodeEvaluationContext>),
            ) {
                BaseTrigger::fire_trigger(&trigger_name, &self.callback_map, &context).await;
            }
        }

        let event_processor =
            AsyncEventProcessor::new(receiver, DataTriggerHandler { callback_map });

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

        self.publisher.unsubscribe(self.get_name()).await;

        // Set the new state and release the mutex.
        *state_lock = TriggerState::Inactive;
    }
}

impl DataTrigger {
    pub(crate) fn new(name: String, publisher: Arc<dyn Publisher>) -> Arc<Self> {
        Arc::new_cyclic(|me: &Weak<Self>| Self {
            base: BaseTrigger::new(name, Weak::clone(me) as Weak<dyn Activatable<_, _>>),
            publisher,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use tokio::sync::oneshot;
    use tokio::sync::Mutex as AsyncMutex;

    use super::*;
    use crate::metrics_config_orchestrator::trigger::mock::{
        MockCallbackListener, MockSubscriptionManager,
    };
    use crate::metrics_config_orchestrator::{
        publisher::mock::MockPublisher, subscription_manager::Activatable,
    };

    const TRIGGER_NAME: &str = "trigger";
    const PUBLISHER_NAME: &str = "publisher";

    fn create_valid_mock_publisher() -> MockPublisher {
        let mut publisher = MockPublisher::new(PUBLISHER_NAME.to_owned());
        publisher.expected_trigger_names.insert(TRIGGER_NAME.to_owned());
        publisher
    }

    fn create_valid_data_trigger(publisher: Arc<MockPublisher>) -> Arc<DataTrigger> {
        let trigger = DataTrigger::new(TRIGGER_NAME.to_owned(), publisher);

        let mut trigger = Arc::into_inner(trigger).unwrap();
        let subscription_manager = MockSubscriptionManager::new();
        trigger.base.subscription_manager = Box::new(subscription_manager.clone());

        Arc::new(trigger)
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_subscribes_to_publisher() {
        let publisher = Arc::new(create_valid_mock_publisher());
        let trigger = create_valid_data_trigger(Arc::clone(&publisher));
        let callback_listener = Arc::new(MockCallbackListener::new());

        // Activate the trigger.
        Arc::clone(&trigger).activate(callback_listener.create_callback_map()).await;

        // Fire publisher message. This should notify subscribed trigger.
        publisher.fire_new_message(Arc::new(NodeEvaluationContext::new())).await;

        // Wait for notification.
        callback_listener.wait_for_callback_fired().await;

        // Verify that the trigger fired once.
        assert_eq!(callback_listener.get_fired_count(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_shares_context_across_subscribers() {
        let publisher = Arc::new(create_valid_mock_publisher());
        let trigger = create_valid_data_trigger(Arc::clone(&publisher));
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

        publisher.fire_new_message(Arc::new(NodeEvaluationContext::new())).await;

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
    #[should_panic]
    async fn activate_active_panics() {
        let publisher = Arc::new(create_valid_mock_publisher());
        let trigger = create_valid_data_trigger(Arc::clone(&publisher));

        // Activate the trigger.
        Arc::clone(&trigger).activate(Arc::new(AsyncMutex::new(HashMap::new()))).await;

        // Try to activate it again.
        trigger.activate(Arc::new(AsyncMutex::new(HashMap::new()))).await;
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn deactivate_unsubscribes_from_publisher() {
        let callback_listener = Arc::new(MockCallbackListener::new());
        let callback_listener_clone = Arc::clone(&callback_listener);
        let mut publisher = create_valid_mock_publisher();
        publisher.set_unsubscribe_callback(move || callback_listener_clone.on_callback_fired(()));
        let publisher = Arc::new(publisher);
        let trigger = create_valid_data_trigger(Arc::clone(&publisher));

        // Activate the trigger.
        Arc::clone(&trigger).activate(Arc::new(AsyncMutex::new(HashMap::new()))).await;

        // Deactivate the trigger.
        trigger.deactivate().await;

        // Verify that publisher was unsubscribed from once.
        assert_eq!(callback_listener.get_fired_count(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    #[should_panic]
    async fn deactivate_inactive_panics() {
        let publisher = Arc::new(create_valid_mock_publisher());
        let trigger = create_valid_data_trigger(Arc::clone(&publisher));

        // Deactivate the trigger.
        trigger.deactivate().await;
    }
}
