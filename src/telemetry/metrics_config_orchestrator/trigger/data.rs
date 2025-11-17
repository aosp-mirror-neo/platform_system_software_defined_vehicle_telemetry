// Copyright 2024 Google LLC

use async_trait::async_trait;
use sdv_telemetry_expr::node_evaluation_context::NodeEvaluationContext;
use std::sync::{Arc, Weak};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

use super::base::{BaseTrigger, TriggerState};
use super::Trigger;
use crate::metrics_config_orchestrator::async_event_processor::{
    AsyncEventHandler, AsyncEventProcessor,
};
use crate::metrics_config_orchestrator::subscription_manager::{Activatable, ActivationId};
use crate::metrics_config_orchestrator::trigger::{TriggerCallbackFn, TriggerSubscriber};
use crate::metrics_config_orchestrator::Publisher;
use crate::metrics_config_uuid::MetricsConfigUuid;

pub(crate) struct DataTrigger {
    base: BaseTrigger,
    publisher: Arc<dyn Publisher>,
}

#[async_trait]
impl Trigger for DataTrigger {
    fn get_name(&self) -> &str {
        self.base.get_name()
    }

    fn get_metrics_config_uuid(&self) -> &MetricsConfigUuid {
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

    async fn fire_manually(&self, context: Arc<NodeEvaluationContext>) {
        let maybe_activation_id = self.base.subscription_manager.get_current_activation_id().await;
        if let Some(activation_id) = maybe_activation_id {
            self.base.fire_trigger(activation_id, context).await;
        }
    }
}

#[async_trait]
impl Activatable<TriggerSubscriber, Arc<NodeEvaluationContext>> for DataTrigger {
    async fn activate(self: Arc<Self>, activation_id: ActivationId) {
        // Lock state mutex and hold it until the state transition is complete.
        let mut state_lock = self.base.state.lock().await;
        assert!(
            matches!(*state_lock, TriggerState::Inactive),
            "Unable to activate trigger {}: it's in {} state",
            self.get_name(),
            state_lock
        );

        let (sender, receiver) = mpsc::unbounded_channel();
        self.publisher
            .subscribe(
                self.get_name(),
                Box::new(move |context| {
                    sender.send(context).expect("Callback should only be called when subscribed");
                }),
            )
            .await;

        struct DataTriggerHandler {
            activation_id: ActivationId,
            trigger: Weak<DataTrigger>,
        }

        #[async_trait]
        impl AsyncEventHandler<Arc<NodeEvaluationContext>> for DataTriggerHandler {
            async fn handle(&mut self, context: Arc<NodeEvaluationContext>) {
                let Some(trigger) = self.trigger.upgrade() else {
                    return;
                };
                trigger.base.fire_trigger(self.activation_id, context).await;
            }
        }

        let event_processor = AsyncEventProcessor::new(
            UnboundedReceiverStream::new(receiver),
            DataTriggerHandler { activation_id, trigger: Arc::downgrade(&self) },
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

        self.publisher.unsubscribe(self.get_name()).await;

        // Set the new state and release the mutex.
        *state_lock = TriggerState::Inactive;
    }
}

impl DataTrigger {
    pub(crate) fn new(
        name: String,
        metrics_config_uuid: MetricsConfigUuid,
        publisher: Arc<dyn Publisher>,
    ) -> Arc<Self> {
        Arc::new_cyclic(|me: &Weak<Self>| Self {
            base: BaseTrigger::new(
                name,
                metrics_config_uuid,
                Weak::clone(me) as Weak<dyn Activatable<_, _>>,
            ),
            publisher,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use lazy_static::lazy_static;

    use crate::metrics_config_orchestrator::trigger::mock::{
        MockCallbackListener, MockSubscriptionManager,
    };
    use crate::metrics_config_orchestrator::{
        publisher::mock::MockPublisher, subscription_manager::Activatable,
    };

    lazy_static! {
        static ref CONFIG_UUID: MetricsConfigUuid =
            "01bf0754-df95-46bb-bb35-522b6af66600".parse().unwrap();
    }

    const TRIGGER_NAME: &str = "trigger";
    const PUBLISHER_NAME: &str = "publisher";
    const ACTIVATION_ID: ActivationId = ActivationId::new(3);

    fn create_valid_mock_publisher() -> MockPublisher {
        let mut publisher = MockPublisher::new(PUBLISHER_NAME.to_owned(), *CONFIG_UUID);
        publisher.expected_trigger_names.insert(TRIGGER_NAME.to_owned());
        publisher
    }

    fn create_valid_data_trigger(publisher: Arc<MockPublisher>) -> Arc<DataTrigger> {
        DataTrigger::new(TRIGGER_NAME.to_owned(), *CONFIG_UUID, publisher)
    }

    fn create_valid_data_trigger_with_mock_subscription_manager(
        publisher: Arc<MockPublisher>,
    ) -> (Arc<DataTrigger>, MockSubscriptionManager) {
        let mut trigger = Arc::into_inner(create_valid_data_trigger(publisher)).unwrap();
        let subscription_manager = MockSubscriptionManager::new();
        trigger.base.subscription_manager = Box::new(subscription_manager.clone());

        (Arc::new(trigger), subscription_manager)
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_subscribes_to_publisher() {
        let publisher = Arc::new(create_valid_mock_publisher());
        let (trigger, subscription_manager) =
            create_valid_data_trigger_with_mock_subscription_manager(Arc::clone(&publisher));
        let callback_listener = Arc::new(MockCallbackListener::new());

        // Activate the trigger.
        Arc::clone(&trigger).activate(ACTIVATION_ID).await;

        subscription_manager
            .subscribe_for_test(
                TriggerSubscriber::MetricsConfigStart,
                callback_listener.create_callback(),
            )
            .await;

        // Fire publisher message. This should notify subscribed trigger.
        publisher.fire_new_message(Arc::new(NodeEvaluationContext::new(None))).await;

        // Wait for notification.
        let (activation_id, _context) = callback_listener.wait_for_callback_fired().await;

        assert_eq!(activation_id, ACTIVATION_ID);
        // Verify that the trigger fired once.
        assert_eq!(callback_listener.get_fired_count(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn fire_manually_inactive_does_nothing() {
        let publisher = Arc::new(create_valid_mock_publisher());
        let (trigger, subscription_manager) =
            create_valid_data_trigger_with_mock_subscription_manager(publisher);

        trigger.fire_manually(Arc::new(NodeEvaluationContext::new(None))).await;
        assert_eq!(subscription_manager.get_notify_count(), 0);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn fire_manually_notifies_subscriber() {
        let publisher = Arc::new(create_valid_mock_publisher());
        let (trigger, subscription_manager) =
            create_valid_data_trigger_with_mock_subscription_manager(publisher);
        let callback_listener = Arc::new(MockCallbackListener::new());

        subscription_manager
            .subscribe_for_test(
                TriggerSubscriber::MetricsConfigStart,
                callback_listener.create_callback(),
            )
            .await;

        // Manually set activation_id to simulate an active state
        subscription_manager.set_activation_id(ACTIVATION_ID).await;

        let context = Arc::new(NodeEvaluationContext::new(None));
        trigger.fire_manually(Arc::clone(&context)).await;

        let (activation_id, received_context) = callback_listener.wait_for_callback_fired().await;

        assert_eq!(activation_id, ACTIVATION_ID);
        assert!(Arc::ptr_eq(&received_context, &context));
        assert_eq!(callback_listener.get_fired_count(), 1);
        assert_eq!(subscription_manager.get_notify_count(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn shares_context_across_subscribers() {
        let publisher = Arc::new(create_valid_mock_publisher());
        let trigger = create_valid_data_trigger(Arc::clone(&publisher));

        let callback_listener1 = Arc::new(MockCallbackListener::new());
        let callback_listener2 = Arc::new(MockCallbackListener::new());

        Arc::clone(&trigger)
            .subscribe(&TriggerSubscriber::MetricsConfigStart, callback_listener1.create_callback())
            .await;
        Arc::clone(&trigger)
            .subscribe(&TriggerSubscriber::MetricsConfigEnd, callback_listener2.create_callback())
            .await;

        publisher.fire_new_message(Arc::new(NodeEvaluationContext::new(None))).await;

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
    #[should_panic]
    async fn activate_active_panics() {
        let publisher = Arc::new(create_valid_mock_publisher());
        let (trigger, _subscription_manager) =
            create_valid_data_trigger_with_mock_subscription_manager(Arc::clone(&publisher));

        // Activate the trigger.
        Arc::clone(&trigger).activate(ActivationId::new(1)).await;

        // Try to activate it again.
        trigger.activate(ActivationId::new(2)).await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn deactivate_unsubscribes_from_publisher() {
        let callback_listener = Arc::new(MockCallbackListener::new());
        let callback_listener_clone = Arc::clone(&callback_listener);
        let mut publisher = create_valid_mock_publisher();
        publisher.set_unsubscribe_callback(move || callback_listener_clone.on_callback_fired(()));
        let publisher = Arc::new(publisher);
        let (trigger, _subscription_manager) =
            create_valid_data_trigger_with_mock_subscription_manager(Arc::clone(&publisher));

        // Activate the trigger.
        Arc::clone(&trigger).activate(ActivationId::new(1)).await;

        // Deactivate the trigger.
        trigger.deactivate().await;

        // Verify that publisher was unsubscribed from once.
        assert_eq!(callback_listener.get_fired_count(), 1);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[should_panic]
    async fn deactivate_inactive_panics() {
        let publisher = Arc::new(create_valid_mock_publisher());
        let (trigger, _subscription_manager) =
            create_valid_data_trigger_with_mock_subscription_manager(Arc::clone(&publisher));

        // Deactivate the trigger.
        trigger.deactivate().await;
    }
}
