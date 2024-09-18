// Copyright 2024 Google LLC

use async_trait::async_trait;
use log::error;
use protobuf::MessageDyn;
use sdv_telemetry_utils::{
    collection_extension::HashMapExtension, operation_result::OperationResult,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex as AsyncMutex;

use super::{
    base::BasePublisher, CallbackFn, Publisher, PublisherActivateError, PublisherDeactivateError,
};
use crate::expr::node::NodeEvaluationContext;
use crate::metrics_config_orchestrator::message_builder::MessageBuilder;
use crate::metrics_config_orchestrator::trigger::{Trigger, TriggerSubscriber};

pub(crate) struct AggregationPublisher {
    pub(crate) reset_on_get: bool,
    pub(crate) triggers: Vec<Arc<dyn Trigger>>,

    base: BasePublisher,
    message_builder: Arc<dyn MessageBuilder>,
    state: AsyncMutex<AggregationPublisherState>,
    trigger_subscriber: TriggerSubscriber,
}

#[derive(strum_macros::Display)]
#[strum(serialize_all = "title_case")]
enum AggregationPublisherState {
    /// The publisher is not subscribed to the underlying data source
    /// and doesn't provide the latest message.
    Inactive,
    /// The publisher is subscribed to the underlying data source
    /// and maintains the latest message.
    Active {
        // Map of unique data trigger names to the subscribed callbacks.
        callback_map: HashMap<String, CallbackFn>,
    },
}

#[async_trait]
impl Publisher for AggregationPublisher {
    fn get_name(&self) -> &str {
        self.base.get_name()
    }

    fn is_subscribable(&self) -> bool {
        true
    }

    fn get_latest_message(
        self: Arc<Self>,
    ) -> OperationResult<'static, Option<Arc<dyn MessageDyn>>, anyhow::Error> {
        let latest_message = self.base.get_latest_message();
        if self.reset_on_get {
            self.base.reset_latest_message();
            self.message_builder.reset();
        }
        Ok(latest_message).into()
    }

    async fn activate(self: Arc<Self>) -> Result<(), PublisherActivateError> {
        // Lock state mutex and hold it until the state transition is complete.
        let mut state_lock = self.state.lock().await;
        assert!(
            matches!(*state_lock, AggregationPublisherState::Inactive),
            "Unable to activate publisher {}: it's in {} state",
            self.get_name(),
            state_lock
        );

        // Subscribe to parent triggers.
        for trigger in &self.triggers {
            let self_clone = Arc::clone(&self);
            Arc::clone(trigger)
                .subscribe(
                    &self.trigger_subscriber,
                    Box::new(move |context| {
                        self_clone.on_trigger_fired(context);
                    }),
                )
                .await;
        }

        // Set the new state and release the mutex.
        *state_lock = AggregationPublisherState::Active { callback_map: HashMap::new() };

        Ok(())
    }

    async fn deactivate(&self) -> Result<(), PublisherDeactivateError> {
        // Lock state mutex and hold it until the state transition is complete.
        let mut state_lock = self.state.lock().await;
        match &*state_lock {
            AggregationPublisherState::Active { callback_map, .. } => {
                assert!(
                    callback_map.is_empty(),
                    "Unable to deactivate publisher {} with active subscriptions",
                    self.get_name()
                );
            }
            AggregationPublisherState::Inactive => {
                panic!("Unable to deactivate publisher {}: it's inactive", self.get_name())
            }
        }

        // Perform deactivation logic specific to the publisher type.

        // Verify that the publisher is in the subscribed state.
        assert!(
            matches!(&*state_lock, AggregationPublisherState::Active { .. }),
            "Unable to deactivate Aggregation publisher {} because it's in {} state",
            self.get_name(),
            state_lock
        );

        // Unsubscribe from parent triggers.
        for trigger in &self.triggers {
            trigger.unsubscribe(&self.trigger_subscriber).await;
        }

        // Reset latest message.
        // The publisher is unsubscribed from underlying data sources, so no callback will change it back to some stale message.
        self.base.reset_latest_message();

        // Change state to Inactive and release the mutex.
        *state_lock = AggregationPublisherState::Inactive;

        Ok(())
    }

    async fn subscribe(&self, data_trigger_name: &str, callback: CallbackFn) {
        // Lock state mutex and hold it until operation is complete.
        let mut state_lock = self.state.lock().await;

        if let AggregationPublisherState::Active { callback_map, .. } = &mut *state_lock {
            callback_map.try_insert_value(data_trigger_name.to_owned(), callback).unwrap_or_else(|_| panic!("Unable to subscribe to publisher {}: data trigger {} is already subscribed", self.get_name(), data_trigger_name));
        } else {
            panic!(
                "Unable to subscribe to publisher {}: it's in {} state",
                self.get_name(),
                state_lock
            );
        }
    }

    async fn unsubscribe(&self, data_trigger_name: &str) {
        // Lock state mutex and hold it until operation is complete.
        let mut state_lock = self.state.lock().await;

        if let AggregationPublisherState::Active { callback_map, .. } = &mut *state_lock {
            let _ = callback_map.remove(data_trigger_name).unwrap_or_else(|| {
                panic!(
                    "Unable to unsubscribe from publisher {}: data trigger {} wasn't subscribed",
                    self.get_name(),
                    data_trigger_name
                )
            });
        } else {
            panic!(
                "Unable to unsubscribe from publisher {}: it's in {} state",
                self.get_name(),
                state_lock
            );
        }
    }
}

impl AggregationPublisher {
    pub(crate) fn new(
        name: String,
        message_builder: Arc<dyn MessageBuilder>,
        reset_on_get: bool,
        triggers: Vec<Arc<dyn Trigger>>,
    ) -> Self {
        Self {
            base: BasePublisher::new(name.clone()),
            state: AggregationPublisherState::Inactive.into(),

            message_builder,
            reset_on_get,
            trigger_subscriber: TriggerSubscriber::AggregationPublisher { name },
            triggers,
        }
    }

    fn on_trigger_fired(self: &Arc<Self>, context: Arc<NodeEvaluationContext>) {
        let res = Arc::clone(&self.message_builder).build(&context);
        let self_clone = Arc::clone(self);
        tokio::spawn(async move {
            // Lock state mutex and hold it to make sure the publisher is not deactivated.
            let mut state_lock = self_clone.state.lock().await;

            let AggregationPublisherState::Active { callback_map, .. } = &mut *state_lock else {
                panic!(
                    "Publisher {} received callback when it was in the {} state",
                    self_clone.get_name(),
                    state_lock
                );
            };

            let msg = match res.into_future().await {
                Ok(msg) => msg.into(),
                Err(err) => {
                    error!(
                        "Error when trying to evaluate expression for publisher {}: {}",
                        self_clone.get_name(),
                        err
                    );
                    return;
                }
            };

            let context =
                context.to_child_context(self_clone.get_name().to_owned(), Arc::clone(&msg)).into();
            self_clone.base.set_new_message(msg, &context, callback_map);
        });
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use protobuf::reflect::ReflectEqMode;
    use protobuf::well_known_types::wrappers::Int32Value;
    use sdv_telemetry_test_proto::test_message::TestMessage;
    use std::{future, sync::Arc};
    use tokio::sync::oneshot;

    use super::*;
    use crate::metrics_config_orchestrator::message_builder::mock::MockMessageBuilder;
    use crate::metrics_config_orchestrator::trigger::mock::MockTrigger;

    const DATA_TRIGGER_NAME: &str = "datatrigger";
    const PUBLISHER_NAME: &str = "publisher";
    const PUBLISHER_NAME_2: &str = "publisher2";
    const TRIGGER_NAME: &str = "trigger";

    fn create_inactive_aggregation_publisher(
        triggers: Vec<Arc<dyn Trigger>>,
    ) -> (Arc<dyn Publisher>, Arc<MockMessageBuilder>) {
        let message_builder = Arc::new(MockMessageBuilder::new());
        let publisher = Arc::new(AggregationPublisher::new(
            PUBLISHER_NAME.to_owned(),
            Arc::clone(&message_builder) as Arc<dyn MessageBuilder>,
            false,
            triggers,
        ));
        (publisher, message_builder)
    }

    async fn create_active_aggregation_publisher(
        triggers: Vec<Arc<dyn Trigger>>,
    ) -> (Arc<dyn Publisher>, Arc<MockMessageBuilder>) {
        let (publisher, message_builder) = create_inactive_aggregation_publisher(triggers);

        Arc::clone(&publisher).activate().await.unwrap();

        (publisher, message_builder)
    }

    async fn create_aggregation_publisher_with_subscription(
        triggers: Vec<Arc<dyn Trigger>>,
    ) -> Arc<dyn Publisher> {
        let (publisher, _) = create_active_aggregation_publisher(triggers).await;

        publisher.subscribe(DATA_TRIGGER_NAME, create_empty_fn_box()).await;
        publisher
    }

    /// Creates a Box with a callback that accepts context and does nothing.
    pub fn create_empty_fn_box() -> CallbackFn {
        Box::new(|_| {})
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    #[should_panic]
    async fn get_latest_message_resets() {
        let trigger = Arc::new(MockTrigger::new_for_subscriber(
            TRIGGER_NAME.to_owned(),
            TriggerSubscriber::AggregationPublisher { name: PUBLISHER_NAME.to_owned() },
        ));
        let message_builder = Arc::new(MockMessageBuilder::new());
        let publisher = Arc::new(AggregationPublisher::new(
            PUBLISHER_NAME.to_owned(),
            Arc::<MockMessageBuilder>::clone(&message_builder),
            true,
            vec![Arc::<MockTrigger>::clone(&trigger)],
        ));

        Arc::clone(&publisher).activate().await.unwrap();
        trigger.fire().await;
        let msg1 = Arc::clone(&publisher)
            .get_latest_message()
            .unwrap_sync()
            .expect("Message retrieval should be successful");
        let msg2 = publisher
            .get_latest_message()
            .unwrap_sync()
            .expect("Message retrieval should be successful");

        assert!(msg1.is_some());
        assert!(msg2.is_none(), "Unexpected message: {:?}", msg2.unwrap());
        assert_eq!(message_builder.get_num_reset_calls(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    #[should_panic]
    async fn activate_when_active_panics() {
        let (publisher, _) = create_active_aggregation_publisher(vec![]).await;

        let _ = publisher.activate().await;
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    #[should_panic]
    async fn deactivate_when_inactive_panics() {
        let (publisher, _) = create_inactive_aggregation_publisher(vec![]);

        let _ = publisher.deactivate().await;
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    #[should_panic]
    async fn deactivate_with_subscriptions_panics() {
        let publisher = create_aggregation_publisher_with_subscription(vec![]).await;

        let _ = publisher.deactivate().await;
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    #[should_panic]
    async fn subscribe_when_inactive_panics() {
        let (publisher, _) = create_inactive_aggregation_publisher(vec![]);

        publisher.subscribe(DATA_TRIGGER_NAME, create_empty_fn_box()).await;
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    #[should_panic]
    async fn subscribe_already_subscribed_panics() {
        let publisher = create_aggregation_publisher_with_subscription(vec![]).await;

        publisher.subscribe(DATA_TRIGGER_NAME, create_empty_fn_box()).await;
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn subscribe_notifies_subscriber_when_triggered() {
        let trigger = Arc::new(MockTrigger::new_for_subscriber(
            TRIGGER_NAME.to_owned(),
            TriggerSubscriber::AggregationPublisher { name: PUBLISHER_NAME.to_owned() },
        ));
        let (publisher, _) =
            create_active_aggregation_publisher(vec![Arc::<MockTrigger>::clone(&trigger)]).await;
        let (tx, rx) = oneshot::channel();
        let mut maybe_tx = Some(tx);

        publisher
            .subscribe(
                DATA_TRIGGER_NAME,
                Box::new(move |context| {
                    let msg = context
                        .get_message(PUBLISHER_NAME)
                        .expect("Publisher message should be present in the context");
                    maybe_tx.take().unwrap().send(Arc::clone(msg)).unwrap()
                }),
            )
            .await;
        trigger.fire().await;

        let messages = [
            rx.await.unwrap(),
            Arc::clone(&publisher).get_latest_message().unwrap_sync().unwrap().unwrap(),
        ];
        for message in messages {
            let test_message =
                <dyn MessageDyn>::downcast_ref::<TestMessage>(message.as_ref()).unwrap();
            assert_eq!(test_message.int_field, 1);
        }
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn subscribe_passes_trigger_context_when_triggered() {
        let trigger = Arc::new(MockTrigger::new_for_subscriber(
            TRIGGER_NAME.to_owned(),
            TriggerSubscriber::AggregationPublisher { name: PUBLISHER_NAME.to_owned() },
        ));
        let (publisher, _) =
            create_active_aggregation_publisher(vec![Arc::<MockTrigger>::clone(&trigger)]).await;
        let msg = Int32Value { value: 42, ..Default::default() };

        let (context_tx, context_rx) = oneshot::channel();
        let mut context_tx = Some(context_tx);
        publisher
            .subscribe(
                DATA_TRIGGER_NAME,
                Box::new(move |context| context_tx.take().unwrap().send(context).unwrap()),
            )
            .await;

        let trigger_context = Arc::new(NodeEvaluationContext::with_publisher_message(
            PUBLISHER_NAME_2.to_owned(),
            Arc::new(msg.clone()),
        ));
        trigger.fire_with_context(Arc::clone(&trigger_context)).await;

        let context = context_rx.await.unwrap();

        assert_eq!(context.root_id(), trigger_context.root_id());
        let context_msg = context
            .get_message(PUBLISHER_NAME_2)
            .expect("Publisher message should be present in the context");
        assert!(
            context_msg.reflect_eq_dyn(&msg, &ReflectEqMode::default()),
            "Expected {context_msg:?} to equal {msg:?}"
        );
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn subscribe_handles_async_expression_result() {
        let trigger = Arc::new(MockTrigger::new_for_subscriber(
            TRIGGER_NAME.to_owned(),
            TriggerSubscriber::AggregationPublisher { name: PUBLISHER_NAME.to_owned() },
        ));
        let (publisher, message_builder) =
            create_active_aggregation_publisher(vec![Arc::<MockTrigger>::clone(&trigger)]).await;
        let (tx, rx) = oneshot::channel();
        let mut maybe_tx = Some(tx);
        publisher
            .subscribe(
                DATA_TRIGGER_NAME,
                Box::new(move |context| {
                    let msg = context
                        .get_message(PUBLISHER_NAME)
                        .expect("Publisher message should be present in the context");
                    maybe_tx.take().unwrap().send(Arc::clone(msg)).unwrap()
                }),
            )
            .await;

        message_builder.set_next_async_result(
            future::ready(Ok(
                Box::new(TestMessage { int_field: 2, ..Default::default() }) as Box<dyn MessageDyn>
            ))
            .boxed(),
        );
        trigger.fire().await;

        let messages =
            [rx.await.unwrap(), publisher.get_latest_message().unwrap_sync().unwrap().unwrap()];
        for message in messages {
            let test_message =
                <dyn MessageDyn>::downcast_ref::<TestMessage>(message.as_ref()).unwrap();
            assert_eq!(test_message.int_field, 2);
        }
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    #[should_panic]
    async fn unsubscribe_when_inactive_panics() {
        let (publisher, _) = create_inactive_aggregation_publisher(vec![]);

        publisher.unsubscribe(DATA_TRIGGER_NAME).await;
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    #[should_panic]
    async fn unsubscribe_not_subscribed_panics() {
        let (publisher, _) = create_active_aggregation_publisher(vec![]).await;

        publisher.unsubscribe(DATA_TRIGGER_NAME).await;
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn unsubscribe_success() {
        let publisher = create_aggregation_publisher_with_subscription(vec![]).await;

        publisher.unsubscribe(DATA_TRIGGER_NAME).await;
    }
}
