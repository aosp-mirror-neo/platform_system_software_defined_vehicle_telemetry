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
use crate::{
    expr::node::NodeEvaluationContext,
    metrics_config_orchestrator::{
        message_builder::MessageBuilder,
        trigger::{Trigger, TriggerSubscriber},
    },
};

pub(crate) struct AggregationPublisher {
    base: BasePublisher,
    state: AsyncMutex<AggregationPublisherState>,

    #[allow(dead_code)]
    pub(crate) reset_on_get: bool,
    pub(crate) triggers: Vec<Arc<dyn Trigger>>,
    message_builder: Arc<dyn MessageBuilder>,
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
        Ok(self.base.get_latest_message()).into()
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

    async fn subscribe(
        &self,
        data_trigger_name: &str,
        callback: Box<dyn for<'a> FnMut(&'a dyn MessageDyn, NodeEvaluationContext) + Send + Sync>,
    ) {
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

    fn on_trigger_fired(self: &Arc<Self>, context: NodeEvaluationContext) {
        let res = Arc::clone(&self.message_builder).build(&Arc::new(context.clone()));
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
                Ok(msg) => msg,
                Err(err) => {
                    error!(
                        "Error when trying to evaluate expression for publisher {}: {}",
                        self_clone.get_name(),
                        err
                    );
                    return;
                }
            };

            self_clone.base.set_new_message(msg.into(), context, callback_map);
        });
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use sdv_telemetry_test_proto::test_message::TestMessage;
    use std::{future, sync::Arc};
    use tokio::sync::oneshot;

    use super::*;
    use crate::metrics_config_orchestrator::message_builder::mock::MockMessageBuilder;
    use crate::metrics_config_orchestrator::trigger::mock::MockTrigger;

    const DATA_TRIGGER_NAME: &str = "datatrigger";
    const PUBLISHER_NAME: &str = "publisher";
    const TRIGGER_NAME: &str = "trigger";

    fn create_inactive_aggregation_publisher(
        triggers: Vec<Arc<dyn Trigger>>,
        message_builder: Arc<dyn MessageBuilder>,
    ) -> Arc<dyn Publisher> {
        Arc::new(AggregationPublisher::new(
            PUBLISHER_NAME.to_owned(),
            message_builder,
            false,
            triggers,
        ))
    }

    async fn create_active_aggregation_publisher(
        triggers: Vec<Arc<dyn Trigger>>,
    ) -> (Arc<dyn Publisher>, Arc<MockMessageBuilder>) {
        let message_builder = Arc::new(MockMessageBuilder::new());
        let publisher = create_inactive_aggregation_publisher(
            triggers,
            Arc::clone(&message_builder) as Arc<dyn MessageBuilder>,
        );

        Arc::clone(&publisher).activate().await.unwrap();
        (publisher, message_builder)
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn subscribe_aggregation_publisher_triggered_notifies_subscriber() {
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
                Box::new(move |message, _context| {
                    maybe_tx.take().unwrap().send(message.clone_box()).unwrap()
                }),
            )
            .await;
        trigger.fire().await;

        let messages = [
            rx.await.unwrap().into(),
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
    async fn aggregation_publisher_notifies_all_subscriber_with_trigger_context() {
        let trigger = Arc::new(MockTrigger::new_for_subscriber(
            TRIGGER_NAME.to_owned(),
            TriggerSubscriber::AggregationPublisher { name: PUBLISHER_NAME.to_owned() },
        ));
        let (publisher, _) =
            create_active_aggregation_publisher(vec![Arc::<MockTrigger>::clone(&trigger)]).await;
        let (context_tx, context_rx) = oneshot::channel();
        let mut context_tx = Some(context_tx);

        publisher
            .subscribe(
                DATA_TRIGGER_NAME,
                Box::new(move |_message, context| {
                    context_tx.take().unwrap().send(context).unwrap()
                }),
            )
            .await;
        let trigger_context = NodeEvaluationContext::new();
        trigger.fire_with_context(trigger_context.clone()).await;

        let context = context_rx.await.unwrap();

        assert_eq!(
            trigger_context, context,
            "Node evaluation context should be equal to the trigger context."
        );
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn subscribe_aggregation_publisher_handles_async_expression_result() {
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
                Box::new(move |message, _context| {
                    maybe_tx.take().unwrap().send(message.clone_box()).unwrap()
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

        let messages = [
            rx.await.unwrap().into(),
            publisher.get_latest_message().unwrap_sync().unwrap().unwrap(),
        ];
        for message in messages {
            let test_message =
                <dyn MessageDyn>::downcast_ref::<TestMessage>(message.as_ref()).unwrap();
            assert_eq!(test_message.int_field, 2);
        }
    }
}
