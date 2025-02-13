// Copyright 2024 Google LLC

use log::trace;
use std::sync::{Arc, Weak};
use tokio::sync::Mutex as AsyncMutex;

use crate::expr::node_evaluation_context::NodeEvaluationContext;
use crate::metrics_config_orchestrator::async_event_processor::AsyncEventProcessor;
use crate::metrics_config_orchestrator::subscription_manager::{
    Activatable, ActivationId, SubscriptionManager, SubscriptionManagerImpl,
};
use crate::metrics_config_orchestrator::trigger::TriggerSubscriber;

pub(super) struct BaseTrigger {
    name: String,
    metrics_config_uuid: String,

    pub(super) state: AsyncMutex<TriggerState>,

    // Manages trigger subscriptions. The first subscription activates the trigger, passing a
    // reference to the callback map. The last unsubscription deactivates the trigger.
    pub(super) subscription_manager:
        Box<dyn SubscriptionManager<TriggerSubscriber, Arc<NodeEvaluationContext>>>,
}

#[derive(strum_macros::Display)]
#[strum(serialize_all = "title_case")]
pub(super) enum TriggerState {
    Inactive,
    Active {
        // Processor of incoming events.
        _event_processor: AsyncEventProcessor,
    },
}

impl BaseTrigger {
    pub(super) fn new(
        name: String,
        metrics_config_uuid: String,
        trigger: Weak<dyn Activatable<TriggerSubscriber, Arc<NodeEvaluationContext>>>,
    ) -> Self {
        BaseTrigger {
            name,
            metrics_config_uuid,
            state: TriggerState::Inactive.into(),
            subscription_manager: Box::new(SubscriptionManagerImpl::new(trigger)),
        }
    }

    pub(super) fn get_name(&self) -> &str {
        &self.name
    }

    #[allow(dead_code)]
    pub(super) fn get_metrics_config_uuid(&self) -> &str {
        &self.metrics_config_uuid
    }

    /// Fires the trigger by invoking provided callback.
    pub(super) async fn fire_trigger(
        &self,
        activation_id: ActivationId,
        context: Arc<NodeEvaluationContext>,
    ) {
        trace!("Trigger {} is firing", self.name);
        self.subscription_manager.notify(activation_id, context).await;
    }
}
