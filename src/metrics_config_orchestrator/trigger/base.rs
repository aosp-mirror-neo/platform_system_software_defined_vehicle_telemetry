// Copyright 2024 Google LLC

use log::trace;
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use tokio::sync::Mutex as AsyncMutex;

use crate::expr::node_evaluation_context::NodeEvaluationContext;
use crate::metrics_config_orchestrator::subscription_manager::{
    Activatable, SubscriptionManager, SubscriptionManagerImpl,
};
use crate::metrics_config_orchestrator::trigger::{TriggerCallbackFn, TriggerSubscriber};

pub(super) struct BaseTrigger {
    name: String,

    // Manages trigger subscriptions. The first subscription activates the trigger, passing a
    // reference to the callback map. The last unsubscription deactivates the trigger.
    pub(super) subscription_manager:
        Box<dyn SubscriptionManager<TriggerSubscriber, Arc<NodeEvaluationContext>>>,
}

impl BaseTrigger {
    pub(super) fn new(
        name: String,
        trigger: Weak<dyn Activatable<TriggerSubscriber, Arc<NodeEvaluationContext>>>,
    ) -> Self {
        BaseTrigger { name, subscription_manager: Box::new(SubscriptionManagerImpl::new(trigger)) }
    }

    pub(super) fn get_name(&self) -> &str {
        &self.name
    }

    /// Fires the trigger by invoking provided callback.
    /// The operation is performed after asynchronously awaiting on the mutex.
    pub(super) async fn fire_trigger(
        trigger_name: &str,
        callback_map: &Arc<AsyncMutex<HashMap<TriggerSubscriber, TriggerCallbackFn>>>,
        context: &Arc<NodeEvaluationContext>,
    ) {
        // Retrieve callbacks.
        let mut callbacks = callback_map.lock().await;

        // Verify that the map hasn't been cleared.
        if callbacks.is_empty() {
            return;
        }

        trace!("Trigger {} is firing", trigger_name);

        // Synchronously invoke callbacks.
        // The loop needs to wait before they finish to release the mutex lock when they're
        // done because they're allowed to mutate themselves.
        for callback in callbacks.values_mut() {
            callback(Arc::clone(context));
        }
    }
}
