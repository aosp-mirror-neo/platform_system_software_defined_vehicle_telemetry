// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use log::trace;
use sdv_telemetry_expr::node_evaluation_context::NodeEvaluationContext;
use std::sync::{Arc, Weak};
use tokio::sync::Mutex as AsyncMutex;

use crate::metrics_config_orchestrator::async_event_processor::AsyncEventProcessor;
use crate::metrics_config_orchestrator::subscription_manager::{
    Activatable, ActivationId, SubscriptionManager, SubscriptionManagerImpl,
};
use crate::metrics_config_orchestrator::trigger::TriggerSubscriber;
use crate::metrics_config_uuid::MetricsConfigUuid;

pub(super) struct BaseTrigger {
    name: String,
    metrics_config_uuid: MetricsConfigUuid,

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
        metrics_config_uuid: MetricsConfigUuid,
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

    pub(super) fn get_metrics_config_uuid(&self) -> &MetricsConfigUuid {
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
