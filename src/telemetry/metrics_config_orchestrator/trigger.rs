/*
 * Copyright (C) 2026 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use async_trait::async_trait;
use sdv_dump::DumpStateReporter;
use sdv_telemetry_expr::node_evaluation_context::NodeEvaluationContext;
use std::marker::{Send, Sync};
use std::sync::Arc;

use super::subscription_manager::{CallbackFn, ComputeOrder};
use crate::metrics_config_uuid::MetricsConfigUuid;

mod base;
mod condition_checker;
pub(crate) mod conditional;
pub(crate) mod data;
pub(crate) mod periodic;

#[cfg(test)]
pub(crate) mod mock;

// This cannot be CallbackFn<&Arc<NodeEvaluationContext>> due to a known issue in async_trait:
// https://github.com/dtolnay/async-trait/issues/8.
type TriggerCallbackFn = CallbackFn<Arc<NodeEvaluationContext>>;

#[async_trait]
pub(crate) trait Trigger: DumpStateReporter + Send + Sync {
    /// Returns the name of the trigger.
    fn get_name(&self) -> &str;

    fn get_metrics_config_uuid(&self) -> &MetricsConfigUuid;

    /// Subscribe to trigger fire events.
    ///
    /// First subscription activates the trigger.
    /// The callback mustn't block because it's executed on the trigger thread.
    async fn subscribe(
        self: Arc<Self>,
        subscriber: &TriggerSubscriber,
        on_fired_callback: TriggerCallbackFn,
    );

    /// Unsubscribe from trigger fire events.
    ///
    /// Last unsubscription deactivates the trigger.
    /// After asynchronous function is complete, the callback is guaranteed not to be called.
    async fn unsubscribe(&self, subscriber: &TriggerSubscriber);

    // Fire the trigger out of order. Does nothing if there are currently no subscribers.
    async fn fire_manually(&self, context: Arc<NodeEvaluationContext>);
}

#[derive(Clone, Eq, Hash, PartialEq, strum_macros::Display)]
#[strum(serialize_all = "title_case")]
pub(crate) enum TriggerSubscriber {
    #[strum(to_string = "Aggregation Publisher {name}")]
    AggregationPublisher {
        name: String,
    },
    #[strum(to_string = "Conditional Trigger {name}")]
    ConditionalTrigger {
        name: String,
    },
    MetricsConfigStart,
    MetricsConfigEnd,
    MetricsConfigFinish,
    #[strum(to_string = "Metrics Report Generator {name}")]
    MetricsReportGenerator {
        name: String,
    },
}

impl ComputeOrder for TriggerSubscriber {
    fn compute_order(&self) -> u32 {
        match self {
            Self::MetricsConfigStart => 0,
            Self::AggregationPublisher { .. } => 1,
            Self::ConditionalTrigger { .. } => 2,
            Self::MetricsReportGenerator { .. } => 2,
            Self::MetricsConfigEnd => 3,
            Self::MetricsConfigFinish => 3,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::metrics_config_orchestrator::{
        subscription_manager::ComputeOrder, TriggerSubscriber,
    };

    #[test]
    fn ensure_non_zero_compute_orders() {
        assert_ne!(TriggerSubscriber::MetricsConfigEnd.compute_order(), 0);
        assert_ne!(TriggerSubscriber::MetricsConfigFinish.compute_order(), 0);
        assert_ne!(
            TriggerSubscriber::MetricsReportGenerator { name: "".to_owned() }.compute_order(),
            0
        );
    }
}
