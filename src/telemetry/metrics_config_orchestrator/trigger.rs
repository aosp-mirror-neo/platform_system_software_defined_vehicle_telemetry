// Copyright 2023 Google LLC

use async_trait::async_trait;
use std::marker::{Send, Sync};
use std::sync::Arc;

use super::subscription_manager::CallbackFn;
use crate::expr::node_evaluation_context::NodeEvaluationContext;

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
#[cfg_attr(test, sdv_telemetry_utils_macros::with_super_trait(downcast_rs::DowncastSync))]
pub(crate) trait Trigger: Send + Sync {
    /// Returns the name of the trigger.
    fn get_name(&self) -> &str;

    fn get_metrics_config_uuid(&self) -> &str;

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
}

#[cfg(test)]
downcast_rs::impl_downcast!(sync Trigger);

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
