// Copyright 2023 Google LLC

use async_trait::async_trait;
use downcast_rs::DowncastSync;
use std::marker::{Send, Sync};
use std::sync::Arc;

#[cfg(test)]
use downcast_rs::impl_downcast;

use super::subscription_manager::CallbackFn;
use crate::expr::node::NodeEvaluationContext;

mod base;
pub(crate) mod conditional;
pub(crate) mod data;
pub(crate) mod periodic;

#[cfg(test)]
pub(crate) mod mock;

type TriggerCallbackFn = CallbackFn<NodeEvaluationContext>;

#[async_trait]
pub(crate) trait Trigger: Send + Sync + DowncastSync {
    /// Returns the name of the trigger.
    fn get_name(&self) -> &str;

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
impl_downcast!(sync Trigger);

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
