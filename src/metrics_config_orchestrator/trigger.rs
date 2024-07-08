// Copyright 2023 Google LLC

use async_trait::async_trait;
use downcast_rs::DowncastSync;
use std::fmt;
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

#[derive(Clone, Eq, Hash, PartialEq)]
pub(crate) enum TriggerSubscriber {
    AggregationPublisher { name: String },
    ConditionalTrigger { name: String },
    MetricsConfigStart,
    MetricsConfigEnd,
    MetricsConfigFinish,
    MetricsReportGenerator { name: String },
}

impl fmt::Display for TriggerSubscriber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TriggerSubscriber::AggregationPublisher { name } => {
                write!(f, "Aggregation Publisher {}", name)
            }
            TriggerSubscriber::ConditionalTrigger { name } => {
                write!(f, "Periodic Trigger {}", name)
            }
            TriggerSubscriber::MetricsConfigStart => write!(f, "Metrics Config Start"),
            TriggerSubscriber::MetricsConfigEnd => write!(f, "Metrics Config End"),
            TriggerSubscriber::MetricsConfigFinish => write!(f, "Metrics Config Finish"),
            TriggerSubscriber::MetricsReportGenerator { name } => {
                write!(f, "Metrics Report Generator {}", name)
            }
        }
    }
}
