// Copyright 2023 Google LLC. All Rights Reserved.

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

use super::*;
use crate::metrics_config_orchestrator::NodeEvaluationContext;

/// Mock implementation of Trigger trait for unit tests.
pub(crate) struct MockTrigger {
    pub expected_subscribers: Vec<TriggerSubscriber>,
    pub subscribe_callback: Option<Box<dyn Fn(&str) + Send + Sync>>,
    pub unsubscribe_callback: Option<Box<dyn Fn(&str) + Send + Sync>>,

    name: String,
    on_fired_callback: AsyncMutex<Option<TriggerCallbackFn>>,
}

#[async_trait]
impl Trigger for MockTrigger {
    fn get_name(&self) -> &str {
        &self.name
    }

    async fn subscribe(
        self: Arc<Self>,
        subscriber: &TriggerSubscriber,
        on_fired_callback: TriggerCallbackFn,
    ) {
        self.verify_subscriber(subscriber);
        self.on_fired_callback.lock().await.replace(on_fired_callback);

        if let Some(callback) = &self.subscribe_callback {
            callback(&self.name);
        }
    }

    async fn unsubscribe(&self, subscriber: &TriggerSubscriber) {
        self.verify_subscriber(subscriber);

        if let Some(callback) = &self.unsubscribe_callback {
            callback(&self.name);
        }
    }
}

impl MockTrigger {
    pub(crate) fn new(name: String) -> Self {
        Self {
            name,
            expected_subscribers: vec![],
            subscribe_callback: None,
            unsubscribe_callback: None,
            on_fired_callback: None.into(),
        }
    }

    pub(crate) fn new_for_subscriber(name: String, expected_subscriber: TriggerSubscriber) -> Self {
        let mut trigger = Self::new(name);
        trigger.expected_subscribers.push(expected_subscriber);
        trigger
    }

    pub(crate) async fn fire(&self) {
        self.fire_with_context(NodeEvaluationContext::new()).await;
    }

    pub(crate) async fn fire_with_context(&self, context: NodeEvaluationContext) {
        self.on_fired_callback.lock().await.as_mut().unwrap()(context);
    }

    pub(crate) fn set_subscribe_callback(
        &mut self,
        callback: impl Fn(&str) + Send + Sync + 'static,
    ) {
        self.subscribe_callback.replace(Box::new(callback));
    }

    pub(crate) fn set_unsubscribe_callback(
        &mut self,
        callback: impl Fn(&str) + Send + Sync + 'static,
    ) {
        self.unsubscribe_callback.replace(Box::new(callback));
    }

    fn verify_subscriber(&self, subscriber: &TriggerSubscriber) {
        assert!(self.expected_subscribers.iter().any(|other| match subscriber {
            TriggerSubscriber::AggregationPublisher { name } => matches!(other, TriggerSubscriber::AggregationPublisher { name: other_name } if name == other_name),
            TriggerSubscriber::ConditionalTrigger { name } => matches!(other, TriggerSubscriber::ConditionalTrigger { name: other_name } if name == other_name),
            TriggerSubscriber::MetricsConfigStart => matches!(other, TriggerSubscriber::MetricsConfigStart),
            TriggerSubscriber::MetricsConfigEnd => matches!(other, TriggerSubscriber::MetricsConfigEnd),
            TriggerSubscriber::MetricsConfigFinish => matches!(other, TriggerSubscriber::MetricsConfigFinish),
            TriggerSubscriber::MetricsReportGenerator { name } => matches!(other, TriggerSubscriber::MetricsReportGenerator { name: other_name } if name == other_name),
        }), "Subscriber {} wasn't expected", subscriber);
    }
}
