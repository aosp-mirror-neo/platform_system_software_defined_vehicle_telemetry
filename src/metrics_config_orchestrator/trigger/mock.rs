// Copyright 2023 Google LLC

use super::*;

use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;

use crate::expr::node_evaluation_context::NodeEvaluationContext;
use crate::metrics_config_orchestrator::subscription_manager::SubscriptionManager;

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
        self.fire_with_context(Arc::new(NodeEvaluationContext::new())).await;
    }

    pub(crate) async fn fire_with_context(&self, context: Arc<NodeEvaluationContext>) {
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

pub(super) struct MockCallbackListener<T: Send + 'static> {
    callback_tx: mpsc::UnboundedSender<T>,
    callback_rx: AsyncMutex<mpsc::UnboundedReceiver<T>>,
    fired_count: AtomicU32,
}

impl<T: Send + 'static> MockCallbackListener<T> {
    pub(super) fn new() -> Self {
        let (callback_tx, callback_rx) = mpsc::unbounded_channel();
        MockCallbackListener { callback_tx, callback_rx: callback_rx.into(), fired_count: 0.into() }
    }

    pub(super) fn get_fired_count(&self) -> u32 {
        self.fired_count.load(Ordering::Acquire)
    }

    // Creates a map with a callback that is handled by this listener instance.
    pub(super) fn create_callback_map(
        self: &Arc<Self>,
    ) -> Arc<AsyncMutex<HashMap<TriggerSubscriber, CallbackFn<T>>>> {
        let self_clone = Arc::clone(self);
        let callback: CallbackFn<T> = Box::new(move |data| self_clone.on_callback_fired(data));
        Arc::new(AsyncMutex::new(HashMap::from([(
            TriggerSubscriber::MetricsConfigStart,
            callback,
        )])))
    }

    /// Increases fired count and triggers the notify.
    pub(super) fn on_callback_fired(&self, data: T) {
        self.fired_count.fetch_add(1, Ordering::AcqRel);
        self.callback_tx.send(data).unwrap();
    }

    pub(super) async fn wait_for_callback_fired(&self) -> T {
        self.callback_rx.lock().await.recv().await.unwrap()
    }
}

#[derive(Clone)]
pub(super) struct MockSubscriptionManager {
    pub(super) key: Arc<Mutex<Option<TriggerSubscriber>>>,
    pub(super) callback: Arc<Mutex<Option<TriggerCallbackFn>>>,
}

#[async_trait]
impl SubscriptionManager<TriggerSubscriber, Arc<NodeEvaluationContext>>
    for MockSubscriptionManager
{
    async fn subscribe(&self, key: TriggerSubscriber, callback: TriggerCallbackFn) {
        self.key.lock().replace(key);
        self.callback.lock().replace(callback);
    }

    async fn unsubscribe(&self, key: &TriggerSubscriber) {
        self.key.lock().replace(key.to_owned());
    }
}

impl MockSubscriptionManager {
    pub(super) fn new() -> MockSubscriptionManager {
        MockSubscriptionManager {
            key: Arc::new(Mutex::new(None)),
            callback: Arc::new(Mutex::new(None)),
        }
    }
}
