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

use super::*;

use async_trait::async_trait;
use parking_lot::Mutex;
use sdv_telemetry_expr::node_evaluation_context::NodeEvaluationContext;
use sdv_telemetry_utils::countdown_latch::{Level, MultiLevelCountdownLatch};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;

use crate::metrics_config_orchestrator::subscription_manager::{ActivationId, SubscriptionManager};
use crate::metrics_config_uuid::MetricsConfigUuid;

/// Mock implementation of Trigger trait for unit tests.
pub(crate) struct MockTrigger {
    pub expected_subscribers: Vec<TriggerSubscriber>,
    pub subscribe_callback: Option<Box<dyn Fn(&str) + Send + Sync>>,
    pub unsubscribe_callback: Option<Box<dyn Fn(&str) + Send + Sync>>,

    name: String,
    metrics_config_uuid: MetricsConfigUuid,
    on_fired_callback: AsyncMutex<Option<(TriggerSubscriber, TriggerCallbackFn)>>,
}

#[async_trait]
impl Trigger for MockTrigger {
    fn get_name(&self) -> &str {
        &self.name
    }

    fn get_metrics_config_uuid(&self) -> &MetricsConfigUuid {
        &self.metrics_config_uuid
    }

    async fn subscribe(
        self: Arc<Self>,
        subscriber: &TriggerSubscriber,
        on_fired_callback: TriggerCallbackFn,
    ) {
        self.verify_subscriber(subscriber);
        self.on_fired_callback.lock().await.replace((subscriber.clone(), on_fired_callback));

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

    async fn fire_manually(&self, context: Arc<NodeEvaluationContext>) {
        self.fire_with_context(context).await;
    }
}

impl MockTrigger {
    pub(crate) fn new(name: String, metrics_config_uuid: MetricsConfigUuid) -> Self {
        Self {
            name,
            metrics_config_uuid,
            expected_subscribers: vec![],
            subscribe_callback: None,
            unsubscribe_callback: None,
            on_fired_callback: None.into(),
        }
    }

    pub(crate) fn new_for_subscriber(
        name: String,
        metrics_config_uuid: MetricsConfigUuid,
        expected_subscriber: TriggerSubscriber,
    ) -> Self {
        let mut trigger = Self::new(name, metrics_config_uuid);
        trigger.expected_subscribers.push(expected_subscriber);
        trigger
    }

    pub(crate) async fn fire(&self) {
        self.fire_with_context(Arc::new(NodeEvaluationContext::new(None))).await;
    }

    pub(crate) async fn fire_with_context(&self, context: Arc<NodeEvaluationContext>) {
        let mut lock = self.on_fired_callback.lock().await;
        let (subscriber, on_fired_callback) = lock.as_mut().unwrap();

        let compute_order = Level(subscriber.compute_order());
        let countdown_latch =
            MultiLevelCountdownLatch::builder().with_level(compute_order, 1).build();
        on_fired_callback(context, compute_order, countdown_latch);
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
        }), "Subscriber {subscriber} wasn't expected");
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

    pub(super) fn create_callback(self: &Arc<Self>) -> CallbackFn<T> {
        let self_clone = Arc::clone(self);
        Box::new(move |data, _, _| self_clone.on_callback_fired(data))
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
    activation_id: Arc<Mutex<Option<ActivationId>>>,
    notify_count: Arc<Mutex<u32>>,
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

    async fn notify(&self, activation_id: ActivationId, arg: Arc<NodeEvaluationContext>) {
        *self.notify_count.lock() += 1;

        self.set_activation_id(activation_id).await;

        let mut callback_lock = self.callback.lock();
        if let Some(callback) = &mut *callback_lock {
            let compute_order = Level(self.key.lock().as_ref().unwrap().compute_order());
            let countdown_latch =
                MultiLevelCountdownLatch::builder().with_level(compute_order, 1).build();
            callback(arg, compute_order, countdown_latch.clone());
        }
    }

    async fn get_current_activation_id(&self) -> Option<ActivationId> {
        self.activation_id.lock().as_ref().cloned()
    }
}

impl MockSubscriptionManager {
    pub(super) fn new() -> MockSubscriptionManager {
        MockSubscriptionManager {
            key: Arc::new(Mutex::new(None)),
            callback: Arc::new(Mutex::new(None)),
            activation_id: Arc::new(Mutex::new(None)),
            notify_count: Arc::new(Mutex::new(0)),
        }
    }

    pub(super) fn get_notify_count(&self) -> u32 {
        *self.notify_count.lock()
    }

    // Sets activation id
    pub(super) async fn set_activation_id(&self, activation_id: ActivationId) {
        *self.activation_id.lock() = Some(activation_id);
    }

    /// Just like `subscribe`, but also passes the `ActivationId` of the notification to the
    /// callback.
    pub(super) async fn subscribe_for_test(
        &self,
        key: TriggerSubscriber,
        mut callback: CallbackFn<(ActivationId, Arc<NodeEvaluationContext>)>,
    ) {
        let activation_id = Arc::clone(&self.activation_id);
        self.subscribe(
            key,
            Box::new(move |arg, compute_order, countdown_latch| {
                callback((activation_id.lock().unwrap(), arg), compute_order, countdown_latch)
            }),
        )
        .await;
    }
}
