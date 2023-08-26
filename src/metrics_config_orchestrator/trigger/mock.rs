// Copyright 2023 Google LLC. All Rights Reserved.

use std::sync::{Arc, Mutex};

use super::{Trigger, TriggerSubscriber};

pub(crate) struct MockTrigger {
    pub expected_subscribers: Vec<TriggerSubscriber>,
    pub subscribe_callback: Option<Box<dyn Fn(&str) -> Option<String> + Send + Sync>>,
    pub unsubscribe_callback: Option<Box<dyn Fn(&str) -> Option<String> + Send + Sync>>,

    name: String,
    on_fired_callback: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

/// Mock implementation of Trigger trait for unit tests.
impl MockTrigger {
    /// Returns a new instance of MockTrigger
    pub(crate) fn new(name: String) -> Self {
        MockTrigger {
            name,
            expected_subscribers: vec![],
            subscribe_callback: None,
            unsubscribe_callback: None,
            on_fired_callback: None.into(),
        }
    }

    pub(crate) fn new_for_subscriber(name: String, expected_subscriber: TriggerSubscriber) -> Self {
        let mut trigger = MockTrigger::new(name);
        trigger.expected_subscribers.push(expected_subscriber);
        trigger
    }

    /// Fires the trigger.
    pub(crate) fn fire(&self) {
        let callback = { self.on_fired_callback.lock().unwrap().as_ref().unwrap().clone() };
        callback();
    }

    pub(crate) fn set_subscribe_callback(
        &mut self,
        callback: impl Fn(&str) -> Option<String> + Send + Sync + 'static,
    ) {
        self.subscribe_callback.replace(Box::new(callback));
    }

    pub(crate) fn set_unsubscribe_callback(
        &mut self,
        callback: impl Fn(&str) -> Option<String> + Send + Sync + 'static,
    ) {
        self.unsubscribe_callback.replace(Box::new(callback));
    }

    fn verify_subscriber(&self, subscriber: &TriggerSubscriber) {
        assert!(self.expected_subscribers.iter().any(|other| match subscriber {
            TriggerSubscriber::AggregationPublisher { name } => matches!(other, TriggerSubscriber::AggregationPublisher { name: other_name } if name == other_name),
            TriggerSubscriber::MetricsConfigStart => matches!(other, TriggerSubscriber::MetricsConfigStart),
            TriggerSubscriber::MetricsConfigEnd => matches!(other, TriggerSubscriber::MetricsConfigEnd),
            TriggerSubscriber::MetricsConfigFinish => matches!(other, TriggerSubscriber::MetricsConfigFinish),
            TriggerSubscriber::MetricsReportGenerator { name } => matches!(other, TriggerSubscriber::MetricsReportGenerator { name: other_name } if name == other_name),
            TriggerSubscriber::PeriodicTrigger { name } => matches!(other, TriggerSubscriber::PeriodicTrigger { name: other_name } if name == other_name),
        }), "Subscriber {} wasn't expected", subscriber);
    }
}

impl Trigger for MockTrigger {
    fn get_name(&self) -> &str {
        &self.name
    }

    fn subscribe(
        &self,
        subscriber: &TriggerSubscriber,
        on_fired_callback: Box<dyn Fn() + Send + Sync>,
    ) -> Result<(), String> {
        self.verify_subscriber(subscriber);
        self.on_fired_callback.lock().unwrap().replace(on_fired_callback.into());

        if let Some(callback) = &self.subscribe_callback {
            if let Some(error) = callback(&self.name) {
                return Err(error);
            }
        }

        Ok(())
    }

    fn unsubscribe(&self, subscriber: &TriggerSubscriber) -> Result<(), String> {
        self.verify_subscriber(subscriber);
        *self.on_fired_callback.lock().unwrap() = None;

        if let Some(callback) = &self.unsubscribe_callback {
            if let Some(error) = callback(&self.name) {
                return Err(error);
            }
        }

        Ok(())
    }
}
