// Copyright 2024 Google LLC. All Rights Reserved.

use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{ISubscriber::ISubscriber, IConfigurablePublisherSubscription::IConfigurablePublisherSubscription};
use google_sdv_telemetry_configurable_publisher::binder::{Interface, Strong, Result as BinderResult};

use std::sync::{Mutex, Weak};

pub struct RandomIntegerPublisherSubscription {
    // TODO(b/329691740): Support multiple subscriptions per publisher.
    subscriber: Weak<Mutex<Option<Strong<dyn ISubscriber>>>>,
}

impl Interface for RandomIntegerPublisherSubscription {}

impl IConfigurablePublisherSubscription for RandomIntegerPublisherSubscription {
    fn unsubscribe(&self) -> BinderResult<()> {
        if let Some(data_reference) = self.subscriber.upgrade() {
            let mut guard = data_reference.lock().unwrap();
            *guard = None;
        }
        Ok(())
    }
}

impl RandomIntegerPublisherSubscription {
    pub fn new(subscriber: Weak<Mutex<Option<Strong<dyn ISubscriber>>>>) -> Self {
        Self { subscriber }
    }
}
