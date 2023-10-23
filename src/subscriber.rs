// Copyright 2023 Google LLC. All Rights Reserved.

#![allow(missing_docs)]
use android_sdv_telemetry_configurable_publisher::aidl::android::sdv::telemetry::configurable_publisher::ISubscriber::ISubscriber;
use android_sdv_telemetry_configurable_publisher::binder::{Interface, Result as BinderResult};

/// Interface used to send data from a registered publisher to its subscriber
pub trait Subscriber: Send + Sync {
    /// sends data to instance of Subscriber
    fn send_data(&self, data: &[u8]);
}

/// Wrapper from Subscriber and implements ISubscriber interface
pub struct SubscriberWrapper {
    listener: Box<dyn Subscriber>,
}

impl Interface for SubscriberWrapper {}

impl ISubscriber for SubscriberWrapper {
    fn sendData(&self, data: &[u8]) -> BinderResult<()> {
        self.listener.send_data(data);
        Ok(())
    }
}

impl SubscriberWrapper {
    pub fn new(subscriber: Box<dyn Subscriber>) -> Self {
        SubscriberWrapper { listener: subscriber }
    }
}
