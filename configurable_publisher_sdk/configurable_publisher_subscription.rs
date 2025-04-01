// Copyright 2025 Google LLC
#![allow(dead_code)]

use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{
    IConfigurablePublisherSubscription::IConfigurablePublisherSubscription,
};

pub trait ConfigurablePublisherSubscription: Send + Sync {
    // Ends the underlying subscription.
    fn unsubscribe(&self);
}
pub(crate) struct ConfigurablePublisherSubscriptionAidlImpl {
    configurable_publisher_subscription: Box<dyn ConfigurablePublisherSubscription>,
}

impl ConfigurablePublisherSubscriptionAidlImpl {
    pub(crate) fn new(
        configurable_publisher_subscription: Box<dyn ConfigurablePublisherSubscription>,
    ) -> Self {
        Self { configurable_publisher_subscription }
    }
}

impl binder::Interface for ConfigurablePublisherSubscriptionAidlImpl {}

impl IConfigurablePublisherSubscription for ConfigurablePublisherSubscriptionAidlImpl {
    fn unsubscribe(&self) -> binder::Result<()> {
        self.configurable_publisher_subscription.unsubscribe();
        Ok(())
    }
}
