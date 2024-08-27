// Copyright 2024 Google LLC

use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisherSubscription::IConfigurablePublisherSubscription;
use log::info;
use sdv_telemetry_utils::atomic::SequenceNumberType;
use std::sync::Weak;

use crate::random_integer_publisher::RandomIntegerPublisher;

pub struct RandomIntegerPublisherSubscription {
    publisher: Weak<RandomIntegerPublisher>,
    subscription_id: SequenceNumberType,
}

impl binder::Interface for RandomIntegerPublisherSubscription {}

impl IConfigurablePublisherSubscription for RandomIntegerPublisherSubscription {
    fn unsubscribe(&self) -> binder::Result<()> {
        info!("Removing subscription {}", self.subscription_id);
        if let Some(publisher) = self.publisher.upgrade() {
            publisher.remove_subscription(self.subscription_id);
        }
        Ok(())
    }
}

impl RandomIntegerPublisherSubscription {
    pub fn new(
        publisher: Weak<RandomIntegerPublisher>,
        subscription_id: SequenceNumberType,
    ) -> Self {
        info!("Creating subscription {}", subscription_id);
        Self { publisher, subscription_id }
    }
}
