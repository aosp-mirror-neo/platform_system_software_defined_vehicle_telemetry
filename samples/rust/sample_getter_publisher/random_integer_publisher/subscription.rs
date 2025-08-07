// Copyright 2024 Google LLC

use log::info;
use sdv_telemetry_utils::atomic::SequenceNumberType;

use crate::random_integer_publisher::RandomIntegerPublisher;
use sdv_configurable_publisher_rust_wrapper::configurable_publisher_subscription::ConfigurablePublisherSubscription;
use std::sync::Weak;

pub struct RandomIntegerPublisherSubscription {
    publisher: Weak<RandomIntegerPublisher>,
    subscription_id: SequenceNumberType,
}

impl ConfigurablePublisherSubscription for RandomIntegerPublisherSubscription {
    fn unsubscribe(&self) {
        info!("Removing subscription {}", self.subscription_id);
        if let Some(publisher) = self.publisher.upgrade() {
            publisher.remove_subscription(self.subscription_id);
        }
    }
}

impl RandomIntegerPublisherSubscription {
    pub fn new(
        publisher: Weak<RandomIntegerPublisher>,
        subscription_id: SequenceNumberType,
    ) -> Self {
        info!("Creating subscription {subscription_id}");
        Self { publisher, subscription_id }
    }
}
