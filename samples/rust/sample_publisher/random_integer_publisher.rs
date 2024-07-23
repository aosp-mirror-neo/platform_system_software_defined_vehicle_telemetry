// Copyright 2023 Google LLC

mod subscription;

use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::ISubscriber::ISubscriber;

use subscription::RandomIntegerPublisherSubscription;

pub struct RandomIntegerPublisher {}

impl RandomIntegerPublisher {
    pub fn create_subscription(
        &self,
        subscriber: &binder::Strong<dyn ISubscriber>,
        _config: Option<&[u8]>,
    ) -> RandomIntegerPublisherSubscription {
        RandomIntegerPublisherSubscription::new(subscriber)
    }
}
