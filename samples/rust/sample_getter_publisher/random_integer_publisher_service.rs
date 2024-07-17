// Copyright 2023 Google LLC

use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{
    ISubscriber::ISubscriber, IConfigurablePublisher::IConfigurablePublisher, IConfigurablePublisherSubscription::IConfigurablePublisherSubscription
};
use std::sync::Arc;

use crate::random_integer_publisher::RandomIntegerPublisher;

pub struct RandomIntegerPublisherService {
    publisher: Arc<RandomIntegerPublisher>,
}

impl binder::Interface for RandomIntegerPublisherService {}

impl IConfigurablePublisher for RandomIntegerPublisherService {
    #[allow(non_snake_case)]
    fn createSubscription(
        &self,
        subscriber: &binder::Strong<dyn ISubscriber>,
        config: Option<&[u8]>,
    ) -> binder::Result<binder::Strong<dyn IConfigurablePublisherSubscription>> {
        Ok(self.publisher.create_subscription(subscriber, config))
    }

    #[allow(non_snake_case)]
    fn getLatestMessage(&self, _configuration: Option<&[u8]>) -> binder::Result<Option<Vec<u8>>> {
        Ok(self.publisher.get_latest_message())
    }
}

impl RandomIntegerPublisherService {
    pub fn new(publisher: Arc<RandomIntegerPublisher>) -> Self {
        Self { publisher }
    }
}
