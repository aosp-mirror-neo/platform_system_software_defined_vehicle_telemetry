// Copyright 2023 Google LLC

use binder::BinderFeatures;
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisherSubscription::{IConfigurablePublisherSubscription, BnConfigurablePublisherSubscription};
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{ISubscriber::ISubscriber, IConfigurablePublisher::IConfigurablePublisher};

use std::sync::Arc;

use crate::random_integer_publisher::RandomIntegerPublisher;

pub struct RandomIntegerPublisherService {
    pub publisher: Arc<RandomIntegerPublisher>,
}

impl binder::Interface for RandomIntegerPublisherService {}

impl IConfigurablePublisher for RandomIntegerPublisherService {
    fn createSubscription(
        &self,
        subscriber: &binder::Strong<dyn ISubscriber>,
        config: Option<&[u8]>,
    ) -> binder::Result<binder::Strong<dyn IConfigurablePublisherSubscription>> {
        let subscription = self.publisher.create_subscription(subscriber, config);
        Ok(BnConfigurablePublisherSubscription::new_binder(subscription, BinderFeatures::default()))
    }

    fn getLatestMessage(&self, _configuration: Option<&[u8]>) -> binder::Result<Option<Vec<u8>>> {
        Err(binder::Status::new_exception(binder::ExceptionCode::UNSUPPORTED_OPERATION, None))
    }
}
