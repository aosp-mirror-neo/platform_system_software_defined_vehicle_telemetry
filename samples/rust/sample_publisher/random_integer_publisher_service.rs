// Copyright 2023 Google LLC. All Rights Reserved.

use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{
    ISubscriber::ISubscriber, IConfigurablePublisher::IConfigurablePublisher,
};
use google_sdv_telemetry_configurable_publisher::binder::{Interface, Strong, Result as BinderResult};

use crate::random_integer_publisher::RandomIntegerPublisher;

use std::sync::Arc;

pub struct RandomIntegerPublisherService {
    pub publisher: Arc<RandomIntegerPublisher>,
}

impl Interface for RandomIntegerPublisherService {}

impl IConfigurablePublisher for RandomIntegerPublisherService {
    fn subscribe(
        &self,
        subscriber: &Strong<dyn ISubscriber>,
        config: Option<&[u8]>,
    ) -> BinderResult<()> {
        self.publisher.subscribe(subscriber, config);
        Ok(())
    }

    fn unsubscribe(&self, config: Option<&[u8]>) -> BinderResult<()> {
        self.publisher.unsubscribe(config);
        Ok(())
    }
}
