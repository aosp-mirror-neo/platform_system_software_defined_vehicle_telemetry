// Copyright 2024 Google LLC

use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{ISubscriber::ISubscriber, IConfigurablePublisher::IConfigurablePublisher, IConfigurablePublisherSubscription::{IConfigurablePublisherSubscription, BnConfigurablePublisherSubscription}};
use binder::{BinderFeatures, ExceptionCode, Interface, Result as BinderResult, Status, Strong};
use std::{sync::Arc, hash::Hash};

use crate::{
    publisher_config::PublisherConfig,
    simulation_publisher_subscription::SimulationPublisherSubscription,
};

pub struct SimulationPublisherWrapper {
    pub publisher: Arc<SimulationPublisher>,
}

impl Interface for SimulationPublisherWrapper {}

impl IConfigurablePublisher for SimulationPublisherWrapper {
    fn createSubscription(
        &self,
        subscriber: &Strong<dyn ISubscriber>,
        config: Option<&[u8]>,
    ) -> BinderResult<Strong<dyn IConfigurablePublisherSubscription>> {
        self.publisher.create_subscription(subscriber, config)
    }

    fn getLatestMessage(&self, _configuration: Option<&[u8]>) -> BinderResult<Option<Vec<u8>>> {
        Err(Status::new_exception(ExceptionCode::UNSUPPORTED_OPERATION, None))
    }
}

#[derive(Hash, PartialEq, Eq)]
pub struct SimulationPublisher {
    pub config: PublisherConfig,
}

impl SimulationPublisher {
    pub fn new(config: PublisherConfig) -> Self {
        Self { config }
    }

    fn create_subscription(
        &self,
        subscriber: &Strong<dyn ISubscriber>,
        _config: Option<&[u8]>,
    ) -> BinderResult<Strong<dyn IConfigurablePublisherSubscription>> {
        let mut subscription = SimulationPublisherSubscription::new(subscriber.clone());
        subscription.publish_data(self.config.parsed_data.clone(), self.config.loop_data);
        Ok(BnConfigurablePublisherSubscription::new_binder(subscription, BinderFeatures::default()))
    }
}
