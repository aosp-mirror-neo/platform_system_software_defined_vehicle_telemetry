// Copyright 2024 Google LLC

use binder::{Interface, Result as BinderResult};
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisherSubscription::IConfigurablePublisherSubscription;

pub struct SimulationPublisherSubscription {
    on_unsubscribe: Box<dyn Fn() + Send + Sync + 'static>,
}

impl Interface for SimulationPublisherSubscription {}

impl SimulationPublisherSubscription {
    pub fn new(on_unsubscribe: impl Fn() + Send + Sync + 'static) -> Self {
        Self { on_unsubscribe: Box::new(on_unsubscribe) }
    }
}

impl IConfigurablePublisherSubscription for SimulationPublisherSubscription {
    fn unsubscribe(&self) -> BinderResult<()> {
        (self.on_unsubscribe)();
        Ok(())
    }
}
