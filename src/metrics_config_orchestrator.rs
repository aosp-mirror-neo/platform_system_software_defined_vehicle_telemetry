// Copyright 2023 Google LLC. All Rights Reserved.

mod publisher;
mod trigger;

use sdvtelemetry_proto_rust::metrics_configuration::MetricsConfig as MetricsConfigProto;
use std::collections::HashMap;

use publisher::Publisher;
use trigger::Trigger;

/// Interface of a service that orchestrates lifecycle of Metrics Configurations.
pub trait MetricsConfigOrchestrator {
    /// Initialize lifecycle of the specified Metrics Configuration.
    fn initialize_config(&self, config: &MetricsConfigProto);
    /// Finish lifecycle of Metrics Configuration with the specified uuid.
    fn finish_config(&self, config_uuid: &str);
}

/// Struct for default implementation of MetricsConfigOrchestrator.
pub struct MetricsConfigOrchestratorImpl {
    #[allow(dead_code)]
    configs: HashMap<String, MetricsConfig>,
}

#[allow(dead_code)]
struct MetricsConfig {
    publishers: Vec<Publisher>,
    triggers: Vec<Trigger>,
}

impl MetricsConfigOrchestratorImpl {
    pub fn new() -> MetricsConfigOrchestratorImpl {
        MetricsConfigOrchestratorImpl { configs: HashMap::new() }
    }
}

impl MetricsConfigOrchestrator for MetricsConfigOrchestratorImpl {
    #[allow(unused_variables)]
    fn initialize_config(&self, config: &MetricsConfigProto) {}

    #[allow(unused_variables)]
    fn finish_config(&self, config_uuid: &str) {}
}
