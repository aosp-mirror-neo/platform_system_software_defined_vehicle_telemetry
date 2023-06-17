// Copyright 2023 Google LLC. All Rights Reserved.

use sdvtelemetry_proto_rust::metrics_configuration::MetricsConfig;

/// Interface of a service that orchestrates lifecycle of Metrics Configurations.
pub trait MetricsConfigOrchestrator {
    /// Initialize lifecycle of the specified Metrics Configuration.
    fn initialize_config(&self, config: MetricsConfig);
    /// Finish lifecycle of Metrics Configuration with the specified uuid.
    fn finish_config(&self, config_uuid: &str);
}

/// Struct for default implementation of MetricsConfigOrchestrator.
pub struct MetricsConfigOrchestratorImpl {}

impl MetricsConfigOrchestratorImpl {
    pub fn create() -> MetricsConfigOrchestratorImpl {
        MetricsConfigOrchestratorImpl {}
    }
}

impl MetricsConfigOrchestrator for MetricsConfigOrchestratorImpl {
    #[allow(unused_variables)]
    fn initialize_config(&self, config: MetricsConfig) {}

    #[allow(unused_variables)]
    fn finish_config(&self, config_uuid: &str) {}
}
