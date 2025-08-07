// Copyright 2025 Google LLC

use log::{error, info};
use sdv_telemetry_platform_abstraction_layer::PlatformAbstractionLayer;
use sdv_telemetry_proto_rust::metrics_configuration::MetricsReport;
use sdv_telemetry_service_publisher_accessor::ServicePublisherAccessor;
use sdv_telemetry_utils::countdown_latch::Level;
use std::marker::{Send, Sync};
use std::sync::{Arc, Weak};

use crate::metrics_config_manager::{MetricsConfigManager, MetricsConfigManagerError};
use crate::metrics_config_orchestrator::{
    MetricsConfigOrchestrator, MetricsConfigOrchestratorImpl,
};
use crate::status_reporter::StatusReporter;

pub(crate) trait MetricsConfigOrchestratorFactory: Send + Sync {
    fn create(
        &self,
        manager_reference: Weak<dyn MetricsConfigManager + Send + Sync>,
        service_publisher_manager: Arc<dyn ServicePublisherAccessor>,
        pal: Arc<dyn PlatformAbstractionLayer>,
        report_ready_callback: Box<dyn Fn(MetricsReport) + Send + Sync + 'static>,
        status_reporter: Arc<dyn StatusReporter>,
    ) -> Arc<dyn MetricsConfigOrchestrator + Send + Sync>;
}

pub(crate) struct MetricsConfigOrchestratorFactoryImpl;

impl MetricsConfigOrchestratorFactory for MetricsConfigOrchestratorFactoryImpl {
    fn create(
        &self,
        manager_reference: Weak<dyn MetricsConfigManager + Send + Sync>,
        service_publisher_manager: Arc<dyn ServicePublisherAccessor>,
        pal: Arc<dyn PlatformAbstractionLayer>,
        report_ready_callback: Box<dyn Fn(MetricsReport) + Send + Sync + 'static>,
        status_reporter: Arc<dyn StatusReporter>,
    ) -> Arc<dyn MetricsConfigOrchestrator + Send + Sync> {
        let manager_reference_clone = Weak::clone(&manager_reference);
        Arc::new(MetricsConfigOrchestratorImpl::new(
            service_publisher_manager,
            pal,
            move |config_uuid, _context, Level(compute_order), countdown_latch| {
                let Some(manager) = manager_reference_clone.upgrade() else {
                    return;
                };
                tokio::spawn(async move {
                    // Wait until aggregation publishers which depend on finish_trigger
                    // finish their aggregations
                    countdown_latch.wait(Level(compute_order - 1)).await;

                    manager.deactivate_metrics_config(&config_uuid).await.unwrap_or_else(|e| {
                        if let MetricsConfigManagerError::InvalidConfigUuid { .. } = e {
                            // In theory it's possible that by the time the task is executed
                            // the OEM service manually finishes the config
                            info!("Unable to deactivate config. Invalid Config UUID: {e}")
                        } else {
                            error!("Unable to deactivate config. Reported error: {e}")
                        }
                    });
                });
            },
            report_ready_callback,
            status_reporter,
        ))
    }
}
