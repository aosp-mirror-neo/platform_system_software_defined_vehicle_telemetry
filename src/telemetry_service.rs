// Copyright 2023 Google LLC. All Rights Reserved.

//! This module implements the ITelemetryService AIDL interface
#![allow(non_snake_case)]

mod collection_extension;
mod expression_evaluation;
mod metrics_config_orchestrator;

use android_sdv_telemetry::aidl::android::sdv::telemetry::{
    ITelemetryReportListener::ITelemetryReportListener,
    ITelemetryReportReadyListener::ITelemetryReportReadyListener,
    ITelemetryService::ITelemetryServiceAsyncServer,
};

use android_sdv_telemetry::binder::{Interface, Result as BinderResult, Strong};
use async_trait::async_trait;
use log::info;
use sdvtelemetry_proto_rust::metrics_configuration::MetricsConfig;
use std::marker::{Send, Sync};

use metrics_config_orchestrator::MetricsConfigOrchestratorImpl;

pub use metrics_config_orchestrator::MetricsConfigOrchestrator;

/// This struct is defined to implement ITelemetryService AIDL interface.
pub struct SdvTelemetryService {
    /// Instance of Metrics Configuration Orchestrator.
    pub metrics_config_orchestrator: Box<dyn MetricsConfigOrchestrator + Send + Sync>,
}

impl Interface for SdvTelemetryService {}

#[async_trait]
impl ITelemetryServiceAsyncServer for SdvTelemetryService {
    async fn addMetricsConfig(&self, metrics_config: &[u8]) -> BinderResult<()> {
        let config: MetricsConfig = protobuf::Message::parse_from_bytes(metrics_config).unwrap();
        info!("addMetricsConfig: {:?}\n", config);
        let _ = self.metrics_config_orchestrator.initialize_config(&config);
        Ok(())
    }

    async fn removeMetricsConfig(&self, metrics_config_uuid: &str) -> BinderResult<()> {
        info!("removeMetricsConfig: {}", metrics_config_uuid);
        self.metrics_config_orchestrator.finish_config(metrics_config_uuid);
        Ok(())
    }

    #[allow(unused_variables)]
    async fn removeAllMetricsConfigs(&self) -> BinderResult<()> {
        info!("removeAllMetricsConfigs");
        Ok(())
    }

    #[allow(unused_variables)]
    async fn getFinishedReport(
        &self,
        metrics_config_name: &str,
        listener: &Strong<(dyn ITelemetryReportListener)>,
    ) -> BinderResult<()> {
        info!("getFinishedReport: {}", metrics_config_name);
        Ok(())
    }

    async fn getAllFinishedReports(
        &self,
        _listener: &Strong<dyn ITelemetryReportListener>,
    ) -> BinderResult<()> {
        info!("getAllFinishedReports");
        Ok(())
    }

    async fn setReportReadyListener(
        &self,
        listener: &Strong<dyn ITelemetryReportReadyListener>,
    ) -> BinderResult<()> {
        info!("setReportReadyListener");
        let _err = listener.onReady("Received CB from Telemetry Binder service");
        Ok(())
    }

    async fn clearReportReadyListener(&self) -> BinderResult<()> {
        info!("clearReportReadyListener");
        Ok(())
    }
}

/// Returns an instance of SdvTelemetryService with default dependencies.
pub fn create_telemetry_service() -> SdvTelemetryService {
    SdvTelemetryService {
        metrics_config_orchestrator: Box::new(MetricsConfigOrchestratorImpl::new()),
    }
}

#[cfg(test)]
mod tests {

    extern crate protobuf;
    use super::*;
    use binder::ParcelFileDescriptor;
    use protobuf::Message;
    use sdvtelemetry_proto_rust::metrics_configuration::MetricsConfig;
    use tokio::runtime::Runtime;

    /// This struct is defined to implement ITelemetryReportListener AIDL interface for unit testing purposes
    pub struct TelemetryReportListener;

    impl Interface for TelemetryReportListener {}

    impl ITelemetryReportListener for TelemetryReportListener {
        #[allow(unused_variables)]
        fn onResult(
            &self,
            metrics_config_name: &str,
            parcel: &ParcelFileDescriptor,
            err: &[u8],
            status: i32,
        ) -> BinderResult<()> {
            info!("onResult");
            Ok(())
        }
    }

    /// This struct is defined to implement ITelemetryReportReadyListener AIDL interface for unit testing purposes
    pub struct TelemetryReportReadyListener;

    impl Interface for TelemetryReportReadyListener {}

    impl ITelemetryReportReadyListener for TelemetryReportReadyListener {
        #[allow(unused_variables)]
        fn onReady(&self, metrics_config_name: &str) -> BinderResult<()> {
            info!("onResult");
            Ok(())
        }
    }

    #[test]
    fn add_metrics_config() {
        // create new Runtime
        let rt = Runtime::new().unwrap();

        // behind a reference
        let service: Box<dyn ITelemetryServiceAsyncServer> =
            Box::new(sdv_telemetry::create_telemetry_service());

        // block on async block
        rt.block_on(async {
            let mut config = MetricsConfig::new();
            config.uuid = "01bf0754-df95-46bb-bb35-522b6af66600".to_string();
            config.version = 2;
            config.script = "Sample Script".to_string();

            info!("Serializing metric config");
            let serialized_config: &[u8] = &config.write_to_bytes().unwrap();

            let result = service.addMetricsConfig(serialized_config).await;

            assert!(result.is_ok());
        });
    }

    #[test]
    fn remove_metrics_config() {
        // create new Runtime
        let rt = Runtime::new().unwrap();

        // behind a reference
        let service: Box<dyn ITelemetryServiceAsyncServer> =
            Box::new(sdv_telemetry::create_telemetry_service());

        // block on async block
        rt.block_on(async {
            let result = service.removeMetricsConfig("TestConfig").await;
            assert!(result.is_ok());
        });
    }

    #[test]
    fn remove_all_metrics_configs() {
        // create new Runtime
        let rt = Runtime::new().unwrap();

        // behind a reference
        let service: Box<dyn ITelemetryServiceAsyncServer> =
            Box::new(sdv_telemetry::create_telemetry_service());

        // block on async block
        rt.block_on(async {
            let result = service.removeAllMetricsConfigs().await;
            assert!(result.is_ok());
        });
    }

    #[test]
    fn clear_report_ready_listener() {
        // create new Runtime
        let rt = Runtime::new().unwrap();

        // behind a reference
        let service: Box<dyn ITelemetryServiceAsyncServer> =
            Box::new(sdv_telemetry::create_telemetry_service());

        // block on async block
        rt.block_on(async {
            let result = service.clearReportReadyListener().await;
            assert!(result.is_ok());
        });
    }

    #[test]
    fn set_report_ready_listener() {
        // create new Runtime
        let rt = Runtime::new().unwrap();

        // behind a reference
        let service: Box<dyn ITelemetryServiceAsyncServer> =
            Box::new(sdv_telemetry::create_telemetry_service());

        let listener: Strong<dyn ITelemetryReportReadyListener> =
            Strong::new(Box::new(TelemetryReportReadyListener));

        // block on async block
        rt.block_on(async {
            let result = service.setReportReadyListener(&listener).await;
            assert!(result.is_ok());
        });
    }

    #[test]
    fn get_finished_report() {
        // create new Runtime
        let rt = Runtime::new().unwrap();

        // behind a reference
        let service: Box<dyn ITelemetryServiceAsyncServer> =
            Box::new(sdv_telemetry::create_telemetry_service());

        let listener: Strong<dyn ITelemetryReportListener> =
            Strong::new(Box::new(TelemetryReportListener));

        // block on async block
        rt.block_on(async {
            let result = service.getFinishedReport("TestConfig", &listener).await;
            assert!(result.is_ok());
        });
    }

    #[test]
    fn get_all_finished_report() {
        // create new Runtime
        let rt = Runtime::new().unwrap();

        // behind a reference
        let service: Box<dyn ITelemetryServiceAsyncServer> =
            Box::new(sdv_telemetry::create_telemetry_service());

        let listener: Strong<dyn ITelemetryReportListener> =
            Strong::new(Box::new(TelemetryReportListener));

        // block on async block
        rt.block_on(async {
            let result = service.getAllFinishedReports(&listener).await;
            assert!(result.is_ok());
        });
    }
}
