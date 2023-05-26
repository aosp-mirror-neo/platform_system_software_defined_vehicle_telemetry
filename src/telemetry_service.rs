// Copyright 2023 Google LLC. All Rights Reserved.

//! This module implements the ITelemetryService AIDL interface
#![allow(non_snake_case)]

use android_sdv_telemetry::aidl::android::sdv::telemetry::{
    ITelemetryReportListener::ITelemetryReportListener,
    ITelemetryReportReadyListener::ITelemetryReportReadyListener,
    ITelemetryService::ITelemetryServiceAsyncServer,
};

use android_sdv_telemetry::binder::{Interface, Result as BinderResult, Strong};

use sdvtelemetry_proto_rust::metrics_configuration::MetricsConfig;

use log::info;

/// This struct is defined to implement ITelemetryService AIDL interface.
pub struct SdvTelemetryService;

impl Interface for SdvTelemetryService {}

#[::async_trait::async_trait]
impl ITelemetryServiceAsyncServer for SdvTelemetryService {
    #[allow(unused_variables)]
    async fn addMetricsConfig(
        &self,
        metrics_config_name: &str,
        metrics_config: &[u8],
    ) -> BinderResult<()> {
        let config: MetricsConfig = protobuf::Message::parse_from_bytes(metrics_config).unwrap();
        info!("addMetricsConfig: {:?}\n", config);
        Ok(())
    }

    #[allow(unused_variables)]
    async fn removeMetricsConfig(&self, metrics_config_name: &str) -> BinderResult<()> {
        info!("removeMetricsConfig: {}", metrics_config_name);
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
