// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![cfg(test)]

mod deactivate_publisher;
mod fetch_last_message;
mod immediate_message_from_publisher;
mod journey_start;

use google_sdv_telemetry::aidl::google::sdv::telemetry::status::TelemetryServiceStatus::TelemetryServiceStatus;
use sdv_telemetry_platform_abstraction_layer::new_stub_pal;
use sdv_telemetry_proto_rust::metrics_configuration::MetricsReport as MetricsReportProto;
use sdv_telemetry_service_publisher_accessor::ServicePublisherAccessor;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::{
    sync::mpsc,
    time::{sleep, Duration},
};
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::{
    metrics_config_manager::MetricsConfigManager, metrics_config_uuid::MetricsConfigUuid,
    service_publisher_manager::ServicePublisherManager, MetricsConfigManagerImpl,
    MetricsConfigOrchestratorFactoryImpl, StatusReporterImpl,
};

/// Uses polling to wait for a Metrics Config with the provided UUID to become inactive. Will fail
/// if the Metrics Config is not active when this function is called.
async fn wait_for_metrics_config_inactive(
    manager: &Arc<impl MetricsConfigManager>,
    config_uuid: &MetricsConfigUuid,
) {
    assert!(manager.get_active_metrics_config_uuids().await.contains(config_uuid));

    while manager.get_active_metrics_config_uuids().await.contains(config_uuid) {
        sleep(Duration::from_millis(100)).await;
    }
}

/// Creates a `MetricsConfigManager` with the provided `ServicePublisherAccessor`s that uses no
/// mocks, except for a stub `PlatformAbstractionLayer`.
async fn create_manager(
    service_publisher_accessors: Vec<Arc<dyn ServicePublisherAccessor>>,
) -> (
    Arc<impl MetricsConfigManager>,
    UnboundedReceiverStream<MetricsReportProto>,
    UnboundedReceiverStream<TelemetryServiceStatus>,
    (TempDir, TempDir),
) {
    const CONFIG_STORE_SIZE: usize = 10000;

    let active_configs_dir = TempDir::new().unwrap();
    let inactive_configs_dir = TempDir::new().unwrap();

    let service_publisher_manager =
        Arc::new(ServicePublisherManager::new(service_publisher_accessors));

    let pal = new_stub_pal();

    let (metrics_report_tx, metrics_report_rx) = mpsc::unbounded_channel();
    let report_ready_callback = move |metrics_report| {
        eprintln!("Report: {metrics_report:?}");
        metrics_report_tx.send(metrics_report).unwrap();
    };

    let (status_tx, status_rx) = mpsc::unbounded_channel();
    let status_reporter = Arc::new(StatusReporterImpl::new(move |status| {
        eprintln!("Status: {status:?}");
        status_tx.send(status).unwrap();
    }));

    let manager = MetricsConfigManagerImpl::new(
        active_configs_dir.path().to_owned(),
        inactive_configs_dir.path().to_owned(),
        CONFIG_STORE_SIZE,
        service_publisher_manager,
        pal,
        report_ready_callback,
        status_reporter,
        Box::new(MetricsConfigOrchestratorFactoryImpl),
        Arc::new(sdv_dump::DumpManager::new("Component Tests Dump Manager")),
    )
    .await
    .unwrap();

    (
        manager,
        UnboundedReceiverStream::new(metrics_report_rx),
        UnboundedReceiverStream::new(status_rx),
        (active_configs_dir, inactive_configs_dir),
    )
}
