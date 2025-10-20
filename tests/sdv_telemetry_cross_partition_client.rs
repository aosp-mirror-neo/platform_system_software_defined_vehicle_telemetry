// Copyright 2025 Google LLC

//! Binary for testing Telemetry service cross partitions

use log::info;

use protobuf::Message;
use sdv_telemetry_proto_rust::metrics_configuration::MetricsConfig;
use sdv_telemetry_rust_wrapper::TelemetryServiceHolder;
use sdv_telemetry_sample_utils::proto_reader;
use std::path::Path;
use tokio::sync::mpsc;

const CONFIG_PATH: &str = "/product/etc/telemetry_test_config.textproto";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    binder::ProcessState::start_thread_pool();
    info!("Connecting to service");
    let service = TelemetryServiceHolder::get().unwrap();

    info!("Initializing service");
    let (report_ready_tx, mut report_ready_rx) = mpsc::unbounded_channel();
    service
        .init(
            move |status| {
                info!("Received status message {:?}: {}", status.code, status.message);
            },
            move |report_info| {
                report_ready_tx.send(report_info).unwrap();
            },
        )
        .await
        .unwrap();

    let serialized_config =
        proto_reader::read_as_bytes::<MetricsConfig>(Path::new(CONFIG_PATH)).unwrap();
    let config = MetricsConfig::parse_from_bytes(&serialized_config).unwrap();

    info!("Adding config");
    service.add_metrics_config(&serialized_config).await.unwrap();

    info!("Activating config");
    service.activate_metrics_config(&config.uuid).await.unwrap();

    info!("Waiting for metrics report ready notification");
    let report_info = report_ready_rx.recv().await.unwrap();

    info!("Reading metrics reports info");
    let ready_reports_info = service.get_ready_reports_info().await.unwrap();
    assert_eq!(ready_reports_info.len(), 1);
    assert_eq!(ready_reports_info[0].config_uuid, report_info.config_uuid);
    assert_eq!(ready_reports_info[0].report_uuid, report_info.report_uuid);

    info!("Retrieving metrics report");
    let metrics_report = service.get_metrics_report(&report_info.report_uuid).await.unwrap();
    info!("Report received: {metrics_report:?}");
    assert_eq!(metrics_report.report_uuid, report_info.report_uuid);
    assert_eq!(metrics_report.metrics_config_uuid, config.uuid);

    let ready_reports_info = service.get_ready_reports_info().await.unwrap();
    assert!(ready_reports_info.is_empty());

    info!("Removing metrics config");
    service.remove_metrics_config(&config.uuid).await.unwrap();

    info!("Finishing service");
    service.finish().unwrap();

    info!("Successfully finished service");
    Ok(())
}
