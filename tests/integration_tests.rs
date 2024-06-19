// Copyright 2023 Google LLC

#![cfg(test)]

/// Telemetry Service device integration tests
use anyhow::anyhow;
use log::info;
use protobuf::{text_format::parse_from_str, Message};
use sdv_telemetry_rust_wrapper::TelemetryServiceHolder;
use sdv_telemetry_sample_utils::metrics_config_reader_utils;
use sdv_telemetry_utils::logging::{init_logger_for_samples_or_tests, validated_log_tag};
use sdvtelemetry_proto_rust::metrics_configuration::MetricsConfig;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;

const CONFIG_PATH: &str = "tests/config.textproto";
const LOG_TAG: &str = validated_log_tag("TelemetryIntegrTests");

#[tokio::test]
async fn telemetry_service_simple_config_end_to_end_test() {
    init_logger_for_samples_or_tests(LOG_TAG);
    binder::ProcessState::start_thread_pool();

    info!("Connecting to service");
    let service = TelemetryServiceHolder::get().unwrap();

    info!("Initializing service");
    let (report_ready_tx, mut report_ready_rx) = mpsc::unbounded_channel();
    service
        .init(
            move |status_code, status_msg| {
                info!("Received status message {status_code}: {status_msg}");
            },
            move |config_uuid, report_name, report_number| {
                report_ready_tx.send((config_uuid, report_name, report_number)).unwrap();
            },
        )
        .await
        .unwrap();

    let serialized_config = metrics_config_reader_utils::read(&PathBuf::from(CONFIG_PATH)).unwrap();
    let config = MetricsConfig::parse_from_bytes(&serialized_config).unwrap();

    info!("Adding config");
    service.add_metrics_config(&serialized_config).await.unwrap();

    info!("Activating config");
    service.activate_metrics_config(&config.uuid).await.unwrap();

    info!("Waiting for metrics report ready notification");
    let (config_uuid, report_name, report_number) = report_ready_rx.recv().await.unwrap();
    assert_eq!(config_uuid, config.uuid);

    info!("Retrieving metrics report");
    let metrics_report =
        service.get_metrics_report(config_uuid.clone(), report_name, report_number).await.unwrap();
    info!("Report received: {metrics_report:?}");
    assert_eq!(config_uuid, metrics_report.metrics_config_uuid);

    info!("Removing metrics config");
    service.remove_metrics_config(&config.uuid).await.unwrap();

    info!("Finishing service");
    service.finish().unwrap();

    info!("Successfully finished service");
}

fn read_files_recursively(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut result = vec![];
    if dir.is_dir() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                result.append(&mut read_files_recursively(&path)?);
            } else {
                result.push(entry.path());
            }
        }
    }
    Ok(result)
}

#[test]
fn check_textprotos_are_valid() -> anyhow::Result<()> {
    let paths = read_files_recursively(Path::new("."))?
        .into_iter()
        .filter(|path| path.extension() == Some(OsStr::new("textproto")))
        .collect::<Vec<_>>();

    let mut expected_paths = paths
        .iter()
        .map(|path| path.to_str().ok_or(anyhow!("Invalid path")))
        .collect::<anyhow::Result<Vec<_>>>()?;
    expected_paths.sort();

    // This assert is here to verify that the test is actually doing something.
    // Add new `.textproto` files to the assert below if it fails.
    assert_eq!(
        expected_paths,
        vec![
            "./samples/sample_protos/configurable_publisher.textproto",
            "./samples/sample_protos/data_tunnel_publisher.textproto",
            "./samples/sample_protos/data_tunnel_publisher_aggregation.textproto",
            "./samples/sample_protos/metric_configJan10.textproto",
            "./tests/config.textproto"
        ]
    );

    for path in paths {
        eprintln!("Attempting to parse: {}", path.display());
        let textproto = fs::read_to_string(path)?;
        let _proto = parse_from_str::<MetricsConfig>(&textproto)?;
    }

    Ok(())
}
