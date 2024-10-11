// Copyright 2023 Google LLC

#![cfg(test)]

use anyhow::anyhow;
use log::info;
use protobuf::reflect::{FileDescriptor, ReflectValueRef};
use protobuf::{text_format::parse_from_str, Message, MessageDyn, MessageFull};
use sdv_telemetry_dt_wrapper::data_tunnel_publisher::DataTunnelPublisher;
use sdv_telemetry_metadata_proto_rust::publisher_metadata::TelemetryPublisherMetadata;
use sdv_telemetry_proto_rust::metrics_configuration::{
    metrics_report::ReportReason, MetricsConfig,
};
use sdv_telemetry_rust_wrapper::TelemetryServiceHolder;
use sdv_telemetry_sample_utils::metrics_config_reader_utils;
use sdv_telemetry_test_proto::test_message;
use sdv_telemetry_test_proto::test_message::TestMessage;
use sdv_telemetry_test_utils::unique_dt_topic_name;
use sdv_telemetry_utils::logging::{init_logger_for_samples_or_tests, validated_log_tag};
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Once;
use tokio::sync::mpsc;

const CONFIG_PATH: &str = "tests/config.textproto";
const LOG_TAG: &str = validated_log_tag("TelemetryIntegrTests");

fn setup_once() {
    static SETUP_ONCE: Once = Once::new();

    SETUP_ONCE.call_once(|| {
        init_logger_for_samples_or_tests(LOG_TAG);
        binder::ProcessState::start_thread_pool();
    });
}

#[sdv_telemetry_test_utils_macros::timeout(30_000)]
#[tokio::test]
async fn telemetry_service_simple_config_end_to_end_test() {
    setup_once();

    info!("Connecting to service");
    let service = TelemetryServiceHolder::get().unwrap();

    info!("Initializing service");
    let (report_ready_tx, mut report_ready_rx) = mpsc::unbounded_channel();
    service
        .init(
            move |status_code, status_msg| {
                info!("Received status message {status_code}: {status_msg}");
            },
            move |report_info| {
                report_ready_tx.send(report_info).unwrap();
            },
        )
        .await
        .unwrap();

    let serialized_config = metrics_config_reader_utils::read(Path::new(CONFIG_PATH)).unwrap();
    let config = MetricsConfig::parse_from_bytes(&serialized_config).unwrap();

    info!("Adding config");
    service.add_metrics_config(&serialized_config).await.unwrap();

    info!("Activating config");
    service.activate_metrics_config(&config.uuid).await.unwrap();

    info!("Waiting for metrics report ready notification");
    let report_info = report_ready_rx.recv().await.unwrap();
    assert_eq!(report_info.metrics_config_uuid, config.uuid);

    info!("Retrieving metrics report");
    let metrics_report = service.get_metrics_report(report_info.clone()).await.unwrap();
    info!("Report received: {metrics_report:?}");
    assert_eq!(report_info.metrics_config_uuid, metrics_report.metrics_config_uuid);

    info!("Removing metrics config");
    service.remove_metrics_config(&config.uuid).await.unwrap();

    info!("Finishing service");
    service.finish().unwrap();

    info!("Successfully finished service");
}

fn create_publisher(
    topic_name: &str,
    publisher_metadata: TelemetryPublisherMetadata,
) -> anyhow::Result<DataTunnelPublisher> {
    let publisher_metadata = {
        let mut vec = vec![];
        publisher_metadata.write_to_vec(&mut vec)?;
        vec
    };
    let publisher = DataTunnelPublisher::register_topic(topic_name, 100, 100, publisher_metadata)?;
    Ok(publisher)
}

fn get_int_vector_field_value(message: &dyn MessageDyn, name: &str) -> Vec<i32> {
    let field_descriptor = message.descriptor_dyn().field_by_name(name).unwrap();
    field_descriptor
        .get_repeated(message)
        .into_iter()
        .map(|value| match value {
            ReflectValueRef::I32(value) => value,
            value => panic!("Unexpected value: {value:?}"),
        })
        .collect()
}

#[sdv_telemetry_test_utils_macros::timeout(30_000)]
#[tokio::test]
async fn telemetry_service_vector_aggregation() {
    setup_once();

    let topic_name = unique_dt_topic_name();
    let publisher = create_publisher(
        &topic_name,
        TelemetryPublisherMetadata {
            descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
            message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
            ..Default::default()
        },
    )
    .unwrap();

    // Publish a few messages _before_ subscribing.
    for i in -3..=-1 {
        publisher
            .publish_length_delimited_proto(&TestMessage { int_field: i, ..Default::default() })
            .unwrap();
    }

    info!("Connecting to service");
    let service = TelemetryServiceHolder::get().unwrap();

    info!("Setting report ready listener");
    let (report_ready_tx, mut report_ready_rx) = mpsc::unbounded_channel();
    service
        .init(
            move |status_code, status_msg| {
                info!("Received status message {status_code}: {status_msg}");
            },
            move |report_info| report_ready_tx.send(report_info).unwrap(),
        )
        .await
        .unwrap();

    info!("Adding metrics config");
    let config =
        metrics_config_reader_utils::read(Path::new("tests/aggregation_config.textproto")).unwrap();
    let mut config = MetricsConfig::parse_from_bytes(&config).unwrap();
    assert!(config.publishers[0].has_service_publisher());
    assert_eq!(config.publishers[0].service_publisher().service_name, "<placeholder>");
    config.publishers[0].mut_service_publisher().service_name = topic_name;
    let serialized_config = config.write_to_bytes().unwrap();

    let config_uuid = service.add_metrics_config(&serialized_config).await.unwrap();
    info!("Activating metrics config");
    service.activate_metrics_config(&config_uuid).await.unwrap();

    for i in 1..=70 {
        publisher
            .publish_length_delimited_proto(&TestMessage { int_field: i, ..Default::default() })
            .unwrap();
    }

    let report_info = report_ready_rx.recv().await.unwrap();
    info!("Received metrics report notification");
    assert_eq!(config_uuid, report_info.metrics_config_uuid);

    let metrics_report = service.get_metrics_report(report_info).await.unwrap();
    info!("Received metrics report");
    eprintln!("{metrics_report:?}");

    info!("Removing metrics config");
    service.remove_all_metrics_configs().await.unwrap();

    service.finish().unwrap();

    assert_eq!(metrics_report.report_reason, ReportReason::REGULAR.into());
    assert_eq!(metrics_report.metrics_config_uuid, config_uuid);
    assert_eq!(metrics_report.metrics_config_version, 1);
    assert_eq!(metrics_report.report_config_name, "Report");
    assert_eq!(
        metrics_report.report_data.type_url,
        "type.googleapis.com/android.automotive.telemetry.messages.Report"
    );
    let file_descriptor =
        FileDescriptor::new_dynamic(config.descriptor_protos[0].clone(), &[]).unwrap();
    let report_descriptor = file_descriptor.message_by_package_relative_name("Report").unwrap();

    let report_data = metrics_report.report_data.unpack_dyn(&report_descriptor).unwrap().unwrap();
    let result = get_int_vector_field_value(&*report_data, "int_vector");
    eprintln!("{result:?}");

    assert_eq!(result, (1..=70).collect::<Vec<_>>());
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
    setup_once();

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
            "./samples/sample_protos/average_speed.textproto",
            "./samples/sample_protos/average_speed_vector.textproto",
            "./samples/sample_protos/config_without_service_publishers.textproto",
            "./samples/sample_protos/configurable_publisher.textproto",
            "./samples/sample_protos/configurable_publisher_getter.textproto",
            "./samples/sample_protos/crash_reports.textproto",
            "./samples/sample_protos/data_tunnel_publisher.textproto",
            "./samples/sample_protos/data_tunnel_publisher_aggregation.textproto",
            "./samples/sample_protos/error_publisher.textproto",
            "./samples/sample_protos/frequent_reports.textproto",
            "./samples/sample_protos/journey_summary.textproto",
            "./samples/sample_protos/processing_frequent_data.textproto",
            "./samples/sample_protos/sudden_tire_pressure_drop.textproto",
            "./tests/aggregation_config.textproto",
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
