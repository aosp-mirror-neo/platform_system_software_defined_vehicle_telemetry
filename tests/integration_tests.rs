// Copyright 2023 Google LLC

#![cfg(test)]

use anyhow::anyhow;
use log::{debug, info};
use protobuf::reflect::{FileDescriptor, ReflectValueRef};
use protobuf::well_known_types::{empty::Empty, wrappers::Int32Value};
use protobuf::{text_format::parse_from_str, Message, MessageDyn, MessageFull};
use sdv_comms::id::ServiceFqin;
use sdv_comms::sd::UnitType;
use sdv_comms::ContextRef;
use sdv_mw_rs_com_sdv_telemetry_sample_tire_sensors_reader::{publisher, TireSensorsReader};
use sdv_mw_rs_sdv_comms::SdvComms;
use sdv_proto_reflection_rs::message_metadata::MessageMetadata;
use sdv_telemetry_comms_utils::wait_for_sdv_agents_and_create_context;
use sdv_telemetry_dt_wrapper::data_tunnel_publisher::DataTunnelPublisher;
use sdv_telemetry_proto_rust::expression::{
    combination_node::ArithmeticOperator, combination_node::Operator,
    constant_leaf_node::Node_value, node::Node_type, CombinationNode, ConstantLeafNode, Node,
};
use sdv_telemetry_proto_rust::metrics_configuration::{
    conditional_trigger::Condition_type, metrics_report::ReportReason, publisher::Publisher_type,
    trigger::Trigger_type, AggregationPublisher, ConditionalTrigger, MetricsConfig,
    MetricsReportConfig, PeriodicTrigger, ProtoMessageBuilder, Publisher, Trigger,
};
use sdv_telemetry_rust_wrapper::{
    ConditionalTriggerErrorDetails, StatusDetails, TelemetryServiceHolder,
};
use sdv_telemetry_sample_middleware_proto::tire::TirePressure;
use sdv_telemetry_sample_utils::metrics_config_reader_utils;
use sdv_telemetry_test_proto::test_message;
use sdv_telemetry_test_proto::test_message::TestMessage;
use sdv_telemetry_test_utils::unique_dt_topic_name;
use sdv_telemetry_utils::logging::{init_logger_for_samples_or_tests, validated_log_tag};
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Once};
use std::time::Duration;
use tokio::sync::{mpsc, OnceCell};

const CONFIG_PATH: &str = "tests/config.textproto";
const LOG_TAG: &str = validated_log_tag("TelemetryIntegrTests");
const VSOMEIP_SERVER_BINARY: &str = "/vendor/bin/sdv_vsomeip_group_tester";
const VSOMEIP_CONFIG_FILE: &str = "/vendor/etc/vsomeip/group_tester.json";

async fn make_context() -> ContextRef {
    static CONTEXT_REF: OnceCell<ContextRef> = OnceCell::const_new();
    *CONTEXT_REF
        .get_or_init(|| async {
            // Tests may run in multiple parallel processes, therefore we need to ensure that the FQINs
            // created by the processes do not conflict with each other. This is achieved by embedding
            // the process ID into the FQIN.
            let fqin = ServiceFqin::from_str(&format!(
                "vm:com.google.sdv.telemetry.tests.IntegrationTest{}/instance",
                std::process::id()
            ))
            .unwrap();

            wait_for_sdv_agents_and_create_context(fqin).await
        })
        .await
}

fn make_unique_unit_type() -> UnitType {
    UnitType::from_str(&format!(
        "com.google.sdv.telemetry.tests.IntegrationTest#{}",
        unique_dt_topic_name()
    ))
    .unwrap()
}

fn setup_once() {
    static SETUP_ONCE: Once = Once::new();

    SETUP_ONCE.call_once(|| {
        init_logger_for_samples_or_tests(LOG_TAG);
        binder::ProcessState::start_thread_pool();
    });
}

#[sdv_telemetry_test_utils::timeout(30_000)]
#[tokio::test]
async fn telemetry_service_simple_config_end_to_end_test() {
    setup_once();

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

    let serialized_config = metrics_config_reader_utils::read(Path::new(CONFIG_PATH)).unwrap();
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
}

#[sdv_telemetry_test_utils::timeout(30_000)]
#[tokio::test]
async fn telemetry_service_status_reporting_test() {
    setup_once();

    let config = MetricsConfig {
        uuid: "9b1750a9-2de9-40ea-b8f8-b4caaa005d54".to_owned(),

        triggers: vec![
            Trigger {
                name: "periodic_trigger".to_owned(),
                trigger_type: Some(Trigger_type::PeriodicTrigger(PeriodicTrigger {
                    interval: Some(Duration::from_millis(1).into()).into(),
                    ..Default::default()
                })),
                ..Default::default()
            },
            Trigger {
                name: "finish_trigger".to_owned(),
                trigger_type: Some(Trigger_type::PeriodicTrigger(PeriodicTrigger {
                    interval: Some(Duration::from_secs(1).into()).into(),
                    ..Default::default()
                })),
                ..Default::default()
            },
            Trigger {
                name: "conditional_trigger".to_owned(),
                trigger_type: Some(Trigger_type::ConditionalTrigger(ConditionalTrigger {
                    trigger_names: vec!["periodic_trigger".to_owned()],
                    selector_node_index: Some(0),
                    condition_type: Some(Condition_type::IsTrue(Default::default())),
                    ..Default::default()
                })),
                ..Default::default()
            },
        ],

        expression_nodes: vec![
            Node {
                node_type: Some(Node_type::CombinationNode(CombinationNode {
                    left_index: Some(1),
                    right_index: Some(2),
                    operator: Some(Operator::ArithmeticOperator(ArithmeticOperator::DIVIDE.into())),
                    ..Default::default()
                })),
                ..Default::default()
            },
            Node {
                node_type: Some(Node_type::ConstantLeafNode(ConstantLeafNode {
                    node_value: Some(Node_value::FloatValue(42.0)),
                    ..Default::default()
                })),
                ..Default::default()
            },
            Node {
                node_type: Some(Node_type::ConstantLeafNode(ConstantLeafNode {
                    node_value: Some(Node_value::FloatValue(0.0)),
                    ..Default::default()
                })),
                ..Default::default()
            },
        ],

        finish_trigger_name: "finish_trigger".to_owned(),
        metrics_report_configs: vec![MetricsReportConfig {
            name: "some_metrics_report_config".to_owned(),
            trigger_names: vec!["conditional_trigger".to_owned()],
            report_incomplete: true,
            message_builder: Some(ProtoMessageBuilder {
                message_type: Empty::descriptor().full_name().to_owned(),
                ..Default::default()
            })
            .into(),
            ..Default::default()
        }],
        ..Default::default()
    };

    let serialized_config = config.write_to_bytes().unwrap();

    info!("Connecting to service");
    let service = TelemetryServiceHolder::get().unwrap();

    info!("Initializing service");
    let (status_sender, mut status_receiver) = mpsc::unbounded_channel();
    service
        .init(
            move |status| {
                status_sender.send(status).unwrap();
            },
            move |report_info| {
                info!("Received report info {report_info:?}");
            },
        )
        .await
        .unwrap();

    info!("Adding config");
    service.add_metrics_config(&serialized_config).await.unwrap();

    info!("Activating config");
    service.activate_metrics_config(&config.uuid).await.unwrap();

    info!("Awaiting error status");
    let status = status_receiver.recv().await.unwrap();

    assert!(
        matches!(
            &status.details,
            Some(StatusDetails::ConditionalTriggerErrorDetails(ConditionalTriggerErrorDetails {
                metrics_config_uuid, trigger_name,
            }))
            if metrics_config_uuid == &config.uuid && trigger_name == &config.triggers[2].name),
        "Unexpected status: {status:?}",
    );

    info!("Removing metrics config");
    service.remove_metrics_config(&config.uuid).await.unwrap();

    info!("Finishing service");
    service.finish().unwrap();

    info!("Successfully finished service");
}

/// Test for b/382231226.
///
/// Creates a bunch of aggregation publishers that are all triggered by a periodic trigger with a
/// very small interval (1ns). This test verifies that there is no race condition caused by a
/// aggregation publisher being triggered when it is being deactivated.
///
/// The test is not perfect, and failures usually only show up when running it multiple times.
#[sdv_telemetry_test_utils::timeout(30_000)]
#[tokio::test]
async fn telemetry_service_no_aggregation_publisher_race_condition_during_metrics_config_deactivation(
) {
    setup_once();

    const NUM_PUBLISHERS: usize = 500;

    let config = MetricsConfig {
        uuid: "77619dc5-b19b-436c-89d7-b7b9d7619275".to_owned(),

        triggers: vec![
            Trigger {
                name: "periodic_trigger".to_owned(),
                trigger_type: Some(Trigger_type::PeriodicTrigger(PeriodicTrigger {
                    interval: Some(Duration::from_nanos(1).into()).into(),
                    ..Default::default()
                })),
                ..Default::default()
            },
            Trigger {
                name: "finish_trigger".to_owned(),
                trigger_type: Some(Trigger_type::PeriodicTrigger(PeriodicTrigger {
                    interval: Some(Duration::from_secs(1).into()).into(),
                    ..Default::default()
                })),
                ..Default::default()
            },
        ],

        publishers: (0..NUM_PUBLISHERS)
            .map(|i| Publisher {
                name: format!("publisher_{i}"),
                publisher_type: Some(Publisher_type::AggregationPublisher(AggregationPublisher {
                    trigger_names: vec!["periodic_trigger".to_owned()],
                    message_builder: Some(ProtoMessageBuilder {
                        message_type: Empty::descriptor().full_name().to_owned(),
                        ..Default::default()
                    })
                    .into(),
                    ..Default::default()
                })),
                ..Default::default()
            })
            .collect::<Vec<_>>(),

        finish_trigger_name: "finish_trigger".to_owned(),
        metrics_report_configs: vec![MetricsReportConfig {
            name: "some_metrics_report_config".to_owned(),
            report_incomplete: true,
            message_builder: Some(ProtoMessageBuilder {
                message_type: Empty::descriptor().full_name().to_owned(),
                ..Default::default()
            })
            .into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    run_race_condition_test(config).await;
}

/// Test for b/382231226.
///
/// Creates a bunch of conditional triggers and aggregation publishers that are all triggered by a
/// periodic trigger with a very small interval (1ns). This test verifies that there is no race
/// condition caused by a conditional trigger being triggered when it is being deactivated.
///
/// The test is not perfect, and failures usually only show up when running it multiple times.
#[sdv_telemetry_test_utils::timeout(30_000)]
#[tokio::test]
async fn telemetry_service_no_conditional_trigger_race_condition_during_metrics_config_deactivation(
) {
    setup_once();

    const NUM_ENTITIES: usize = 500;

    let config = MetricsConfig {
        uuid: "78619dc5-b19b-436c-89d7-b7b9d7619275".to_owned(),

        triggers: [
            Trigger {
                name: "periodic_trigger".to_owned(),
                trigger_type: Some(Trigger_type::PeriodicTrigger(PeriodicTrigger {
                    interval: Some(Duration::from_nanos(1).into()).into(),
                    ..Default::default()
                })),
                ..Default::default()
            },
            Trigger {
                name: "finish_trigger".to_owned(),
                trigger_type: Some(Trigger_type::PeriodicTrigger(PeriodicTrigger {
                    interval: Some(Duration::from_secs(1).into()).into(),
                    ..Default::default()
                })),
                ..Default::default()
            },
        ]
        .into_iter()
        .chain((0..NUM_ENTITIES).map(|i| Trigger {
            name: format!("conditional_trigger_{i}"),
            trigger_type: Some(Trigger_type::ConditionalTrigger(ConditionalTrigger {
                trigger_names: vec!["periodic_trigger".to_owned()],
                selector_node_index: Some(0),
                condition_type: Some(Condition_type::IsTrue(Default::default())),
                ..Default::default()
            })),
            ..Default::default()
        }))
        .collect::<Vec<_>>(),

        publishers: (0..NUM_ENTITIES)
            .map(|i| Publisher {
                name: format!("publisher_{i}"),
                publisher_type: Some(Publisher_type::AggregationPublisher(AggregationPublisher {
                    trigger_names: vec![format!("conditional_trigger_{i}")],
                    message_builder: Some(ProtoMessageBuilder {
                        message_type: Empty::descriptor().full_name().to_owned(),
                        ..Default::default()
                    })
                    .into(),
                    ..Default::default()
                })),
                ..Default::default()
            })
            .collect::<Vec<_>>(),

        expression_nodes: vec![Node {
            node_type: Some(Node_type::ConstantLeafNode(ConstantLeafNode {
                node_value: Some(Node_value::BoolValue(true)),
                ..Default::default()
            })),
            ..Default::default()
        }],

        finish_trigger_name: "finish_trigger".to_owned(),
        metrics_report_configs: vec![MetricsReportConfig {
            name: "some_metrics_report_config".to_owned(),
            report_incomplete: true,
            message_builder: Some(ProtoMessageBuilder {
                message_type: Empty::descriptor().full_name().to_owned(),
                ..Default::default()
            })
            .into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    run_race_condition_test(config).await;
}

async fn run_race_condition_test(config: MetricsConfig) {
    let serialized_config = config.write_to_bytes().unwrap();

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

    info!("Adding config");
    service.add_metrics_config(&serialized_config).await.unwrap();

    info!("Activating config");
    service.activate_metrics_config(&config.uuid).await.unwrap();

    info!("Waiting for metrics report ready notification");
    let report_info = report_ready_rx.recv().await.unwrap();

    info!("Retrieving metrics report");
    let metrics_report = service.get_metrics_report(&report_info.report_uuid).await.unwrap();
    info!("Report received: {metrics_report:?}");
    assert_eq!(metrics_report.report_uuid, report_info.report_uuid);
    assert_eq!(metrics_report.metrics_config_uuid, config.uuid);

    info!("Removing metrics config");
    service.remove_metrics_config(&config.uuid).await.unwrap();

    info!("Finishing service");
    service.finish().unwrap();

    info!("Successfully finished service");
}

async fn create_publisher(
    ctx: ContextRef,
    unit_type: &UnitType,
    unit_name: &str,
    publisher_metadata: MessageMetadata,
) -> anyhow::Result<DataTunnelPublisher> {
    let publisher_metadata = {
        let mut vec = vec![];
        publisher_metadata.write_to_vec(&mut vec)?;
        vec
    };
    DataTunnelPublisher::register_topic(ctx, unit_type, unit_name, 100, 100, publisher_metadata)
        .await
}

fn get_int_field_value(message: &dyn MessageDyn, name: &str) -> i32 {
    let field_descriptor = message.descriptor_dyn().field_by_name(name).unwrap();
    field_descriptor
        .get_singular(message)
        .map(|value| match value {
            ReflectValueRef::I32(value) => value,
            value => panic!("Unexpected value: {value:?}"),
        })
        .unwrap()
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

async fn run_vector_aggregation_test(publisher: DataTunnelPublisher, service_name: String) {
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
            move |status| {
                info!("Received status message {:?}: {}", status.code, status.message);
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
    config.publishers[0].mut_service_publisher().service_name = service_name;
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

    let metrics_report = service.get_metrics_report(&report_info.report_uuid).await.unwrap();
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

#[sdv_telemetry_test_utils::timeout(30_000)]
#[tokio::test]
async fn telemetry_service_vector_aggregation() {
    setup_once();

    const UNIT_NAME: &str = "default";

    let ctx = make_context().await;
    let unit_type = make_unique_unit_type();
    let publisher = create_publisher(
        ctx,
        &unit_type,
        UNIT_NAME,
        MessageMetadata {
            file_descriptors: vec![test_message::file_descriptor().proto().clone()],
            message_full_name: format!(".{}", TestMessage::descriptor().full_name()),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    run_vector_aggregation_test(publisher, format!("{unit_type}|{UNIT_NAME}")).await;
}

#[sdv_telemetry_test_utils::timeout(30_000)]
#[tokio::test]
async fn telemetry_service_vector_aggregation_with_fqin() {
    setup_once();

    const UNIT_NAME: &str = "default";

    let ctx = make_context().await;
    let unit_type = make_unique_unit_type();
    let publisher = create_publisher(
        ctx,
        &unit_type,
        UNIT_NAME,
        MessageMetadata {
            file_descriptors: vec![test_message::file_descriptor().proto().clone()],
            message_full_name: format!(".{}", TestMessage::descriptor().full_name()),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    run_vector_aggregation_test(publisher, format!("{}|{UNIT_NAME}", ctx.get_self_fqin())).await;
}

#[sdv_telemetry_test_utils::timeout(100_000)]
#[tokio::test]
async fn telemetry_service_someip() {
    setup_once();

    let service = TelemetryServiceHolder::get().unwrap();

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
        metrics_config_reader_utils::read(Path::new("tests/someip.textproto")).unwrap();
    let config = MetricsConfig::parse_from_bytes(&serialized_config).unwrap();
    service.add_metrics_config(&serialized_config).await.unwrap();
    service.activate_metrics_config(&config.uuid).await.unwrap();

    let mut server =
        sdv_someip_integration_test::run_vsomeip_server(VSOMEIP_SERVER_BINARY, VSOMEIP_CONFIG_FILE);

    let report_info = report_ready_rx.recv().await.unwrap();

    service.remove_metrics_config(&config.uuid).await.unwrap();
    let _ = server.kill();
    let _ = server.wait();
    service.finish().unwrap();

    let metrics_report = service.get_metrics_report(&report_info.report_uuid).await.unwrap();
    debug!("Report received: {metrics_report:?}");

    assert_eq!(metrics_report.report_uuid, report_info.report_uuid);
    assert_eq!(metrics_report.metrics_config_uuid, config.uuid);
    assert_eq!(metrics_report.report_data.unpack::<Int32Value>().unwrap().unwrap().value, 42);
}

#[sdv_telemetry_test_utils::timeout(30_000)]
#[tokio::test]
async fn telemetry_service_middleware() {
    setup_once();

    let ctx = make_context().await;

    let mut publisher_service =
        TireSensorsReader::new(Arc::new(SdvComms { context: ctx })).await.unwrap();

    let tire_pressure_publishers = publisher::Variant::<TirePressure>::ALL_VARIANTS
        .iter()
        .map(|variant| publisher_service.take_publisher(variant).unwrap())
        .collect::<Vec<_>>();

    info!("Connecting to service");
    let service = TelemetryServiceHolder::get().unwrap();

    info!("Setting report ready listener");
    let (report_ready_tx, mut report_ready_rx) = mpsc::unbounded_channel();
    service
        .init(
            move |status| {
                info!("Received status message {:?}: {}", status.code, status.message);
            },
            move |report_info| report_ready_tx.send(report_info).unwrap(),
        )
        .await
        .unwrap();

    info!("Adding metrics config");
    let config =
        metrics_config_reader_utils::read(Path::new("tests/middleware_config.textproto")).unwrap();
    let config = MetricsConfig::parse_from_bytes(&config).unwrap();
    let serialized_config = config.write_to_bytes().unwrap();
    let config_uuid = service.add_metrics_config(&serialized_config).await.unwrap();

    info!("Activating metrics config");
    service.activate_metrics_config(&config_uuid).await.unwrap();

    for (i, publisher) in tire_pressure_publishers.iter().enumerate() {
        let message = TirePressure { pressure: i.try_into().unwrap(), ..Default::default() };
        publisher.publish(&message).unwrap();
    }

    let report_info = report_ready_rx.recv().await.unwrap();
    info!("Received metrics report notification");
    assert_eq!(config_uuid, report_info.config_uuid);

    let metrics_report = service.get_metrics_report(&report_info.report_uuid).await.unwrap();
    info!("Received metrics report");
    eprintln!("{metrics_report:?}");

    info!("Removing metrics config");
    service.remove_all_metrics_configs().await.unwrap();

    service.finish().unwrap();

    assert_eq!(metrics_report.report_uuid, report_info.report_uuid);
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
    let result = [
        get_int_field_value(&*report_data, "front_left"),
        get_int_field_value(&*report_data, "front_right"),
        get_int_field_value(&*report_data, "rear_left"),
        get_int_field_value(&*report_data, "rear_right"),
    ];

    assert_eq!(result, [0, 1, 2, 3]);
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
            "./samples/sample_protos/sdv_configurable_publisher.textproto",
            "./samples/sample_protos/sudden_tire_pressure_drop.textproto",
            "./tests/aggregation_config.textproto",
            "./tests/config.textproto",
            "./tests/middleware_config.textproto",
            "./tests/someip.textproto",
        ]
    );

    for path in paths {
        eprintln!("Attempting to parse: {}", path.display());
        let textproto = fs::read_to_string(path)?;
        let _proto = parse_from_str::<MetricsConfig>(&textproto)?;
    }

    Ok(())
}
