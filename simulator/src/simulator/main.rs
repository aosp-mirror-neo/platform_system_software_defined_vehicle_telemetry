// Copyright 2025 Google LLC

//! Command line tool to simulate the life cycle of a metrics configuration by simulating service
//! publishers with provided replay-data.

use clap::Parser;
use log::error;
use sdv_telemetry_simulator_lib::{run, CliArgs};
use sdv_telemetry_simulator_publishing_strategy::PublishingStrategyFactory;
use sdv_telemetry_simulator_publishing_strategy_configurable_publisher_registry::PublishingStrategyFactoryImpl as ConfigurablePublisherRegistryPublishingStrategyFactory;
use sdv_telemetry_utils::logging::validated_log_tag;

mod test_support;

const LOG_TAG: &str = validated_log_tag("SdvTelemetrySimulator");

fn get_publishing_strategy_factories(
) -> [Box<dyn PublishingStrategyFactory>; 1 + if cfg!(feature = "comms_stack") { 1 } else { 0 }] {
    [
        Box::new(ConfigurablePublisherRegistryPublishingStrategyFactory::new())
            as Box<dyn PublishingStrategyFactory>,
        #[cfg(feature = "comms_stack")]
        create_comms_publishing_strategy_factory(),
    ]
}

#[cfg(feature = "comms_stack")]
fn create_comms_publishing_strategy_factory() -> Box<dyn PublishingStrategyFactory> {
    use sdv_comms::{id::ServiceFqin, ContextRef};
    use sdv_telemetry_comms_utils::wait_for_sdv_agents_and_create_context;
    use sdv_telemetry_simulator_publishing_strategy_sdv_comms::PublishingStrategyFactoryImpl as SdvCommsPublishingStrategyFactory;
    use std::str::FromStr;
    use tokio::sync::OnceCell;

    let ctx_factory = async move || {
        static CTX: OnceCell<ContextRef> = OnceCell::const_new();
        *CTX.get_or_init(async || {
            // WARNING: The VM name is automatically updated in the created `ContextRef` to match
            // the actual VM name. Do not use this string anywhere else, always retrieve the actual
            // FQIN using `ContextRef::get_self_fqin`!
            const SERVICE_FQIN: &str = "vm:com.google.sdv.telemetry.Simulator/default";
            let fqin = ServiceFqin::from_str(SERVICE_FQIN)
                .expect("Service FQIN should be parsed successfully");

            wait_for_sdv_agents_and_create_context(fqin).await
        })
        .await
    };

    Box::new(SdvCommsPublishingStrategyFactory::new(ctx_factory))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    sdv_log::init_logger(LOG_TAG)?;
    sdv_tracing::init_tracing();

    run(CliArgs::parse(), &mut get_publishing_strategy_factories()).await.inspect_err(|error| {
        error!("Simulator failed with error: {error:?}");
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::expect_that;
    use googletest::matchers::{elements_are, eq, pat, predicate};
    use protobuf::well_known_types::any::Any;
    use protobuf::well_known_types::wrappers::Int32Value;
    use protobuf::{EnumOrUnknown, Message, MessageField, MessageFull};
    use sdv_telemetry_proto_rust::expression::{node::Node_type, FieldLeafNode, Node};
    use sdv_telemetry_proto_rust::metrics_configuration::{
        metrics_report::ReportReason,
        proto_message_builder::field_assignment::Aggregated_field_value,
        proto_message_builder::field_assignment::NoAggregation,
        proto_message_builder::FieldAssignment, publisher::Publisher_type, trigger::Trigger_type,
        DataTrigger, MetricsConfig, MetricsReport, MetricsReportConfig, PeriodicTrigger,
        ProtoMessageBuilder, Publisher, ServicePublisher, Trigger,
    };
    use sdv_telemetry_simulator_proto::simulation_publisher::{
        data_generator::Data_source, publishing_strategy, CSVFile, DataGenerator,
        PublishingStrategy, SimulationPublisher,
    };
    use sdv_telemetry_test_utils::parameterized_test;
    use std::{fs, path::Path, time::Duration};
    use tempfile::tempdir;
    use test_support::{into_csv_bytes, CsvRow};

    const SERVICE_NAME: &str = "com.google.sdv.telemetry.tests.simulator.SomeService|unit-name";

    fn make_metrics_config() -> MetricsConfig {
        MetricsConfig {
            uuid: "2834f669-e4c2-4d28-8dd7-3717fceff706".to_owned(),
            version: 1,
            descriptor_protos: vec![Int32Value::descriptor().file_descriptor_proto().to_owned()],
            expression_nodes: vec![Node {
                node_type: Some(Node_type::FieldLeafNode(FieldLeafNode {
                    publisher_name: "publisher".to_owned(),
                    field_names: vec!["value".to_owned()],
                    ..Default::default()
                })),
                ..Default::default()
            }],
            publishers: vec![Publisher {
                name: "publisher".to_owned(),
                publisher_type: Some(Publisher_type::ServicePublisher(ServicePublisher {
                    service_name: SERVICE_NAME.to_owned(),
                    ..Default::default()
                })),
                ..Default::default()
            }],
            triggers: vec![
                Trigger {
                    name: "data_trigger".to_owned(),
                    trigger_type: Some(Trigger_type::DataTrigger(DataTrigger {
                        publisher_name: "publisher".to_owned(),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
                Trigger {
                    name: "finish_trigger".to_owned(),
                    trigger_type: Some(Trigger_type::PeriodicTrigger(PeriodicTrigger {
                        interval: Some(Duration::from_millis(300).into()).into(),
                        ..Default::default()
                    })),
                    ..Default::default()
                },
            ],
            finish_trigger_name: "finish_trigger".to_owned(),
            metrics_report_configs: vec![MetricsReportConfig {
                name: "report_config".to_owned(),
                trigger_names: vec!["data_trigger".to_owned()],
                report_incomplete: false,
                message_builder: Some(ProtoMessageBuilder {
                    message_type: format!(".{}", Int32Value::descriptor().full_name()),
                    field_assignments: vec![FieldAssignment {
                        field_name: "value".to_owned(),
                        aggregated_field_value: Some(Aggregated_field_value::NoAggregation(
                            NoAggregation { expression_node_index: Some(0), ..Default::default() },
                        )),
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .into(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn make_publisher_config(
        data_generator: DataGenerator,
        publishing_strategy: Option<PublishingStrategy>,
    ) -> SimulationPublisher {
        SimulationPublisher {
            service_name: SERVICE_NAME.to_owned(),
            data_format_message_name: ".google.protobuf.Int32Value".to_owned(),
            data_format_protos: vec![],
            data_generators: vec![data_generator],
            publishing_strategy: publishing_strategy.into(),
            ..Default::default()
        }
    }

    fn make_data_generator<T: MessageFull>(
        data: &[CsvRow<T>],
        loop_data: bool,
        default_delay: Option<&Duration>,
        csv_file_path: &Path,
    ) -> DataGenerator {
        fs::write(csv_file_path, into_csv_bytes(data)).unwrap();

        DataGenerator {
            loop_data,
            default_delay: default_delay.map(|duration| (*duration).into()).into(),
            data_source: Some(Data_source::CsvFile(CSVFile {
                file_path: csv_file_path.to_str().unwrap().to_owned(),
                delimiter: ",".to_owned(),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    async fn test_run(publishing_strategy: Option<PublishingStrategy>) {
        env_logger::builder().is_test(true).filter_level(log::LevelFilter::max()).init();

        let temp_dir = tempdir().unwrap();
        let metrics_config_path = temp_dir.path().join("metrics_config.pb");
        let simulation_publisher_path = temp_dir.path().join("simulation_publisher.pb");
        let csv_file_path = temp_dir.path().join("data.csv");
        let output_path = temp_dir.path().join("output");

        fs::write(&metrics_config_path, make_metrics_config().write_to_bytes().unwrap()).unwrap();
        fs::write(
            &simulation_publisher_path,
            make_publisher_config(
                make_data_generator(
                    &[
                        CsvRow::no_delay(Int32Value { value: 1, ..Default::default() }),
                        CsvRow::no_delay(Int32Value { value: 2, ..Default::default() }),
                        CsvRow::no_delay(Int32Value { value: 3, ..Default::default() }),
                    ],
                    false,
                    Some(&Duration::from_millis(20)),
                    &csv_file_path,
                ),
                publishing_strategy,
            )
            .write_to_bytes()
            .unwrap(),
        )
        .unwrap();
        fs::create_dir(&output_path).unwrap();

        let args = CliArgs::parse_from([
            "sdv_telemetry_simulator",
            "--max-simulation-time",
            "seconds:0.5",
            "full-simulation",
            "--metrics-configs",
            metrics_config_path.to_str().unwrap(),
            "--publisher-configs",
            simulation_publisher_path.to_str().unwrap(),
            "--output-directory",
            output_path.to_str().unwrap(),
        ]);
        run(args, &mut get_publishing_strategy_factories()).await.unwrap();

        let reports = fs::read_dir(output_path.join("reports"))
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                entry.path()
            })
            .map(|report_path| {
                let bytes = fs::read(report_path).unwrap();
                MetricsReport::parse_from_bytes(&bytes).unwrap()
            })
            .collect::<Vec<_>>();

        expect_that!(
            reports,
            elements_are![
                pat!(MetricsReport {
                    metrics_config_uuid: eq("2834f669-e4c2-4d28-8dd7-3717fceff706"),
                    report_config_name: eq("report_config"),
                    report_number: eq(&1),
                    report_reason: eq(&EnumOrUnknown::new(ReportReason::REGULAR)),
                    report_data: predicate(|report_data: &MessageField<Any>| report_data
                        .as_ref()
                        .unwrap()
                        .unpack::<Int32Value>()
                        .unwrap()
                        .unwrap()
                        .value
                        == 1),
                }),
                pat!(MetricsReport {
                    metrics_config_uuid: eq("2834f669-e4c2-4d28-8dd7-3717fceff706"),
                    report_config_name: eq("report_config"),
                    report_number: eq(&2),
                    report_reason: eq(&EnumOrUnknown::new(ReportReason::REGULAR)),
                    report_data: predicate(|report_data: &MessageField<Any>| report_data
                        .as_ref()
                        .unwrap()
                        .unpack::<Int32Value>()
                        .unwrap()
                        .unwrap()
                        .value
                        == 2),
                }),
                pat!(MetricsReport {
                    metrics_config_uuid: eq("2834f669-e4c2-4d28-8dd7-3717fceff706"),
                    report_config_name: eq("report_config"),
                    report_number: eq(&3),
                    report_reason: eq(&EnumOrUnknown::new(ReportReason::REGULAR)),
                    report_data: predicate(|report_data: &MessageField<Any>| report_data
                        .as_ref()
                        .unwrap()
                        .unpack::<Int32Value>()
                        .unwrap()
                        .unwrap()
                        .value
                        == 3),
                }),
            ]
        );
    }

    parameterized_test!(
        #[googletest::test]
        #[sdv_telemetry_test_utils::timeout]
        #[tokio::test]
        run_with_publishing_strategy,

        |publishing_strategy| async move {
            test_run(publishing_strategy).await;
        },

        default: (None),
        configurable_publisher_registry: (Some(PublishingStrategy {
            type_: Some(publishing_strategy::Type::ConfigurablePublisherRegistry(
                publishing_strategy::ConfigurablePublisherRegistry { ..Default::default() },
            )),
            ..Default::default()
        })),
        sdv_comms_data_tunnel: (Some(PublishingStrategy {
            type_: Some(publishing_strategy::Type::SdvCommsDataTunnel(
                publishing_strategy::SdvCommsDataTunnel {
                    message_count: 100,
                    message_size: 100,
                    ..Default::default()
                },
            )),
            ..Default::default()
        })),
    );
}
