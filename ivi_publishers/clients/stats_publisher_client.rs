// Copyright 2025 Google LLC

//! Binary for testing StatsPublisher with Telemetry service.
// Prints out each atom received from the service.

use anyhow::{anyhow, Context};
use clap::Parser;
use futures::future::join_all;
use log::{error, info, trace};
use protobuf::well_known_types::any::Any;
use protobuf::Message;
use rust_to_c::TelemetryServiceHolder;
use rust_to_c::TelemetryServiceStatus;
use sdv_telemetry_rust_wrapper::StatusDetails::{
    AggregationPublisherErrorDetails, MetricsReportGeneratorErrorDetails,
    ServicePublisherErrorDetails,
};
use sdv_telemetry_rust_wrapper::{self as rust_to_c, TelemetryServiceStatusCodes};
use sdv_telemetry_sample_utils::metrics_config_reader_utils;
use sdv_telemetry_utils::logging::{init_logger_for_samples_or_tests, validated_log_tag};
use stats_publisher_descriptor_proto_rust::atoms::Atom;
use stats_publisher_descriptor_proto_rust::stats_publisher_descriptor::AtomReportList;
use stats_publisher_descriptor_proto_rust::stats_publisher_descriptor::AtomReportWrapper;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;
use tokio::task;

const LOG_TAG: &str = validated_log_tag("StatsPublisherClient");

async fn process_config(path: &Path, service: &TelemetryServiceHolder) -> anyhow::Result<String> {
    if !path.is_file() {
        return Err(anyhow!("Path is not a file or does not exist"));
    }

    // Metrics config is either read directly from a binary file into serialized config or from a
    // textproto file and then converted into serialized config.
    let serialized_config = metrics_config_reader_utils::read(path)
        .with_context(|| "Failed to read config".to_owned())?;

    let config_uuid = service
        .add_metrics_config(&serialized_config)
        .await
        .with_context(|| "Failed to add config".to_owned())?;

    service
        .activate_metrics_config(&config_uuid)
        .await
        .with_context(|| "Failed to activate config".to_owned())?;

    Ok(config_uuid)
}

#[derive(Parser, Debug)]
#[command(allow_missing_positional = true, long_about = None)]
struct Args {
    // Config files to add to telemetry service
    #[arg(required = true, num_args = 1.., trailing_var_arg = true, value_hint =
        clap::ValueHint::FilePath)]
    configs: Vec<PathBuf>,
}

// Communicate with the telemetry service.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logger_for_samples_or_tests(LOG_TAG);

    // Error handling, so we can view logs in case of main exiting early.
    wrapped_main().await.inspect_err(|err| {
        error!("{err:?}");
    })
}

async fn wrapped_main() -> anyhow::Result<()> {
    let args = Args::parse();
    binder::ProcessState::start_thread_pool();

    info!("Connecting to service");
    let service = TelemetryServiceHolder::get()?;

    // Unbounded channel, will consume as much memory as needed (not ideal for production)
    let (report_ready_tx, mut report_ready_rx) = mpsc::unbounded_channel();

    let (misc_status_tx, mut misc_status_rx) = mpsc::unbounded_channel::<TelemetryServiceStatus>();

    // Spawn a task to log all received misc status callbacks.
    task::spawn(async move {
        let mut errored_uuids = HashSet::new();
        while let Some(status) = misc_status_rx.recv().await {
            if status.code != TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS {
                let config_uuid = match status.details {
                    Some(AggregationPublisherErrorDetails(d)) => Some(d.metrics_config_uuid),
                    Some(ServicePublisherErrorDetails(d)) => Some(d.metrics_config_uuid),
                    Some(MetricsReportGeneratorErrorDetails(d)) => Some(d.metrics_config_uuid),
                    _ => None,
                };
                if let Some(config_uuid) = config_uuid {
                    errored_uuids.insert(config_uuid);
                }

                info!(
                    "Received error from service, code: {:?} msg: {}",
                    status.code, status.message
                );
            } else {
                info!(
                    "Received success message from service, code: {:?} msg: {}",
                    status.code, status.message
                );
            }
        }
    });

    service
        .init(
            move |status| {
                misc_status_tx.send(status).expect(
                    "Data should be received until the application exits and closes the channel",
                );
            },
            move |report_info| {
                report_ready_tx.send(report_info).expect(
                    "Data should be received until the application exits and closes the channel",
                );
            },
        )
        .await?;

    // Add the configs to the Telemetry Service.
    let add_config_futures = args
        .configs
        .iter()
        .map(|path| async {
            process_config(path, &service)
                .await
                .with_context(|| format!("Failed to process config: {}", path.display()))
                .inspect(|config_uuid| {
                    info!(
                        "Config with UUID {config_uuid} added successfully from {}",
                        path.display()
                    )
                })
                .inspect_err(|e| error!("{e:?}"))
        })
        .collect::<Vec<_>>();

    // Collect all activated configs
    let add_config_results = join_all(add_config_futures).await;
    let active_configs = add_config_results.into_iter().flatten().collect::<HashSet<_>>();
    trace!("Active configs: {active_configs:?}");

    // The service will continue retrieving reports until manually exited with an interrupt
    // (Ctrl + C)
    while !active_configs.is_empty() {
        let Some(report_info) = report_ready_rx.recv().await else {
            error!("Report ready callback dropped while still expecting additional reports.");
            break;
        };
        let metrics_report = service.get_metrics_report(&report_info.report_uuid).await?;

        info!("Report is {:?}", metrics_report);

        // Parse the report and print it to the console
        let mut metrics_report_data = metrics_report.report_data;
        info!("Metrics report parsed: {:?}", metrics_report_data);

        let atom_report_wrapper: Option<AtomReportWrapper> =
            metrics_report_data.take().and_then(parse_any_to_type_protobuf);

        info!("Atom Report Wrapper: {:?}", atom_report_wrapper);

        match AtomReportList::parse_from_bytes(&atom_report_wrapper.unwrap().data) {
            Ok(list) => {
                println!("Successfully parsed AtomReportList from bytes.");

                // Iterate over the list of AtomReports
                let data_items_view = list.report;

                if data_items_view.is_empty() {
                    info!("The 'report' field in AtomReportList is empty.");
                } else {
                    info!("Iterating over 'report' items:");
                    for item_view in data_items_view.iter() {
                        match Atom::parse_from_bytes(&item_view.data) {
                            // Print the atom to logcat and console for demo purposes
                            Ok(atom) => {
                                println!("Successfully got the atom {}", atom);
                                info!("{}", atom);
                            }

                            Err(e) => {
                                eprintln!("Failed to parse Atom from bytes: {:?}", e);
                                info!("Failed to parse Atom from bytes: {:?}", e)
                            }
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to parse AtomReportList from bytes: {:?}", e);
            }
        }
    }

    // Should never be called, the service will end by interrupt.
    service.finish()?;

    Ok(())
}

// Converts an Any protobuf format to the AtomReportWrapper. This is what will be received from the
// telemetry service.
fn parse_any_to_type_protobuf(any_msg: Any) -> Option<AtomReportWrapper> {
    // Unpack using built in cast type. It will be wrapped with an Option<> so we need to manually
    // unpack into an Ok or Err.
    match any_msg.unpack::<AtomReportWrapper>() {
        Ok(optional_message) => optional_message,
        Err(e) => {
            eprintln!("Failed to unpack Any to AtomReportWrapper: {:?}", e);
            None
        }
    }
}
