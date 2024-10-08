// Copyright 2024 Google LLC

//! Command line tool to simulate the life cycle of a metrics configuration by
//! simulating service publishers with provided replay-data.

mod cli;
mod io;
mod metrics_config_info;
mod metrics_configs_tracker;
mod output_strings;
mod protobuf_parsing;
mod publisher_config;
mod publisher_config_parser;
mod simulation_publisher;
mod simulation_publisher_subscription;
mod simulator_errors;
mod telemetry_service_handler;
mod util;

use anyhow::Error;
use binder::BinderFeatures;
use clap::Parser;
use io::REPORTS_DIR;
use protobuf::Message;
use log::{error, info};
use sdv_telemetry_rust_wrapper::{ReportInfo, TelemetryServiceStatusCodes};
use sdv_telemetry_utils::{collection_extension::HashMapExtension, logging::validated_log_tag};
use std::{
    collections::HashMap, ops::Deref, path::Path, path::PathBuf, sync::Arc, time::Duration
};
use tokio::{
    sync::{mpsc::UnboundedReceiver, oneshot, oneshot::error::TryRecvError},
    task,
    time::sleep,
};
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{
    IConfigurablePublisher::BnConfigurablePublisher, PublisherInfo::PublisherInfo, IConfigurablePublisherRegistry::{IConfigurablePublisherRegistry, BpConfigurablePublisherRegistry}};

use crate::{
    cli::Commands,
    io::create_output_directory,
    metrics_configs_tracker::MetricsConfigsTracker,
    publisher_config_parser::parse_publisher_config,
    simulation_publisher::{SimulationPublisher, SimulationPublisherWrapper},
    simulator_errors::SimulatorError,
    telemetry_service_handler::{
        get_report, get_telemetry_service_instance, get_telemetry_service_listeners,
    },
};

const LOG_TAG: &str = validated_log_tag("SdvTelemetrySimulator");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    sdv_log::init_logger(LOG_TAG)?;
    sdv_tracing::init_tracing();

    wrapped_main().await.inspect_err(|error| {
        error!("Simulator failed with error: {error:?}");
    })
}

async fn wrapped_main() -> anyhow::Result<()> {
    let cli_args = cli::CliArgs::parse();
    info!("Running simulator with args:\n{cli_args}");

    binder::ProcessState::start_thread_pool();

    match cli_args.command {
        Commands::TelemetryClient { telemetry_client_args } => {
            run_telemetry_client(
                &telemetry_client_args.metrics_config_paths,
                telemetry_client_args.no_binary_reports,
                telemetry_client_args.max_report_count,
                cli_args.max_simulation_time,
            )
            .await?;
        }
        Commands::SimulationPublishers { simulation_publisher_args } => {
            // Publishers will unregister when dropped.
            let _registered_publishers = parse_register_and_start_simulation_publishers(
                &simulation_publisher_args.publisher_config_paths,
            )?;
            sleep(cli_args.max_simulation_time).await;
        }
        Commands::FullSimulation { telemetry_client_args, simulation_publisher_args } => {
            // Publishers will unregister when dropped.
            let _registered_publishers = parse_register_and_start_simulation_publishers(
                &simulation_publisher_args.publisher_config_paths,
            )?;
            run_telemetry_client(
                &telemetry_client_args.metrics_config_paths,
                telemetry_client_args.no_binary_reports,
                telemetry_client_args.max_report_count,
                cli_args.max_simulation_time,
            )
            .await?;
        }
    }

    Ok(())
}

async fn run_telemetry_client(
    metrics_config_paths: &Vec<PathBuf>,
    no_binary_reports: bool,
    max_report_count: Option<u64>,
    max_simulation_time: Duration,
) -> anyhow::Result<()> {
    create_output_directory(
        Path::new(io::OUTPUT_DIR),
        if no_binary_reports { None } else { Some(Path::new(REPORTS_DIR)) },
    )?;
    let mut metrics_configs_tracker = MetricsConfigsTracker::new(!no_binary_reports);

    for path in metrics_config_paths {
        metrics_configs_tracker.parse_metrics_config(path)?;
    }

    let telemetry_service = get_telemetry_service_instance()?;
    let (mut report_ready_rx, misc_status_rx) =
        get_telemetry_service_listeners(&telemetry_service).await?;
    handle_telemetry_service_status_updates(misc_status_rx);
    let metrics_config_uuids = metrics_configs_tracker.get_metrics_config_uuids();

    for uuid in metrics_config_uuids {
        metrics_configs_tracker.activate_metrics_config(&telemetry_service, &uuid).await?;
    }

    let mut num_reports = 0;
    let mut simulator_is_done = get_finish_condition(max_simulation_time, max_report_count)?;
    while !simulator_is_done(num_reports) {
        tokio::select! {
            report_info = await_next_report(&mut report_ready_rx) => {
                let report_info = report_info?;
                let report = get_report(&telemetry_service, &report_info).await?;
                metrics_configs_tracker.process_new_report(&report_info, report)?;
                num_reports += 1;
            }
            _ = sleep(max_simulation_time) => {
                info!{"Simulator time limit reached before any reports were returned by the telemetry service."}
            }
        }
    }

    telemetry_service.remove_all_metrics_configs().await?;
    Ok(())
}

/// Parses, registers, and starts the simulation publishers as configured in the provided config
/// paths. Returns a vector of `SimulationPublisher`s wrapped in `scopeguard::ScopeGuard`, which
/// ensures that the returned publishers will automatically be unregistered when dropped.
fn parse_register_and_start_simulation_publishers(
    publisher_config_paths: &Vec<PathBuf>,
) -> anyhow::Result<
    Vec<scopeguard::ScopeGuard<Arc<SimulationPublisher>, impl FnOnce(Arc<SimulationPublisher>)>>,
> {
    let publishers = parse_simulation_publishers(publisher_config_paths)?;
    let registered_publishers = register_simulation_publishers(publishers.into_values())?;
    start_simulation_publishers(registered_publishers.iter().map(Deref::deref));
    Ok(registered_publishers)
}

fn parse_simulation_publishers(
    publisher_config_paths: &Vec<PathBuf>,
) -> Result<HashMap<String, SimulationPublisher>, Error> {
    let mut publishers = HashMap::new();
    for path in publisher_config_paths {
        let publisher_config = parse_publisher_config(path)?;
        let service_name = publisher_config.service_name.clone();
        let simulation_publisher = SimulationPublisher::new(publisher_config);
        publishers.try_insert_value(service_name.clone(), simulation_publisher).map_err(|_| {
            SimulatorError::Error {
                error_message: format!(
                    "Found more than one publisher config for the service name {service_name}"
                ),
            }
        })?;
    }
    Ok(publishers)
}

/// Registers the provided simulation publishers with the configurable publisher registry. Returns a
/// vector of `SimulationPublisher`s wrapped in `scopeguard::ScopeGuard`, which ensures that the
/// returned publishers will automatically be unregistered when dropped.
fn register_simulation_publishers(
    publishers: impl IntoIterator<Item = SimulationPublisher>,
) -> Result<
    Vec<scopeguard::ScopeGuard<Arc<SimulationPublisher>, impl FnOnce(Arc<SimulationPublisher>)>>,
    Error,
> {
    let configurable_publisher_registry_descriptor = format!(
        "{}/default",
        <BpConfigurablePublisherRegistry as IConfigurablePublisherRegistry>::get_descriptor()
    );
    let configurable_publisher_registry = binder::get_interface::<
        dyn IConfigurablePublisherRegistry,
    >(&configurable_publisher_registry_descriptor)?;

    let mut registered_publishers = vec![];
    for simulation_publisher in publishers {
        let simulation_publisher = Arc::new(simulation_publisher);

        let simulation_publisher_wrapper =
            SimulationPublisherWrapper { publisher: Arc::clone(&simulation_publisher) };

        let binder_publisher = BnConfigurablePublisher::new_binder(
            simulation_publisher_wrapper,
            BinderFeatures::default(),
        );

        let publisher_info = PublisherInfo {
            serviceName: simulation_publisher.config.service_name.clone(),
            fileDescriptorProto: simulation_publisher
                .config
                .message_descriptor
                .file_descriptor_proto()
                .write_to_bytes()
                .map_err(|error| SimulatorError::Error {
                    error_message: format!(
                        "Failed to write file descriptor proto for publisher \"{}\" to bytes: {error}",
                        simulation_publisher.config.service_name
                    ),
                })?,
            messageTypeName: format!(
                ".{}",
                simulation_publisher.config.message_descriptor.full_name()
            ),
            supportsGet: true,
        };

        configurable_publisher_registry.registerPublisher(&publisher_info, &binder_publisher)?;
        let configurable_publisher_registry_clone = configurable_publisher_registry.clone();
        registered_publishers.push(scopeguard::guard(simulation_publisher, move |simulation_publisher| {
            if let Err(err) = configurable_publisher_registry_clone.unregisterPublisher(&simulation_publisher.config.service_name) {
                error!("Failed to unregister publisher {} from configurable publisher registry: {err:?}", simulation_publisher.config.service_name);
            }
        }));
    }

    Ok(registered_publishers)
}

fn start_simulation_publishers<'a>(
    publishers: impl IntoIterator<Item = &'a Arc<SimulationPublisher>>,
) {
    for publisher in publishers {
        publisher.start_publishing();
    }
}

/// Logs miscellaneous telemetry service status updates with info level.
fn handle_telemetry_service_status_updates(
    mut misc_status_rx: UnboundedReceiver<(TelemetryServiceStatusCodes, String)>,
) {
    task::spawn(async move {
        while let Some((_, status_msg)) = misc_status_rx.recv().await {
            info!("Telemetry service sent status update: {status_msg}");
        }
    });
}

/// Returns when the receiving channel receives a new report.
async fn await_next_report(
    report_ready_tx: &mut UnboundedReceiver<ReportInfo>,
) -> Result<ReportInfo, SimulatorError> {
    report_ready_tx.recv().await.ok_or_else(|| SimulatorError::Error {
        error_message: "Report listener dropped.".to_owned(),
    })
}

/// Creates a function that returns true once the specified max number of
/// reports or the specified max duration is reached. If neither is specified,
/// it uses the default values.
fn get_finish_condition(
    max_duration: Duration,
    max_reports: Option<u64>,
) -> Result<impl FnMut(u64) -> bool, SimulatorError> {
    let (simulator_done_tx, mut simulator_done_rx) = oneshot::channel();
    task::spawn(async move {
        tokio::time::sleep(max_duration).await;
        let _ = simulator_done_tx.send(());
    });

    Ok(move |num_reports: u64| {
        let time_limit_reached = !matches!(simulator_done_rx.try_recv(), Err(TryRecvError::Empty));
        let reports_limit_reached =
            max_reports.map(|max_reports| num_reports >= max_reports).unwrap_or(false);
        time_limit_reached || reports_limit_reached
    })
}
