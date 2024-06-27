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
mod simulator_errors;
mod telemetry_service_handler;
mod util;

use clap::Parser;
use log::{error, info};
use sdv_telemetry_rust_wrapper::TelemetryServiceStatusCodes;
use sdv_telemetry_utils::logging::validated_log_tag;
use std::time::Duration;
use tokio::{
    sync::{mpsc::UnboundedReceiver, oneshot, oneshot::error::TryRecvError},
    task,
    time::sleep,
};

use crate::{
    io::create_output_directory,
    metrics_configs_tracker::MetricsConfigsTracker,
    publisher_config_parser::parse_publisher_config,
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
    let cli_args = cli::Args::parse();
    info!("Running simulator with args:\n{cli_args}");

    create_output_directory(!cli_args.no_binary_reports)?;

    let mut metrics_configs_tracker = MetricsConfigsTracker::new(!cli_args.no_binary_reports);

    for path in &cli_args.metrics_config_paths {
        metrics_configs_tracker.parse_metrics_config(path)?;
    }

    for path in &cli_args.publisher_config_paths {
        let publisher_config = parse_publisher_config(path)?;
        info!("Parsed publisher config:\n{}", publisher_config);
    }

    binder::ProcessState::start_thread_pool();
    let telemetry_service = get_telemetry_service_instance()?;
    let (mut report_ready_rx, misc_status_rx) =
        get_telemetry_service_listeners(&telemetry_service).await?;
    handle_telemetry_service_status_updates(misc_status_rx);

    metrics_configs_tracker.activate_metrics_configs(&telemetry_service).await?;

    let mut num_reports = 0;
    let mut simulator_is_done =
        get_finish_condition(cli_args.max_simulation_time, cli_args.max_report_count)?;

    while !simulator_is_done(num_reports) {
        tokio::select! {
            report_info = await_next_report(&mut report_ready_rx) => {
                let report_info = report_info?;
                let report = get_report(&telemetry_service, &report_info).await?;
                 metrics_configs_tracker.process_new_report(&report_info, report)?;
                num_reports += 1;
            }
            _ = sleep(cli_args.max_simulation_time) => {
                info!{"Simulator time limit reached before any reports were returned by the telemetry service."}
            }
        }
    }

    telemetry_service.remove_all_metrics_configs().await?;
    telemetry_service.finish()?;
    Ok(())
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

struct ReportInfo {
    metrics_config_uuid: String,
    report_config_name: String,
    report_number: i32,
}

impl ReportInfo {
    fn new(
        (metrics_config_uuid, report_config_name, report_number): (String, String, i32),
    ) -> Self {
        Self { metrics_config_uuid, report_config_name, report_number }
    }
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
    max_reports: u64,
) -> Result<impl FnMut(u64) -> bool, SimulatorError> {
    let (simulator_done_tx, mut simulator_done_rx) = oneshot::channel();
    task::spawn(async move {
        tokio::time::sleep(max_duration).await;
        let _ = simulator_done_tx.send(());
    });

    Ok(move |num_reports: u64| {
        let time_limit_reached = !matches!(simulator_done_rx.try_recv(), Err(TryRecvError::Empty));
        let reports_limit_reached = num_reports >= max_reports;
        time_limit_reached || reports_limit_reached
    })
}
