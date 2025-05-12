// Copyright 2023 Google LLC

//! Binary for testing Telemetry service.

use anyhow::{anyhow, Context};
use clap::Parser;
use futures::future::join_all;
use log::{error, info, trace};
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
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use tokio::sync::mpsc;
use tokio::task;

const LOG_TAG: &str = validated_log_tag("TelemetryRustCClient");

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
    /// Folder to write reports to
    #[arg(long = "write-reports-to", default_value = None, value_hint = clap::ValueHint::DirPath)]
    out_folder: Option<PathBuf>,

    /// Config files to execute
    #[arg(required = true, num_args = 1.., trailing_var_arg = true, value_hint = clap::ValueHint::FilePath)]
    configs: Vec<PathBuf>,
}

/// Communicate with the telemetry service.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logger_for_samples_or_tests(LOG_TAG);

    // `wrapped_main` is used solely to make sure that any errors are logged via
    // `error!` so that they show up in logs.
    wrapped_main().await.inspect_err(|err| {
        error!("{err:?}");
    })
}

async fn wrapped_main() -> anyhow::Result<()> {
    let args = Args::parse();
    binder::ProcessState::start_thread_pool();

    info!("Connecting to service");
    let service = TelemetryServiceHolder::get()?;

    // Note: Creating an unbounded channel will consume unbounded memory if this
    // sample client does not read reports fast enough. Consider using a bounded
    // channel in real-world applications.
    let (report_ready_tx, mut report_ready_rx) = mpsc::unbounded_channel();

    // Note: Creating an unbounded channel will consume unbounded memory if this
    // sample client does not read misc status fast enough. Consider using a bounded
    // channel in real-world applications.
    let (misc_status_tx, mut misc_status_rx) = mpsc::unbounded_channel::<TelemetryServiceStatus>();

    // Spawn a task to log all received misc status callbacks.
    // Note: In real world application this should happen in a separate method, and extra logic needs
    // to be added to take appropriate actions based on the status codes received.
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
    let mut active_configs = add_config_results.into_iter().flatten().collect::<HashSet<_>>();
    trace!("Active configs: {active_configs:?}");

    while !active_configs.is_empty() {
        let Some(report_info) = report_ready_rx.recv().await else {
            error!("Report ready callback dropped while still expecting additional reports.");
            break;
        };
        let metrics_report = service.get_metrics_report(&report_info.report_uuid).await?;

        if let Some(report_directory) = args.out_folder.as_ref() {
            if !report_directory.is_dir() {
                error!("Report cannot be written to a file because {} is not a directory or does not exist", report_directory.display());
            } else {
                let time_marker =
                    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos();
                let report_name = &metrics_report.report_config_name;
                let report_file_path =
                    report_directory.join(format!("{time_marker}_{report_name}.pb"));
                match fs::File::create(report_file_path) {
                    Err(error) => error!("Error creating report file: {}", error),
                    Ok(mut file) => {
                        if let Err(e) = metrics_report.write_to_writer(&mut file) {
                            error!("Error while writing report file: {}", e);
                        };
                    }
                };
            }
        }

        if active_configs.contains(&metrics_report.metrics_config_uuid) {
            match service.remove_metrics_config(&metrics_report.metrics_config_uuid).await {
                Ok(_) => {
                    info!(
                        "Successfully removed config with UUID {}",
                        metrics_report.metrics_config_uuid
                    );
                    active_configs.remove(&metrics_report.metrics_config_uuid);
                }
                Err(e) => {
                    error!(
                        "Error while removing config with UUID {}: {e:?}",
                        metrics_report.metrics_config_uuid
                    );
                }
            }
        } else {
            error!("Received a metrics report for a config with UUID {} that should no longer be active.", metrics_report.metrics_config_uuid)
        }
    }

    service.finish()?;

    Ok(())
}
