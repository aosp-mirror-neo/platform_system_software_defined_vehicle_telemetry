// Copyright 2023 Google LLC. All Rights Reserved.

//! Binary for testing Telemetry service.
#![allow(missing_docs)]

extern crate clap;
extern crate protobuf;

use crate::clap::Parser;
use crate::protobuf::Message;
use futures::future::join_all;
use log::{error, info, LevelFilter};
use std::collections::HashSet;
use std::fs;
use std::time::SystemTime;
use tokio::sync::mpsc;

use rust_to_c::TelemetryServiceHolder;
use sdv_telemetry_rust_wrapper as rust_to_c;
use sdv_telemetry_sample_utils::metrics_config_reader_utils;

// Arbitrarily chosen buffer capacity
const MPSC_CHANNEL_CAPACITY: usize = 32;

async fn process_config(
    path: std::path::PathBuf,
    service: &TelemetryServiceHolder,
) -> Result<String, (i32, String)> {
    if !path.is_file() {
        let error_message = format!("{:?} is not a file or does not exist", path);
        error!("{error_message}");
        return Err((
            rust_to_c::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_UNKNOWN_ERROR,
            error_message,
        ));
    }

    // Metrics config is either read directly from a binary file into serialized config or from a
    // textproto file and then converted into serialized config.
    let serialized_config = metrics_config_reader_utils::read(&path).map_err(|e| {
        (rust_to_c::TelemetryServiceStatusCodes_TELEMETRY_SERVICE_UNKNOWN_ERROR, e.to_string())
    })?;

    let config_name;
    let add_config_res = service.add_metrics_config(&serialized_config).await;

    let activate_config_res = match add_config_res {
        Ok(message) => {
            config_name = message.clone();
            service.activate_metrics_config(&message).await
        }
        Err(e) => {
            error!("Error in adding config: {:?}", e);
            return Err(e);
        }
    };

    match activate_config_res {
        Ok(_) => Ok(config_name),
        Err(e) => {
            error!("Error in activating config: {:?}", e);
            Err(e)
        }
    }
}

#[derive(Parser, Debug)]
#[command(allow_missing_positional = true, long_about = None)]
struct Args {
    /// Folder to write reports to
    #[arg(long = "write-reports-to", default_value = None, value_hint = clap::ValueHint::DirPath)]
    out_folder: Option<std::path::PathBuf>,

    /// Config files to execute
    #[arg(required = true, num_args = 1.., trailing_var_arg = true, value_hint = clap::ValueHint::FilePath)]
    configs: Vec<std::path::PathBuf>,
}

/// Communicate with the telemetry service.
#[tokio::main]
async fn main() -> Result<(), binder::Status> {
    let args = Args::parse();

    binder::ProcessState::start_thread_pool();
    let _init_success = logger::init(
        logger::Config::default()
            .with_tag_on_device("telemetry_rust_c_client")
            .with_max_level(LevelFilter::Trace),
    );

    info!("Connecting to service");
    let service = TelemetryServiceHolder::get().unwrap();

    let (sender, mut receiver) = mpsc::channel(MPSC_CHANNEL_CAPACITY);

    service
        .set_report_ready_listener(move |metrics_config_uuid, report_config_name, report_number| {
            sender.try_send((metrics_config_uuid, report_config_name, report_number)).unwrap();
        })
        .await
        .unwrap();

    let add_config_futures: Vec<_> =
        args.configs.iter().cloned().map(|path| process_config(path, &service)).collect();

    let add_config_results = join_all(add_config_futures).await;
    let mut active_configs = HashSet::new();

    // Collect all activated configs
    for res in add_config_results.into_iter().flatten() {
        info!("Config added successfully: {:?}", res);
        active_configs.insert(res);
    }

    while !active_configs.is_empty() {
        // SAFETY: The data inside the mutable reference will never move out of the
        // mutable reference and the function is safe to call.
        let (config_uuid, config_name, report_number) = receiver.recv().await.unwrap();
        let metrics_report =
            service.get_metrics_report(config_uuid, config_name, report_number).await.unwrap();

        let report_name = &metrics_report.report_config_name;
        let time_marker =
            SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos();

        if let Some(report_directory) = args.out_folder.as_ref() {
            if !report_directory.is_dir() {
                error!("Report cannot be written to a file because {:?} is not a directory or does not exist", report_directory);
            } else {
                let report_file_path =
                    report_directory.join(format!("{time_marker}_{report_name}.pb"));
                match fs::File::create(report_file_path) {
                    Err(error) => error!("Error creating report file: {}", error),
                    Ok(mut file) => {
                        if let Err(e) = metrics_report.write_to_writer(&mut file) {
                            error!("Error in writing report file: {}", e);
                        };
                    }
                };
            }
        }

        if active_configs.contains(&metrics_report.metrics_config_uuid) {
            let remove_config_res =
                service.remove_metrics_config(&metrics_report.metrics_config_uuid).await;
            match remove_config_res {
                Ok(_) => {
                    info!("Successfully removed config");
                    active_configs.remove(&metrics_report.metrics_config_uuid);
                }
                Err(e) => {
                    error!("Error in removing config: {:?}", e);
                }
            }
        }
    }

    service.clear_status_listener().await.unwrap();
    service.clear_report_ready_listener().await.unwrap();

    Ok(())
}
