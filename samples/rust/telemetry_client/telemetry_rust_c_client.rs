// Copyright 2023 Google LLC. All Rights Reserved.

//! Binary for testing Telemetry service.
#![allow(missing_docs)]

extern crate protobuf;

use futures::future::join_all;
use log::{error, info, LevelFilter};
use std::collections::HashSet;
use std::env;
use std::fs;
use tokio::sync::mpsc;

use rust_to_c::TelemetryServiceHolder;
use sdv_telemetry_rust_wrapper as rust_to_c;

// Arbitrarily chosen buffer capacity
const MPSC_CHANNEL_CAPACITY: usize = 32;

async fn process_config(
    path: String,
    service: &TelemetryServiceHolder,
) -> Result<String, (i32, String)> {
    // Read binary into a vector
    let serialized_config = fs::read(path).unwrap();

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

/// Communicate with the telemetry service.
#[tokio::main]
async fn main() -> Result<(), binder::Status> {
    let args = env::args().collect::<Vec<_>>();

    if args.len() < 2 {
        eprintln!("Usage: {} <path_to_binary_file1> <path_to_binary_file2> ...", args[0]);
        return Ok(());
    }

    binder::ProcessState::start_thread_pool();
    let _init_success = logger::init(
        logger::Config::default()
            .with_tag_on_device("telemetry_rust_c_client")
            .with_max_level(LevelFilter::Trace),
    );

    info!("Connecting to service");
    let service = TelemetryServiceHolder::get().unwrap();

    let (status_sender, _receiver) = mpsc::channel(MPSC_CHANNEL_CAPACITY);
    service
        .set_status_listener(move |status_code, status_msg| {
            status_sender.try_send((status_code, status_msg)).unwrap();
        })
        .await
        .unwrap();

    let (sender, mut receiver) = mpsc::channel(MPSC_CHANNEL_CAPACITY);

    service
        .set_report_ready_listener(move |metrics_config_uuid, report_config_name, report_number| {
            sender.try_send((metrics_config_uuid, report_config_name, report_number)).unwrap();
        })
        .await
        .unwrap();

    let add_config_futures: Vec<_> =
        args.iter().skip(1).cloned().map(|path| process_config(path, &service)).collect();

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
