// Copyright 2023 Google LLC. All Rights Reserved.

//! Binary for testing Telemetry service.
mod metrics_config_manager;
mod metrics_report_manager;

extern crate protobuf;

use futures::future::join_all;
use log::{error, info, LevelFilter};
use std::collections::HashSet;
use std::env;
use std::ffi::CStr;
use std::fs;
use std::pin::Pin;
use std::sync::Arc;

use crate::metrics_config_manager::MetricsConfigManager;
use crate::metrics_report_manager::MetricsReportManager;
use sdv_telemetry_bindgen as rust_to_c;

extern "C" fn on_status(
    _listener: *mut std::ffi::c_void,
    status_code: i32,
    status_msg: *const std::ffi::c_char,
) {
    // SAFETY: The caller has provided pointers that satisfy `CStr::from_ptr`
    // safety requirements.
    let msg = unsafe {
        // Convert *const std::ffi::c_char to &CStr
        let c_str = CStr::from_ptr(status_msg);

        // Convert &CStr to String
        c_str.to_str().unwrap().to_string()
    };

    info!("Status code: {} status message: {}", status_code, msg);
}

async fn process_config(
    path: String,
    config_manager: Arc<Pin<Box<MetricsConfigManager>>>,
) -> Option<String> {
    // Read binary into a vector
    let serialized_config = fs::read(path).unwrap();

    let config_name;
    let add_config_res = config_manager.add_metrics_config(&serialized_config).await;

    let activate_config_res = match add_config_res {
        Ok(message) => {
            config_name = message.clone();
            config_manager.activate_metrics_config(&message).await
        }
        Err(e) => {
            error!("Error in adding config: {}", e);
            return None;
        }
    };

    match activate_config_res {
        Ok(_) => Some(config_name),
        Err(e) => {
            error!("Error in activating config: {}", e);
            None
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

    let mut service;

    info!("Connecting to service");
    // Unsafe block needed to be able to call C API functions
    // More info at: https://doc.rust-lang.org/book/ch19-01-unsafe-rust.html
    // SAFETY: safe to use as all pointers needed to satify safety requirements
    // for on_status are passed.
    unsafe {
        service = rust_to_c::get_telemetry_service();
        rust_to_c::set_status_listener(
            service,
            &mut service as *mut _ as *mut std::ffi::c_void,
            Some(on_status),
        );
    }

    let mut report_manager = MetricsReportManager::new(service);
    let config_manager = Arc::new(MetricsConfigManager::new(service));

    let mut add_config_futures = Vec::with_capacity(args.len() - 1);

    let config_paths: Vec<String> = args.iter().skip(1).cloned().collect();
    for path in config_paths {
        add_config_futures.push(process_config(path, config_manager.clone()));
    }

    let add_config_results = join_all(add_config_futures).await;
    let mut active_configs = HashSet::new();

    // Collect all activated configs
    for res in add_config_results.into_iter().flatten() {
        info!("Config added successfully: {}", res.clone());
        active_configs.insert(res);
    }

    while !active_configs.is_empty() {
        let metrics_report = report_manager.as_mut().get_metrics_report().await.unwrap();
        info!("Report received: {:?}", metrics_report);
        if active_configs.contains(&metrics_report.metrics_config_uuid) {
            let remove_config_res = config_manager
                .as_ref()
                .remove_metrics_config(&metrics_report.metrics_config_uuid)
                .await;
            match remove_config_res {
                Ok(_) => {
                    info!("Successfully removed config");
                    active_configs.remove(&metrics_report.metrics_config_uuid);
                }
                Err(e) => {
                    error!("Error in removing config: {}", e);
                }
            }
        }
    }

    // Unsafe block needed to be able to call C API functions
    // More info at: https://doc.rust-lang.org/book/ch19-01-unsafe-rust.html
    // SAFETY: safe to use as service is a valid pointer to ServiceHolder.
    unsafe {
        rust_to_c::clear_status_listener(service);
        rust_to_c::clear_report_ready_listener(service);
    }

    Ok(())
}
