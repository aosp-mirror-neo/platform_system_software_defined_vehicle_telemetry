// Copyright 2023 Google LLC. All Rights Reserved.

//! Binary for testing Telemetry service.

mod metrics_config_manager;
mod metrics_report_manager;

extern crate protobuf;

use log::{error, info, Level};
use std::env;
use std::ffi::CStr;
use std::fs;

use crate::metrics_config_manager::MetricsConfigManager;
use crate::metrics_report_manager::MetricsReportManager;
use sdv_telemetry_bindgen as rust_to_c;

extern "C" fn on_status(
    _listener: *mut std::ffi::c_void,
    status_code: i32,
    status_msg: *const std::ffi::c_char,
) {
    // SAFETY: This function is always safe to call
    let msg = unsafe {
        // Convert *const std::ffi::c_char to &CStr
        let c_str = CStr::from_ptr(status_msg);

        // Convert &CStr to String
        c_str.to_str().unwrap().to_string()
    };

    info!("Status code: {} status message: {}", status_code, msg);
}

/// Communicate with the telemetry service.
#[tokio::main]
async fn main() -> Result<(), binder::Status> {
    let args = env::args().collect::<Vec<_>>();

    if args.len() < 2 {
        eprintln!("Usage: {} <path_to_binary_file>", args[0]);
        return Ok(());
    }

    let path = &args[1];

    // Read binary into a vector
    let serialized_config = fs::read(path).unwrap();

    binder::ProcessState::start_thread_pool();
    let _init_success = logger::init(
        logger::Config::default()
            .with_tag_on_device("telemetry_rust_c_client")
            .with_min_level(Level::Trace),
    );

    let mut service;

    info!("Connecting to service");
    // Unsafe block needed to be able to call C API functions
    // More info at: https://doc.rust-lang.org/book/ch19-01-unsafe-rust.html
    // SAFETY: This function is always safe to call.
    unsafe {
        service = rust_to_c::get_telemetry_service();
        rust_to_c::set_status_listener(
            service,
            &mut service as *mut _ as *mut std::ffi::c_void,
            Some(on_status),
        );
    }

    let mut config_manager = MetricsConfigManager::new(service);
    let mut report_manager = MetricsReportManager::new(service);

    let (status, message) = config_manager.as_mut().add_metrics_config(&serialized_config).await;

    let activate_successful = if status {
        config_manager.as_mut().activate_metrics_config(&message).await
    } else {
        error!("Error in adding config: {}", message);
        return Ok(());
    };

    let metrics_report = if activate_successful {
        report_manager.as_mut().get_metrics_report().await.unwrap()
    } else {
        error!("Error in activating config");
        return Ok(());
    };

    info!("Report received: {:?}", metrics_report);

    let remove_config_status = config_manager.as_mut().remove_metrics_config(&message).await;

    if remove_config_status {
        info!("Successfully removed config");
    }

    Ok(())
}
