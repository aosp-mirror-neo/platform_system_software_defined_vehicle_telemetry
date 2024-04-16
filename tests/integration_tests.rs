// Copyright 2023 Google LLC
/// Telemetry Service device integration tests
#[cfg(test)]
mod metrics_config_manager;
mod metrics_report_manager;

use crate::metrics_config_manager::MetricsConfigManager;
use crate::metrics_report_manager::MetricsReportManager;
extern crate protobuf;
use sdv_telemetry_bindgen as rust_to_c;

use log::{error, info, LevelFilter};
use std::ffi::CStr;
use std::fs;
use std::pin::Pin;
use std::sync::Arc;

const CONFIG_PATH: &str = "tests/config.bin";

unsafe extern "C" fn on_status(
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
#[tokio::test]
async fn telemetry_service_simple_config_end_to_end_test() {
    binder::ProcessState::start_thread_pool();
    logger::init(
        logger::Config::default()
            .with_tag_on_device("telemetry_service_integration_test")
            .with_max_level(LevelFilter::Trace),
    );

    let service;

    info!("Connecting to service");
    // Unsafe block needed to be able to call C API functions
    // More info at: https://doc.rust-lang.org/book/ch19-01-unsafe-rust.html
    // SAFETY: safe to use as all pointers needed to satisfy the safety requirements
    // for on_status are passed.
    unsafe {
        service = rust_to_c::get_telemetry_service();
        rust_to_c::set_status_listener(service, std::ptr::null_mut(), Some(on_status));
    }

    let mut report_manager = MetricsReportManager::new(service);
    let config_manager = Arc::new(MetricsConfigManager::new(service));

    let config_path = CONFIG_PATH.to_string();
    let config_name = process_config(config_path, config_manager.clone()).await.unwrap();
    info!("Config added: {:?}", config_name);

    let metrics_report = report_manager.as_mut().get_metrics_report().await.unwrap();
    info!("Report received: {:?}", metrics_report);

    config_manager
        .as_ref()
        .remove_metrics_config(&metrics_report.metrics_config_uuid)
        .await
        .unwrap();
    info!("Successfully removed config");

    // Unsafe block needed to be able to call C API functions
    // More info at: https://doc.rust-lang.org/book/ch19-01-unsafe-rust.html
    // SAFETY: safe to use as service is a valid pointer to ServiceHolder.
    unsafe {
        rust_to_c::clear_status_listener(service);
    }
}
