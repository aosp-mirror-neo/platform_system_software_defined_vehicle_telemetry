// Copyright 2023 Google LLC
/// Telemetry Service device integration tests
#[cfg(test)]
mod metrics_config_manager;
mod metrics_report_manager;

use crate::metrics_config_manager::MetricsConfigManager;
use crate::metrics_report_manager::MetricsReportManager;
extern crate protobuf;
use anyhow::{anyhow, Context};
use protobuf::text_format::parse_from_str;
use sdv_telemetry_bindgen as rust_to_c;
use sdv_telemetry_sample_utils::metrics_config_reader_utils;
use sdv_telemetry_utils::logging::{init_logger_for_samples_or_tests, validated_log_tag};
use sdvtelemetry_proto_rust::metrics_configuration::MetricsConfig;

use log::info;
use std::ffi::{CStr, OsStr};
use std::fs;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use tokio::sync::oneshot;

const CONFIG_PATH: &str = "tests/config.textproto";
const LOG_TAG: &str = validated_log_tag("telemetry_integr_test");

/// Callback that receives status for API calls.
///
/// # Safety
///
/// * `status_msg` must point to C raw strings that satisfy `CStr::from_ptr` safety requirements.
///
/// * `listener` points to a valid `oneshot::Sender` originally sent to the caller during
///    the add_metrics_config api call. Listener will not be valid after the function completes
pub unsafe extern "C" fn api_callback(
    listener: *mut std::ffi::c_void,
    status_code: i32,
    status_msg: *const std::ffi::c_char,
) {
    // SAFETY: safe to use as the listener passed by the caller was originally allocated using a
    // global allocator using Box::new(). For more info check
    // https://doc.rust-lang.org/std/boxed/index.html#memory-layout
    let sender = unsafe { Box::from_raw(listener as *mut oneshot::Sender<(i32, String)>) };

    // SAFETY: The caller has provided pointers that satisfy `CStr::from_ptr`
    // safety requirements.
    let msg = unsafe {
        // Convert *const std::ffi::c_char to &CStr
        let c_str = CStr::from_ptr(status_msg);

        // Convert &CStr to String
        c_str.to_str().unwrap().to_string()
    };
    info!("status code: {} msg: {}", status_code, msg);
    sender.send((status_code, msg)).unwrap();
}

async fn process_config(
    path: &PathBuf,
    config_manager: &Pin<Box<MetricsConfigManager>>,
) -> anyhow::Result<String> {
    let serialized_config = metrics_config_reader_utils::read(path)
        .with_context(|| format!("Error while reading config: {}", path.display()))?;

    let config_name = config_manager
        .add_metrics_config(&serialized_config)
        .await
        .map_err(|e| anyhow!(e))
        .with_context(|| format!("Error while adding config: {}", path.display()))?;

    config_manager
        .activate_metrics_config(&config_name)
        .await
        .map_err(|e| anyhow!(e))
        .with_context(|| format!("Error while activating config: {}", &config_name))?;

    Ok(config_name)
}

/// Communicate with the telemetry service.
#[tokio::test]
async fn telemetry_service_simple_config_end_to_end_test() {
    init_logger_for_samples_or_tests(LOG_TAG);
    binder::ProcessState::start_thread_pool();

    info!("Connecting to service");
    // Unsafe block needed to be able to call C API functions
    // More info at: https://doc.rust-lang.org/book/ch19-01-unsafe-rust.html
    // SAFETY: safe to use as the return value is asserted against before moving forward.
    let service = unsafe { rust_to_c::get_telemetry_service() };
    assert!(!service.is_null());

    // SAFETY: safe to use as the service passed is non null and points to a valid ServiceHolder.
    let (mut report_manager, config_manager) =
        unsafe { (MetricsReportManager::new(service).await, MetricsConfigManager::new(service)) };

    let config_path = PathBuf::from(CONFIG_PATH);
    info!("Adding config: {}", config_path.display());
    let config_name = process_config(&config_path, &config_manager).await.unwrap();
    info!("Config added: {}", config_name);

    let metrics_report = report_manager.as_mut().get_metrics_report().await.unwrap();
    info!("Report received: {:?}", metrics_report);

    config_manager.remove_metrics_config(&metrics_report.metrics_config_uuid).await.unwrap();
    info!("successfully removed config");
}

fn read_files_recursively(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut result = vec![];
    if dir.is_dir() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                result.append(&mut read_files_recursively(&path)?);
            } else {
                result.push(entry.path());
            }
        }
    }
    Ok(result)
}

#[test]
fn check_textprotos_are_valid() -> anyhow::Result<()> {
    let paths = read_files_recursively(Path::new("."))?
        .into_iter()
        .filter(|path| path.extension() == Some(OsStr::new("textproto")))
        .collect::<Vec<_>>();

    let mut expected_paths = paths
        .iter()
        .map(|path| path.to_str().ok_or(anyhow!("Invalid path")))
        .collect::<anyhow::Result<Vec<_>>>()?;
    expected_paths.sort();

    // This assert is here to verify that the test is actually doing something.
    // Add new `.textproto` files to the assert below if it fails.
    assert_eq!(
        expected_paths,
        vec![
            "./samples/sample_protos/configurable_publisher.textproto",
            "./samples/sample_protos/data_tunnel_publisher.textproto",
            "./samples/sample_protos/data_tunnel_publisher_aggregation.textproto",
            "./samples/sample_protos/metric_configJan10.textproto",
            "./tests/config.textproto"
        ]
    );

    for path in paths {
        eprintln!("Attempting to parse: {}", path.display());
        let textproto = fs::read_to_string(path)?;
        let _proto = parse_from_str::<MetricsConfig>(&textproto)?;
    }

    Ok(())
}
