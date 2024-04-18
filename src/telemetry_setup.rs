// Copyright 2023 Google LLC. All Rights Reserved.

use google_sdv_telemetry::aidl::google::sdv::telemetry::ITelemetryService::{
    BnTelemetryService, BpTelemetryService, ITelemetryService,
};

use rustutils::system_properties::PropertyWatcher;
use sdv_telemetry::service_publisher_manager::ServicePublisherManager;
use sdv_telemetry::SdvTelemetryService;

use binder_tokio::TokioRuntime;
use log::info;
use std::sync::Arc;
use tokio::runtime::Builder;

const SIZE_MB: usize = 1024 * 1024;

static METRIC_CONFIG_STORE_SIZE_PROPERTY: &str = "ro.telemetry.metric_config_store_in_mb";
static ROOT_DIR: &str = "/data/misc";

pub fn register_services(
    service_publisher_manager_impl: Arc<ServicePublisherManager>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut watcher = PropertyWatcher::new(METRIC_CONFIG_STORE_SIZE_PROPERTY).unwrap();
    let value = watcher.read(|_, value| Ok(value.trim().to_string())).unwrap_or("1".to_string());

    let configured_size: usize = value.parse().unwrap_or(1); // Default size 1 MB

    let telemetry_runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    info!("Creating telemetry service with {}MBs metric store size", configured_size);

    let telemetry_service = match telemetry_runtime.block_on(SdvTelemetryService::new(
        ROOT_DIR,
        configured_size * SIZE_MB,
        service_publisher_manager_impl,
    )) {
        Some(telemetry_service) => telemetry_service,
        None => {
            return Err("Error in initialization telemetry service. Aborting.".into());
        }
    };

    let telemetry_descriptor =
        format!("{}/default", <BpTelemetryService as ITelemetryService>::get_descriptor());

    println!("Adding {telemetry_descriptor} service to binder");
    let telemetry_binder = BnTelemetryService::new_async_binder(
        telemetry_service,
        TokioRuntime(telemetry_runtime),
        binder::BinderFeatures::default(),
    );
    binder::add_service(&telemetry_descriptor, telemetry_binder.as_binder())
        .expect("Failed to register service");

    Ok(())
}
