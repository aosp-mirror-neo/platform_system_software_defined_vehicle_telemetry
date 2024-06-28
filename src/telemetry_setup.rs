// Copyright 2023 Google LLC

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

const METRIC_CONFIG_STORE_SIZE_PROPERTY: &str = "ro.vendor.telemetry.metric_config_store_in_mb";
const ROOT_DIR: &str = "/data/vendor/";

pub fn register_services(
    service_publisher_manager_impl: Arc<ServicePublisherManager>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut watcher = PropertyWatcher::new(METRIC_CONFIG_STORE_SIZE_PROPERTY).unwrap();
    let value = watcher.read(|_, value| Ok(value.trim().to_owned())).ok();

    // Default size 1 MB
    let configured_size: usize = value.and_then(|value| value.parse().ok()).unwrap_or(1);

    let telemetry_runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    info!("Creating telemetry service with {}MBs metric store size", configured_size);

    let Some(telemetry_service) = telemetry_runtime.block_on(SdvTelemetryService::new(
        ROOT_DIR,
        configured_size * SIZE_MB,
        service_publisher_manager_impl,
    )) else {
        return Err("Error in initialization telemetry service. Aborting.".into());
    };

    let telemetry_descriptor =
        format!("{}/default", <BpTelemetryService as ITelemetryService>::get_descriptor());

    println!("Adding {telemetry_descriptor} service to binder");
    let telemetry_binder = BnTelemetryService::new_async_binder(
        telemetry_service,
        TokioRuntime(telemetry_runtime),
        binder::BinderFeatures::default(),
    );

    info!("Adding telemetry service");
    binder::add_service(&telemetry_descriptor, telemetry_binder.as_binder())
        .expect("Failed to register service");

    println!("Service {telemetry_descriptor} added to binder");

    Ok(())
}
