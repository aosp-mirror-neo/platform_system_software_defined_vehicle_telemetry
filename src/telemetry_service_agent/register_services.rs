// Copyright 2023 Google LLC

use google_sdv_telemetry::aidl::google::sdv::telemetry::ITelemetryService::{
    BnTelemetryService, BpTelemetryService, ITelemetryService,
};

use rustutils::android::system_properties::PropertyWatcher;
use sdv_telemetry::service_publisher_manager::ServicePublisherManager;
use sdv_telemetry::SdvTelemetryService;
use sdv_telemetry_platform_abstraction_layer::PlatformAbstractionLayer;

use binder_tokio::TokioRuntime;
use log::info;
use std::sync::Arc;

const SIZE_MB: usize = 1024 * 1024;

const METRIC_CONFIG_STORE_SIZE_PROPERTY: &str = "ro.vendor.telemetry.metric_config_store_in_mb";
const ROOT_DIR: &str = "/data/vendor/";

pub async fn register_services(
    service_publisher_manager_impl: Arc<ServicePublisherManager>,
    pal: Arc<dyn PlatformAbstractionLayer>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut watcher = PropertyWatcher::new(METRIC_CONFIG_STORE_SIZE_PROPERTY).unwrap();
    let value = watcher.read(|_, value| value.trim().to_owned()).ok();

    // Default size 1 MB
    let configured_size: usize = value.and_then(|value| value.parse().ok()).unwrap_or(1);

    info!("Creating telemetry service with {configured_size}MBs metric store size");

    let Some(telemetry_service) = SdvTelemetryService::new(
        ROOT_DIR,
        configured_size * SIZE_MB,
        service_publisher_manager_impl,
        pal,
    )
    .await
    else {
        return Err("Error in initialization telemetry service. Aborting.".into());
    };

    let telemetry_descriptor =
        format!("{}/default", <BpTelemetryService as ITelemetryService>::get_descriptor());

    info!("Adding {telemetry_descriptor} service to binder");
    let telemetry_binder = BnTelemetryService::new_async_binder(
        telemetry_service,
        TokioRuntime(tokio::runtime::Handle::current()),
        binder::BinderFeatures::default(),
    );

    binder::add_service(&telemetry_descriptor, telemetry_binder.as_binder())
        .expect("Failed to register service");

    info!("Service {telemetry_descriptor} added to binder");

    Ok(())
}
