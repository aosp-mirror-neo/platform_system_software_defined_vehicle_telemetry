// Copyright 2023 Google LLC. All Rights Reserved.

//! Binary for registering Telemetry service.
use android_sdv_telemetry_configurable_publisher::aidl::android::sdv::telemetry::configurable_publisher::IConfigurablePublisherRegistry::{
    BnConfigurablePublisherRegistry, BpConfigurablePublisherRegistry, IConfigurablePublisherRegistry,
};
use android_sdv_telemetry::aidl::android::sdv::telemetry::ITelemetryService::{
    BnTelemetryService, BpTelemetryService, ITelemetryService,
};

use rustutils::system_properties::PropertyWatcher;
use sdv_telemetry::SdvTelemetryService;
use sdv_telemetry_configurable_publisher::{
    ConfigurablePublisherRegistryImpl, ConfigurablePublisherRegistryService,
};

use binder_tokio::TokioRuntime;
use log::{error, info};
use std::sync::Arc;
use tokio::runtime::Builder;

const SIZE_MB: usize = 1024 * 1024;

static METRIC_CONFIG_STORE_SIZE_PROPERTY: &str = "ro.telemetry.metric_config_store_in_mb";
static LOG_TAG: &str = "SdvTelemetryService";
static ROOT_DIR: &str = "/data/local/tmp";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    sdv_log::init_logger(LOG_TAG)?;

    // Create Configurable Publishers

    let configurable_publisher_registry = Arc::new(ConfigurablePublisherRegistryImpl::new());

    let mut watcher = PropertyWatcher::new(METRIC_CONFIG_STORE_SIZE_PROPERTY).unwrap();
    let value = watcher.read(|_, value| Ok(value.trim().to_string())).unwrap_or("1".to_string());

    let configured_size: usize = value.parse().unwrap_or(1); // Default size 1 MB

    let telemetry_runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    info!("Creating telemetry service with {}MBs metric store size", configured_size);

    let telemetry_service = match telemetry_runtime.block_on(SdvTelemetryService::new(
        ROOT_DIR,
        configured_size * SIZE_MB,
        configurable_publisher_registry.clone(),
    )) {
        Some(telemetry_service) => telemetry_service,
        None => {
            error!("Error in initialization telemetry service. Aborting.");
            return Err("Error in initialization telemetry service. Aborting.".into());
        }
    };

    let telemetry_descriptor = <BpTelemetryService as ITelemetryService>::get_descriptor();

    println!("Adding {telemetry_descriptor} service to binder");
    let telemetry_binder = BnTelemetryService::new_async_binder(
        telemetry_service,
        TokioRuntime(telemetry_runtime),
        binder::BinderFeatures::default(),
    );
    binder::add_service(telemetry_descriptor, telemetry_binder.as_binder())
        .expect("Failed to register service");

    let configurable_publisher_registry_descriptor =
        <BpConfigurablePublisherRegistry as IConfigurablePublisherRegistry>::get_descriptor();

    println!("Adding {configurable_publisher_registry_descriptor} service to binder");
    let configurable_publisher_runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    let configurable_publisher_service =
        ConfigurablePublisherRegistryService::new(configurable_publisher_registry);
    let configurable_publisher_binder = BnConfigurablePublisherRegistry::new_async_binder(
        configurable_publisher_service,
        TokioRuntime(configurable_publisher_runtime),
        binder::BinderFeatures::default(),
    );
    binder::add_service(
        configurable_publisher_registry_descriptor,
        configurable_publisher_binder.as_binder(),
    )
    .expect("Failed to register service");

    binder::ProcessState::start_thread_pool();
    // Does not return - spawn or perform any work you mean to do before this call.
    binder::ProcessState::join_thread_pool();

    Ok(())
}
