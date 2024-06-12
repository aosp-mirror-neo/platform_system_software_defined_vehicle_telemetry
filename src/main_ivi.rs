// Copyright 2023 Google LLC. All Rights Reserved.

//! Binary for registering Telemetry service.

mod telemetry_setup;

use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisherRegistry::{
    BnConfigurablePublisherRegistry, BpConfigurablePublisherRegistry, IConfigurablePublisherRegistry,
};
use sdv_telemetry::service_publisher_manager::ServicePublisherManager;
use sdv_telemetry::ServicePublisherAccessor;
use sdv_telemetry_configurable_publisher::{
    ConfigurablePublisherRegistry, ConfigurablePublisherRegistryService,
};
use sdv_telemetry_utils::logging::validated_log_tag;

use binder_tokio::TokioRuntime;
use log::error;
use std::sync::Arc;
use tokio::runtime::Builder;

const LOG_TAG: &str = validated_log_tag("SdvTelemetryService");

fn main() -> Result<(), Box<dyn std::error::Error>> {
    sdv_log::init_logger(LOG_TAG)?;
    sdv_tracing::init_tracing();

    // Create Configurable Publishers
    let configurable_publisher_registry = Arc::new(ConfigurablePublisherRegistry::new());

    // Add all service publishers to the manager in order of priority
    let service_publishers =
        vec![Arc::clone(&configurable_publisher_registry) as Arc<dyn ServicePublisherAccessor>];

    // Create Service Publisher Manager
    let service_publisher_manager_impl = Arc::new(ServicePublisherManager::new(service_publishers));

    let res = telemetry_setup::register_services(service_publisher_manager_impl);

    if res.is_err() {
        error!("Error {:?} in initialization telemetry service", res);
    }

    let configurable_publisher_registry_descriptor = format!(
        "{}/default",
        <BpConfigurablePublisherRegistry as IConfigurablePublisherRegistry>::get_descriptor()
    );

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
        &configurable_publisher_registry_descriptor,
        configurable_publisher_binder.as_binder(),
    )
    .expect("Failed to register service");

    binder::ProcessState::start_thread_pool();
    // Does not return - spawn or perform any work you mean to do before this call.
    binder::ProcessState::join_thread_pool();

    Ok(())
}
