// Copyright 2023 Google LLC

//! Binary for registering Telemetry service.

mod telemetry_setup;

use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisherRegistry::{
    BnConfigurablePublisherRegistry, BpConfigurablePublisherRegistry, IConfigurablePublisherRegistry,
};
use log::{info, error};
use sdv_telemetry::service_publisher_manager::ServicePublisherManager;
use sdv_telemetry::ServicePublisherAccessor;
use sdv_telemetry_dt_wrapper::data_tunnel_subscriber::DeprecatedDataTunnelSubscriber;
use sdv_telemetry_configurable_publisher::{
    ConfigurablePublisherRegistry, ConfigurablePublisherRegistryService,
};
use sdv_telemetry_utils::logging::validated_log_tag;

use binder_tokio::TokioRuntime;
use std::sync::Arc;

const LOG_TAG: &str = validated_log_tag("SdvTelemetryService");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    sdv_log::init_logger(LOG_TAG)?;
    sdv_tracing::init_tracing();
    binder::ProcessState::start_thread_pool();

    // TODO (b/343460855): Remove configurable publisher from SDV after RPC support
    // Create Configurable Publishers
    let configurable_publisher_registry = Arc::new(ConfigurablePublisherRegistry::new());

    // Data Tunnel Wrapper to manage subscriptions
    let data_tunnel_wrapper = Arc::new(DeprecatedDataTunnelSubscriber::new());

    // Add all service publishers to the manager in order of priority
    let service_publishers = vec![
        Arc::clone(&configurable_publisher_registry) as Arc<dyn ServicePublisherAccessor>,
        data_tunnel_wrapper as Arc<dyn ServicePublisherAccessor>,
    ];

    // Create Service Publisher Manager
    let service_publisher_manager_impl = Arc::new(ServicePublisherManager::new(service_publishers));

    let res = telemetry_setup::register_services(service_publisher_manager_impl).await;

    if res.is_err() {
        error!("Error {:?} in initialization telemetry service", res);
    }

    let configurable_publisher_registry_descriptor = format!(
        "{}/default",
        <BpConfigurablePublisherRegistry as IConfigurablePublisherRegistry>::get_descriptor()
    );

    info!("Adding {configurable_publisher_registry_descriptor} service to binder");
    let configurable_publisher_service =
        ConfigurablePublisherRegistryService::new(configurable_publisher_registry);
    let configurable_publisher_binder = BnConfigurablePublisherRegistry::new_async_binder(
        configurable_publisher_service,
        TokioRuntime(tokio::runtime::Handle::current()),
        binder::BinderFeatures::default(),
    );
    binder::add_service(
        &configurable_publisher_registry_descriptor,
        configurable_publisher_binder.as_binder(),
    )
    .expect("Failed to register service");

    // Does not return - spawn or perform any work you mean to do before this call.
    tokio::task::block_in_place(binder::ProcessState::join_thread_pool);

    Ok(())
}
