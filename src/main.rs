// Copyright 2023 Google LLC. All Rights Reserved.

//! Binary for registering Telemetry service.

mod telemetry_setup;

use log::error;
use sdv_telemetry::service_publisher_manager::ServicePublisherManager;
use sdv_telemetry::ServicePublisherAccessor;
use sdv_telemetry_dt_wrapper::data_tunnel_subscriber::DataTunnelSubscriber;

use std::sync::Arc;

const LOG_TAG: &str = "SdvTelemetryService";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    sdv_log::init_logger(LOG_TAG)?;
    sdv_tracing::init_tracing();

    // Data Tunnel Wrapper to manage subscriptions
    let data_tunnel_wrapper = Arc::new(DataTunnelSubscriber::new());

    // Add all service publishers to the manager in order of priority
    let service_publishers = vec![data_tunnel_wrapper as Arc<dyn ServicePublisherAccessor>];

    // Create Service Publisher Manager
    let service_publisher_manager_impl = Arc::new(ServicePublisherManager::new(service_publishers));

    let res = telemetry_setup::register_services(service_publisher_manager_impl);

    if res.is_err() {
        error!("Error {:?} in initialization telemetry service", res);
    }

    binder::ProcessState::start_thread_pool();
    // Does not return - spawn or perform any work you mean to do before this call.
    binder::ProcessState::join_thread_pool();

    Ok(())
}
