// Copyright 2024 Google LLC

//! This module includes utility functions used in Telemetry for interacting with the new Comm's
//! stack.

use log::info;
use tokio::time::{sleep, Duration};

use sdv_comms::id::ServiceFqin;
use sdv_comms::ContextRef;

use google_sdv_service_discovery_discovery_aidl::aidl::google::sdv::service_discovery::discovery::IServiceDiscoveryAgent::{BpServiceDiscoveryAgent, IServiceDiscoveryAgent};

/// Waits for Service Discovery to be ready and then converts the provided FQIN into a `ContextRef`
/// object.
pub async fn wait_for_service_discovery_ready_and_create_context(fqin: ServiceFqin) -> ContextRef {
    wait_for_service_discovery().await;
    // This just crashes if the Service Discovery is not yet ready.
    ContextRef::create(fqin)
}

async fn wait_for_service_discovery() {
    let descriptor = <BpServiceDiscoveryAgent as IServiceDiscoveryAgent>::get_descriptor()
        .to_owned()
        + "/default";

    loop {
        let interface =
            binder_tokio::wait_for_interface::<dyn IServiceDiscoveryAgent>(&descriptor).await;
        if interface.is_ok() {
            return;
        }
        info!("Retrying wait for service discovery.");
        sleep(Duration::from_secs(10)).await;
    }
}
