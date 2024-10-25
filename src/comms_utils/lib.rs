// Copyright 2024 Google LLC

//! This module includes utility functions used in Telemetry for interacting with the new Comm's
//! stack.

use log::info;
use tokio::time::{sleep, Duration};

use sdv_comms::id::ServiceFqin;
use sdv_comms::ContextRef;
use sdv_comms_ctx::wait_for_agents;

/// Waits for Service Discovery to be ready and then converts the provided FQIN into a `ContextRef`
/// object.
pub async fn wait_for_service_discovery_ready_and_create_context(fqin: ServiceFqin) -> ContextRef {
    wait_for_comms().await;
    // This just crashes if the Service Discovery is not yet ready.
    ContextRef::create(fqin)
}

async fn wait_for_comms() {
    loop {
        let Err(err) = tokio::task::spawn_blocking(wait_for_agents).await else {
            return;
        };
        info!("Retrying wait for service discovery: {err:?}");
        sleep(Duration::from_secs(10)).await;
    }
}
