// Copyright 2023 Google LLC. All Rights Reserved.

//! This module implements the IConfigurablePublisherRegistry AIDL interface

mod configurable_publisher_registry;
mod subscriber_service;

// External dependencies.
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{IConfigurablePublisherRegistry::IConfigurablePublisherRegistryAsyncServer, IConfigurablePublisher::IConfigurablePublisher, PublisherInfo::PublisherInfo};
use google_sdv_telemetry_configurable_publisher::binder::{Interface, Result as BinderResult, Strong};
use async_trait::async_trait;
use binder::Status;
use log::info;
use std::fmt::Debug;
use std::sync::Arc;

// Child module dependencies.
pub use configurable_publisher_registry::ConfigurablePublisherRegistry;

/// Implementation of IConfigurablePublisherRegistryAsyncServer binder interface
/// that uses ConfigurablePublisherRegistryImpl to maintain the registry in memory.
pub struct ConfigurablePublisherRegistryService {
    configurable_publisher_registry: Arc<ConfigurablePublisherRegistry>,
}

/// Convert a Result<T, E> to BinderResult<T> to allow it to be returned from a binder RPC,
/// preserving the content as far as possible.
/// Also log the error if there is one.
fn to_binder_result<T, E: Debug>(result: Result<T, E>) -> BinderResult<T> {
    result.map_err(|e| {
        let message = format!("{:?}", e);
        info!("Returning binder error: {}", &message);
        Status::new_service_specific_error_str(-1, Some(message))
    })
}

impl Interface for ConfigurablePublisherRegistryService {}

#[async_trait]
impl IConfigurablePublisherRegistryAsyncServer for ConfigurablePublisherRegistryService {
    async fn registerPublisher(
        &self,
        publisher_info: &PublisherInfo,
        publisher: &Strong<dyn IConfigurablePublisher>,
    ) -> BinderResult<()> {
        info!("Registering publisher with telemetry service name {}", publisher_info.serviceName);
        let res = self
            .configurable_publisher_registry
            .register_publisher(publisher_info, publisher)
            .await;

        to_binder_result(res)
    }

    async fn unregisterPublisher(&self, service_name: &str) -> BinderResult<()> {
        info!("Unregistering publisher with telemetry service name {}", service_name);
        let res = self.configurable_publisher_registry.unregister_publisher(service_name).await;

        to_binder_result(res)
    }
}

impl ConfigurablePublisherRegistryService {
    /// Instantiates an instance of ConfigurablePublisherRegistryService
    pub fn new(configurable_publisher_registry: Arc<ConfigurablePublisherRegistry>) -> Self {
        Self { configurable_publisher_registry }
    }
}
