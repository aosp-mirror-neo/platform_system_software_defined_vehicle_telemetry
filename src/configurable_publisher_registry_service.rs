// Copyright 2023 Google LLC. All Rights Reserved.

//! This module implements the IConfigurablePublisherRegistry AIDL interface

pub mod configurable_publisher_registry;
pub mod subscriber;

use android_sdv_telemetry_configurable_publisher::aidl::android::sdv::telemetry::configurable_publisher::{
    IConfigurablePublisherRegistry::IConfigurablePublisherRegistryAsyncServer, IConfigurablePublisher::IConfigurablePublisher,
    PublisherInfo::PublisherInfo,
};
use android_sdv_telemetry_configurable_publisher::binder::{Interface, Result as BinderResult, Strong};
use async_trait::async_trait;
use binder::Status;
use log::info;
use std::{fmt::Debug, sync::Arc};

use configurable_publisher_registry::ConfigurablePublisherRegistryImpl;

/// Convert a Result<T, E> to BinderResult<T> to allow it to be returned from a binder RPC,
/// preserving the content as far as possible.
/// Also log the error if there is one.
pub fn to_binder_result<T, E: Debug>(result: Result<T, E>) -> BinderResult<T> {
    result.map_err(|e| {
        let message = format!("{:?}", e);
        info!("Returning binder error: {}", &message);
        Status::new_service_specific_error_str(-1, Some(message))
    })
}

/// This struct is used as a wrapper around ConfigurablePublisherRegistryImpl so that Arc<ConfigurablePublisherRegistryImpl>
/// can be shared with multiple services. Implements IConfigurablePublisherRegistryAsyncServer and registered
/// as a binder service.
pub struct ConfigurablePublisherRegistryService {
    configurable_publisher_registry: Arc<ConfigurablePublisherRegistryImpl>,
}

impl Interface for ConfigurablePublisherRegistryService {}

impl ConfigurablePublisherRegistryService {
    /// Returns in instance of ConfigurablePublisherRegistryService
    pub async fn new(registry: Arc<ConfigurablePublisherRegistryImpl>) -> Self {
        Self { configurable_publisher_registry: registry }
    }
}

#[async_trait]
impl IConfigurablePublisherRegistryAsyncServer for ConfigurablePublisherRegistryService {
    async fn registerPublisher(
        &self,
        publisher_info: &PublisherInfo,
        callback: &Strong<dyn IConfigurablePublisher>,
    ) -> BinderResult<()> {
        info!(
            "Registering publisher with telemetry service with topic {}",
            publisher_info.topicName
        );
        let res =
            self.configurable_publisher_registry.register_publisher(publisher_info, callback).await;

        if res.is_err() {
            return to_binder_result(res);
        }

        Ok(())
    }
}
