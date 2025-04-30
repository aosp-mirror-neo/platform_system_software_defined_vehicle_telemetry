// Copyright 2025 Google LLC

#![allow(dead_code)]

use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{
    IConfigurablePublisher::BnConfigurablePublisher,
    IConfigurablePublisherRegistry::{IConfigurablePublisherRegistry, BpConfigurablePublisherRegistry}};

use binder::{BinderFeatures, Strong};
use std::collections::HashSet;
use std::sync::Arc;

use crate::configurable_publisher::ConfigurablePublisher;
use crate::configurable_publisher::ConfigurablePublisherAidlImpl;
use crate::publisher_info::PublisherInfo;

/// Errors returned to client when interacting with the configurable publisher registry service
#[derive(thiserror::Error, Debug)]
pub enum ConfigurablePublisherRegistryError {
    /// Failed to initialize the underlying service, often due to a binder error.
    #[error("Unable to initialize service: {error:?}")]
    CreateServiceFailed {
        /// The specific status code from the binder service indicating the cause.
        #[from]
        error: binder::StatusCode,
    },

    /// An error occurred during a call to the Binder interface.
    #[error("Error when making a call to Binder: {error:?}")]
    BinderError {
        /// The detailed status from the binder call.
        #[from]
        error: binder::Status,
    },
}
/// ConfigurablePublisherRegistry that maintains the list of configurable publishers in memory.
pub struct ConfigurablePublisherRegistry {
    /// Reference to IConfigurablePublisherRegistry binder service
    configurable_publisher_registry_service: Strong<dyn IConfigurablePublisherRegistry>,
    configurable_publisher_map: HashSet<String>,
}

impl ConfigurablePublisherRegistry {
    /// Fetched a strong reference to IConfigurablePublisherRegistry and initializes client
    /// facing ConfigurablePublisherRegistry
    pub fn new() -> Result<Self, ConfigurablePublisherRegistryError> {
        let configurable_publisher_registry_descriptor = format!(
            "{}/default",
            <BpConfigurablePublisherRegistry as IConfigurablePublisherRegistry>::get_descriptor()
        );
        let configurable_publisher_registry_service =
            binder::get_interface::<dyn IConfigurablePublisherRegistry>(
                &configurable_publisher_registry_descriptor,
            )?;

        Ok(ConfigurablePublisherRegistry {
            configurable_publisher_registry_service,
            configurable_publisher_map: HashSet::new(),
        })
    }

    /// Attempts to register a ConfigurablePublisher with telemetry service
    pub fn register_publisher(
        &mut self,
        publisher_info: PublisherInfo,
        configurable_publisher: Arc<dyn ConfigurablePublisher>,
    ) -> Result<(), ConfigurablePublisherRegistryError> {
        let publisher = ConfigurablePublisherAidlImpl::new(configurable_publisher);
        let binder_publisher =
            BnConfigurablePublisher::new_binder(publisher, BinderFeatures::default());

        let publisher_info_aidl = PublisherInfo::to_aidl_representation(&publisher_info);

        self.configurable_publisher_registry_service
            .registerPublisher(&publisher_info_aidl, &binder_publisher)?;

        self.configurable_publisher_map.insert(publisher_info.service_name);
        Ok(())
    }

    /// Attempts to unregister a ConfigurablePublisher from telemetry service
    pub fn unregister_publisher(
        &mut self,
        service_name: &str,
    ) -> Result<(), ConfigurablePublisherRegistryError> {
        self.configurable_publisher_registry_service.unregisterPublisher(service_name)?;
        self.configurable_publisher_map.remove(service_name);
        Ok(())
    }
}
