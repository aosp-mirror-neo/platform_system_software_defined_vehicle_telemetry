// Copyright 2023 Google LLC. All Rights Reserved.

#![allow(missing_docs)]
use android_sdv_telemetry_configurable_publisher::aidl::android::sdv::telemetry::configurable_publisher::{
    ISubscriber::BnSubscriber, IConfigurablePublisher::IConfigurablePublisher,
    PublisherInfo::PublisherInfo,
};
use android_sdv_telemetry_configurable_publisher::binder::Strong;

use crate::subscriber::{Subscriber, SubscriberWrapper};

use async_trait::async_trait;
use binder::BinderFeatures;
use log::error;
use protobuf::descriptor::FileDescriptorProto;
use protobuf::reflect::FileDescriptor;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::{Send, Sync};
use std::sync::Mutex;
use thiserror::Error;

/// Interface used to register publishers and subscribe to registered publishers
#[async_trait]
pub trait ConfigurablePublisherRegistry: Send + Sync {
    /// subscribes to non-sdv aware publishers
    async fn subscribe(
        &self,
        service_name: String,
        topic_name: String,
        subscriber: Box<dyn Subscriber>,
        configuration: Option<&[u8]>,
    ) -> Result<(), ConfigurablePublisherRegistryError>;

    async fn register_publisher(
        &self,
        publisher_info: &PublisherInfo,
        callback: &Strong<dyn IConfigurablePublisher>,
    ) -> Result<(), ConfigurablePublisherRegistryError>;
}

#[derive(Error, Debug)]
#[non_exhaustive]
/// List of errors when using ConfigurablePublisherRegistryImpl
pub enum ConfigurablePublisherRegistryError {
    /// publisher with given service and topic name not found
    #[error("publisher with given service and signal name not found {service_name} {topic_name}")]
    PublisherNotFound { service_name: String, topic_name: String },
    /// publisher with given service and topic name already exists
    #[error(
        "publisher with given service and signal name already exists {service_name} {topic_name}"
    )]
    PublisherAlreadyExists { service_name: String, topic_name: String },
    /// Parsing error
    #[error("Unable to parse publisher file descriptor proto")]
    FileDescriptorProtoParseError,
}

/// This struct is defined to implement ConfigurablePublisherRegistry trait.
pub struct ConfigurablePublisherRegistryImpl {
    /// Hashmap to store publishers info.
    publisher_info_store: Mutex<HashMap<(String, String), (String, FileDescriptor)>>,
    /// Hashmap to store configurable publishers that can be subscriber to.
    publisher_store: Mutex<HashMap<(String, String), Strong<dyn IConfigurablePublisher>>>,
}

impl Default for ConfigurablePublisherRegistryImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ConfigurablePublisherRegistry for ConfigurablePublisherRegistryImpl {
    /// subscribes to the non-sdv publisher with the given service and topic name
    async fn subscribe(
        &self,
        service_name: String,
        topic_name: String,
        subscriber: Box<dyn Subscriber>,
        configuration: Option<&[u8]>,
    ) -> Result<(), ConfigurablePublisherRegistryError> {
        let publisher = {
            let publisher_map = self.publisher_store.lock().unwrap();
            match publisher_map.get(&(service_name.clone(), topic_name.clone())) {
                Some(v) => v.clone(),
                None => {
                    return Err(ConfigurablePublisherRegistryError::PublisherNotFound {
                        service_name,
                        topic_name,
                    })
                }
            }
        };

        let listener_wrapper =
            BnSubscriber::new_binder(SubscriberWrapper::new(subscriber), BinderFeatures::default());

        let _ = publisher.subscribe(&listener_wrapper, configuration);

        Ok(())
    }

    async fn register_publisher(
        &self,
        publisher_info: &PublisherInfo,
        callback: &Strong<dyn IConfigurablePublisher>,
    ) -> Result<(), ConfigurablePublisherRegistryError> {
        let publisher_info_proto: FileDescriptorProto =
            match protobuf::Message::parse_from_bytes(&publisher_info.fileDescriptorProto) {
                Ok(proto) => proto,
                Err(_) => {
                    error!("Error in parsing file descriptor proto");
                    return Err(ConfigurablePublisherRegistryError::FileDescriptorProtoParseError);
                }
            };
        let file_descriptor = FileDescriptor::new_dynamic(publisher_info_proto, &[]).unwrap();
        {
            let mut publisher_info_map = self.publisher_info_store.lock().unwrap();
            match publisher_info_map
                .entry((publisher_info.serviceName.clone(), publisher_info.topicName.clone()))
            {
                Entry::Occupied(_) => {
                    return Err(ConfigurablePublisherRegistryError::PublisherAlreadyExists {
                        service_name: publisher_info.serviceName.clone(),
                        topic_name: publisher_info.topicName.clone(),
                    })
                }
                Entry::Vacant(v) => {
                    v.insert((publisher_info.messageTypeName.clone(), file_descriptor));
                }
            }
        }

        self.publisher_store.lock().unwrap().insert(
            (publisher_info.serviceName.clone(), publisher_info.topicName.clone()),
            callback.clone(),
        );

        Ok(())
    }
}

impl ConfigurablePublisherRegistryImpl {
    /// Instantiates an instance of ConfigurablePublisherRegistryImpl
    pub fn new() -> Self {
        ConfigurablePublisherRegistryImpl {
            publisher_info_store: Mutex::new(HashMap::new()),
            publisher_store: Mutex::new(HashMap::new()),
        }
    }
}
