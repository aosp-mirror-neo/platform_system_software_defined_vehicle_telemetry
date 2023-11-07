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
use protobuf::reflect::{FileDescriptor, MessageDescriptor};
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
    #[error("Unable to parse publisher file descriptor proto: {error}")]
    FileDescriptorProtoParseError {
        #[from]
        error: protobuf::Error,
    },
    #[error("Message type {message_type_name} is not defined in the file descriptor")]
    InvalidMessageTypeName { message_type_name: String },
}

/// This struct is defined to implement ConfigurablePublisherRegistry trait.
pub struct ConfigurablePublisherRegistryImpl {
    /// Hashmap to store configurable publishers that can be subscribed to and their metadata.
    publisher_entries: Mutex<HashMap<(String, String), PublisherEntry>>,
}

/// Publisher registry entry.
pub struct PublisherEntry {
    message_descriptor: MessageDescriptor,
    publisher: Strong<dyn IConfigurablePublisher>,
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
        let (message_descriptor, publisher) = {
            let key = (service_name, topic_name);
            match self.publisher_entries.lock().unwrap().get(&key) {
                Some(entry) => (entry.message_descriptor.clone(), entry.publisher.clone()),
                None => {
                    let (service_name, topic_name) = key;
                    return Err(ConfigurablePublisherRegistryError::PublisherNotFound {
                        service_name,
                        topic_name,
                    });
                }
            }
        };

        let listener_wrapper = BnSubscriber::new_binder(
            SubscriberWrapper::new(message_descriptor, subscriber),
            BinderFeatures::default(),
        );

        let _ = publisher.subscribe(&listener_wrapper, configuration);

        Ok(())
    }
}

impl ConfigurablePublisherRegistryImpl {
    /// Instantiates an instance of ConfigurablePublisherRegistryImpl
    pub fn new() -> Self {
        ConfigurablePublisherRegistryImpl { publisher_entries: HashMap::new().into() }
    }

    pub async fn register_publisher(
        &self,
        publisher_info: &PublisherInfo,
        publisher: &Strong<dyn IConfigurablePublisher>,
    ) -> Result<(), ConfigurablePublisherRegistryError> {
        let publisher_info_proto: FileDescriptorProto =
            protobuf::Message::parse_from_bytes(&publisher_info.fileDescriptorProto)?;
        let file_descriptor = FileDescriptor::new_dynamic(publisher_info_proto, &[])?;
        let message_descriptor = file_descriptor
            .message_by_full_name(&publisher_info.messageTypeName)
            .ok_or_else(|| ConfigurablePublisherRegistryError::InvalidMessageTypeName {
                message_type_name: publisher_info.messageTypeName.clone(),
            })?;

        match self
            .publisher_entries
            .lock()
            .unwrap()
            .entry((publisher_info.serviceName.clone(), publisher_info.topicName.clone()))
        {
            Entry::Occupied(_) => {
                return Err(ConfigurablePublisherRegistryError::PublisherAlreadyExists {
                    service_name: publisher_info.serviceName.clone(),
                    topic_name: publisher_info.topicName.clone(),
                })
            }
            Entry::Vacant(v) => {
                v.insert(PublisherEntry { message_descriptor, publisher: publisher.clone() });
            }
        }

        Ok(())
    }
}
