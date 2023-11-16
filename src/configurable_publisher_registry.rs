// Copyright 2023 Google LLC. All Rights Reserved.

#![allow(missing_docs)]
use android_sdv_telemetry_configurable_publisher::aidl::android::sdv::telemetry::configurable_publisher::{
    ISubscriber::BnSubscriber, IConfigurablePublisher::IConfigurablePublisher,
    PublisherInfo::PublisherInfo,
};
use android_sdv_telemetry_configurable_publisher::binder::Strong;
use async_trait::async_trait;
use binder::BinderFeatures;
use log::error;
use protobuf::MessageDyn;
use protobuf::descriptor::FileDescriptorProto;
use protobuf::reflect::{FileDescriptor, MessageDescriptor};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::{Send, Sync};
use std::sync::Mutex;
use thiserror::Error;

use crate::subscriber::SubscriberImpl;

/// Interface used to register publishers and subscribe to registered publishers
#[async_trait]
pub trait ConfigurablePublisherRegistry: Send + Sync {
    /// Returns message descriptor of the specified topic published by the specified service.
    fn get_message_descriptor(
        &self,
        service_name: String,
        topic_name: String,
    ) -> Option<MessageDescriptor>;

    /// Subscribes specified callback to the new messages from the specified topic
    /// with certain configuration, if provided.
    async fn subscribe(
        &self,
        service_name: String,
        topic_name: String,
        callback: Box<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>,
        configuration: Option<&[u8]>,
    ) -> Result<(), ConfigurablePublisherRegistrySubscribeError>;
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ConfigurablePublisherRegistrySubscribeError {
    #[error("Topic {topic_name} is not registered for publisher {service_name}")]
    TopicNotFound { service_name: String, topic_name: String },
    #[error("Unable to subscribe to the topic: {error}")]
    SubscriptionFailed {
        #[from]
        error: binder::Status,
    },
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ConfigurablePublisherRegistryRegisterError {
    #[error("Topic {topic_name} is already registered by service {service_name}")]
    TopicAlreadyExists { service_name: String, topic_name: String },
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
    fn get_message_descriptor(
        &self,
        service_name: String,
        topic_name: String,
    ) -> Option<MessageDescriptor> {
        self.publisher_entries
            .lock()
            .unwrap()
            .get(&(service_name, topic_name))
            .map(|entry| entry.message_descriptor.clone())
    }

    async fn subscribe(
        &self,
        service_name: String,
        topic_name: String,
        callback: Box<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>,
        configuration: Option<&[u8]>,
    ) -> Result<(), ConfigurablePublisherRegistrySubscribeError> {
        let (message_descriptor, publisher) = {
            let key = (service_name, topic_name);
            match self.publisher_entries.lock().unwrap().get(&key) {
                Some(entry) => (entry.message_descriptor.clone(), entry.publisher.clone()),
                None => {
                    let (service_name, topic_name) = key;
                    return Err(ConfigurablePublisherRegistrySubscribeError::TopicNotFound {
                        service_name,
                        topic_name,
                    });
                }
            }
        };

        let listener_wrapper = BnSubscriber::new_binder(
            SubscriberImpl::new(message_descriptor, callback),
            BinderFeatures::default(),
        );

        publisher.subscribe(&listener_wrapper, configuration)?;

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
    ) -> Result<(), ConfigurablePublisherRegistryRegisterError> {
        let publisher_info_proto: FileDescriptorProto =
            protobuf::Message::parse_from_bytes(&publisher_info.fileDescriptorProto)?;
        let file_descriptor = FileDescriptor::new_dynamic(publisher_info_proto, &[])?;
        let message_descriptor = file_descriptor
            .message_by_full_name(&publisher_info.messageTypeName)
            .ok_or_else(|| ConfigurablePublisherRegistryRegisterError::InvalidMessageTypeName {
                message_type_name: publisher_info.messageTypeName.clone(),
            })?;

        match self
            .publisher_entries
            .lock()
            .unwrap()
            .entry((publisher_info.serviceName.clone(), publisher_info.topicName.clone()))
        {
            Entry::Occupied(_) => {
                return Err(ConfigurablePublisherRegistryRegisterError::TopicAlreadyExists {
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
