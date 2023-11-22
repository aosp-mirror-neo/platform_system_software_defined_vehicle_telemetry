// Copyright 2023 Google LLC. All Rights Reserved.

// External dependencies.
use android_sdv_telemetry_configurable_publisher::aidl::android::sdv::telemetry::configurable_publisher::{IConfigurablePublisher::IConfigurablePublisher, ISubscriber::BnSubscriber, PublisherInfo::PublisherInfo};
use android_sdv_telemetry_configurable_publisher::binder::Strong;
use async_trait::async_trait;
use binder::BinderFeatures;
use binder_tokio::TokioRuntime;
use protobuf::descriptor::FileDescriptorProto;
use protobuf::reflect::{FileDescriptor, MessageDescriptor};
use protobuf::MessageDyn;
use sdv_telemetry::{ConfigurablePublisherRegistry, ConfigurablePublisherRegistrySubscribeError};
use std::collections::{hash_map::Entry, HashMap};
use std::fmt::Debug;
use std::sync::Mutex;
use tokio::runtime::Builder;
use thiserror::Error;

// Crate dependencies.
use crate::subscriber_service::SubscriberService;

/// ConfigurablePublisherRegistry that maintains the list of publishers in memory.
pub struct ConfigurablePublisherRegistryImpl {
    /// Hashmap to store configurable publishers that can be subscribed to and their metadata.
    publisher_entries: Mutex<HashMap<String, HashMap<String, PublisherEntry>>>,
}

/// Publisher registry entry.
struct PublisherEntry {
    message_descriptor: MessageDescriptor,
    publisher: Strong<dyn IConfigurablePublisher>,
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

#[async_trait]
impl ConfigurablePublisherRegistry for ConfigurablePublisherRegistryImpl {
    fn get_message_descriptor(
        &self,
        service_name: &str,
        topic_name: &str,
    ) -> Result<MessageDescriptor, ConfigurablePublisherRegistrySubscribeError> {
        self.visit_entry(service_name, topic_name, |entry| entry.message_descriptor.clone())
    }

    async fn subscribe(
        &self,
        service_name: &str,
        topic_name: &str,
        callback: Box<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>,
        configuration: Option<&[u8]>,
    ) -> Result<(), ConfigurablePublisherRegistrySubscribeError> {
        let (message_descriptor, publisher) =
            self.visit_entry(service_name, topic_name, |entry| {
                (entry.message_descriptor.clone(), entry.publisher.clone())
            })?;

        let runtime = Builder::new_multi_thread().enable_all().build().unwrap();

        let listener_wrapper = BnSubscriber::new_async_binder(
            SubscriberService::new(message_descriptor, callback),
            TokioRuntime(runtime),
            BinderFeatures::default(),
        );

        publisher.subscribe(&listener_wrapper, configuration)?;

        Ok(())
    }
}

impl Default for ConfigurablePublisherRegistryImpl {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigurablePublisherRegistryImpl {
    /// Creates a new instance of the struct.
    pub fn new() -> Self {
        ConfigurablePublisherRegistryImpl { publisher_entries: HashMap::new().into() }
    }

    pub(crate) async fn register_publisher(
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

        match self.publisher_entries.lock().unwrap().entry(publisher_info.serviceName.clone()) {
            Entry::Occupied(mut o) => {
                match o.get_mut().entry(publisher_info.topicName.clone()) {
                    Entry::Occupied(_) => {
                        return Err(
                            ConfigurablePublisherRegistryRegisterError::TopicAlreadyExists {
                                service_name: publisher_info.serviceName.clone(),
                                topic_name: publisher_info.topicName.clone(),
                            },
                        );
                    }
                    Entry::Vacant(v) => {
                        v.insert(PublisherEntry {
                            message_descriptor,
                            publisher: publisher.clone(),
                        });
                    }
                };
            }
            Entry::Vacant(v) => {
                v.insert(HashMap::from([(
                    publisher_info.topicName.clone(),
                    PublisherEntry { message_descriptor, publisher: publisher.clone() },
                )]));
            }
        }

        Ok(())
    }

    fn visit_entry<T>(
        &self,
        service_name: &str,
        topic_name: &str,
        visitor: impl FnOnce(&PublisherEntry) -> T,
    ) -> Result<T, ConfigurablePublisherRegistrySubscribeError> {
        let publisher_entries_lock = self.publisher_entries.lock().unwrap();
        let entry = publisher_entries_lock
            .get(service_name)
            .ok_or_else(|| ConfigurablePublisherRegistrySubscribeError::ServiceNotFound {
                service_name: service_name.to_owned(),
            })?
            .get(topic_name)
            .ok_or_else(|| ConfigurablePublisherRegistrySubscribeError::TopicNotFound {
                service_name: service_name.to_owned(),
                topic_name: topic_name.to_owned(),
            })?;
        Ok(visitor(entry))
    }
}
