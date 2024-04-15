// Copyright 2023 Google LLC. All Rights Reserved.

// External dependencies.
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{IConfigurablePublisher::IConfigurablePublisher, ISubscriber::BnSubscriber, PublisherInfo::PublisherInfo};
use google_sdv_telemetry_configurable_publisher::binder::Strong;
use async_trait::async_trait;
use binder::BinderFeatures;
use log::info;
use protobuf::descriptor::FileDescriptorProto;
use protobuf::reflect::{FileDescriptor, MessageDescriptor};
use protobuf::MessageDyn;
use sdv_telemetry::ServicePublisherAccessor;
use sdv_telemetry_utils::descriptor_utils::WELL_KNOWN_FILE_DESCRIPTORS;
use sdv_telemetry_utils::collection_extension::HashMapExtension;
use std::collections::{hash_map::Entry, HashMap};
use std::{fmt::Debug, sync::Mutex};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc;

// Crate dependencies.
use crate::subscriber_service::SubscriberService;

// Arbitrarily chosen buffer capacity
const TOKIO_CHANNEL_CAPACITY: usize = 32;

/// ConfigurablePublisherRegistry that maintains the list of configurable publishers in memory.
pub struct ConfigurablePublisherRegistryImpl {
    /// Hashmap to store configurable publishers that can be subscribed to and their metadata.
    publisher_entries: Mutex<HashMap<String, PublisherEntry>>,
    /// Hashmap to store handles for the spawned tokio tasks
    // TODO: b/317902294 Design how to key subscriptions to configurable publishers
    tokio_handles: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
}

/// Publisher registry entry.
struct PublisherEntry {
    message_descriptor: MessageDescriptor,
    publisher: Strong<dyn IConfigurablePublisher>,
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ConfigurablePublisherRegistryRegisterError {
    #[error("Service with name {service_name} is already registered")]
    ServiceAlreadyRegistered { service_name: String },
    #[error("Unable to parse publisher file descriptor proto: {error}")]
    FileDescriptorProtoParseError {
        #[from]
        error: protobuf::Error,
    },
    #[error("Message type {message_type_name} is not defined in the file descriptor")]
    InvalidMessageTypeName { message_type_name: String },
}

#[async_trait]
impl ServicePublisherAccessor for ConfigurablePublisherRegistryImpl {
    fn get_message_descriptor(
        &self,
        service_name: &str,
    ) -> Result<Option<MessageDescriptor>, anyhow::Error> {
        Ok(self.visit_entry(service_name, |entry| entry.message_descriptor.clone()))
    }

    async fn try_subscribe(
        &self,
        service_name: &str,
        callback: Arc<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>,
        configuration: Option<&[u8]>,
    ) -> Result<bool, anyhow::Error> {
        let result = self.visit_entry(service_name, |entry| {
            (entry.message_descriptor.clone(), entry.publisher.clone())
        });

        let Some((message_descriptor, publisher)) = result else {
            return Ok(false);
        };

        let (tx, mut rx) = mpsc::channel(TOKIO_CHANNEL_CAPACITY);

        let handle = tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                (callback)(message);
            }
        });

        let listener_wrapper = BnSubscriber::new_binder(
            SubscriberService::new(message_descriptor, tx),
            BinderFeatures::default(),
        );

        publisher.subscribe(&listener_wrapper, configuration)?;

        self.tokio_handles
            .lock()
            .unwrap()
            .try_insert_value(service_name.to_owned(), handle)
            .unwrap();

        Ok(true)
    }

    async fn unsubscribe(
        &self,
        service_name: &str,
        configuration: Option<&[u8]>,
    ) -> Result<(), anyhow::Error> {
        let maybe_publisher = self.visit_entry(service_name, |entry| entry.publisher.clone());
        let Some(publisher) = maybe_publisher else {
            panic!("Unable to find publisher for service {}", service_name)
        };

        let handle = self.tokio_handles.lock().unwrap().remove(service_name).unwrap();

        if !handle.is_finished() {
            handle.abort();
        }

        publisher.unsubscribe(configuration)?;

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
        Self { publisher_entries: HashMap::new().into(), tokio_handles: HashMap::new().into() }
    }

    pub(crate) async fn register_publisher(
        &self,
        publisher_info: &PublisherInfo,
        publisher: &Strong<dyn IConfigurablePublisher>,
    ) -> Result<(), ConfigurablePublisherRegistryRegisterError> {
        let publisher_info_proto: FileDescriptorProto =
            protobuf::Message::parse_from_bytes(&publisher_info.fileDescriptorProto)?;
        let file_descriptor =
            FileDescriptor::new_dynamic(publisher_info_proto, &WELL_KNOWN_FILE_DESCRIPTORS)?;
        let message_descriptor = file_descriptor
            .message_by_full_name(&publisher_info.messageTypeName)
            .ok_or_else(|| ConfigurablePublisherRegistryRegisterError::InvalidMessageTypeName {
                message_type_name: publisher_info.messageTypeName.clone(),
            })?;

        match self.publisher_entries.lock().unwrap().entry(publisher_info.serviceName.clone()) {
            Entry::Occupied(_) => {
                return Err(ConfigurablePublisherRegistryRegisterError::ServiceAlreadyRegistered {
                    service_name: publisher_info.serviceName.clone(),
                });
            }
            Entry::Vacant(v) => {
                v.insert(PublisherEntry { message_descriptor, publisher: publisher.clone() });
            }
        }

        Ok(())
    }

    pub(crate) async fn unregister_publisher(&self, service_name: &str) -> crate::BinderResult<()> {
        let mut publisher_entries_lock = self.publisher_entries.lock().unwrap();

        match publisher_entries_lock.entry(service_name.to_string()) {
            Entry::Occupied(o) => {
                let _ = o.remove_entry();
            }
            Entry::Vacant(_) => {
                info!("{} not found. It might have been unregistered already.", service_name);
            }
        }

        Ok(())
    }

    fn visit_entry<T>(
        &self,
        service_name: &str,
        visitor: impl FnOnce(&PublisherEntry) -> T,
    ) -> Option<T> {
        let publisher_entries_lock = self.publisher_entries.lock().unwrap();
        let entry = publisher_entries_lock.get(service_name)?;
        Some(visitor(entry))
    }
}

#[cfg(test)]
mod tests {
    use protobuf::{Message, MessageFull};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{IConfigurablePublisher::BnConfigurablePublisher, ISubscriber::ISubscriber};
    use google_sdv_telemetry_configurable_publisher::binder::{Interface, Result as BinderResult, Strong};
    use sample_publisher_proto::sample_publisher;
    use sample_publisher_proto::sample_publisher::SampleMessage;

    use super::*;

    const SERVICE_NAME: &str = "TEST_SERVICE_NAME";
    const SERVICE_NAME2: &str = "TEST_SERVICE_NAME2";

    #[derive(Clone)]
    struct MockConfigurablePublisher {
        /// The number of times subscribe function of the mock was called.
        num_subscribe_calls: Arc<AtomicU32>,
        /// The number of times subscribe function of the mock was called.
        num_unsubscribe_calls: Arc<AtomicU32>,
    }

    impl Interface for MockConfigurablePublisher {}

    impl IConfigurablePublisher for MockConfigurablePublisher {
        fn subscribe(
            &self,
            _subscriber: &Strong<dyn ISubscriber>,
            _config: Option<&[u8]>,
        ) -> BinderResult<()> {
            self.num_subscribe_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn unsubscribe(&self, _config: Option<&[u8]>) -> BinderResult<()> {
            self.num_unsubscribe_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    impl MockConfigurablePublisher {
        pub fn new() -> Self {
            Self {
                num_subscribe_calls: Arc::new(AtomicU32::new(0)),
                num_unsubscribe_calls: Arc::new(AtomicU32::new(0)),
            }
        }

        pub fn get_num_subscribe_calls(&self) -> u32 {
            self.num_subscribe_calls.load(Ordering::SeqCst)
        }

        pub fn get_num_unsubscribe_calls(&self) -> u32 {
            self.num_unsubscribe_calls.load(Ordering::SeqCst)
        }
    }

    async fn create_configurable_publisher_registry(
        configurable_publisher: MockConfigurablePublisher,
    ) -> ConfigurablePublisherRegistryImpl {
        let configurable_publisher_registry = ConfigurablePublisherRegistryImpl::new();

        let file_descriptor = sample_publisher::file_descriptor();
        let file_descriptor_proto = file_descriptor.proto();
        let descriptor = SampleMessage::descriptor();

        let publisher_info = PublisherInfo {
            serviceName: SERVICE_NAME.to_string(),
            topicName: String::default(),
            fileDescriptorProto: file_descriptor_proto.write_to_bytes().unwrap(),
            messageTypeName: format!(".{}", descriptor.full_name()),
        };

        let binder_publisher =
            BnConfigurablePublisher::new_binder(configurable_publisher, BinderFeatures::default());

        configurable_publisher_registry
            .register_publisher(&publisher_info, &binder_publisher)
            .await
            .unwrap();

        configurable_publisher_registry
    }

    #[test]
    fn get_message_descriptor_incorrect_service_name_returns_none() {
        let configurable_publisher_registry = ConfigurablePublisherRegistryImpl::new();

        let res = configurable_publisher_registry.get_message_descriptor(SERVICE_NAME).unwrap();

        assert!(res.is_none(), "Unexpected result: {:?}", res);
    }

    #[tokio::test]
    async fn get_message_descriptor_success() {
        let configurable_publisher = MockConfigurablePublisher::new();
        let configurable_publisher_registry =
            create_configurable_publisher_registry(configurable_publisher).await;

        let res =
            configurable_publisher_registry.get_message_descriptor(SERVICE_NAME).unwrap().unwrap();

        assert_eq!(res.proto(), SampleMessage::descriptor().proto());
    }

    #[tokio::test]
    async fn subscribe_success() {
        let configurable_publisher = MockConfigurablePublisher::new();
        let configurable_publisher_registry =
            create_configurable_publisher_registry(configurable_publisher.clone()).await;

        let is_subscribed = configurable_publisher_registry
            .try_subscribe(SERVICE_NAME, Arc::new(|_| {}), None)
            .await
            .unwrap();

        assert!(is_subscribed);
        assert_eq!(configurable_publisher.get_num_subscribe_calls(), 1);
    }

    #[tokio::test]
    async fn subscribe_incorrect_service_name_returns_false() {
        let configurable_publisher_registry = ConfigurablePublisherRegistryImpl::new();

        let is_subscribed = configurable_publisher_registry
            .try_subscribe(SERVICE_NAME, Arc::new(|_| {}), None)
            .await
            .unwrap();

        assert!(!is_subscribed);
    }

    #[tokio::test]
    async fn unsubscribe_success() {
        let configurable_publisher = MockConfigurablePublisher::new();
        let configurable_publisher_registry =
            create_configurable_publisher_registry(configurable_publisher.clone()).await;

        configurable_publisher_registry
            .try_subscribe(SERVICE_NAME, Arc::new(|_| {}), None)
            .await
            .unwrap();
        configurable_publisher_registry.unsubscribe(SERVICE_NAME, None).await.unwrap();

        assert_eq!(configurable_publisher.get_num_subscribe_calls(), 1);
        assert_eq!(configurable_publisher.get_num_unsubscribe_calls(), 1);
    }

    #[tokio::test]
    #[should_panic]
    async fn unsubscribe_incorrect_service_name_panics() {
        let configurable_publisher = MockConfigurablePublisher::new();
        let configurable_publisher_registry =
            create_configurable_publisher_registry(configurable_publisher).await;

        configurable_publisher_registry
            .try_subscribe(SERVICE_NAME, Arc::new(|_| {}), None)
            .await
            .unwrap();
        configurable_publisher_registry.unsubscribe(SERVICE_NAME2, None).await.unwrap();
    }

    #[tokio::test]
    async fn unregister_success() {
        let configurable_publisher = MockConfigurablePublisher::new();
        let configurable_publisher_registry =
            create_configurable_publisher_registry(configurable_publisher).await;

        let is_subscribed = configurable_publisher_registry
            .try_subscribe(SERVICE_NAME, Arc::new(|_| {}), None)
            .await
            .unwrap();

        assert!(is_subscribed);
        assert!(configurable_publisher_registry
            .publisher_entries
            .lock()
            .unwrap()
            .contains_key(SERVICE_NAME));

        configurable_publisher_registry.unregister_publisher(SERVICE_NAME).await.unwrap();
        assert!(!configurable_publisher_registry
            .publisher_entries
            .lock()
            .unwrap()
            .contains_key(SERVICE_NAME));
    }

    #[tokio::test]
    async fn unregister_incorrect_service_name_gets_ignored() {
        let configurable_publisher = MockConfigurablePublisher::new();
        let configurable_publisher_registry =
            create_configurable_publisher_registry(configurable_publisher).await;

        let is_subscribed = configurable_publisher_registry
            .try_subscribe(SERVICE_NAME, Arc::new(|_| {}), None)
            .await
            .unwrap();

        assert!(is_subscribed);
        configurable_publisher_registry.unregister_publisher(SERVICE_NAME2).await.unwrap();
        assert!(configurable_publisher_registry
            .publisher_entries
            .lock()
            .unwrap()
            .contains_key(SERVICE_NAME));
    }
}
