// Copyright 2023 Google LLC. All Rights Reserved.

// External dependencies.
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{IConfigurablePublisher::IConfigurablePublisher, ISubscriber::BnSubscriber, PublisherInfo::PublisherInfo, IConfigurablePublisherSubscription::IConfigurablePublisherSubscription};
use google_sdv_telemetry_configurable_publisher::binder::Strong;
use async_trait::async_trait;
use binder::BinderFeatures;
use log::info;
use protobuf::descriptor::FileDescriptorProto;
use protobuf::reflect::{FileDescriptor, MessageDescriptor};
use protobuf::MessageDyn;
use sdv_telemetry::service_publisher_accessor::{CreateSubscriptionOutcome, GetLatestMessageOutcome, ServicePublisherMetadata, ServicePublisherSubscription};
use sdv_telemetry::ServicePublisherAccessor;
use sdv_telemetry_utils::descriptor_utils::WELL_KNOWN_FILE_DESCRIPTORS;
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
pub struct ConfigurablePublisherRegistry {
    /// Hashmap to store configurable publishers that can be subscribed to and their metadata.
    publisher_entries: Arc<Mutex<HashMap<String, PublisherEntry>>>,
}

pub struct ConfigurablePublisherRegistrySubscription {
    /// Handles for the spawned tokio task that reads the data.
    join_handle: tokio::task::JoinHandle<()>,
    /// The underlying configurable publisher subscription.
    configurable_publisher_subscription: Strong<dyn IConfigurablePublisherSubscription>,
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

fn visit_entry<T>(
    publisher_entries: &Arc<Mutex<HashMap<String, PublisherEntry>>>,
    service_name: &str,
    visitor: impl FnOnce(&PublisherEntry) -> T,
) -> Option<T> {
    let publisher_entries_lock = publisher_entries.lock().unwrap();
    let entry = publisher_entries_lock.get(service_name)?;
    Some(visitor(entry))
}

impl Default for ConfigurablePublisherRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ServicePublisherAccessor for ConfigurablePublisherRegistry {
    fn get_metadata(&self, service_name: &str) -> anyhow::Result<Option<ServicePublisherMetadata>> {
        let maybe_message_descriptor =
            visit_entry(&self.publisher_entries, service_name, |entry| {
                entry.message_descriptor.clone()
            });

        let metadata = maybe_message_descriptor.map(|message_descriptor| {
            ServicePublisherMetadata { supports_get: false, message_descriptor }
        });
        Ok(metadata)
    }

    fn get_latest_message(&self, _service_name: &str) -> anyhow::Result<GetLatestMessageOutcome> {
        Ok(GetLatestMessageOutcome::NotSupported)
    }

    async fn create_subscription(
        &self,
        service_name: &str,
        configuration: Option<&[u8]>,
        mut callback: Box<dyn FnMut(Box<dyn MessageDyn>) + Send + Sync>,
    ) -> anyhow::Result<CreateSubscriptionOutcome> {
        let result = visit_entry(&self.publisher_entries, service_name, |entry| {
            (entry.message_descriptor.clone(), entry.publisher.clone())
        });

        let Some((message_descriptor, publisher)) = result else {
            return Ok(CreateSubscriptionOutcome::NotAvailable(callback));
        };

        let (tx, mut rx) = mpsc::channel(TOKIO_CHANNEL_CAPACITY);

        let join_handle = tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                (callback)(message);
            }
        });

        let listener_wrapper = BnSubscriber::new_binder(
            SubscriberService::new(message_descriptor, tx),
            BinderFeatures::default(),
        );

        let subscription = publisher.create_subscription(&listener_wrapper, configuration)?;

        Ok(CreateSubscriptionOutcome::Subscribed(Box::new(
            ConfigurablePublisherRegistrySubscription {
                join_handle,
                configurable_publisher_subscription: subscription,
            },
        )))
    }
}

impl ConfigurablePublisherRegistry {
    /// Creates a new instance of the struct.
    pub fn new() -> Self {
        Self { publisher_entries: Mutex::new(HashMap::new()).into() }
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
                o.remove_entry();
            }
            Entry::Vacant(_) => {
                info!(
                    "Unable to unregister service {}. It might have been unregistered already.",
                    service_name
                );
            }
        }

        Ok(())
    }
}

#[async_trait]
impl ServicePublisherSubscription for ConfigurablePublisherRegistrySubscription {
    async fn unsubscribe(self: Box<Self>) -> Result<(), anyhow::Error> {
        if !self.join_handle.is_finished() {
            self.join_handle.abort();
        }

        self.configurable_publisher_subscription.unsubscribe()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{IConfigurablePublisher::BnConfigurablePublisher, IConfigurablePublisherSubscription::{IConfigurablePublisherSubscription, BnConfigurablePublisherSubscription}, ISubscriber::ISubscriber};
    use google_sdv_telemetry_configurable_publisher::binder::{Interface, Result as BinderResult, Strong};
    use protobuf::{Message, MessageFull};
    use sample_publisher_proto::sample_publisher;
    use sample_publisher_proto::sample_publisher::SampleMessage;
    use sdv_telemetry_utils::test_utils::create_empty_fn_box;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Weak};

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

    struct ConfigurablePublisherSubscription {
        num_unsubscribe_calls: Weak<AtomicU32>,
    }

    impl Interface for MockConfigurablePublisher {}
    impl Interface for ConfigurablePublisherSubscription {}

    impl IConfigurablePublisherSubscription for ConfigurablePublisherSubscription {
        fn unsubscribe(&self) -> BinderResult<()> {
            self.num_unsubscribe_calls.upgrade().unwrap().fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    impl IConfigurablePublisher for MockConfigurablePublisher {
        fn create_subscription(
            &self,
            _subscriber: &Strong<dyn ISubscriber>,
            _config: Option<&[u8]>,
        ) -> BinderResult<Strong<dyn IConfigurablePublisherSubscription>> {
            let binder_subscription = BnConfigurablePublisherSubscription::new_binder(
                ConfigurablePublisherSubscription {
                    num_unsubscribe_calls: Arc::downgrade(&self.num_unsubscribe_calls),
                },
                BinderFeatures::default(),
            );
            self.num_subscribe_calls.fetch_add(1, Ordering::SeqCst);
            Ok(binder_subscription)
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
    ) -> ConfigurablePublisherRegistry {
        let configurable_publisher_registry = ConfigurablePublisherRegistry::new();

        let file_descriptor = sample_publisher::file_descriptor();
        let file_descriptor_proto = file_descriptor.proto();
        let descriptor = SampleMessage::descriptor();

        let publisher_info = PublisherInfo {
            serviceName: SERVICE_NAME.to_string(),
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
    fn get_metadata_incorrect_service_name_returns_none() {
        let configurable_publisher_registry = ConfigurablePublisherRegistry::new();

        let res = configurable_publisher_registry.get_metadata(SERVICE_NAME).unwrap();

        assert!(res.is_none());
    }

    #[tokio::test]
    async fn get_metadata_success() {
        let configurable_publisher = MockConfigurablePublisher::new();
        let configurable_publisher_registry =
            create_configurable_publisher_registry(configurable_publisher).await;

        let res = configurable_publisher_registry.get_metadata(SERVICE_NAME).unwrap().unwrap();

        assert!(!res.supports_get);
        assert_eq!(res.message_descriptor.proto(), SampleMessage::descriptor().proto());
    }

    #[tokio::test]
    async fn get_latest_message_returns_not_supported() {
        let configurable_publisher = MockConfigurablePublisher::new();
        let configurable_publisher_registry =
            create_configurable_publisher_registry(configurable_publisher).await;

        let res = configurable_publisher_registry.get_latest_message(SERVICE_NAME).unwrap();

        assert!(matches!(res, GetLatestMessageOutcome::NotSupported));
    }

    #[tokio::test]
    async fn create_subscription_success() {
        let configurable_publisher = MockConfigurablePublisher::new();
        let configurable_publisher_registry =
            create_configurable_publisher_registry(configurable_publisher.clone()).await;

        configurable_publisher_registry
            .create_subscription(SERVICE_NAME, None, create_empty_fn_box())
            .await
            .unwrap();

        assert_eq!(configurable_publisher.get_num_subscribe_calls(), 1);
        assert!(configurable_publisher_registry
            .publisher_entries
            .lock()
            .unwrap()
            .contains_key(SERVICE_NAME));
    }

    #[tokio::test]
    async fn create_subscription_incorrect_service_name_returns_not_available() {
        let configurable_publisher_registry = ConfigurablePublisherRegistry::new();

        let res = configurable_publisher_registry
            .create_subscription(SERVICE_NAME, None, create_empty_fn_box())
            .await
            .unwrap();

        assert!(matches!(res, CreateSubscriptionOutcome::NotAvailable(_)));
    }

    #[tokio::test]
    async fn unsubscribe_success() {
        let configurable_publisher = MockConfigurablePublisher::new();
        let configurable_publisher_registry =
            create_configurable_publisher_registry(configurable_publisher.clone()).await;

        let subscription = configurable_publisher_registry
            .create_subscription(SERVICE_NAME, None, create_empty_fn_box())
            .await
            .unwrap()
            .unwrap();
        subscription.unsubscribe().await.unwrap();

        assert_eq!(configurable_publisher.get_num_subscribe_calls(), 1);
        assert_eq!(configurable_publisher.get_num_unsubscribe_calls(), 1);
    }

    #[tokio::test]
    async fn unregister_success() {
        let configurable_publisher = MockConfigurablePublisher::new();
        let configurable_publisher_registry =
            create_configurable_publisher_registry(configurable_publisher.clone()).await;

        configurable_publisher_registry
            .create_subscription(SERVICE_NAME, None, create_empty_fn_box())
            .await
            .unwrap()
            .unwrap();
        configurable_publisher_registry.unregister_publisher(SERVICE_NAME).await.unwrap();

        assert!(!configurable_publisher_registry
            .publisher_entries
            .lock()
            .unwrap()
            .contains_key(SERVICE_NAME));

        // Unregistering doesn't unsubscribe from the publisher.
        assert_eq!(configurable_publisher.get_num_unsubscribe_calls(), 0);
    }

    #[tokio::test]
    async fn unregister_incorrect_service_name_gets_ignored() {
        let configurable_publisher = MockConfigurablePublisher::new();
        let configurable_publisher_registry =
            create_configurable_publisher_registry(configurable_publisher).await;

        configurable_publisher_registry
            .create_subscription(SERVICE_NAME, None, create_empty_fn_box())
            .await
            .unwrap()
            .unwrap();
        configurable_publisher_registry.unregister_publisher(SERVICE_NAME2).await.unwrap();

        assert!(configurable_publisher_registry
            .publisher_entries
            .lock()
            .unwrap()
            .contains_key(SERVICE_NAME));
    }
}
