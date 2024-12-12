// Copyright 2023 Google LLC

// External dependencies.
use async_trait::async_trait;
use binder_tokio::TokioRuntime;
use binder::BinderFeatures;
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{
    IConfigurablePublisher::IConfigurablePublisher,
    IConfigurablePublisherSubscription::IConfigurablePublisherSubscription,
    ISubscriber::BnSubscriber,
    PublisherInfo::PublisherInfo,
};
use log::info;
use parking_lot::Mutex;
use protobuf::descriptor::FileDescriptorProto;
use protobuf::reflect::{FileDescriptor, MessageDescriptor};
use protobuf::well_known_types::any::Any;
use protobuf::{Message, MessageDyn};
use sdv_telemetry::binder_adapter::{BinderAdapter, BinderAdapterImpl};
use sdv_telemetry::metrics_config_runtime_error::ServiceRuntimeError;
use sdv_telemetry::service_publisher_accessor::{
    AvailabilityCallbackRegistration, CreateSubscriptionError, CreateSubscriptionOutcome,
    GetMessageProviderError, GetMessageProviderOutcome, GetMetadataError, ServiceDiscoveryError,
    ServicePublisherAccessor, ServicePublisherMessageProvider, ServicePublisherMetadata,
    ServicePublisherSubscription,
};
use sdv_telemetry::sub_sampling_interval::SubSamplingInterval;
use sdv_telemetry_utils::descriptor_utils::WELL_KNOWN_FILE_DESCRIPTORS;
use std::collections::{hash_map::Entry, HashMap};
use std::mem;
use std::fmt::Debug;
use std::sync::{Arc, Weak};
use thiserror::Error;
use tokio::sync::mpsc;

// Crate dependencies.
use crate::subscriber_service::SubscriberService;

// Arbitrarily chosen buffer capacity
const TOKIO_CHANNEL_CAPACITY: usize = 32;

/// ConfigurablePublisherRegistry that maintains the list of configurable publishers in memory.
pub struct ConfigurablePublisherRegistry {
    availability_callbacks: Arc<Mutex<HashMap<String, Vec<Weak<dyn Fn() + Send + Sync>>>>>,
    /// Adapter to make calls into Binder library.
    binder_adapter: Box<dyn BinderAdapter<dyn IConfigurablePublisher>>,
    /// Hashmap to store configurable publishers that can be subscribed to and their metadata.
    publisher_entries: Arc<Mutex<HashMap<String, PublisherEntry>>>,
}

pub struct ConfigurablePublisherRegistrySubscription {
    /// Handles for the spawned tokio task that reads the data.
    join_handle: tokio::task::JoinHandle<()>,
    /// The underlying configurable publisher subscription.
    configurable_publisher_subscription: binder::Strong<dyn IConfigurablePublisherSubscription>,
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ConfigurablePublisherRegistryRegisterError {
    #[error("Error when making a call to Binder: {error:?}")]
    BinderError {
        #[from]
        error: binder::StatusCode,
    },
    #[error("Unable to parse publisher file descriptor proto: {error}")]
    FileDescriptorProtoParseError {
        #[from]
        error: protobuf::Error,
    },
    #[error("Message type {message_type_name} is not defined in the file descriptor")]
    InvalidMessageTypeName { message_type_name: String },
    #[error("Service with name {service_name} is already registered")]
    ServiceAlreadyRegistered { service_name: String },
}

struct AvailabilityCallbackRegistrationImpl {
    availability_callbacks: Arc<Mutex<HashMap<String, Vec<Weak<dyn Fn() + Send + Sync>>>>>,
    service_name: String,
    callback: Arc<dyn Fn() + Send + Sync>,
}

/// Publisher registry entry.
struct PublisherEntry {
    _death_recipient: binder::DeathRecipient,
    message_descriptor: MessageDescriptor,
    publisher: binder::Strong<dyn IConfigurablePublisher>,
    supports_get: bool,
}

struct ServicePublisherMessageProviderImpl {
    message_descriptor: MessageDescriptor,
    publisher: binder::Weak<dyn IConfigurablePublisher>,
}

fn serialize_configuration(configuration: &Any) -> Vec<u8> {
    configuration.write_to_bytes().unwrap()
}

impl Default for ConfigurablePublisherRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ServicePublisherAccessor for ConfigurablePublisherRegistry {
    async fn get_metadata(
        &self,
        service_name: &str,
    ) -> (Result<Option<ServicePublisherMetadata>, GetMetadataError>, Vec<ServiceDiscoveryError>)
    {
        let maybe_metadata = Self::visit_entry(&self.publisher_entries, service_name, |entry| {
            (entry.message_descriptor.clone(), entry.supports_get)
        });

        let metadata = maybe_metadata.map(|(message_descriptor, supports_get)| {
            ServicePublisherMetadata { message_descriptor, supports_get }
        });
        (Ok(metadata), vec![])
    }

    async fn get_message_provider(
        &self,
        service_name: &str,
    ) -> (Result<GetMessageProviderOutcome, GetMessageProviderError>, Vec<ServiceDiscoveryError>)
    {
        let provider = Self::visit_entry(&self.publisher_entries, service_name, |entry| {
            if entry.supports_get {
                GetMessageProviderOutcome::Success(Box::new(ServicePublisherMessageProviderImpl {
                    message_descriptor: entry.message_descriptor.clone(),
                    publisher: binder::Strong::downgrade(&entry.publisher),
                })
                    as Box<dyn ServicePublisherMessageProvider>)
            } else {
                GetMessageProviderOutcome::GetNotSupported
            }
        })
        .unwrap_or(GetMessageProviderOutcome::NotRegistered);
        (Ok(provider), vec![])
    }

    async fn create_subscription(
        &self,
        service_name: &str,
        configuration: Option<&Any>,
        // TODO: b/373332101 - Not yet implemented.
        _sub_sampling_interval: &SubSamplingInterval,
        mut callback: Box<
            dyn FnMut(Result<Box<dyn MessageDyn>, ServiceRuntimeError>) + Send + Sync,
        >,
    ) -> (Result<CreateSubscriptionOutcome, CreateSubscriptionError>, Vec<ServiceDiscoveryError>)
    {
        let result = (async {
            let result = Self::visit_entry(&self.publisher_entries, service_name, |entry| {
                (entry.message_descriptor.clone(), entry.publisher.clone())
            });

            let Some((message_descriptor, publisher)) = result else {
                return Ok(CreateSubscriptionOutcome::NotRegistered(callback));
            };

            let (tx, mut rx) = mpsc::channel(TOKIO_CHANNEL_CAPACITY);

            let join_handle = tokio::spawn(async move {
                while let Some(message) = rx.recv().await {
                    (callback)(Ok(message));
                }
            });

            let listener_wrapper = BnSubscriber::new_async_binder(
                SubscriberService::new(message_descriptor, tx),
                TokioRuntime(tokio::runtime::Handle::current()),
                BinderFeatures::default(),
            );

            let configuration_bytes = configuration.map(serialize_configuration);
            let subscription = publisher
                .createSubscription(&listener_wrapper, configuration_bytes.as_deref())
                .map_err(|err| CreateSubscriptionError::FailedToSubscribe(err.into()))?;

            Ok(CreateSubscriptionOutcome::Subscribed(Box::new(
                ConfigurablePublisherRegistrySubscription {
                    join_handle,
                    configurable_publisher_subscription: subscription,
                },
            )))
        })
        .await;
        (result, vec![])
    }

    async fn register_availability_callback(
        &self,
        service_name: &str,
        callback: &Arc<dyn Fn() + Send + Sync>,
    ) -> anyhow::Result<Option<Box<dyn AvailabilityCallbackRegistration>>> {
        let callback_weak = Arc::downgrade(callback);

        match self.availability_callbacks.lock().entry(service_name.to_owned()) {
            Entry::Occupied(mut o) => o.get_mut().push(callback_weak),
            Entry::Vacant(v) => {
                v.insert(vec![callback_weak]);
            }
        };

        Ok(Some(Box::new(AvailabilityCallbackRegistrationImpl {
            availability_callbacks: Arc::clone(&self.availability_callbacks),
            service_name: service_name.to_owned(),
            callback: Arc::clone(callback),
        })))
    }
}

impl ConfigurablePublisherRegistry {
    /// Creates a new instance of the struct.
    pub fn new() -> Self {
        Self {
            availability_callbacks: Arc::new(Mutex::new(HashMap::new())),
            binder_adapter: Box::new(BinderAdapterImpl {}),
            publisher_entries: Mutex::new(HashMap::new()).into(),
        }
    }

    pub(crate) async fn register_publisher(
        &self,
        publisher_info: &PublisherInfo,
        publisher: &binder::Strong<dyn IConfigurablePublisher>,
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

        match self.publisher_entries.lock().entry(publisher_info.serviceName.clone()) {
            Entry::Occupied(_) => {
                return Err(ConfigurablePublisherRegistryRegisterError::ServiceAlreadyRegistered {
                    service_name: publisher_info.serviceName.clone(),
                });
            }
            Entry::Vacant(v) => {
                let publisher_entries_reference = Arc::downgrade(&self.publisher_entries);
                let service_name_clone = v.key().clone();
                let death_recipient = self.binder_adapter.link_to_death(
                    publisher,
                    Box::new(move || {
                        Self::on_publisher_death(&publisher_entries_reference, &service_name_clone)
                    }),
                )?;

                v.insert(PublisherEntry {
                    _death_recipient: death_recipient,
                    message_descriptor,
                    publisher: publisher.clone(),
                    supports_get: publisher_info.supportsGet,
                });
            }
        }

        // Notify availability callbacks for the service.
        if let Some(callbacks) = self.availability_callbacks.lock().get(&publisher_info.serviceName)
        {
            for callback in callbacks.iter() {
                if let Some(cb) = callback.upgrade() {
                    cb();
                }
            }
        }

        Ok(())
    }

    pub(crate) async fn unregister_publisher(&self, service_name: &str) -> binder::Result<()> {
        let mut publisher_entries_lock = self.publisher_entries.lock();

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

    fn visit_entry<T>(
        publisher_entries: &Arc<Mutex<HashMap<String, PublisherEntry>>>,
        service_name: &str,
        visitor: impl FnOnce(&PublisherEntry) -> T,
    ) -> Option<T> {
        let publisher_entries_lock = publisher_entries.lock();
        let entry = publisher_entries_lock.get(service_name)?;
        Some(visitor(entry))
    }

    fn on_publisher_death(
        publisher_entries: &Weak<Mutex<HashMap<String, PublisherEntry>>>,
        service_name: &str,
    ) {
        if let Some(publisher_entries) = publisher_entries.upgrade() {
            if publisher_entries.lock().remove(service_name).is_some() {
                info!("Unregistered publisher {service_name} due to Binder death notification.");
            }
        }
    }
}

#[async_trait]
impl ServicePublisherSubscription for ConfigurablePublisherRegistrySubscription {
    async fn unsubscribe(self: Box<Self>) -> anyhow::Result<()> {
        if !self.join_handle.is_finished() {
            self.join_handle.abort();
        }

        self.configurable_publisher_subscription.unsubscribe()?;
        Ok(())
    }
}

#[async_trait]
impl AvailabilityCallbackRegistration for AvailabilityCallbackRegistrationImpl {
    async fn unregister(mut self: Box<Self>) {
        self.unregister_impl();
    }
}

impl Drop for AvailabilityCallbackRegistrationImpl {
    fn drop(&mut self) {
        self.unregister_impl();
    }
}

impl AvailabilityCallbackRegistrationImpl {
    fn unregister_impl(&mut self) {
        let availability_callbacks = mem::take(&mut self.availability_callbacks);
        let mut availability_callbacks_lock = availability_callbacks.lock();
        let Some(callbacks) = availability_callbacks_lock.get_mut(&self.service_name) else {
            return;
        };
        let Some(callback_index) = callbacks
            .iter()
            .position(|each| each.upgrade().is_some_and(|cb| Arc::ptr_eq(&cb, &self.callback)))
        else {
            return;
        };
        callbacks.swap_remove(callback_index);
        if callbacks.is_empty() {
            availability_callbacks_lock.remove(&self.service_name);
        }
    }
}

#[async_trait]
impl ServicePublisherMessageProvider for ServicePublisherMessageProviderImpl {
    async fn get_latest_message(
        &self,
        configuration: Option<&Any>,
    ) -> anyhow::Result<Option<Box<dyn MessageDyn>>> {
        let publisher = self.publisher.upgrade()?;
        let configuration_bytes = configuration.map(serialize_configuration);
        let message = publisher
            .getLatestMessage(configuration_bytes.as_deref())?
            .map(|data| self.message_descriptor.parse_from_bytes(&data))
            .transpose()?;
        Ok(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use binder::BinderFeatures;
    use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::ISubscriber::ISubscriber;
    use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisher::BnConfigurablePublisher;
    use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisherSubscription::{
        IConfigurablePublisherSubscription,
        BnConfigurablePublisherSubscription,
    };
    use protobuf::reflect::ReflectValueRef;
    use protobuf::well_known_types::{any::Any, wrappers::Int32Value};
    use protobuf::{Message, MessageFull};
    use sample_publisher_proto::sample_publisher;
    use sample_publisher_proto::sample_publisher::SampleMessage;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Weak};

    const SERVICE_NAME: &str = "TEST_SERVICE_NAME";
    const SERVICE_NAME2: &str = "TEST_SERVICE_NAME2";

    fn create_empty_fn_box(
    ) -> Box<dyn FnMut(Result<Box<dyn MessageDyn>, ServiceRuntimeError>) + Send + Sync> {
        Box::new(|_| {})
    }

    #[derive(Clone)]
    struct MockConfigurablePublisher {
        /// The last configuration object that was passed to the mock.
        last_configuration: Arc<Mutex<Option<Vec<u8>>>>,
        /// The next message that will be returned by getLatestMessage.
        next_latest_message: Arc<Mutex<Option<Vec<u8>>>>,
        /// The number of times subscribe function of the mock was called.
        num_subscribe_calls: Arc<AtomicU32>,
        /// The number of times subscribe function of the mock was called.
        num_unsubscribe_calls: Arc<AtomicU32>,
        /// Specifies whether the mock supports gets or returns unsupported error when called.
        supports_get: bool,
    }

    impl binder::Interface for MockConfigurablePublisher {}

    impl IConfigurablePublisher for MockConfigurablePublisher {
        fn createSubscription(
            &self,
            _subscriber: &binder::Strong<dyn ISubscriber>,
            configuration: Option<&[u8]>,
        ) -> binder::Result<binder::Strong<dyn IConfigurablePublisherSubscription>> {
            *self.last_configuration.lock() = configuration.map(|config| config.to_owned());
            self.num_subscribe_calls.fetch_add(1, Ordering::AcqRel);

            let binder_subscription = BnConfigurablePublisherSubscription::new_binder(
                MockConfigurablePublisherSubscription {
                    num_unsubscribe_calls: Arc::downgrade(&self.num_unsubscribe_calls),
                },
                BinderFeatures::default(),
            );
            Ok(binder_subscription)
        }

        fn getLatestMessage(
            &self,
            configuration: Option<&[u8]>,
        ) -> binder::Result<Option<Vec<u8>>> {
            *self.last_configuration.lock() = configuration.map(|config| config.to_owned());

            if self.supports_get {
                Ok(self.next_latest_message.lock().take())
            } else {
                Err(binder::Status::new_exception(
                    binder::ExceptionCode::UNSUPPORTED_OPERATION,
                    None,
                ))
            }
        }
    }

    impl MockConfigurablePublisher {
        pub fn new(supports_get: bool) -> Self {
            Self {
                last_configuration: Arc::new(Mutex::new(None)),
                next_latest_message: Arc::new(Mutex::new(None)),
                num_subscribe_calls: Arc::new(AtomicU32::new(0)),
                num_unsubscribe_calls: Arc::new(AtomicU32::new(0)),
                supports_get,
            }
        }

        pub fn get_num_subscribe_calls(&self) -> u32 {
            self.num_subscribe_calls.load(Ordering::Acquire)
        }

        pub fn get_num_unsubscribe_calls(&self) -> u32 {
            self.num_unsubscribe_calls.load(Ordering::Acquire)
        }
    }

    struct MockConfigurablePublisherSubscription {
        num_unsubscribe_calls: Weak<AtomicU32>,
    }

    impl binder::Interface for MockConfigurablePublisherSubscription {}

    impl IConfigurablePublisherSubscription for MockConfigurablePublisherSubscription {
        fn unsubscribe(&self) -> binder::Result<()> {
            self.num_unsubscribe_calls.upgrade().unwrap().fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }

    #[derive(Clone)]
    struct MockBinderAdapter {
        callback: Arc<Mutex<Option<Box<dyn Fn() + Send + Sync>>>>,
    }

    impl<T: binder::FromIBinder + ?Sized> BinderAdapter<T> for MockBinderAdapter {
        fn link_to_death(
            &self,
            _binder: &binder::Strong<T>,
            callback: Box<dyn Fn() + Send + Sync>,
        ) -> Result<binder::DeathRecipient, binder::StatusCode> {
            self.callback.lock().replace(callback);
            Ok(binder::DeathRecipient::new(|| {}))
        }
    }

    impl MockBinderAdapter {
        fn new() -> Self {
            Self { callback: Arc::new(Mutex::new(None)) }
        }
    }

    async fn create_configurable_publisher_registry(
        configurable_publisher: MockConfigurablePublisher,
    ) -> (ConfigurablePublisherRegistry, MockBinderAdapter) {
        let mut configurable_publisher_registry = ConfigurablePublisherRegistry::new();
        let binder_adapter = MockBinderAdapter::new();
        configurable_publisher_registry.binder_adapter = Box::new(binder_adapter.clone());

        let file_descriptor = sample_publisher::file_descriptor();
        let file_descriptor_proto = file_descriptor.proto();
        let descriptor = SampleMessage::descriptor();

        let publisher_info = PublisherInfo {
            serviceName: SERVICE_NAME.to_string(),
            fileDescriptorProto: file_descriptor_proto.write_to_bytes().unwrap(),
            messageTypeName: format!(".{}", descriptor.full_name()),
            supportsGet: configurable_publisher.supports_get,
        };

        let binder_publisher =
            BnConfigurablePublisher::new_binder(configurable_publisher, BinderFeatures::default());

        configurable_publisher_registry
            .register_publisher(&publisher_info, &binder_publisher)
            .await
            .unwrap();

        (configurable_publisher_registry, binder_adapter)
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_metadata_incorrect_service_name_returns_none() {
        let configurable_publisher_registry = ConfigurablePublisherRegistry::new();

        let res = configurable_publisher_registry.get_metadata(SERVICE_NAME).await.0.unwrap();

        assert!(res.is_none());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_metadata_success() {
        let configurable_publisher = MockConfigurablePublisher::new(true);
        let (configurable_publisher_registry, _) =
            create_configurable_publisher_registry(configurable_publisher).await;

        let res =
            configurable_publisher_registry.get_metadata(SERVICE_NAME).await.0.unwrap().unwrap();

        assert!(res.supports_get);
        assert_eq!(res.message_descriptor.proto(), SampleMessage::descriptor().proto());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_message_provider_incorrect_service_name_success() {
        let configurable_publisher_registry = ConfigurablePublisherRegistry::new();

        let outcome = configurable_publisher_registry.get_message_provider(SERVICE_NAME).await.0;

        assert!(
            matches!(outcome, Ok(GetMessageProviderOutcome::NotRegistered)),
            "Unexpected outcome: {outcome:?}"
        );
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_message_provider_get_not_supported_success() {
        let configurable_publisher = MockConfigurablePublisher::new(false);
        let (configurable_publisher_registry, _) =
            create_configurable_publisher_registry(configurable_publisher).await;

        let outcome = configurable_publisher_registry.get_message_provider(SERVICE_NAME).await.0;

        assert!(
            matches!(outcome, Ok(GetMessageProviderOutcome::GetNotSupported)),
            "Unexpected outcome: {outcome:?}"
        );
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_message_provider_success() {
        let config = Int32Value { value: 42, ..Default::default() };
        let msg = SampleMessage { sample_data: 442, ..Default::default() };
        let configurable_publisher = MockConfigurablePublisher::new(true);
        configurable_publisher.next_latest_message.lock().replace(msg.write_to_bytes().unwrap());
        let (configurable_publisher_registry, _) =
            create_configurable_publisher_registry(configurable_publisher.clone()).await;

        let outcome = configurable_publisher_registry.get_message_provider(SERVICE_NAME).await.0;
        let Ok(GetMessageProviderOutcome::Success(message_provider)) = outcome else {
            panic!("Unexpected outcome: {outcome:?}")
        };
        let res = message_provider
            .get_latest_message(Some(&Any::pack(&config).unwrap()))
            .await
            .unwrap()
            .unwrap();

        let last_config = Any::parse_from_bytes(
            configurable_publisher.last_configuration.lock().as_ref().unwrap(),
        )
        .unwrap()
        .unpack::<Int32Value>()
        .unwrap()
        .unwrap();
        assert_eq!(last_config, config);
        let field_descriptor = res.descriptor_dyn().field_by_name("sample_data").unwrap();
        assert_eq!(
            field_descriptor.get_singular(&*res).unwrap(),
            ReflectValueRef::I32(msg.sample_data)
        );
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn register_publisher_death_notification_removes_entry() {
        let configurable_publisher = MockConfigurablePublisher::new(false);
        let (configurable_publisher_registry, binder_adapter) =
            create_configurable_publisher_registry(configurable_publisher).await;

        binder_adapter.callback.lock().as_ref().unwrap()();

        assert!(configurable_publisher_registry.publisher_entries.lock().is_empty());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn create_subscription_success() {
        let configurable_publisher = MockConfigurablePublisher::new(false);
        let (configurable_publisher_registry, _) =
            create_configurable_publisher_registry(configurable_publisher.clone()).await;

        configurable_publisher_registry
            .create_subscription(
                SERVICE_NAME,
                None,
                &SubSamplingInterval::ZERO,
                create_empty_fn_box(),
            )
            .await
            .0
            .unwrap();

        assert_eq!(configurable_publisher.get_num_subscribe_calls(), 1);
        assert!(configurable_publisher_registry
            .publisher_entries
            .lock()
            .contains_key(SERVICE_NAME));
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn create_subscription_incorrect_service_name_returns_not_available() {
        let configurable_publisher_registry = ConfigurablePublisherRegistry::new();

        let res = configurable_publisher_registry
            .create_subscription(
                SERVICE_NAME,
                None,
                &SubSamplingInterval::ZERO,
                create_empty_fn_box(),
            )
            .await
            .0
            .unwrap();

        assert!(matches!(res, CreateSubscriptionOutcome::NotRegistered(_)));
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn unsubscribe_success() {
        let configurable_publisher = MockConfigurablePublisher::new(false);
        let (configurable_publisher_registry, _) =
            create_configurable_publisher_registry(configurable_publisher.clone()).await;

        let subscription = configurable_publisher_registry
            .create_subscription(
                SERVICE_NAME,
                None,
                &SubSamplingInterval::ZERO,
                create_empty_fn_box(),
            )
            .await
            .0
            .unwrap()
            .unwrap();
        subscription.unsubscribe().await.unwrap();

        assert_eq!(configurable_publisher.get_num_subscribe_calls(), 1);
        assert_eq!(configurable_publisher.get_num_unsubscribe_calls(), 1);
    }

    #[tokio::test]
    async fn unregister_success() {
        let configurable_publisher = MockConfigurablePublisher::new(false);
        let (configurable_publisher_registry, _) =
            create_configurable_publisher_registry(configurable_publisher.clone()).await;

        configurable_publisher_registry
            .create_subscription(
                SERVICE_NAME,
                None,
                &SubSamplingInterval::ZERO,
                create_empty_fn_box(),
            )
            .await
            .0
            .unwrap()
            .unwrap();
        configurable_publisher_registry.unregister_publisher(SERVICE_NAME).await.unwrap();

        assert!(!configurable_publisher_registry
            .publisher_entries
            .lock()
            .contains_key(SERVICE_NAME));

        // Unregistering doesn't unsubscribe from the publisher.
        assert_eq!(configurable_publisher.get_num_unsubscribe_calls(), 0);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn unregister_incorrect_service_name_gets_ignored() {
        let configurable_publisher = MockConfigurablePublisher::new(false);
        let (configurable_publisher_registry, _) =
            create_configurable_publisher_registry(configurable_publisher).await;

        configurable_publisher_registry
            .create_subscription(
                SERVICE_NAME,
                None,
                &SubSamplingInterval::ZERO,
                create_empty_fn_box(),
            )
            .await
            .0
            .unwrap()
            .unwrap();
        configurable_publisher_registry.unregister_publisher(SERVICE_NAME2).await.unwrap();

        assert!(configurable_publisher_registry
            .publisher_entries
            .lock()
            .contains_key(SERVICE_NAME));
    }

    #[tokio::test]
    async fn register_availability_callback_success() {
        let registry = ConfigurablePublisherRegistry::new();
        let callback1 = Arc::new(|| {}) as Arc<dyn Fn() + Send + Sync>;
        let callback2 = Arc::new(|| {}) as Arc<dyn Fn() + Send + Sync>;

        let _reg1 = registry
            .register_availability_callback(SERVICE_NAME, &callback1)
            .await
            .unwrap()
            .unwrap();
        let _reg2 = registry
            .register_availability_callback(SERVICE_NAME, &callback2)
            .await
            .unwrap()
            .unwrap();

        // Verify the callbacks are stored
        let callbacks = registry.availability_callbacks.lock();
        let registered_callbacks =
            callbacks.get(SERVICE_NAME).expect("Service should be registered");

        assert_eq!(registered_callbacks.len(), 2, "Expected 2 callbacks registered");

        // Check that the stored callbacks are weak references to the originals
        assert!(Arc::ptr_eq(&callback1, &registered_callbacks[0].upgrade().unwrap()));
        assert!(Arc::ptr_eq(&callback2, &registered_callbacks[1].upgrade().unwrap()));
    }

    #[tokio::test]
    async fn unregister_availability_callback_success() {
        let registry = ConfigurablePublisherRegistry::new();
        let callback1 = Arc::new(|| {}) as Arc<dyn Fn() + Send + Sync>;
        let callback2 = Arc::new(|| {}) as Arc<dyn Fn() + Send + Sync>;

        let reg1 = registry
            .register_availability_callback(SERVICE_NAME, &callback1)
            .await
            .unwrap()
            .unwrap();
        let reg2 = registry
            .register_availability_callback(SERVICE_NAME, &callback2)
            .await
            .unwrap()
            .unwrap();

        // Unregister the first callback
        reg1.unregister().await;

        // Verify only the second callback remains
        {
            let callbacks = registry.availability_callbacks.lock();
            let remaining_callbacks =
                callbacks.get(SERVICE_NAME).expect("Service should still be registered");

            assert_eq!(remaining_callbacks.len(), 1, "Expected only 1 callback remaining");
            assert!(Arc::ptr_eq(&callback2, &remaining_callbacks[0].upgrade().unwrap()));
        }

        // Unregister the second callback
        reg2.unregister().await;

        // Verify the service entry is removed after all callbacks are unregistered
        let callbacks = registry.availability_callbacks.lock();
        assert!(
            !callbacks.contains_key(SERVICE_NAME),
            "Service entry should be removed when no callbacks are left"
        );
    }
}
