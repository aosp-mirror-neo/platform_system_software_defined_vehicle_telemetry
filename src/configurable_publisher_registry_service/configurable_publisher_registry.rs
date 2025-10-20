// Copyright 2023 Google LLC

// External dependencies.
use anyhow::Context;
use async_trait::async_trait;
use binder_tokio::TokioRuntime;
use binder::BinderFeatures;
use dashmap::{mapref::entry::Entry as DashMapEntry, DashMap};
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{
    IConfigurablePublisher::IConfigurablePublisher,
    IConfigurablePublisherSubscription::IConfigurablePublisherSubscription,
    ISubscriber::BnSubscriber,
    PublisherInfo::PublisherInfo,
};
use log::{info, warn};
use protobuf::descriptor::FileDescriptorProto;
use protobuf::reflect::{FileDescriptor, MessageDescriptor};
use protobuf::well_known_types::any::Any;
use protobuf::{Message, MessageDyn};
use sdv_telemetry_binder_utils::{BinderAdapter, BinderAdapterImpl};
use sdv_telemetry_service_publisher_accessor::{
    AvailabilityCallback, AvailabilityCallbackRegistration, CancellationCallback, CreateSubscriptionOutcome, GetMessageProviderOutcome, NewMessageCallback, ServicePublisherAccessor, ServicePublisherAccessorError, ServicePublisherMessageProvider, ServicePublisherMetadata, ServicePublisherSubscription, SubSamplingInterval
};
use sdv_telemetry_utils::descriptor_utils::WELL_KNOWN_FILE_DESCRIPTORS;
use std::mem;
use std::fmt::Debug;
use std::sync::{Arc, Weak};
use tokio::sync::mpsc;

// Crate dependencies.
use crate::subscriber_service::SubscriberService;

/// ConfigurablePublisherRegistry that maintains the list of configurable publishers in memory.
pub struct ConfigurablePublisherRegistry {
    availability_callbacks: Arc<DashMap<String, Vec<Weak<dyn Fn() + Send + Sync>>>>,
    /// Adapter to make calls into Binder library.
    binder_adapter: Box<dyn BinderAdapter<dyn IConfigurablePublisher>>,
    /// Sender of cancellation callbacks.
    cancellation_sender: mpsc::UnboundedSender<Vec<Box<dyn FnOnce() + Send + Sync>>>,
    /// Hashmap to store configurable publishers that can be subscribed to and their metadata.
    publisher_entries: DashMap<String, PublisherEntry>,
}

pub struct ConfigurablePublisherRegistrySubscription {
    /// The underlying configurable publisher subscription.
    configurable_publisher_subscription: binder::Strong<dyn IConfigurablePublisherSubscription>,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum ConfigurablePublisherRegistryRegisterError {
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
    availability_callbacks: Arc<DashMap<String, Vec<Weak<dyn Fn() + Send + Sync>>>>,
    service_name: String,
    callback: Arc<dyn Fn() + Send + Sync>,
}

/// Publisher registry entry.
struct PublisherEntry {
    cancellation_callbacks: Vec<Box<dyn FnOnce() + Send + Sync>>,
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
    ) -> Result<Option<ServicePublisherMetadata>, ServicePublisherAccessorError> {
        let metadata = self
            .publisher_entries
            .view(service_name, |_k, v| v.message_descriptor.clone())
            .map(|message_descriptor| ServicePublisherMetadata { message_descriptor });
        Ok(metadata)
    }

    async fn get_message_provider(
        &self,
        service_name: &str,
    ) -> Result<GetMessageProviderOutcome, ServicePublisherAccessorError> {
        let provider = self
            .publisher_entries
            .view(service_name, |_k, v| {
                if v.supports_get {
                    GetMessageProviderOutcome::Success(Box::new(
                        ServicePublisherMessageProviderImpl {
                            message_descriptor: v.message_descriptor.clone(),
                            publisher: binder::Strong::downgrade(&v.publisher),
                        },
                    )
                        as Box<dyn ServicePublisherMessageProvider>)
                } else {
                    GetMessageProviderOutcome::GetNotSupported
                }
            })
            .unwrap_or(GetMessageProviderOutcome::NotRegistered);
        Ok(provider)
    }

    async fn create_subscription<'a>(
        &'a self,
        service_name: &'a str,
        configuration: Option<&'a Any>,
        _fetch_last_message: bool,
        // TODO: b/373332101 - Not yet implemented.
        _sub_sampling_interval: &'a SubSamplingInterval,
        new_message_callback: NewMessageCallback,
        cancellation_callback: CancellationCallback,
    ) -> Result<CreateSubscriptionOutcome, ServicePublisherAccessorError> {
        let (message_descriptor, publisher) = {
            let Some(mut entry) = self.publisher_entries.get_mut(service_name) else {
                return Ok(CreateSubscriptionOutcome::NotRegistered {
                    new_message_callback,
                    cancellation_callback,
                });
            };
            entry.cancellation_callbacks.push(cancellation_callback);
            (entry.message_descriptor.clone(), entry.publisher.clone())
        };

        let listener_wrapper = BnSubscriber::new_async_binder(
            SubscriberService::new(message_descriptor, new_message_callback),
            TokioRuntime(tokio::runtime::Handle::current()),
            BinderFeatures::default(),
        );

        let configuration_bytes = configuration.map(serialize_configuration);
        let subscription = publisher
            .createSubscription(&listener_wrapper, configuration_bytes.as_deref())
            .map_err(|err| ServicePublisherAccessorError::FailedToSubscribe(err.into()))?;

        Ok(CreateSubscriptionOutcome::Subscribed(Box::new(
            ConfigurablePublisherRegistrySubscription {
                configurable_publisher_subscription: subscription,
            },
        )))
    }

    async fn register_availability_callback(
        &self,
        service_name: &str,
        callback: &AvailabilityCallback,
    ) -> anyhow::Result<Option<Box<dyn AvailabilityCallbackRegistration>>> {
        let callback_weak = Arc::downgrade(callback);

        match self.availability_callbacks.entry(service_name.to_owned()) {
            DashMapEntry::Occupied(mut o) => o.get_mut().push(callback_weak),
            DashMapEntry::Vacant(v) => {
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
        let (cancellation_sender, mut cancellation_receiver) =
            mpsc::unbounded_channel::<Vec<Box<dyn FnOnce() + Send + Sync>>>();
        tokio::spawn(async move {
            while let Some(cancellation_callbacks) = cancellation_receiver.recv().await {
                for cancellation_callback in cancellation_callbacks.into_iter() {
                    cancellation_callback();
                }
            }
        });

        Self {
            availability_callbacks: Arc::new(DashMap::new()),
            binder_adapter: Box::new(BinderAdapterImpl {}),
            cancellation_sender,
            publisher_entries: DashMap::new(),
        }
    }

    pub(crate) async fn register_publisher(
        self: &Arc<Self>,
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

        match self.publisher_entries.entry(publisher_info.serviceName.clone()) {
            DashMapEntry::Occupied(_) => {
                return Err(ConfigurablePublisherRegistryRegisterError::ServiceAlreadyRegistered {
                    service_name: publisher_info.serviceName.clone(),
                });
            }
            DashMapEntry::Vacant(v) => {
                let self_reference = Arc::downgrade(self);
                let service_name_clone = v.key().clone();
                let death_recipient = self.binder_adapter.link_to_death(
                    publisher,
                    Box::new(move || {
                        if let Some(self_clone) = self_reference.upgrade() {
                            self_clone.on_publisher_death(&service_name_clone);
                        }
                    }),
                )?;

                v.insert(PublisherEntry {
                    cancellation_callbacks: vec![],
                    _death_recipient: death_recipient,
                    message_descriptor,
                    publisher: publisher.clone(),
                    supports_get: publisher_info.supportsGet,
                });
            }
        }

        // Notify availability callbacks for the service.
        if let Some(callbacks) =
            self.availability_callbacks.view(&publisher_info.serviceName, |_k, v| {
                v.iter().filter_map(|callback| callback.upgrade()).collect::<Vec<_>>()
            })
        {
            for callback in callbacks {
                callback();
            }
        }

        Ok(())
    }

    pub(crate) async fn unregister_publisher(&self, service_name: &str) -> binder::Result<()> {
        if !self.try_unregister_publisher(service_name) {
            info!(
                "Unable to unregister service {service_name}. It might have been unregistered already."
            );
        }
        Ok(())
    }

    fn try_unregister_publisher(&self, service_name: &str) -> bool {
        let Some((_k, v)) = self.publisher_entries.remove(service_name) else {
            return false;
        };

        self.cancellation_sender
            .send(v.cancellation_callbacks)
            .expect("Receiver should have the same lifetime as self");

        true
    }

    fn on_publisher_death(&self, service_name: &str) {
        if self.try_unregister_publisher(service_name) {
            info!("Unregistered publisher {service_name} due to Binder death notification.");
        }
    }
}

#[async_trait]
impl ServicePublisherSubscription for ConfigurablePublisherRegistrySubscription {
    async fn unsubscribe(self: Box<Self>) {
        if let Err(err) = self.configurable_publisher_subscription.unsubscribe() {
            warn!("Failed to unsubscribe from configurable publisher: {err:?}");
        }
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
        let DashMapEntry::Occupied(mut o) =
            availability_callbacks.entry(mem::take(&mut self.service_name))
        else {
            return;
        };
        let callbacks = o.get_mut();
        let Some(callback_index) = callbacks
            .iter()
            .position(|each| each.upgrade().is_some_and(|cb| Arc::ptr_eq(&cb, &self.callback)))
        else {
            return;
        };
        callbacks.swap_remove(callback_index);
        if callbacks.is_empty() {
            o.remove();
        }
    }
}

#[async_trait]
impl ServicePublisherMessageProvider for ServicePublisherMessageProviderImpl {
    async fn get_latest_message(
        &self,
        configuration: Option<&Any>,
    ) -> Result<Option<Box<dyn MessageDyn>>, ServicePublisherAccessorError> {
        let publisher = self
            .publisher
            .upgrade()
            .with_context(|| "Publisher is no longer registered.")
            .map_err(ServicePublisherAccessorError::MessageRetrievalFailed)?;
        let configuration_bytes = configuration.map(serialize_configuration);
        let message = publisher
            .getLatestMessage(configuration_bytes.as_deref())
            .map_err(|err| ServicePublisherAccessorError::MessageRetrievalFailed(err.into()))?
            .map(|data| {
                self.message_descriptor
                    .parse_from_bytes(&data)
                    .map_err(|err| ServicePublisherAccessorError::MessageParsingFailed(err.into()))
            })
            .transpose()?;
        Ok(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use binder::BinderFeatures;
    use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{
        IConfigurablePublisher::BnConfigurablePublisher,
        ISubscriber::ISubscriber,
    };
    use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisherSubscription::{
        IConfigurablePublisherSubscription,
        BnConfigurablePublisherSubscription,
    };
    use parking_lot::Mutex;
    use protobuf::reflect::ReflectValueRef;
    use protobuf::well_known_types::{any::Any, wrappers::Int32Value};
    use protobuf::{Message, MessageFull};
    use sample_publisher_proto::sample_publisher;
    use sample_publisher_proto::sample_publisher::SampleMessage;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Weak};

    const SERVICE_NAME: &str = "TEST_SERVICE_NAME";
    const SERVICE_NAME2: &str = "TEST_SERVICE_NAME2";

    fn create_empty_fn_box() -> Box<dyn FnOnce() + Send + Sync> {
        Box::new(|| unreachable!())
    }

    fn create_empty_message_handler(
    ) -> Box<dyn FnMut(Result<Box<dyn MessageDyn>, ServicePublisherAccessorError>) + Send + Sync>
    {
        Box::new(|_| unreachable!())
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
    ) -> (Arc<ConfigurablePublisherRegistry>, MockBinderAdapter) {
        let mut configurable_publisher_registry = ConfigurablePublisherRegistry::new();
        let binder_adapter = MockBinderAdapter::new();
        configurable_publisher_registry.binder_adapter = Box::new(binder_adapter.clone());
        let configurable_publisher_registry = Arc::new(configurable_publisher_registry);

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

        Arc::clone(&configurable_publisher_registry)
            .register_publisher(&publisher_info, &binder_publisher)
            .await
            .unwrap();

        (configurable_publisher_registry, binder_adapter)
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_metadata_incorrect_service_name_returns_none() {
        let configurable_publisher_registry = ConfigurablePublisherRegistry::new();

        let res = configurable_publisher_registry.get_metadata(SERVICE_NAME).await.unwrap();

        assert!(res.is_none());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_metadata_success() {
        let configurable_publisher = MockConfigurablePublisher::new(true);
        let (configurable_publisher_registry, _) =
            create_configurable_publisher_registry(configurable_publisher).await;

        let res =
            configurable_publisher_registry.get_metadata(SERVICE_NAME).await.unwrap().unwrap();

        assert_eq!(res.message_descriptor.proto(), SampleMessage::descriptor().proto());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_message_provider_incorrect_service_name_success() {
        let configurable_publisher_registry = ConfigurablePublisherRegistry::new();

        let outcome = configurable_publisher_registry.get_message_provider(SERVICE_NAME).await;

        assert!(
            matches!(outcome, Ok(GetMessageProviderOutcome::NotRegistered)),
            "Unexpected outcome: {outcome:?}"
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_message_provider_get_not_supported_success() {
        let configurable_publisher = MockConfigurablePublisher::new(false);
        let (configurable_publisher_registry, _) =
            create_configurable_publisher_registry(configurable_publisher).await;

        let outcome = configurable_publisher_registry.get_message_provider(SERVICE_NAME).await;

        assert!(
            matches!(outcome, Ok(GetMessageProviderOutcome::GetNotSupported)),
            "Unexpected outcome: {outcome:?}"
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_message_provider_success() {
        let config = Int32Value { value: 42, ..Default::default() };
        let msg = SampleMessage { sample_data: 442, ..Default::default() };
        let configurable_publisher = MockConfigurablePublisher::new(true);
        configurable_publisher.next_latest_message.lock().replace(msg.write_to_bytes().unwrap());
        let (configurable_publisher_registry, _) =
            create_configurable_publisher_registry(configurable_publisher.clone()).await;

        let outcome = configurable_publisher_registry.get_message_provider(SERVICE_NAME).await;
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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn register_publisher_death_notification_removes_entry() {
        let configurable_publisher = MockConfigurablePublisher::new(false);
        let (configurable_publisher_registry, binder_adapter) =
            create_configurable_publisher_registry(configurable_publisher).await;

        binder_adapter.callback.lock().as_ref().unwrap()();

        assert!(configurable_publisher_registry.publisher_entries.is_empty());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn create_subscription_success() {
        let configurable_publisher = MockConfigurablePublisher::new(false);
        let (configurable_publisher_registry, _) =
            create_configurable_publisher_registry(configurable_publisher.clone()).await;

        configurable_publisher_registry
            .create_subscription(
                SERVICE_NAME,
                None,
                false,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .unwrap();

        assert_eq!(configurable_publisher.get_num_subscribe_calls(), 1);
        assert!(configurable_publisher_registry.publisher_entries.contains_key(SERVICE_NAME));
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn create_subscription_incorrect_service_name_returns_not_available() {
        let configurable_publisher_registry = ConfigurablePublisherRegistry::new();

        let res = configurable_publisher_registry
            .create_subscription(
                SERVICE_NAME,
                None,
                false,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .unwrap();

        assert!(matches!(res, CreateSubscriptionOutcome::NotRegistered { .. }));
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn unsubscribe_success() {
        let configurable_publisher = MockConfigurablePublisher::new(false);
        let (configurable_publisher_registry, _) =
            create_configurable_publisher_registry(configurable_publisher.clone()).await;

        let subscription = configurable_publisher_registry
            .create_subscription(
                SERVICE_NAME,
                None,
                false,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .unwrap()
            .unwrap();
        subscription.unsubscribe().await;

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
                false,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .unwrap()
            .unwrap();
        configurable_publisher_registry.unregister_publisher(SERVICE_NAME).await.unwrap();

        assert!(!configurable_publisher_registry.publisher_entries.contains_key(SERVICE_NAME));

        // Unregistering doesn't unsubscribe from the publisher.
        assert_eq!(configurable_publisher.get_num_unsubscribe_calls(), 0);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn unregister_incorrect_service_name_returns_ok() {
        let configurable_publisher = MockConfigurablePublisher::new(false);
        let (configurable_publisher_registry, _) =
            create_configurable_publisher_registry(configurable_publisher).await;

        configurable_publisher_registry
            .create_subscription(
                SERVICE_NAME,
                None,
                false,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .unwrap()
            .unwrap();
        let res = configurable_publisher_registry.unregister_publisher(SERVICE_NAME2).await;

        assert!(configurable_publisher_registry.publisher_entries.contains_key(SERVICE_NAME));
        assert!(res.is_ok());
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
        let registered_callbacks = registry
            .availability_callbacks
            .get(SERVICE_NAME)
            .expect("Service should be registered");

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
            let remaining_callbacks = registry
                .availability_callbacks
                .get(SERVICE_NAME)
                .expect("Service should still be registered");

            assert_eq!(remaining_callbacks.len(), 1, "Expected only 1 callback remaining");
            assert!(Arc::ptr_eq(&callback2, &remaining_callbacks[0].upgrade().unwrap()));
        }

        // Unregister the second callback
        reg2.unregister().await;

        // Verify the service entry is removed after all callbacks are unregistered
        assert!(
            !registry.availability_callbacks.contains_key(SERVICE_NAME),
            "Service entry should be removed when no callbacks are left"
        );
    }
}
