// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Service Publisher Accessor that provides access to Comms Stack publishers (Data Tunnel and SDV RPC).

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use dashmap::{mapref::entry::Entry as DashMapEntry, DashMap};
use futures::future::{self, BoxFuture};
use futures::{FutureExt, StreamExt};
use log::{debug, error, info};
use protobuf::reflect::{FileDescriptor, MessageDescriptor};
use protobuf::well_known_types::any::Any;
use protobuf::{Message, MessageFull};
use sdv_comms::sd::{
    ServiceDiscoveryManager, ServiceUnitChangeEvent, ServiceUnitDefinition, UnitMetadata,
    UnitNameDiscoveryArgs,
};
use sdv_comms::ContextRef;
use sdv_config_provider::{vsidl_provider, vsidl_provider_api::VsidlConfigProviderV1};
use sdv_proto_reflection_rs::message_metadata::MessageMetadata;
use sdv_status_rs::SdvStatusCode;
use sdv_telemetry_comms_utils::ServiceName;
use sdv_telemetry_service_publisher_accessor::{
    AvailabilityCallback, AvailabilityCallbackRegistration, CancellationCallback,
    CreateSubscriptionOutcome, GetMessageProviderOutcome, NewMessageCallback,
    ServicePublisherAccessor, ServicePublisherAccessorError, ServicePublisherMetadata,
    SubSamplingInterval,
};
use sdv_telemetry_utils::{descriptor_utils, descriptor_utils::WELL_KNOWN_FILE_DESCRIPTORS};
use std::sync::Arc;
use tokio::select;
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::data_tunnel_subscriber::DataTunnelSubscriber;
use crate::sdv_rpc_client::SdvRpcClient;

/// Service Publisher Accessor that provides access to Comms Stack publishers (Data Tunnel and SDV RPC).
pub struct CommsStackAccessor {
    ctx: future::Shared<BoxFuture<'static, ContextRef>>,

    data_tunnel_subscriber: DataTunnelSubscriber,

    /// Cache to store service metadata (`MessageDescriptor`s) and service unit definition,
    /// as well as service availability notifitications registration.
    /// TODO: b/434121693 - update naming of AvailabilityCallbackRegistration
    cached_metadata: Arc<
        DashMap<ServiceName, (CachedMetadata, Option<Box<dyn AvailabilityCallbackRegistration>>)>,
    >,

    sdv_rpc_client: SdvRpcClient,

    vsidl_provider:
        future::Shared<BoxFuture<'static, Option<Arc<dyn VsidlConfigProviderV1 + Send + Sync>>>>,
}

#[derive(Clone)]
struct CachedMetadata {
    service_unit_definition: ServiceUnitDefinition,
    config_descriptor: Option<MessageDescriptor>,
    message_descriptor: MessageDescriptor,
}

struct AvailabilityCallbackRegistrationImpl {
    _availability_callback_drop_guard: DropGuard,
}

#[async_trait]
impl ServicePublisherAccessor for CommsStackAccessor {
    async fn get_metadata(
        &self,
        service_name: &str,
    ) -> Result<Option<ServicePublisherMetadata>, ServicePublisherAccessorError> {
        let Ok(service_name) = ServiceName::parse(service_name) else {
            return Ok(None);
        };

        let Some(metadata) = self.get_or_parse_metadata(service_name).await? else {
            return Ok(None);
        };

        Ok(Some(ServicePublisherMetadata { message_descriptor: metadata.message_descriptor }))
    }

    async fn get_message_provider(
        &self,
        service_name: &str,
    ) -> Result<GetMessageProviderOutcome, ServicePublisherAccessorError> {
        let Ok(service_name) = ServiceName::parse(service_name) else {
            return Ok(GetMessageProviderOutcome::NotRegistered);
        };

        info!("Creating getter for comms stack service '{service_name}'.");

        let Some(metadata) = self.get_or_parse_metadata(service_name).await? else {
            return Ok(GetMessageProviderOutcome::NotRegistered);
        };

        let unit_identifier = metadata.service_unit_definition.get_unit_identifier();

        match metadata.service_unit_definition.get_unit_metadata() {
            UnitMetadata::Publisher(_) => self
                .data_tunnel_subscriber
                .get_message_provider(unit_identifier, metadata.message_descriptor),
            UnitMetadata::Server(_) => {
                self.sdv_rpc_client
                    .get_message_provider(
                        unit_identifier,
                        metadata.config_descriptor.unwrap_or(Any::descriptor()),
                        metadata.message_descriptor,
                    )
                    .await
            }
        }
    }

    async fn create_subscription<'a>(
        &'a self,
        service_name: &'a str,
        configuration: Option<&'a Any>,
        fetch_last_message: bool,
        sub_sampling_interval: &'a SubSamplingInterval,
        new_message_callback: NewMessageCallback,
        cancellation_callback: CancellationCallback,
    ) -> Result<CreateSubscriptionOutcome, ServicePublisherAccessorError> {
        let Ok(service_name) = ServiceName::parse(service_name) else {
            return Ok(CreateSubscriptionOutcome::NotRegistered {
                new_message_callback,
                cancellation_callback,
            });
        };

        info!("Subscribing to comms stack service '{service_name}'.");

        let Some(metadata) = self.get_or_parse_metadata(service_name.clone()).await? else {
            return Ok(CreateSubscriptionOutcome::NotRegistered {
                new_message_callback,
                cancellation_callback,
            });
        };

        let unit_identifier = metadata.service_unit_definition.get_unit_identifier();

        // Ensure that we cleanup cached metadata on cancellation (= the service we subscribe to
        // stopping), before notifying the rest of the system of the cancellation. This is important
        // because otherwise, the `ServicePublisher` doesn't use availability callback
        // notifications, but polling.
        //
        // The availability callback registered in `Self::get_or_parse_metadata`, which also cleans
        // up metadata when a service is stopping, is not sufficient, because we might receive the
        // notification of the service having stopped only after the cancellation callback runs, and
        // therefore, only after the Telemetry Service tries to re-subscribe.
        let cached_metadata_clone = Arc::clone(&self.cached_metadata);
        let cancellation_callback = Box::new(move || {
            cached_metadata_clone.remove(&service_name);
            cancellation_callback();
        });

        match metadata.service_unit_definition.get_unit_metadata() {
            UnitMetadata::Publisher(_) => {
                self.data_tunnel_subscriber
                    .create_subscription(
                        unit_identifier,
                        fetch_last_message,
                        metadata.message_descriptor,
                        sub_sampling_interval,
                        new_message_callback,
                        cancellation_callback,
                    )
                    .await
            }
            UnitMetadata::Server(_) => {
                self.sdv_rpc_client
                    .create_subscription(
                        unit_identifier,
                        configuration,
                        metadata.config_descriptor.unwrap_or(Any::descriptor()),
                        metadata.message_descriptor,
                        new_message_callback,
                        cancellation_callback,
                    )
                    .await
            }
        }
    }

    async fn register_availability_callback(
        &self,
        service_name: &str,
        callback: &AvailabilityCallback,
    ) -> anyhow::Result<Option<Box<dyn AvailabilityCallbackRegistration>>> {
        let Ok(service_name) = ServiceName::parse(service_name) else {
            return Ok(None);
        };

        let registration = self
            .register_unit_change_callback(
                service_name,
                Arc::clone(callback),
                |event: &ServiceUnitChangeEvent| {
                    matches!(event, ServiceUnitChangeEvent::Registered(_))
                },
            )
            .await?;

        Ok(Some(registration))
    }
}

impl CommsStackAccessor {
    /// Creates a new instance of the Comms Stack Accessor.
    pub fn new(ctx: future::Shared<BoxFuture<'static, ContextRef>>) -> Self {
        let ctx_clone = ctx.clone();
        Self::new_with_vsidl_provider_factory(ctx, move || {
            async move {
                Some(vsidl_provider::new_for_telemetry_agent(ctx_clone.await)).map(|provider| {
                    Arc::new(provider) as Arc<dyn VsidlConfigProviderV1 + Send + Sync>
                })
            }
            .boxed()
        })
    }

    fn new_with_vsidl_provider_factory(
        ctx: future::Shared<BoxFuture<'static, ContextRef>>,
        vsidl_provider_factory: impl FnOnce() -> BoxFuture<
            'static,
            Option<Arc<dyn VsidlConfigProviderV1 + Send + Sync>>,
        >,
    ) -> Self {
        let data_tunnel_subscriber = DataTunnelSubscriber::new(ctx.clone());
        let sdv_rpc_client = SdvRpcClient::new(ctx.clone());

        Self {
            ctx,
            data_tunnel_subscriber,
            cached_metadata: Arc::new(DashMap::new()),
            sdv_rpc_client,
            vsidl_provider: vsidl_provider_factory().shared(),
        }
    }

    /// Helper for retrieving metadata.
    /// Discovers service and parses telemetry metadata, saving it to cache.
    /// If the service is not found, returns None
    async fn get_or_parse_metadata(
        &self,
        service_name: ServiceName,
    ) -> Result<Option<CachedMetadata>, ServicePublisherAccessorError> {
        if let Some(metadata) = self.cached_metadata.view(&service_name, |_k, v| v.0.clone()) {
            return Ok(Some(metadata));
        }

        let Some(service_unit_definition) = self.discover_service(&service_name).await else {
            return Ok(None);
        };

        let (config_descriptor, message_descriptor) = self
            .parse_telemetry_metadata(&service_name, &service_unit_definition)
            .await
            .map_err(ServicePublisherAccessorError::MetadataParsingFailed)?;

        match self.cached_metadata.entry(service_name) {
            DashMapEntry::Occupied(o) => {
                // Concurrent task has already added metadata for this service, use it instead
                Ok(Some(o.get().0.clone()))
            }
            DashMapEntry::Vacant(entry) => {
                let service_name = entry.key().clone();

                let cached_metadata_clone = Arc::clone(&self.cached_metadata);
                let service_name_clone = service_name.clone();

                // Callback for invalidating this cache entry when the service becomes unavailable
                let callback: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
                    cached_metadata_clone.remove(&service_name_clone);
                });

                // Register the callback
                let registration = self
                    .register_unit_change_callback(
                        service_name,
                        Arc::clone(&callback),
                        |event: &ServiceUnitChangeEvent| {
                            matches!(
                                event,
                                ServiceUnitChangeEvent::Unregistered(_)
                                    | ServiceUnitChangeEvent::Changed(_)
                            )
                        },
                    )
                    .await
                    .ok();

                let metadata = CachedMetadata {
                    service_unit_definition,
                    config_descriptor,
                    message_descriptor,
                };
                entry.insert((metadata.clone(), registration));
                Ok(Some(metadata))
            }
        }
    }

    async fn discover_service(&self, service_name: &ServiceName) -> Option<ServiceUnitDefinition> {
        let sd = ServiceDiscoveryManager::new(self.ctx.clone().await);
        match service_name {
            ServiceName::UnitTypeBased { unit_type, unit_name } => {
                let mut units = sd
                    .list_units_by_type(unit_type)
                    .filter(|service_unit| {
                        future::ready(unit_name.as_ref().is_none_or(|unit_name| {
                            service_unit.get_unit_identifier().get_unit_name() == unit_name
                        }))
                    })
                    // We only care about whether there are 0, 1, or 2+ matching units, since we
                    // expect exactly 1. Thus we can stop collecting units after at most 2.
                    .take(2)
                    .collect::<Vec<_>>()
                    .await;

                if units.is_empty() {
                    info!("Could not find service for {service_name}.");
                    None
                } else if units.len() == 1 {
                    Some(units.remove(0))
                } else {
                    error!("Found more than one service for {service_name}.");
                    None
                }
            }
            ServiceName::FqinBased { fqin, unit_name } => sd
                .get_unit(fqin, unit_name.to_owned())
                .await
                .inspect_err(|err| {
                    if err.status_code() == SdvStatusCode::NotFound {
                        info!("Could not find service for {service_name}.");
                    } else {
                        error!("Failed to query service discovery for {service_name}: {err}");
                    }
                })
                .ok(),
        }
    }

    async fn parse_telemetry_metadata(
        &self,
        service_name: &ServiceName,
        service_unit_definition: &ServiceUnitDefinition,
    ) -> anyhow::Result<(Option<MessageDescriptor>, MessageDescriptor)> {
        let app_metadata = service_unit_definition.get_application_metadata();
        if !app_metadata.is_empty() {
            debug!("Service unit definition for {service_name} contains application metadata, using it to retrieve descriptor");

            let publisher_metadata = MessageMetadata::parse_from_bytes(app_metadata.as_slice())
                .with_context(|| "Failed to parse publisher app metadata")?;

            // Build the provided proto file descriptors.
            let file_descriptors = FileDescriptor::new_dynamic_fds(
                publisher_metadata.file_descriptors,
                &WELL_KNOWN_FILE_DESCRIPTORS,
            )
            .with_context(|| "Invalid Descriptor Protos")?;

            return descriptor_utils::find_message_by_full_name(
                file_descriptors.iter().chain(WELL_KNOWN_FILE_DESCRIPTORS.iter()),
                &publisher_metadata.message_full_name
            )
                .map(|message_descriptor| (None, message_descriptor))
                .ok_or_else(|| {
                anyhow!(
                    "Message named \"{}\" not found in file descriptors ({} file descriptors provided)",
                    publisher_metadata.message_full_name,
                    file_descriptors.len()
                )
            });
        }

        debug!("Querying VSIDL provider to retrieve descriptor for {service_name}");
        let vsidl_provider = self
            .vsidl_provider
            .clone()
            .await
            .ok_or_else(|| anyhow!("VSIDL provider is not available"))?;
        match service_unit_definition.get_unit_metadata() {
            UnitMetadata::Publisher(_) => Ok((
                None,
                DataTunnelSubscriber::get_message_descriptor(
                    vsidl_provider.as_ref(),
                    service_unit_definition,
                )
                .await?,
            )),
            UnitMetadata::Server(_) => SdvRpcClient::get_message_descriptor(
                vsidl_provider.as_ref(),
                service_unit_definition,
            )
            .await
            .map(|(config_descriptor, message_descriptor)| {
                (Some(config_descriptor), message_descriptor)
            }),
        }
    }

    async fn register_unit_change_callback(
        &self,
        service_name: ServiceName,
        callback: Arc<dyn Fn() + Send + Sync + 'static>,
        event_matcher: impl Fn(&ServiceUnitChangeEvent) -> bool + Send + Sync + 'static,
    ) -> anyhow::Result<Box<dyn AvailabilityCallbackRegistration>> {
        let sd = ServiceDiscoveryManager::new(self.ctx.clone().await);

        let mut event_stream = match service_name {
            ServiceName::UnitTypeBased { unit_type, unit_name } => sd
                .subscribe_service_unit_change_by_type(&unit_type)
                .await
                .with_context(|| {
                    format!("Failed to subscribe to changes for unit type {unit_type}")
                })?
                .filter(move |event| {
                    if !event_matcher(event) {
                        return future::ready(false);
                    }
                    future::ready(match event {
                        ServiceUnitChangeEvent::Registered(service_unit)
                        | ServiceUnitChangeEvent::Unregistered(service_unit)
                        | ServiceUnitChangeEvent::Changed(service_unit) => {
                            unit_name.as_ref().is_none_or(|unit_name| {
                                service_unit.get_unit_identifier().get_unit_name() == unit_name
                            })
                        }
                    })
                })
                .boxed(),
            ServiceName::FqinBased { fqin, unit_name } => {
                let name_args = UnitNameDiscoveryArgs::new_builder()
                    .set_sdv_package_name(fqin.get_sdv_package_name())
                    .set_service_bundle_name(fqin.get_service_bundle_name())
                    .set_service_unit_name(unit_name.clone())
                    .set_sdv_vm_name(fqin.get_sdv_vm_name())
                    .build()
                    .with_context(|| {
                        format!("Failed to build UnitNameDiscoveryArgs for {fqin}|{unit_name}")
                    })?;
                sd.subscribe_service_unit_change_by_name(&name_args)
                    .await
                    .with_context(|| {
                        format!("Failed to subscribe to changes for service {fqin}|{unit_name}")
                    })?
                    .filter(move |event| {
                        if !event_matcher(event) {
                            return future::ready(false);
                        }
                        future::ready(match event {
                            ServiceUnitChangeEvent::Registered(service_unit)
                            | ServiceUnitChangeEvent::Unregistered(service_unit)
                            | ServiceUnitChangeEvent::Changed(service_unit) => {
                                service_unit
                                    .get_service_bundle_identity()
                                    .get_fqin()
                                    .get_service_instance_name()
                                    == fqin.get_service_instance_name()
                            }
                        })
                    })
                    .boxed()
            }
        };

        let callback_clone = Arc::clone(&callback);
        let cancellation_token = CancellationToken::new();
        let availability_callback_drop_guard = cancellation_token.clone().drop_guard();
        tokio::spawn(async move {
            while let Some(_event) = select! {
                biased;
                _ = cancellation_token.cancelled() => None,
                event = event_stream.next() => event,
            } {
                callback_clone();
            }
        });

        Ok(Box::new(AvailabilityCallbackRegistrationImpl {
            _availability_callback_drop_guard: availability_callback_drop_guard,
        }))
    }
}

#[async_trait]
impl AvailabilityCallbackRegistration for AvailabilityCallbackRegistrationImpl {
    async fn unregister(self: Box<Self>) {}
}

#[cfg(all(test, integration_tests))]
mod integration_tests {
    use super::*;

    use futures::{stream, FutureExt};
    use googletest::description::Description;
    use googletest::matcher::{Matcher, MatcherBase, MatcherResult};
    use googletest::matchers::{
        all, contains_substring, displays_as, each, eq, err, is_false, is_true, none, ok, pat,
        result_of, result_of_ref, some,
    };
    use googletest::{assert_that, expect_that};
    use lazy_static::lazy_static;
    use parking_lot::Mutex;
    use protobuf::descriptor::FileDescriptorProto;
    use protobuf::well_known_types::empty::Empty;
    use protobuf::well_known_types::wrappers::{BoolValue, Int32Value};
    use protobuf::{reflect::ReflectValueRef, MessageDyn, MessageFull};
    use sdv_comms::dt;
    use sdv_comms::id::ServiceFqin;
    use sdv_comms::rpc;
    use sdv_config_provider::vsidl_provider_api::{MockVsidlConfigProviderV1, RpcMethodDescriptor};
    use sdv_status_rs::SdvStatus;
    use sdv_telemetry_service_publisher_accessor::FetchLastMessageOutcome;
    use sdv_telemetry_test_proto::test_message::{self, TestMessage};
    use sdv_telemetry_test_proto_bool_value::{
        test_bool_value_1::BoolValue as TestBoolValue1,
        test_bool_value_2::BoolValue as TestBoolValue2,
    };
    use sdv_telemetry_test_utils::{oneshot_callback, parameterized_test};
    use sdv_telemetry_utils::conversions::u32_into_usize;
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::time::Instant;

    use crate::data_tunnel_publisher::DataTunnelPublisher;
    use crate::test_utils::{self, UNIT_NAME};

    const INVALID_SERVICE_NAME: &str = "lalala";
    const VALID_SERVICE_NAME: &str =
        "com.google.sdv.telemetry.tests.DataTunnelSubscriberTest#Topic|default";

    lazy_static! {
        static ref MESSAGE_METADATA_INT32: MessageMetadata = MessageMetadata {
            file_descriptors: vec![Int32Value::descriptor().file_descriptor().proto().clone()],
            message_full_name: Int32Value::descriptor().full_name().to_owned(),
            ..Default::default()
        };
        static ref RPC_DESCRIPTOR_INT32_REQUEST_RESPONSE: RpcMethodDescriptor =
            RpcMethodDescriptor {
                request_message: Some(Int32Value::descriptor()),
                response_message: Some(Int32Value::descriptor()),
            };
        static ref RPC_DESCRIPTOR_BOOL_REQUEST_RESPONSE: RpcMethodDescriptor =
            RpcMethodDescriptor {
                request_message: Some(BoolValue::descriptor()),
                response_message: Some(BoolValue::descriptor()),
            };
        static ref RPC_DESCRIPTOR_BOOL_REQUEST_RESPONSE_TEST1: RpcMethodDescriptor =
            RpcMethodDescriptor {
                request_message: Some(TestBoolValue1::descriptor()),
                response_message: Some(TestBoolValue1::descriptor()),
            };
        static ref RPC_DESCRIPTOR_BOOL_REQUEST_RESPONSE_TEST2: RpcMethodDescriptor =
            RpcMethodDescriptor {
                request_message: Some(TestBoolValue2::descriptor()),
                response_message: Some(TestBoolValue2::descriptor()),
            };
        static ref RPC_DESCRIPTOR_NO_REQUEST: RpcMethodDescriptor = RpcMethodDescriptor {
            request_message: None,
            response_message: Some(BoolValue::descriptor())
        };
        static ref RPC_DESCRIPTOR_NO_RESPONSE: RpcMethodDescriptor = RpcMethodDescriptor {
            request_message: Some(BoolValue::descriptor()),
            response_message: None
        };
    }

    #[test]
    fn test_constants_are_as_expected() {
        let original =
            RPC_DESCRIPTOR_BOOL_REQUEST_RESPONSE.response_message.as_ref().unwrap().clone();
        let test1 =
            RPC_DESCRIPTOR_BOOL_REQUEST_RESPONSE_TEST1.response_message.as_ref().unwrap().clone();
        let test2 =
            RPC_DESCRIPTOR_BOOL_REQUEST_RESPONSE_TEST2.response_message.as_ref().unwrap().clone();

        // None of the message descriptors should be equal, since they all come from different
        // files.
        assert_ne!(original, test1);
        assert_ne!(original, test2);
        assert_ne!(test1, test2);

        // However, all of the `DescriptorProto`s of the message descriptors should be equal, since
        // they all have the same message name and fields.
        assert_eq!(original.proto(), test1.proto());
        assert_eq!(original.proto(), test2.proto());
        assert_eq!(test1.proto(), test2.proto());

        // All messages should be called "BoolValue".
        assert_that!([original.name(), test1.name(), test2.name()], each(eq("BoolValue")));

        // The package, and therefore the full name, should differ between `original` and `test1` /
        // `test2`.
        assert_ne!(original.file_descriptor().package(), test1.file_descriptor().package());
        assert_ne!(original.file_descriptor().package(), test2.file_descriptor().package());
        assert_ne!(original.full_name(), test1.full_name());
        assert_ne!(original.full_name(), test2.full_name());

        // `test1` and `test2` should have the same package, and therefore, full name.
        assert_eq!(test1.file_descriptor().package(), test2.file_descriptor().package());
        assert_eq!(test1.full_name(), test2.full_name());
    }

    #[derive(MatcherBase)]
    struct MessageDescriptorAndFullNameEq {
        expected: MessageDescriptor,
    }

    impl<'a> Matcher<&'a MessageDescriptor> for MessageDescriptorAndFullNameEq {
        fn matches(&self, actual: &'a MessageDescriptor) -> MatcherResult {
            (self.expected.proto() == actual.proto()
                && self.expected.full_name() == actual.full_name())
            .into()
        }

        fn describe(&self, matcher_result: MatcherResult) -> Description {
            match matcher_result {
                MatcherResult::Match => {
                    format!(
                        "has a full name equal to {:?} and a `DescriptorProto` equal to {:?}",
                        self.expected.full_name(),
                        self.expected.proto()
                    ).into()
                }
                MatcherResult::NoMatch => {
                    format!(
                        "does not have a full name equal to {:?} or does not have a `DescriptorProto` equal to {:?}",
                        self.expected.full_name(),
                        self.expected.proto()
                    ).into()
                }
            }
        }
    }

    fn message_descriptor_and_full_name_eq(
        expected: MessageDescriptor,
    ) -> MessageDescriptorAndFullNameEq {
        MessageDescriptorAndFullNameEq { expected }
    }

    fn create_empty_fn_box() -> Box<dyn FnOnce() + Send + Sync> {
        Box::new(|| unreachable!())
    }

    fn create_empty_message_handler(
    ) -> Box<dyn FnMut(Result<Box<dyn MessageDyn>, ServicePublisherAccessorError>) + Send + Sync>
    {
        Box::new(|_| unreachable!())
    }

    fn create_accessor(ctx: ContextRef) -> CommsStackAccessor {
        CommsStackAccessor::new(future::ready(ctx).boxed().shared())
    }

    fn create_accessor_with_context(fqin_extra: u8) -> CommsStackAccessor {
        let ctx = crate::test_utils::make_context(fqin_extra).boxed().shared();
        CommsStackAccessor::new(ctx)
    }

    fn create_accessor_with_mock_vsidl_provider(
        ctx: ContextRef,
        vsidl_provider: MockVsidlConfigProviderV1,
    ) -> CommsStackAccessor {
        CommsStackAccessor::new_with_vsidl_provider_factory(
            future::ready(ctx).boxed().shared(),
            || {
                future::ready(Some(
                    Arc::new(vsidl_provider) as Arc<dyn VsidlConfigProviderV1 + Send + Sync>
                ))
                .boxed()
            },
        )
    }

    fn get_int_field_value(message: Box<dyn MessageDyn>) -> i32 {
        let int_field_descriptor = message.descriptor_dyn().field_by_name("int_field").unwrap();
        match int_field_descriptor.get_singular(&*message) {
            Some(ReflectValueRef::I32(value)) => value,
            result => panic!("Unexpected value: {result:?}"),
        }
    }

    fn get_bool_field_value(message: Box<dyn MessageDyn>, field_name: &str) -> bool {
        let field_descriptor = message.descriptor_dyn().field_by_name(field_name).unwrap();
        match field_descriptor.get_singular_field_or_default(&*message) {
            ReflectValueRef::Bool(value) => value,
            result => panic!("Unexpected value: {result:?}"),
        }
    }

    fn write_message(publisher: &dt::Publisher, message: TestMessage) {
        let mut bytes = vec![];
        message.write_length_delimited_to_vec(&mut bytes).unwrap();

        assert_eq!(
            bytes.len(),
            u32_into_usize(publisher.get_publisher_unit_metadata().get_message_size())
        );

        publisher.write_messages(&bytes).unwrap();
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_metadata_invalid_service_name_success() {
        let accessor = create_accessor_with_context(0);
        assert!(accessor.get_metadata(INVALID_SERVICE_NAME).await.unwrap().is_none());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_metadata_topic_not_registered_returns_none() {
        let service_name = test_utils::make_unique_service_name();

        let accessor = create_accessor_with_context(0);
        // The service should not exist yet, therefore metadata should be `None`.
        assert!(accessor.get_metadata(&service_name).await.unwrap().is_none());
    }

    parameterized_test!(
        #[googletest::test]
        #[sdv_telemetry_test_utils::timeout]
        #[tokio::test]
        get_metadata_success,

        |message_full_name, file_descriptors: Vec<FileDescriptorProto>, expected_name, expected_proto| async move {
            let ctx = test_utils::make_context(0).await;

            let service_name = test_utils::make_unique_service_name();

            let _publisher = test_utils::register_topic(
                ctx,
                &service_name,
                MessageMetadata { file_descriptors, message_full_name, ..Default::default() },
            )
            .await
            .unwrap();

            let accessor = create_accessor(ctx);
            let publisher_metadata = accessor.get_metadata(&service_name).await;

            expect_that!(
                publisher_metadata,
                ok(some(pat!(&ServicePublisherMetadata {
                    message_descriptor: ref pat!(&MessageDescriptor {
                        name(): expected_name,
                        proto(): &expected_proto,
                    })
                })))
            );
        },

        well_known_type_not_in_file_descriptors: (
            ".google.protobuf.Empty".to_owned(),
            vec![],
            "Empty",
            Empty::descriptor().proto().to_owned(),
        ),

        well_known_type_in_file_descriptors: (
            ".google.protobuf.Empty".to_owned(),
            vec![Empty::descriptor().file_descriptor().proto().to_owned()],
            "Empty",
            Empty::descriptor().proto().to_owned(),
        ),

        custom_message_without_dot: (
            TestMessage::descriptor().full_name().to_owned(),
            vec![test_message::file_descriptor().proto().to_owned()],
            "TestMessage",
            TestMessage::descriptor().proto().to_owned(),
        ),
        custom_message_with_dot: (
            format!(".{}", TestMessage::descriptor().full_name()),
            vec![test_message::file_descriptor().proto().to_owned()],
            "TestMessage",
            TestMessage::descriptor().proto().to_owned(),
        ),
    );

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_message_provider_invalid_service_name_success() {
        let accessor = create_accessor_with_context(0);

        let outcome = accessor.get_message_provider(INVALID_SERVICE_NAME).await;

        assert!(
            matches!(outcome, Ok(GetMessageProviderOutcome::NotRegistered)),
            "Unexpected outcome: {outcome:?}"
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_message_provider_success() {
        let accessor = create_accessor_with_context(0);

        let outcome = accessor.get_message_provider(VALID_SERVICE_NAME).await;

        assert!(
            matches!(outcome, Ok(GetMessageProviderOutcome::NotRegistered)),
            "Unexpected outcome: {outcome:?}"
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_metadata_uses_cache() {
        let ctx = test_utils::make_context(0).await;
        let service_name = test_utils::make_unique_service_name();

        // Register a service
        let _server = test_utils::register_server(
            ctx,
            &service_name,
            None, // No application metadata, force VSIDL provider usage.
            HashMap::new(),
        )
        .await
        .unwrap();

        // Set expectation to have descriptors to be requested by get_metadata only once.
        // Note that RPC Client requests two descriptors within one request
        // (one for both create_subscription and get_latest_message methods), so we do .times(2)
        let mut vsidl_provider_mock = MockVsidlConfigProviderV1::new();
        vsidl_provider_mock
            .expect_get_rpc_method_descriptor()
            .times(2)
            .returning(|_, _, _| Some(RPC_DESCRIPTOR_INT32_REQUEST_RESPONSE.clone()));
        let accessor = create_accessor_with_mock_vsidl_provider(ctx, vsidl_provider_mock);

        // Call the VSIDL provider and populate the cache.
        let metadata1 = accessor.get_metadata(&service_name).await.unwrap();
        assert!(metadata1.is_some());

        // The second call should hit the cache, and calling VSIDL provider will panic
        let metadata2 = accessor.get_metadata(&service_name).await.unwrap();
        assert!(metadata2.is_some());

        assert_eq!(
            metadata1.unwrap().message_descriptor.full_name(),
            metadata2.unwrap().message_descriptor.full_name()
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn metadata_cache_is_invalidated_when_service_is_unregistered() {
        // Start binder thread pool to receive notification from Service Discovery
        binder::ProcessState::start_thread_pool();

        let ctx = test_utils::make_context(0).await;
        let service_name = test_utils::make_unique_service_name();
        let accessor = create_accessor(ctx);
        let parsed_service_name = ServiceName::parse(&service_name).unwrap();

        // Get a notification when the service is unregistered
        let (tx, rx) = oneshot_callback();
        let _registration = accessor
            .register_unit_change_callback(
                parsed_service_name.clone(),
                Arc::new(move || tx(())),
                |event: &ServiceUnitChangeEvent| {
                    matches!(event, ServiceUnitChangeEvent::Unregistered(_))
                },
            )
            .await
            .unwrap();

        // Scope for the publisher, so that it gets dropped and unregistered.
        {
            let _publisher =
                test_utils::register_topic(ctx, &service_name, MESSAGE_METADATA_INT32.clone())
                    .await
                    .unwrap();

            // First call to populate the cache.
            assert!(accessor.get_metadata(&service_name).await.unwrap().is_some());
            assert!(accessor.cached_metadata.contains_key(&parsed_service_name));
        }

        // Wait for an unregistration notification
        rx.await;

        // The service is unregistered, but cache invalidation task might take some time to be
        // executed
        loop {
            if !accessor.cached_metadata.contains_key(&parsed_service_name) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(!accessor.cached_metadata.contains_key(&parsed_service_name));
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn create_subscription_invalid_service_name_success() {
        let accessor = create_accessor_with_context(0);

        let outcome = accessor
            .create_subscription(
                INVALID_SERVICE_NAME,
                None,
                false,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .unwrap();

        assert!(matches!(outcome, CreateSubscriptionOutcome::NotRegistered { .. }));
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn create_subscription_when_topic_does_not_exist_failure() {
        let service_name = test_utils::make_unique_service_name();
        let accessor = create_accessor_with_context(0);

        let outcome = accessor
            .create_subscription(
                &service_name,
                None,
                false,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .unwrap();

        assert!(matches!(outcome, CreateSubscriptionOutcome::NotRegistered { .. }));
    }

    parameterized_test!(
        #[sdv_telemetry_test_utils::timeout]
        #[tokio::test]
        create_subscription_success,

        |fetch_last_message| async move {
            let ctx = test_utils::make_context(0).await;

            let service_name = test_utils::make_unique_service_name();

            let _publisher = test_utils::register_topic(
                ctx,
                &service_name,
                MessageMetadata {
                    file_descriptors: vec![test_message::file_descriptor().proto().clone()],
                    message_full_name: format!(".{}", TestMessage::descriptor().full_name()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

            let subscriber = create_accessor(ctx);
            let outcome = subscriber
                .create_subscription(
                    &service_name,
                    None,
                    fetch_last_message,
                    &SubSamplingInterval::ZERO,
                    create_empty_message_handler(),
                    create_empty_fn_box(),
                )
                .await
                .unwrap();
            assert!(
                if fetch_last_message {
                    matches!(outcome, CreateSubscriptionOutcome::Subscribed {
                        subscription: _,
                        fetch_last_message_outcome: FetchLastMessageOutcome::Enabled,
                    })
                } else {
                    matches!(outcome, CreateSubscriptionOutcome::Subscribed {
                        subscription: _,
                        fetch_last_message_outcome: FetchLastMessageOutcome::Disabled,
                    })
                }
            );
        },

        fetch_last_message_true: (true),
        fetch_last_message_false: (false),
    );

    #[sdv_telemetry_test_utils::timeout(10_000)]
    #[tokio::test]
    async fn create_multiple_subscription_success() {
        let ctx = test_utils::make_context(0).await;

        let service_name = test_utils::make_unique_service_name();

        let _publisher = test_utils::register_topic(
            ctx,
            &service_name,
            MessageMetadata {
                file_descriptors: vec![test_message::file_descriptor().proto().clone()],
                message_full_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let subscriber = create_accessor(ctx);
        let outcome1 = subscriber
            .create_subscription(
                &service_name,
                None,
                false,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .unwrap();

        let outcome2 = subscriber
            .create_subscription(
                &service_name,
                None,
                false,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .unwrap();
        assert!(matches!(
            outcome1,
            CreateSubscriptionOutcome::Subscribed {
                subscription: _,
                fetch_last_message_outcome: FetchLastMessageOutcome::Disabled,
            }
        ));
        assert!(matches!(
            outcome2,
            CreateSubscriptionOutcome::Subscribed {
                subscription: _,
                fetch_last_message_outcome: FetchLastMessageOutcome::Disabled,
            }
        ));
    }

    #[sdv_telemetry_test_utils::timeout(10_000)]
    #[tokio::test]
    async fn create_repeated_subscriptions_and_read_messages_success() {
        let ctx = test_utils::make_context(0).await;

        let service_name = test_utils::make_unique_service_name();

        let publisher = test_utils::register_topic(
            ctx,
            &service_name,
            MessageMetadata {
                file_descriptors: vec![test_message::file_descriptor().proto().clone()],
                message_full_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        write_message(&publisher, TestMessage { int_field: 1, ..Default::default() });
        write_message(&publisher, TestMessage { int_field: 2, ..Default::default() });

        let subscriber = create_accessor(ctx);

        let (tx1, mut rx1) = mpsc::unbounded_channel();
        let outcome1 = subscriber
            .create_subscription(
                &service_name,
                None,
                false,
                &SubSamplingInterval::ZERO,
                Box::new(move |message| tx1.send(message).unwrap()),
                create_empty_fn_box(),
            )
            .await
            .unwrap();

        let (tx2, mut rx2) =
            mpsc::unbounded_channel::<Result<Box<dyn MessageDyn>, ServicePublisherAccessorError>>();
        let outcome2 = subscriber
            .create_subscription(
                &service_name,
                None,
                false,
                &SubSamplingInterval::ZERO,
                Box::new(move |message| tx2.send(message).unwrap()),
                create_empty_fn_box(),
            )
            .await
            .unwrap();

        write_message(&publisher, TestMessage { int_field: 1, ..Default::default() });
        write_message(&publisher, TestMessage { int_field: 2, ..Default::default() });

        assert!(matches!(
            outcome1,
            CreateSubscriptionOutcome::Subscribed {
                subscription: _,
                fetch_last_message_outcome: FetchLastMessageOutcome::Disabled,
            }
        ));
        assert!(matches!(
            outcome2,
            CreateSubscriptionOutcome::Subscribed {
                subscription: _,
                fetch_last_message_outcome: FetchLastMessageOutcome::Disabled,
            }
        ));

        assert_eq!(get_int_field_value(rx1.recv().await.unwrap().unwrap()), 1);
        assert_eq!(get_int_field_value(rx1.recv().await.unwrap().unwrap()), 2);

        assert_eq!(get_int_field_value(rx2.recv().await.unwrap().unwrap()), 1);
        assert_eq!(get_int_field_value(rx2.recv().await.unwrap().unwrap()), 2);
    }

    #[sdv_telemetry_test_utils::timeout(10_000)]
    // Force a multi-threaded runtime for this test, to ensure that the notification timestamps are
    // more accurate, even when one of the threads is stuck in synchronous FFI code.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn create_subscription_and_read_messages_with_sub_sampling_interval_success() {
        let ctx = test_utils::make_context(0).await;

        let service_name = test_utils::make_unique_service_name();
        let notification_interval = SubSamplingInterval::try_from(Duration::from_secs(2)).unwrap();

        let publisher = test_utils::register_topic(
            ctx,
            &service_name,
            MessageMetadata {
                file_descriptors: vec![test_message::file_descriptor().proto().clone()],
                message_full_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let subscriber = create_accessor(ctx);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let outcome = subscriber
            .create_subscription(
                &service_name,
                None,
                false,
                &notification_interval,
                Box::new(move |message| tx.send((Instant::now(), message)).unwrap()),
                create_empty_fn_box(),
            )
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            CreateSubscriptionOutcome::Subscribed {
                subscription: _,
                fetch_last_message_outcome: FetchLastMessageOutcome::Disabled,
            }
        ));

        let message1_sent_at = Instant::now();
        write_message(&publisher, TestMessage { int_field: 1, ..Default::default() });
        let (message1_received_at, message1) = rx.recv().await.unwrap();

        write_message(&publisher, TestMessage { int_field: 2, ..Default::default() });
        write_message(&publisher, TestMessage { int_field: 3, ..Default::default() });
        let (message3_received_at, message3) = rx.recv().await.unwrap();

        assert_eq!(get_int_field_value(message1.unwrap()), 1);
        // Message 2 should be silently discarded.
        assert_eq!(get_int_field_value(message3.unwrap()), 3);
        assert!(
            matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
            "The channel should be empty."
        );

        {
            let elapsed = message1_received_at.duration_since(message1_sent_at);
            // The first notification should arrive almost instantly, but to avoid flakiness, add
            // 20% of leeway.
            let max_elapsed = notification_interval.as_duration().mul_f64(0.2);
            assert!(elapsed <= max_elapsed, "Elapsed time before notification should be at most {max_elapsed:?}, but was {elapsed:?}");
        }
        {
            let elapsed = message3_received_at.duration_since(message1_received_at);
            // The second notification should arrive `notification_interval` after the first
            // notification. To avoid flakiness, add 20% of leeway.
            let min_elapsed = notification_interval.as_duration().mul_f64(0.8);
            assert!(elapsed >= min_elapsed, "Elapsed time before notification should be at least {min_elapsed:?}, but was {elapsed:?}");
        }
    }

    parameterized_test!(
        #[sdv_telemetry_test_utils::timeout(10_000)]
        #[tokio::test]
        recovers_after_data_tunnel_overflow,

        |sub_sampling_interval| async move {
            let ctx = test_utils::make_context(0).await;

            let service_name = test_utils::make_unique_service_name();

            let publisher = test_utils::register_topic_with_message_queue_size(
                ctx,
                &service_name,
                MessageMetadata {
                    file_descriptors: vec![test_message::file_descriptor().proto().clone()],
                    message_full_name: format!(".{}", TestMessage::descriptor().full_name()),
                    ..Default::default()
                },
                6,
            )
            .await
            .unwrap();

            let subscriber = create_accessor(ctx);
            let (tx, mut rx) = mpsc::unbounded_channel();
            let outcome = subscriber
                .create_subscription(
                    &service_name,
                    None,
                    false,
                    &sub_sampling_interval,
                    Box::new(move |message| tx.send(message).unwrap()),
                    create_empty_fn_box(),
                )
                .await
                .unwrap();
            assert!(matches!(outcome, CreateSubscriptionOutcome::Subscribed {
                subscription: _,
                fetch_last_message_outcome: FetchLastMessageOutcome::Disabled
            }));

            // Cause an overflow.
            for i in 1..=30 {
                write_message(&publisher, TestMessage { int_field: i, ..Default::default() });
            }

            if sub_sampling_interval == SubSamplingInterval::ZERO {
                let message = rx.recv().await.unwrap();
                let Err(ServicePublisherAccessorError::MessageRetrievalFailed(error)) = message else {
                    panic!("Unexpected message: {message:?}");
                };
                let Some(error) = error.root_cause().downcast_ref::<SdvStatus>() else {
                    panic!("Unexpected error: {error:?}");
                };
                assert_eq!(error.status_code(), SdvStatusCode::DataLoss);

                // After the overflow occurred, we should be able to read the last `n/2` (=3)
                // messages, where `n` (=6) is the message queue size.
                let message = rx.recv().await.unwrap();
                assert_eq!(get_int_field_value(message.unwrap()), 28);
                let message = rx.recv().await.unwrap();
                assert_eq!(get_int_field_value(message.unwrap()), 29);
                let message = rx.recv().await.unwrap();
                assert_eq!(get_int_field_value(message.unwrap()), 30);
            } else {
                // In case of subsampling `CommsStackAccessor` contains the overflow error and
                // retries a read.
                let message = rx.recv().await.unwrap();
                assert_eq!(get_int_field_value(message.unwrap()), 30);
            }
        },

        no_sub_sampling: (SubSamplingInterval::ZERO,),
        with_sub_sampling: (SubSamplingInterval::try_from(Duration::from_secs(1)).unwrap(),),
    );

    parameterized_test!(
        #[sdv_telemetry_test_utils::timeout]
        #[tokio::test]
        get_metadata_for_rpc,

        |
            message_metadata: Option<MessageMetadata>,
            create_subscription_metadata: Option<RpcMethodDescriptor>,
            get_latest_message_metadata: Option<RpcMethodDescriptor>,
            metadata_matcher
        | async move {
            let ctx = test_utils::make_context(0).await;
            let service_name = test_utils::make_unique_service_name();

            let _server = test_utils::register_server(
                ctx,
                &service_name,
                message_metadata,
                HashMap::from([(
                    "CreateSubscription".to_owned(),
                    rpc::RpcMethod::ServerStreaming(Arc::new(move |_, _| {
                        future::ready(Ok(stream::pending().boxed())).boxed()
                    })),
                ), (
                    "GetLatestMessage".to_owned(),
                    rpc::RpcMethod::Unary(Arc::new(move |_, _| {
                        future::pending().boxed()
                    })),
                )]),
            )
            .await
            .unwrap();

            let mut vsidl_provider_mock = MockVsidlConfigProviderV1::new();
            vsidl_provider_mock
                .expect_get_rpc_method_descriptor()
                .withf(|_, _, method_name| method_name == "CreateSubscription")
                .returning(move |_, _, _| create_subscription_metadata.clone());
            vsidl_provider_mock
                .expect_get_rpc_method_descriptor()
                .withf(|_, _, method_name| method_name == "GetLatestMessage")
                .returning(move |_, _, _| get_latest_message_metadata.clone());
            let subscriber = create_accessor_with_mock_vsidl_provider(ctx, vsidl_provider_mock);

            let metadata = subscriber.get_metadata(&service_name).await;

            assert_that!(metadata, metadata_matcher);
        },

        no_metadata: (
            /* message_metadata:             */ None,
            /* create_subscription_metadata: */ None,
            /* get_latest_message_metadata:  */ None,
            err(displays_as(contains_substring("VSIDL provider was unable to provide RPC method descriptor"))),
        ),
        message_metadata: (
            /* message_metadata:             */ Some(MESSAGE_METADATA_INT32.clone()),
            /* create_subscription_metadata: */ None,
            /* get_latest_message_metadata:  */ None,
            ok(some(pat!(ServicePublisherMetadata {
                message_descriptor: message_descriptor_and_full_name_eq(Int32Value::descriptor()),
            }))),
        ),
        vsidl_provider_is_ignored_if_message_metadata_set: (
            /* message_metadata:             */ Some(MESSAGE_METADATA_INT32.clone()),
            /* create_subscription_metadata: */ Some(RPC_DESCRIPTOR_INT32_REQUEST_RESPONSE.clone()),
            /* get_latest_message_metadata:  */ Some(RPC_DESCRIPTOR_BOOL_REQUEST_RESPONSE.clone()),
            ok(some(pat!(ServicePublisherMetadata {
                message_descriptor: message_descriptor_and_full_name_eq(Int32Value::descriptor()),
            }))),
        ),

        create_subscription_metadata: (
            /* message_metadata:             */ None,
            /* create_subscription_metadata: */ Some(RPC_DESCRIPTOR_INT32_REQUEST_RESPONSE.clone()),
            /* get_latest_message_metadata:  */ None,
            ok(some(pat!(ServicePublisherMetadata {
                message_descriptor: message_descriptor_and_full_name_eq(Int32Value::descriptor()),
            }))),
        ),
        create_subscription_metadata_missing_request: (
            /* message_metadata:             */ None,
            /* create_subscription_metadata: */ Some(RPC_DESCRIPTOR_NO_REQUEST.clone()),
            /* get_latest_message_metadata:  */ None,
            err(displays_as(contains_substring("has no request message"))),
        ),
        create_subscription_metadata_missing_response: (
            /* message_metadata:             */ None,
            /* create_subscription_metadata: */ Some(RPC_DESCRIPTOR_NO_RESPONSE.clone()),
            /* get_latest_message_metadata:  */ None,
            err(displays_as(contains_substring("has no response message"))),
        ),

        get_latest_message_metadata: (
            /* message_metadata:             */ None,
            /* create_subscription_metadata: */ None,
            /* get_latest_message_metadata:  */ Some(RPC_DESCRIPTOR_INT32_REQUEST_RESPONSE.clone()),
            ok(some(pat!(ServicePublisherMetadata {
                message_descriptor: message_descriptor_and_full_name_eq(Int32Value::descriptor()),
            }))),
        ),
        get_latest_message_metadata_missing_request: (
            /* message_metadata:             */ None,
            /* create_subscription_metadata: */ None,
            /* get_latest_message_metadata:  */ Some(RPC_DESCRIPTOR_NO_REQUEST.clone()),
            err(displays_as(contains_substring("has no request message"))),
        ),
        get_latest_message_metadata_missing_response: (
            /* message_metadata:             */ None,
            /* create_subscription_metadata: */ None,
            /* get_latest_message_metadata:  */ Some(RPC_DESCRIPTOR_NO_RESPONSE.clone()),
            err(displays_as(contains_substring("has no response message"))),
        ),

        create_subscription_and_get_latest_message_metadata_equal: (
            /* message_metadata:             */ None,
            /* create_subscription_metadata: */ Some(RPC_DESCRIPTOR_INT32_REQUEST_RESPONSE.clone()),
            /* get_latest_message_metadata:  */ Some(RPC_DESCRIPTOR_INT32_REQUEST_RESPONSE.clone()),
            ok(some(pat!(ServicePublisherMetadata {
                message_descriptor: message_descriptor_and_full_name_eq(Int32Value::descriptor()),
            }))),
        ),
        create_subscription_and_get_latest_message_metadata_equal_but_from_different_files_but_same_package: (
            /* message_metadata:             */ None,
            /* create_subscription_metadata: */ Some(RPC_DESCRIPTOR_BOOL_REQUEST_RESPONSE_TEST1.clone()),
            /* get_latest_message_metadata:  */ Some(RPC_DESCRIPTOR_BOOL_REQUEST_RESPONSE_TEST2.clone()),
            ok(some(pat!(ServicePublisherMetadata {
                message_descriptor: all!(
                    message_descriptor_and_full_name_eq(TestBoolValue1::descriptor()),
                    message_descriptor_and_full_name_eq(TestBoolValue2::descriptor())
                ),
            }))),
        ),
        create_subscription_and_get_latest_message_metadata_equal_but_from_different_files_and_different_packages: (
            /* message_metadata:             */ None,
            /* create_subscription_metadata: */ Some(RPC_DESCRIPTOR_BOOL_REQUEST_RESPONSE.clone()),
            /* get_latest_message_metadata:  */ Some(RPC_DESCRIPTOR_BOOL_REQUEST_RESPONSE_TEST2.clone()),
            err(displays_as(contains_substring("VSIDL provider returned conflicting RPC method descriptors"))),
        ),
        create_subscription_and_get_latest_message_metadata_differ_1: (
            /* message_metadata:             */ None,
            /* create_subscription_metadata: */ Some(RPC_DESCRIPTOR_INT32_REQUEST_RESPONSE.clone()),
            /* get_latest_message_metadata:  */ Some(RPC_DESCRIPTOR_BOOL_REQUEST_RESPONSE.clone()),
            err(displays_as(contains_substring("VSIDL provider returned conflicting RPC method descriptors"))),
        ),
        create_subscription_and_get_latest_message_metadata_differ_2: (
            /* message_metadata:             */ None,
            /* create_subscription_metadata: */ Some(RPC_DESCRIPTOR_INT32_REQUEST_RESPONSE.clone()),
            /* get_latest_message_metadata:  */ Some(RPC_DESCRIPTOR_NO_RESPONSE.clone()),
            err(displays_as(contains_substring("has no response message"))),
        ),
        create_subscription_and_get_latest_message_metadata_differ_3: (
            /* message_metadata:             */ None,
            /* create_subscription_metadata: */ Some(RPC_DESCRIPTOR_NO_RESPONSE.clone()),
            /* get_latest_message_metadata:  */ Some(RPC_DESCRIPTOR_INT32_REQUEST_RESPONSE.clone()),
            err(displays_as(contains_substring("has no response message"))),
        ),
    );

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn create_rpc_subscription_success() {
        let ctx = test_utils::make_context(0).await;
        let service_name = test_utils::make_unique_service_name();
        let msg = Int32Value { value: 42, ..Default::default() };
        let msg_bytes = msg.write_to_bytes().expect("Message should be successfully serialized");
        let _server = test_utils::register_server(
            ctx,
            &service_name,
            Some(MESSAGE_METADATA_INT32.clone()),
            HashMap::from([(
                "CreateSubscription".to_owned(),
                rpc::RpcMethod::ServerStreaming(Arc::new(move |_, _| {
                    future::ready(Ok(stream::select(
                        stream::once(future::ready(Ok(msg_bytes.clone()))),
                        stream::pending(),
                    )
                    .boxed()))
                    .boxed()
                })),
            )]),
        )
        .await
        .unwrap();

        let subscriber = create_accessor(ctx);
        let (tx, rx) = oneshot_callback();
        let outcome = subscriber
            .create_subscription(
                &service_name,
                None,
                false,
                &SubSamplingInterval::ZERO,
                Box::new(tx),
                create_empty_fn_box(),
            )
            .await
            .unwrap();
        let received_msg = rx.await.unwrap();

        let CreateSubscriptionOutcome::Subscribed {
            subscription,
            fetch_last_message_outcome: FetchLastMessageOutcome::NotSupported,
        } = outcome
        else {
            panic!("Unexpected outcome");
        };
        let value_field = received_msg.descriptor_dyn().field_by_name("value").unwrap();
        let value = value_field.get_singular(received_msg.as_ref()).unwrap();
        assert!(
            matches!(&value, ReflectValueRef::I32(val) if *val == msg.value),
            "Unexpected value: {value:?}"
        );

        // Unsubscribe to avoid error sent to `create_subscription` callback triggered by publisher
        // being dropped while still subscribed.
        subscription.unsubscribe().await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_dt_message_provider_not_registered_success() {
        let ctx = test_utils::make_context(0).await;

        let service_name = test_utils::make_unique_service_name();

        let subscriber = create_accessor(ctx);
        assert!(matches!(
            subscriber.get_message_provider(&service_name).await.unwrap(),
            GetMessageProviderOutcome::NotRegistered
        ));
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_dt_message_provider_and_read_messages_success() {
        let ctx = test_utils::make_context(0).await;

        let service_name = test_utils::make_unique_service_name();

        let publisher = test_utils::register_topic(
            ctx,
            &service_name,
            MessageMetadata {
                file_descriptors: vec![test_message::file_descriptor().proto().clone()],
                message_full_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let subscriber = create_accessor(ctx);
        let GetMessageProviderOutcome::Success(provider) =
            subscriber.get_message_provider(&service_name).await.unwrap()
        else {
            panic!("Unexpected outcome");
        };

        let message1 = provider.get_latest_message(None).await;

        write_message(&publisher, TestMessage { int_field: 2, ..Default::default() });
        let message2 = provider.get_latest_message(None).await;
        let message3 = provider.get_latest_message(None).await;

        assert!(matches!(message1, Ok(None)));
        assert_eq!(get_int_field_value(message2.unwrap().unwrap()), 2);
        assert_eq!(get_int_field_value(message3.unwrap().unwrap()), 2);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_rpc_message_provider_success() {
        let ctx = test_utils::make_context(0).await;
        let service_name = test_utils::make_unique_service_name();
        let msg = Int32Value { value: 42, ..Default::default() };
        let msg_bytes = msg.write_to_bytes().expect("Message should be successfully serialized");
        let _server = test_utils::register_server(
            ctx,
            &service_name,
            Some(MESSAGE_METADATA_INT32.clone()),
            HashMap::from([(
                "GetLatestMessage".to_owned(),
                rpc::RpcMethod::Unary(Arc::new(move |_, _| {
                    future::ready(Ok(msg_bytes.clone())).boxed()
                })),
            )]),
        )
        .await
        .unwrap();

        let subscriber = create_accessor(ctx);
        let outcome = subscriber
            .get_message_provider(&service_name)
            .await
            .expect("Message provider retrieval should be successful");
        let GetMessageProviderOutcome::Success(provider) = outcome else {
            panic!("Unexpected outcome: {outcome:?}");
        };
        let received_msg = provider
            .get_latest_message(None)
            .await
            .expect("Latest message retrieval should be successful")
            .expect("Latest message should exist");

        let value_field = received_msg.descriptor_dyn().field_by_name("value").unwrap();
        let value = value_field.get_singular(received_msg.as_ref()).unwrap();
        assert!(
            matches!(&value, ReflectValueRef::I32(val) if *val == msg.value),
            "Unexpected value: {value:?}"
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_rpc_message_provider_latest_message_not_found() {
        let ctx = test_utils::make_context(0).await;
        let service_name = test_utils::make_unique_service_name();

        let _server = test_utils::register_server(
            ctx,
            &service_name,
            Some(MESSAGE_METADATA_INT32.clone()),
            HashMap::from([(
                "GetLatestMessage".to_owned(),
                rpc::RpcMethod::Unary(Arc::new(move |_, _| {
                    future::ready(Err(SdvStatus::new(SdvStatusCode::NotFound))).boxed()
                })),
            )]),
        )
        .await
        .unwrap();

        let subscriber = create_accessor(ctx);
        let outcome = subscriber
            .get_message_provider(&service_name)
            .await
            .expect("Message provider retrieval should be successful");
        let GetMessageProviderOutcome::Success(provider) = outcome else {
            panic!("Unexpected outcome: {outcome:?}");
        };

        let maybe_message = provider.get_latest_message(None).await;

        assert_that!(maybe_message, ok(none()));
    }

    // Unit Type -> 0 matches
    #[sdv_telemetry_test_utils::timeout(5_000)]
    #[tokio::test]
    async fn discover_service_by_unit_type_when_exactly_no_matching_service_exists_fails() {
        let accessor = create_accessor_with_context(0);

        let unit_type = test_utils::make_unique_unit_type();

        let service_name =
            ServiceName::UnitTypeBased { unit_type: unit_type.clone(), unit_name: None };
        let result = accessor.discover_service(&service_name).await;

        assert!(result.is_none(), "Unexpected result: {result:?}");
    }

    // Unit Type -> 1 match
    #[sdv_telemetry_test_utils::timeout(5_000)]
    #[tokio::test]
    async fn discover_service_by_unit_type_when_exactly_one_matching_service_exists_success() {
        let ctx = test_utils::make_context(0).await;
        let accessor = create_accessor(ctx);

        let unit_type = test_utils::make_unique_unit_type();
        let _publisher1 = test_utils::register_topic(
            ctx,
            &format!("{unit_type}|{UNIT_NAME}"),
            MessageMetadata {
                file_descriptors: vec![test_message::file_descriptor().proto().clone()],
                message_full_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let _publisher2 = test_utils::register_topic(
            ctx,
            &format!("{}|another-unit-name", test_utils::make_unique_unit_type()),
            MessageMetadata {
                file_descriptors: vec![test_message::file_descriptor().proto().clone()],
                message_full_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let service_name =
            ServiceName::UnitTypeBased { unit_type: unit_type.clone(), unit_name: None };
        let result = accessor.discover_service(&service_name).await;

        let service_unit_definition = result.unwrap();
        assert_eq!(service_unit_definition.get_unit_type(), unit_type);
        assert_eq!(service_unit_definition.get_unit_identifier().get_unit_name(), UNIT_NAME);
    }

    // Unit Type -> 2 matches
    #[sdv_telemetry_test_utils::timeout(5_000)]
    #[tokio::test]
    async fn discover_service_by_unit_type_when_two_matching_services_exist_fail() {
        let accessor = create_accessor_with_context(1);
        let (ctx2, ctx3) = (test_utils::make_context(2).await, test_utils::make_context(3).await);

        let unit_type = test_utils::make_unique_unit_type();
        let _publisher1 = test_utils::register_topic(
            ctx2,
            &format!("{unit_type}|first-unit-name"),
            MessageMetadata {
                file_descriptors: vec![test_message::file_descriptor().proto().clone()],
                message_full_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let _publisher2 = test_utils::register_topic(
            ctx3,
            &format!("{unit_type}|second-unit-name"),
            MessageMetadata {
                file_descriptors: vec![test_message::file_descriptor().proto().clone()],
                message_full_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let service_name =
            ServiceName::UnitTypeBased { unit_type: unit_type.clone(), unit_name: None };
        let result = accessor.discover_service(&service_name).await;

        assert!(result.is_none(), "Unexpected result: {result:?}");
    }

    // Unit Type + Unit Name -> 0 matches
    #[sdv_telemetry_test_utils::timeout(5_000)]
    #[tokio::test]
    async fn discover_service_by_unit_type_and_unit_name_when_no_matching_service_exists_fails() {
        let accessor = create_accessor_with_context(0);

        let unit_type = test_utils::make_unique_unit_type();

        let service_name = ServiceName::UnitTypeBased {
            unit_type: unit_type.clone(),
            unit_name: Some(UNIT_NAME.to_owned()),
        };
        let result = accessor.discover_service(&service_name).await;

        assert!(result.is_none(), "Unexpected result: {result:?}");
    }

    // Unit Type + Unit Name -> 1 match
    #[sdv_telemetry_test_utils::timeout(5_000)]
    #[tokio::test]
    async fn discover_service_by_unit_type_and_unit_name_when_exactly_one_matching_service_exists_success(
    ) {
        let ctx = test_utils::make_context(0).await;
        let accessor = create_accessor(ctx);

        let unit_type = test_utils::make_unique_unit_type();
        let _publisher1 = test_utils::register_topic(
            ctx,
            &format!("{unit_type}|{UNIT_NAME}"),
            MessageMetadata {
                file_descriptors: vec![test_message::file_descriptor().proto().clone()],
                message_full_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let _publisher2 = test_utils::register_topic(
            ctx,
            &format!("{unit_type}|another-unit-name"),
            MessageMetadata {
                file_descriptors: vec![test_message::file_descriptor().proto().clone()],
                message_full_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let service_name = ServiceName::UnitTypeBased {
            unit_type: unit_type.clone(),
            unit_name: Some(UNIT_NAME.to_owned()),
        };
        let result = accessor.discover_service(&service_name).await;

        let service_unit_definition = result.unwrap();
        assert_eq!(service_unit_definition.get_unit_type(), unit_type);
        assert_eq!(service_unit_definition.get_unit_identifier().get_unit_name(), UNIT_NAME);
    }

    /// This madness forks the current process, and creates a publisher in the child process. It
    /// then returns an object that stops the child when dropped, as well as the FQIN of the
    /// publisher of the child process.
    ///
    /// # Safety
    ///
    /// This function must be called from a single-threaded environment. See the warnings around the
    /// use of `fork` [1] for more information.
    ///
    /// [1]: https://docs.rs/nix/latest/nix/unistd/fn.fork.html#safety
    unsafe fn fork_and_create_publisher(
        fqin_extra: u8,
        service_name: String,
    ) -> (scopeguard::ScopeGuard<(), impl FnOnce(())>, ServiceFqin) {
        use nix::sys::wait::waitpid;
        use nix::unistd::{
            fork, pipe as nix_pipe, read as nix_read, write as nix_write, ForkResult,
        };
        use scopeguard::guard;
        use std::os::fd::AsRawFd;

        // This will be used to send the FQIN from the child process to the parent process.
        let (rx_child_fqin, tx_child_fqin) = nix_pipe().unwrap();
        // This will be used to signal to the child when it should exit.
        let (rx_child_exit, tx_child_exit) = nix_pipe().unwrap();

        // SAFETY: Safe because the caller ensured that this function is called in a single-threaded
        // environment.
        match unsafe { fork() }.unwrap() {
            ForkResult::Parent { child: child_pid, .. } => {
                drop(tx_child_fqin);
                drop(rx_child_exit);

                let mut child_fqin = vec![0; 1000];
                let read_count = nix_read(rx_child_fqin.as_raw_fd(), &mut child_fqin).unwrap();
                child_fqin.resize(read_count, 0);

                let child_fqin =
                    ServiceFqin::from_str(std::str::from_utf8(&child_fqin).unwrap()).unwrap();

                let child = guard((), move |_| {
                    // The child will stop when it receives any message; the message content is
                    // inconsequential.
                    nix_write(tx_child_exit, "stop please".as_bytes()).unwrap();
                    waitpid(child_pid, None).unwrap();
                });

                (child, child_fqin)
            }
            ForkResult::Child => {
                drop(rx_child_fqin);
                drop(tx_child_exit);

                tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap().block_on(
                    async {
                        let ctx = test_utils::make_context(fqin_extra).await;

                        let _publisher = test_utils::register_topic(
                            ctx,
                            &service_name,
                            MessageMetadata {
                                file_descriptors: vec![test_message::file_descriptor()
                                    .proto()
                                    .clone()],
                                message_full_name: format!(
                                    ".{}",
                                    TestMessage::descriptor().full_name()
                                ),
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap();

                        nix_write(tx_child_fqin, format!("{}", ctx.get_self_fqin()).as_bytes())
                            .unwrap();

                        // Wait until we receive a message, then exit.
                        let mut child_exit = vec![0; 1000];
                        nix_read(rx_child_exit.as_raw_fd(), &mut child_exit).unwrap();
                    },
                );
                std::process::exit(0);
            }
        }
    }

    // Unit Type + Unit Name -> 2 matches
    #[test]
    fn discover_service_by_unit_type_and_unit_name_when_two_matching_service_exists_fails() {
        let unit_type = test_utils::make_unique_unit_type();
        // We must create these services in separate processes, because it is impossible to create
        // two DT topics with the same unit name in the same process (even if we specified different
        // FQINs).
        let (_child1, _) =
            // Safety: Safe because tests are configured to run single-threaded (`--test-threads=1`)
            // and it is called before setting up Tokio.
            unsafe { fork_and_create_publisher(1, format!("{unit_type}|{UNIT_NAME}")) };
        let (_child2, _) =
            // Safety: Safe because tests are configured to run single-threaded (`--test-threads=1`)
            // and it is called before setting up Tokio.
            unsafe { fork_and_create_publisher(2, format!("{unit_type}|{UNIT_NAME}")) };

        tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap().block_on(async {
            let accessor = create_accessor_with_context(0);

            let service_name = ServiceName::UnitTypeBased {
                unit_type: unit_type.clone(),
                unit_name: Some(UNIT_NAME.to_owned()),
            };
            let result = accessor.discover_service(&service_name).await;

            assert!(result.is_none(), "Unexpected result: {result:?}");
        });
    }

    // FQIN + Unit Name -> 0 matches
    #[test]
    fn discover_service_by_fqin_when_no_service_with_unit_name_exists_fails() {
        let unit_type = test_utils::make_unique_unit_type();
        let (_child, child_fqin) =
            // Safety: Safe because tests are configured to run single-threaded (`--test-threads=1`)
            // and it is called before setting up Tokio.
            unsafe { fork_and_create_publisher(1, format!("{unit_type}|different-name")) };

        tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap().block_on(async {
            let accessor = create_accessor_with_context(2);

            let service_name =
                ServiceName::FqinBased { fqin: child_fqin, unit_name: UNIT_NAME.to_owned() };
            let result = accessor.discover_service(&service_name).await;

            assert!(result.is_none(), "Unexpected result: {result:?}");
        });
    }

    // FQIN + Unit Name -> 1 match
    #[test]
    fn discover_service_by_fqin_when_exactly_one_service_with_unit_name_exists_success() {
        let unit_type = test_utils::make_unique_unit_type();
        let (_child, child_fqin) =
            // Safety: Safe because tests are configured to run single-threaded (`--test-threads=1`)
            // and it is called before setting up Tokio.
            unsafe { fork_and_create_publisher(1, format!("{unit_type}|{UNIT_NAME}")) };

        tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap().block_on(async {
            let accessor = create_accessor_with_context(2);

            let service_name =
                ServiceName::FqinBased { fqin: child_fqin, unit_name: UNIT_NAME.to_owned() };
            let result = accessor.discover_service(&service_name).await;

            let service_unit_definition = result.unwrap();
            assert_eq!(service_unit_definition.get_unit_type(), unit_type);
            assert_eq!(service_unit_definition.get_unit_identifier().get_unit_name(), UNIT_NAME);
        });
    }

    // FQIN + Unit Name -> 2 matches
    #[sdv_telemetry_test_utils::timeout(5_000)]
    #[tokio::test]
    async fn discover_service_by_fqin_when_two_services_with_unit_name_exist_fails() {
        let ctx = test_utils::make_context(0).await;

        let _publisher1 = test_utils::register_topic(
            ctx,
            &format!("{}|{UNIT_NAME}", test_utils::make_unique_unit_type()),
            MessageMetadata {
                file_descriptors: vec![test_message::file_descriptor().proto().clone()],
                message_full_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        )
        .await;

        let publisher2 = test_utils::register_topic(
            ctx,
            &format!("{}|{UNIT_NAME}", test_utils::make_unique_unit_type()),
            MessageMetadata {
                file_descriptors: vec![test_message::file_descriptor().proto().clone()],
                message_full_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        )
        .await;

        // The current DT Comm's implementation does not allow registration of two services with the
        // same unit name on the same FQIN, therefore, the attempt above to register a second topic
        // for the same FQIN and unit name, but different unit type, should fail. If this ever
        // changes, the logic in `discover_service` needs to be updated.
        assert!(publisher2.is_err());
    }

    parameterized_test!(
        #[sdv_telemetry_test_utils::timeout]
        #[googletest::test]
        #[tokio::test]
        read_message_with_zero_bytes_success,

        |create_publisher_and_get_publish_callback| async move {
            let ctx = test_utils::make_context(0).await;

            let unit_type = test_utils::make_unique_unit_type();
            let unit_name = test_utils::UNIT_NAME;
            let service_name = format!("{}|{}", unit_type, unit_name);
            let config = None;
            let message_metadata = MessageMetadata {
                file_descriptors: vec![BoolValue::descriptor().file_descriptor_proto().to_owned()],
                message_full_name: format!(".{}", BoolValue::descriptor().full_name()),
                ..Default::default()
            };

            let (_publisher, publish) = create_publisher_and_get_publish_callback(ctx, unit_type, unit_name, message_metadata).await;

            let (tx, rx) = oneshot_callback();
            let subscriber = create_accessor(ctx);
            let subscription_outcome = subscriber
                .create_subscription(
                    &service_name,
                    config,
                    false,
                    &SubSamplingInterval::ZERO,
                    Box::new(tx),
                    create_empty_fn_box(),
                )
                .await
                .unwrap();
            assert!(matches!(subscription_outcome, CreateSubscriptionOutcome::Subscribed {
                subscription: _, fetch_last_message_outcome: _
            }));

            let message_provider_outcome =
                subscriber.get_message_provider(&service_name).await.unwrap();
            let GetMessageProviderOutcome::Success(message_provider) = message_provider_outcome else {
                panic!("Unexpected outcome: {message_provider_outcome:?}");
            };

            let message = BoolValue { value: false, ..Default::default() };
            // This test checks that a protobuf message that serializes to an empty byte array is
            // still correctly received. Hence, we assert that the message we use for testing actually
            // produces zero bytes when serialized (otherwise the test makes no sense).
            assert_eq!(message.write_to_bytes().unwrap(), vec![]);
            publish(message).await;

            let subscription_message = rx.await.unwrap();
            let maybe_getter_message = message_provider.get_latest_message(config).await.unwrap();

            expect_that!(
                subscription_message.descriptor_dyn().full_name(),
                eq(BoolValue::descriptor().full_name())
            );
            expect_that!(
                maybe_getter_message,
                some(
                    result_of_ref!(
                        #[expect(clippy::borrowed_box)]
                        |getter_message: &Box<dyn MessageDyn>| getter_message.descriptor_dyn().full_name().to_owned(),
                        eq(&BoolValue::descriptor().full_name())
                    )
                )
            );
            expect_that!(get_bool_field_value(subscription_message, "value"), is_false());
            expect_that!(
                maybe_getter_message,
                some(
                    result_of!(
                        #[expect(clippy::borrowed_box)]
                        |getter_message: &Box<dyn MessageDyn>| get_bool_field_value(getter_message.to_owned(), "value"),
                        is_false()
                    )
                )
            );
        },

        data_tunnel: (async |ctx, unit_type, unit_name, message_metadata: MessageMetadata| {
            let publisher = Arc::new(Mutex::new(
                DataTunnelPublisher::register_topic(
                    ctx,
                    &unit_type,
                    unit_name,
                    10,
                    10,
                    message_metadata
                    .write_to_bytes()
                    .unwrap(),
                )
                .await
                .unwrap()
            ));

            (
                Arc::clone(&publisher),
                async move |message: BoolValue| {
                    publisher.lock().publish_length_delimited_proto(&message).unwrap();
                }
            )
        }),
        rpc: (async |ctx, unit_type, unit_name, message_metadata: MessageMetadata| {
            let (tx, rx1) = oneshot_callback();
            let rx1 = rx1.shared();
            let rx2 = rx1.clone();

            let service_name = format!("{}|{}", unit_type, unit_name);
            let server = test_utils::register_server(
                ctx,
                &service_name,
                Some(message_metadata),
                HashMap::from([(
                    "CreateSubscription".to_owned(),
                    rpc::RpcMethod::ServerStreaming(Arc::new(move |_, _| {
                        future::ready(Ok(
                            stream::once(rx1.clone().map(Ok))
                                .chain(stream::pending())
                                .boxed()
                        )).boxed()
                    })),
                ), (
                    "GetLatestMessage".to_owned(),
                    rpc::RpcMethod::Unary(Arc::new(move |_, _| {
                        rx2.clone().map(Ok).boxed()
                    })),
                )]),
            )
            .await
            .unwrap();

            (
                server,
                async move |message: BoolValue| {
                    tx(message.write_to_bytes().unwrap());
                }
            )
        }),
    );

    #[sdv_telemetry_test_utils::timeout]
    #[googletest::test]
    #[tokio::test]
    async fn metadata_cache_is_invalidated_before_cancellation_callback_runs() {
        // Start binder thread pool to receive notification from Service Discovery
        binder::ProcessState::start_thread_pool();

        let ctx = test_utils::make_context(0).await;
        let service_name = test_utils::make_unique_service_name();
        let parsed_service_name = ServiceName::parse(&service_name).unwrap();
        let accessor = Arc::new(create_accessor(ctx));

        let (tx, rx) = oneshot_callback();

        let accessor_clone = Arc::clone(&accessor);
        let parsed_service_name_clone = parsed_service_name.clone();
        let cancellation_callback = Box::new(move || {
            // Check that metadata is cleared from cache
            expect_that!(
                accessor_clone.cached_metadata.contains_key(&parsed_service_name_clone),
                is_false()
            );
            tx(());
        });

        let publisher =
            test_utils::register_topic(ctx, &service_name, MESSAGE_METADATA_INT32.clone())
                .await
                .unwrap();

        // Populate the cache.
        expect_that!(
            accessor.get_metadata(&service_name).await,
            ok(some(pat!(ServicePublisherMetadata { .. })))
        );
        expect_that!(accessor.cached_metadata.contains_key(&parsed_service_name), is_true());

        let outcome = accessor
            .create_subscription(
                &service_name,
                None,
                false,
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                cancellation_callback,
            )
            .await
            .unwrap();

        assert!(matches!(outcome, CreateSubscriptionOutcome::Subscribed { .. }));

        // Drop the publisher to simulate the service stopping.
        drop(publisher);

        // Wait for the cancellation callback to run.
        rx.await;

        expect_that!(accessor.get_metadata(&service_name).await, ok(none()));
    }
}
