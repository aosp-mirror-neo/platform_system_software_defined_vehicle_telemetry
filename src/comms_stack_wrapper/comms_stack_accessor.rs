// Copyright 2023 Google LLC

//! Service Publisher Accessor that provides access to Comms Stack publishers (Data Tunnel and SDV RPC).

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use futures::future::{self, BoxFuture};
use futures::StreamExt;
use log::{error, info};
use protobuf::reflect::{FileDescriptor, MessageDescriptor};
use protobuf::well_known_types::any::Any;
use protobuf::{Message, MessageDyn};
use sdv_comms::sd::{
    ServiceDiscoveryManager, ServiceUnitChangeEvent, ServiceUnitDefinition, UnitMetadata,
    UnitNameDiscoveryArgs,
};
use sdv_comms::ContextRef;
use sdv_proto_reflection_rs::message_metadata::MessageMetadata;
use sdv_status_rs::SdvStatusCode;
use sdv_telemetry_service_publisher_accessor::{
    AvailabilityCallbackRegistration, CreateSubscriptionError, CreateSubscriptionOutcome,
    GetMessageProviderError, GetMessageProviderOutcome, GetMetadataError, ServicePublisherAccessor,
    ServicePublisherMetadata, ServiceRuntimeError, SubSamplingInterval,
};
use sdv_telemetry_utils::descriptor_utils;
use sdv_telemetry_utils::{
    descriptor_utils::WELL_KNOWN_FILE_DESCRIPTORS, option_extension::OptionExtension,
};
use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc;
use tokio::{select, sync::Mutex as AsyncMutex};
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::data_tunnel_subscriber::DataTunnelSubscriber;
use crate::sdv_rpc_client::SdvRpcClient;
use crate::service_name::ServiceName;

/// Service Publisher Accessor that provides access to Comms Stack publishers (Data Tunnel and SDV RPC).
pub struct CommsStackAccessor {
    ctx: future::Shared<BoxFuture<'static, ContextRef>>,

    data_tunnel_subscriber: DataTunnelSubscriber,

    /// Hashmap to store service names and their `MessageDescriptor`s.
    service_message_descriptors: AsyncMutex<HashMap<ServiceName, MessageDescriptor>>,

    sdv_rpc_client: SdvRpcClient,
}

struct AvailabilityCallbackRegistrationImpl {
    _availability_callback_drop_guard: DropGuard,
}

#[async_trait]
impl ServicePublisherAccessor for CommsStackAccessor {
    async fn get_metadata(
        &self,
        service_name: &str,
    ) -> Result<Option<ServicePublisherMetadata>, GetMetadataError> {
        let Ok(service_name) = ServiceName::parse(service_name) else {
            return Ok(None);
        };

        let message_descriptor =
            match self.service_message_descriptors.lock().await.entry(service_name) {
                Entry::Occupied(o) => o.get().clone(),
                Entry::Vacant(v) => {
                    let Some(service_unit_definition) = self.discover_service(v.key()).await else {
                        return Ok(None);
                    };

                    let message_descriptor =
                        Self::parse_telemetry_metadata(v.key(), &service_unit_definition)
                            .map_err(GetMetadataError::FailedToParseMetadata)?;
                    v.insert(message_descriptor.clone());
                    message_descriptor
                }
            };

        Ok(Some(ServicePublisherMetadata { supports_get: false, message_descriptor }))
    }

    async fn get_message_provider(
        &self,
        service_name: &str,
    ) -> Result<GetMessageProviderOutcome, GetMessageProviderError> {
        let Ok(service_name) = ServiceName::parse(service_name) else {
            return Ok(GetMessageProviderOutcome::NotRegistered);
        };

        info!("Creating getter for comms stack service '{service_name}'.");

        let Some(service_unit_definition) = self.discover_service(&service_name).await else {
            return Ok(GetMessageProviderOutcome::NotRegistered);
        };

        let message_descriptor =
            Self::parse_telemetry_metadata(&service_name, &service_unit_definition)
                .map_err(GetMessageProviderError::FailedToParseMetadata)?;

        let unit_identifier = service_unit_definition.get_unit_identifier();

        match service_unit_definition.get_unit_metadata() {
            UnitMetadata::Publisher(_) => self
                .data_tunnel_subscriber
                .get_message_provider(unit_identifier, message_descriptor),
            UnitMetadata::Server(_) => {
                self.sdv_rpc_client.get_message_provider(unit_identifier, message_descriptor).await
            }
        }
    }

    async fn create_subscription(
        &self,
        service_name: &str,
        configuration: Option<&Any>,
        sub_sampling_interval: &SubSamplingInterval,
        new_message_callback: Box<
            dyn FnMut(Result<Box<dyn MessageDyn>, ServiceRuntimeError>) + Send + Sync,
        >,
        cancellation_callback: Box<dyn FnOnce() + Send + Sync>,
    ) -> Result<CreateSubscriptionOutcome, CreateSubscriptionError> {
        let Ok(service_name) = ServiceName::parse(service_name) else {
            return Ok(CreateSubscriptionOutcome::NotRegistered {
                new_message_callback,
                cancellation_callback,
            });
        };

        info!("Subscribing to comms stack service '{service_name}'.");

        let Some(service_unit_definition) = self.discover_service(&service_name).await else {
            return Ok(CreateSubscriptionOutcome::NotRegistered {
                new_message_callback,
                cancellation_callback,
            });
        };

        let message_descriptor =
            Self::parse_telemetry_metadata(&service_name, &service_unit_definition)
                .map_err(CreateSubscriptionError::FailedToParseMetadata)?;

        let unit_identifier = service_unit_definition.get_unit_identifier();

        match service_unit_definition.get_unit_metadata() {
            UnitMetadata::Publisher(_) => {
                self.data_tunnel_subscriber
                    .create_subscription(
                        unit_identifier,
                        sub_sampling_interval,
                        message_descriptor,
                        new_message_callback,
                    )
                    .await
            }
            UnitMetadata::Server(_) => {
                self.sdv_rpc_client
                    .create_subscription(
                        unit_identifier,
                        configuration,
                        message_descriptor,
                        new_message_callback,
                    )
                    .await
            }
        }
    }

    async fn register_availability_callback(
        &self,
        service_name: &str,
        callback: &Arc<dyn Fn() + Send + Sync>,
    ) -> anyhow::Result<Option<Box<dyn AvailabilityCallbackRegistration>>> {
        let Ok(service_name) = ServiceName::parse(service_name) else {
            return Ok(None);
        };

        let sd = ServiceDiscoveryManager::new(self.ctx.clone().await);
        let mut event_stream = match service_name {
            ServiceName::UnitTypeBased { unit_type, unit_name } => sd
                .subscribe_service_unit_change_by_type(&unit_type)
                .await
                .with_context(|| "Failed to subscribe to changes for unit type {unit_type}")?
                .filter(move |event| {
                    let res = if let ServiceUnitChangeEvent::Registered(service_unit) = event {
                        OptionExtension::is_none_or(&unit_name, |unit_name| {
                            service_unit.get_unit_identifier().get_unit_name() == unit_name
                        })
                    } else {
                        false
                    };

                    future::ready(res)
                })
                .boxed(),
            ServiceName::FqinBased { fqin, unit_name } => {
                let name_args = UnitNameDiscoveryArgs::new_builder()
                    .set_sdv_package_name(fqin.get_sdv_package_name())
                    .set_service_bundle_name(fqin.get_service_bundle_name())
                    .set_service_unit_name(unit_name)
                    .set_sdv_vm_name(fqin.get_sdv_vm_name())
                    .build()
                    .with_context(|| {
                        "Failed to build UnitNameDiscoveryArgs for {fqin}|{unit_name}"
                    })?;
                sd.subscribe_service_unit_change_by_name(&name_args)
                    .await
                    .with_context(|| {
                        "Failed to subscribe to changes for service {fqin}|{unit_name}"
                    })?
                    .filter(move |event| {
                        let res = if let ServiceUnitChangeEvent::Registered(service_unit) = event {
                            service_unit
                                .get_service_bundle_identity()
                                .get_fqin()
                                .get_service_instance_name()
                                == fqin.get_service_instance_name()
                        } else {
                            false
                        };

                        future::ready(res)
                    })
                    .boxed()
            }
        };

        let callback_clone = Arc::clone(callback);
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

        Ok(Some(Box::new(AvailabilityCallbackRegistrationImpl {
            _availability_callback_drop_guard: availability_callback_drop_guard,
        })))
    }
}

impl CommsStackAccessor {
    /// Creates a new instance of the Comms Stack Accessor.
    pub fn new(ctx: future::Shared<BoxFuture<'static, ContextRef>>) -> Self {
        let data_tunnel_subscriber = DataTunnelSubscriber::new(ctx.clone());
        let sdv_rpc_client = SdvRpcClient::new(ctx.clone());
        Self {
            ctx,
            data_tunnel_subscriber,
            service_message_descriptors: HashMap::new().into(),
            sdv_rpc_client,
        }
    }

    async fn discover_service(&self, service_name: &ServiceName) -> Option<ServiceUnitDefinition> {
        let sd = ServiceDiscoveryManager::new(self.ctx.clone().await);
        match service_name {
            ServiceName::UnitTypeBased { unit_type, unit_name } => {
                let mut units = sd
                    .list_units_by_type(unit_type)
                    .filter(|service_unit| {
                        future::ready(OptionExtension::is_none_or(unit_name, |unit_name| {
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

    fn parse_telemetry_metadata(
        service_name: &ServiceName,
        service_unit_definition: &ServiceUnitDefinition,
    ) -> anyhow::Result<MessageDescriptor> {
        let app_metadata = service_unit_definition.get_application_metadata();
        let publisher_metadata = MessageMetadata::parse_from_bytes(app_metadata.as_slice())
            .with_context(|| {
                format!("Failed to parse publisher app metadata for service '{service_name}'.")
            })?;

        // Build the provided proto file descriptors.
        let file_descriptors = FileDescriptor::new_dynamic_fds(
            publisher_metadata.file_descriptors,
            &WELL_KNOWN_FILE_DESCRIPTORS,
        )
        .map_err(|e| anyhow!("Invalid Descriptor Protos: {}", e))?;

        descriptor_utils::find_message_by_full_name(
            &file_descriptors,
            &publisher_metadata.message_full_name,
        )
        .ok_or(anyhow!(
            "Message Name {} not found in descriptor",
            publisher_metadata.message_full_name
        ))
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
    use protobuf::reflect::ReflectValueRef;
    use protobuf::well_known_types::wrappers::{self, Int32Value};
    use protobuf::MessageFull;
    use sdv_comms::dt;
    use sdv_comms::id::ServiceFqin;
    use sdv_comms::rpc;
    use sdv_status_rs::SdvStatus;
    use sdv_telemetry_test_proto::test_message::{self, TestMessage};
    use sdv_telemetry_test_utils::{oneshot_callback, parameterized_test};
    use sdv_telemetry_utils::conversions::u32_into_usize;
    use std::str::FromStr;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::time::Instant;

    use crate::test_utils::{self, UNIT_NAME};

    const INVALID_SERVICE_NAME: &str = "lalala";
    const VALID_SERVICE_NAME: &str =
        "com.google.sdv.telemetry.tests.DataTunnelSubscriberTest#Topic|default";

    fn create_empty_fn_box() -> Box<dyn FnOnce() + Send + Sync> {
        Box::new(|| unreachable!())
    }

    fn create_empty_message_handler(
    ) -> Box<dyn FnMut(Result<Box<dyn MessageDyn>, ServiceRuntimeError>) + Send + Sync> {
        Box::new(|_| unreachable!())
    }

    fn create_accessor(ctx: ContextRef) -> CommsStackAccessor {
        CommsStackAccessor::new(std::future::ready(ctx).boxed().shared())
    }

    fn create_accessor_with_context(fqin_extra: u8) -> CommsStackAccessor {
        let ctx = crate::test_utils::make_context(fqin_extra).boxed().shared();
        CommsStackAccessor::new(ctx)
    }

    fn get_int_field_value(message: Box<dyn MessageDyn>) -> i32 {
        let int_field_descriptor = message.descriptor_dyn().field_by_name("int_field").unwrap();
        match int_field_descriptor.get_singular(&*message) {
            Some(ReflectValueRef::I32(value)) => value,
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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_metadata_success() {
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

        let accessor = create_accessor(ctx);
        let publisher_metadata = accessor.get_metadata(&service_name).await.unwrap().unwrap();
        assert!(!publisher_metadata.supports_get);
        assert_eq!(publisher_metadata.message_descriptor.name(), "TestMessage");
        assert_eq!(
            publisher_metadata.message_descriptor.proto(),
            TestMessage::descriptor().proto()
        );
    }

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
    async fn create_subscription_invalid_service_name_success() {
        let accessor = create_accessor_with_context(0);

        let outcome = accessor
            .create_subscription(
                INVALID_SERVICE_NAME,
                None,
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
    async fn create_subscription_success() {
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
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .unwrap();
        assert!(matches!(outcome, CreateSubscriptionOutcome::Subscribed(_)));
    }

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
                &SubSamplingInterval::ZERO,
                create_empty_message_handler(),
                create_empty_fn_box(),
            )
            .await
            .unwrap();
        assert!(matches!(outcome1, CreateSubscriptionOutcome::Subscribed(_)));
        assert!(matches!(outcome2, CreateSubscriptionOutcome::Subscribed(_)));
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

        let subscriber = create_accessor(ctx);

        let (tx1, mut rx1) =
            mpsc::unbounded_channel::<Result<Box<dyn MessageDyn>, ServiceRuntimeError>>();
        let outcome1 = subscriber
            .create_subscription(
                &service_name,
                None,
                &SubSamplingInterval::ZERO,
                Box::new(move |message| tx1.send(message).unwrap()),
                create_empty_fn_box(),
            )
            .await
            .unwrap();

        let (tx2, mut rx2) =
            mpsc::unbounded_channel::<Result<Box<dyn MessageDyn>, ServiceRuntimeError>>();
        let outcome2 = subscriber
            .create_subscription(
                &service_name,
                None,
                &SubSamplingInterval::ZERO,
                Box::new(move |message| tx2.send(message).unwrap()),
                create_empty_fn_box(),
            )
            .await
            .unwrap();

        write_message(&publisher, TestMessage { int_field: 1, ..Default::default() });
        write_message(&publisher, TestMessage { int_field: 2, ..Default::default() });

        assert!(matches!(outcome1, CreateSubscriptionOutcome::Subscribed(_)));
        assert!(matches!(outcome2, CreateSubscriptionOutcome::Subscribed(_)));

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
                &notification_interval,
                Box::new(move |message| tx.send((Instant::now(), message)).unwrap()),
                create_empty_fn_box(),
            )
            .await
            .unwrap();
        assert!(matches!(outcome, CreateSubscriptionOutcome::Subscribed(_)));

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
                    &sub_sampling_interval,
                    Box::new(move |message| tx.send(message).unwrap()),
                    create_empty_fn_box(),
                )
                .await
                .unwrap();
            assert!(matches!(outcome, CreateSubscriptionOutcome::Subscribed(_)));

            // Cause an overflow.
            for i in 1..=30 {
                write_message(&publisher, TestMessage { int_field: i, ..Default::default() });
            }

            let message = rx.recv().await.unwrap();
            let Err(ServiceRuntimeError::MessageRetrievalError(error)) = message else {
                panic!("Unexpected message: {message:?}");
            };
            let Some(error) = error.root_cause().downcast_ref::<SdvStatus>() else {
                panic!("Unexpected error: {error:?}");
            };
            assert_eq!(error.status_code(), SdvStatusCode::DataLoss);

            if sub_sampling_interval == SubSamplingInterval::ZERO {
                // After the overflow occurred, we should be able to read the last `n/2` (=3)
                // messages, where `n` (=6) is the message queue size.
                let message = rx.recv().await.unwrap();
                assert_eq!(get_int_field_value(message.unwrap()), 28);
                let message = rx.recv().await.unwrap();
                assert_eq!(get_int_field_value(message.unwrap()), 29);
                let message = rx.recv().await.unwrap();
                assert_eq!(get_int_field_value(message.unwrap()), 30);
            } else {
                // After the overflow occurred, we should only receive the latest message.
                let message = rx.recv().await.unwrap();
                assert_eq!(get_int_field_value(message.unwrap()), 30);
            }
        },

        no_sub_sampling: (SubSamplingInterval::ZERO,),
        with_sub_sampling: (SubSamplingInterval::try_from(Duration::from_secs(1)).unwrap(),),
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
            MessageMetadata {
                file_descriptors: vec![wrappers::file_descriptor().proto().clone()],
                message_full_name: Int32Value::descriptor().full_name().to_owned(),
                ..Default::default()
            },
            "CreateSubscription".to_owned(),
            rpc::RpcMethod::ServerStreaming(Arc::new(move |_, _| {
                future::ready(Ok(stream::select(
                    stream::once(future::ready(Ok(msg_bytes.clone()))),
                    stream::pending(),
                )
                .boxed()))
                .boxed()
            })),
        )
        .await
        .unwrap();

        let subscriber = create_accessor(ctx);
        let (tx, rx) = oneshot_callback();
        let outcome = subscriber
            .create_subscription(
                &service_name,
                None,
                &SubSamplingInterval::ZERO,
                Box::new(tx),
                create_empty_fn_box(),
            )
            .await
            .unwrap();
        let received_msg = rx.await.unwrap();

        let CreateSubscriptionOutcome::Subscribed(subscription) = outcome else {
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
        subscription.unsubscribe().await.unwrap();
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
            MessageMetadata {
                file_descriptors: vec![wrappers::file_descriptor().proto().clone()],
                message_full_name: Int32Value::descriptor().full_name().to_owned(),
                ..Default::default()
            },
            "GetLatestMessage".to_owned(),
            rpc::RpcMethod::Unary(Arc::new(move |_, _| {
                future::ready(Ok(msg_bytes.clone())).boxed()
            })),
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
}
