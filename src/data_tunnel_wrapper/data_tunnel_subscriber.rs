// Copyright 2023 Google LLC

//! Telemetry subscription logic for DT.

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use futures::{future, StreamExt};
use log::{error, info};
use protobuf::reflect::{FileDescriptor, MessageDescriptor};
use protobuf::well_known_types::any::Any;
use protobuf::{Message, MessageDyn};
use sdv_comms::dt;
use sdv_comms::id::ServiceFqin;
use sdv_comms::sd::{ServiceDiscoveryManager, ServiceUnitDefinition, UnitType};
use sdv_comms::ContextRef;
use sdv_status_rs::SdvStatusCode;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::iter::Iterator;
use std::str::FromStr;
use tokio::select;
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::{CancellationToken, DropGuard};

use sdv_telemetry::service_publisher_accessor::{
    CreateSubscriptionOutcome, ServicePublisherMessageProvider, ServicePublisherMetadata,
    ServicePublisherSubscription, SubSamplingInterval,
};
use sdv_telemetry::ServicePublisherAccessor;
use sdv_telemetry_metadata_proto_rust::publisher_metadata::TelemetryPublisherMetadata;
use sdv_telemetry_utils::conversions::u32_into_usize;
use sdv_telemetry_utils::descriptor_utils::WELL_KNOWN_FILE_DESCRIPTORS;
use sdv_telemetry_utils::option_extension::OptionExtension;

/// The old, deprecated implementation of a data tunnel subscriber for the DT API v0.
pub type DeprecatedDataTunnelSubscriber =
    crate::deprecated_data_tunnel_subscriber::DeprecatedDataTunnelSubscriber;

/// Struct for storing all data tunnel subscriptions that implements `ServicePublisherManager`.
pub struct DataTunnelSubscriber {
    ctx: ContextRef,

    /// Hashmap to store service names and their `MessageDescriptor`s.
    service_message_descriptors: AsyncMutex<HashMap<ServiceName, MessageDescriptor>>,
}

/// Struct for subscription of DT topic.
pub struct DataTunnelSubscription {
    _subscription_drop_guard: DropGuard,
}

#[derive(Eq, PartialEq, Hash, Debug)]
enum ServiceName {
    UnitTypeBased { unit_type: UnitType, unit_name: Option<String> },
    FqinBased { fqin: ServiceFqin, unit_name: String },
}

impl ServiceName {
    fn parse(input: &str) -> anyhow::Result<Self> {
        if input.contains(':') {
            let (fqin, unit_name) = input.rsplit_once("|").ok_or(anyhow!(
                "Missing `|` in input '{input}' to divide the FQIN from the Service Unit Name."
            ))?;
            ServiceFqin::from_str(fqin)
                .with_context(|| "Failed to parse FQIN.")
                .map(|fqin| Self::FqinBased { fqin, unit_name: unit_name.to_owned() })
        } else {
            let (unit_type, unit_name) = input
                .rsplit_once("|")
                .map(|(unit_type, unit_name)| (unit_type, Some(unit_name.to_owned())))
                .unwrap_or((input, None));
            UnitType::from_str(unit_type)
                .with_context(|| "Failed to parse Unit Type.")
                .map(|unit_type| Self::UnitTypeBased { unit_type, unit_name })
        }
    }
}

impl fmt::Display for ServiceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnitTypeBased { unit_type, unit_name } => {
                write!(
                    f,
                    "UnitTypeBased(unit_type: {unit_type}, unit_name: {})",
                    unit_name.as_deref().unwrap_or("N/A")
                )
            }
            Self::FqinBased { fqin, unit_name } => {
                write!(f, "FqinBased(fqin: {fqin}, unit_name: {unit_name})")
            }
        }
    }
}

#[async_trait]
impl ServicePublisherAccessor for DataTunnelSubscriber {
    async fn get_metadata(
        &self,
        service_name: &str,
    ) -> anyhow::Result<Option<ServicePublisherMetadata>> {
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
                        Self::parse_telemetry_metadata(v.key(), &service_unit_definition)?;
                    v.insert(message_descriptor.clone());
                    message_descriptor
                }
            };

        Ok(Some(ServicePublisherMetadata { supports_get: false, message_descriptor }))
    }

    fn get_message_provider(
        &self,
        _service_name: &str,
    ) -> anyhow::Result<Option<Box<dyn ServicePublisherMessageProvider>>> {
        Ok(None)
    }

    async fn create_subscription(
        &self,
        service_name: &str,
        _configuration: Option<&Any>,
        sub_sampling_interval: &SubSamplingInterval,
        mut callback: Box<dyn FnMut(Box<dyn MessageDyn>) + Send + Sync>,
    ) -> anyhow::Result<CreateSubscriptionOutcome> {
        let Ok(service_name) = ServiceName::parse(service_name) else {
            return Ok(CreateSubscriptionOutcome::NotRegistered(callback));
        };

        info!("Subscribing to data tunnel for service '{service_name}'.");

        let Some(service_unit_definition) = self.discover_service(&service_name).await else {
            return Ok(CreateSubscriptionOutcome::NotRegistered(callback));
        };

        let message_descriptor =
            Self::parse_telemetry_metadata(&service_name, &service_unit_definition)
                .with_context(|| format!("Metadata not found for service '{service_name}'."))?;

        let unit_identifier = service_unit_definition.get_unit_identifier();

        let mut subscriber_stream = dt::SubscriberStream::create(
            self.ctx,
            &unit_identifier,
            sub_sampling_interval.as_duration(),
            dt::SubscriberOptions { fetch_last_message: false },
        )
            .await
            .with_context(||
                format!("Failed to subscribe to unit identifier {unit_identifier:?} for service '{service_name}' with sub-sampling interval {sub_sampling_interval:?}.")
            )?;

        let cancellation_token = CancellationToken::new();
        let subscription_drop_guard = cancellation_token.clone().drop_guard();
        let sub_sampling_interval = sub_sampling_interval.to_owned();
        tokio::spawn(async move {
            let publisher_unit_metadata = subscriber_stream.get_publisher_unit_metadata();
            let message_size = publisher_unit_metadata.get_message_size();

            // This loop runs infinitely until the cancellation token fires. In case of an error
            // while message parsing, the data is lost, as data was retrieved successfully from the
            // data tunnel. In case of data tunnel errors, data is not lost, and can be fetched in
            // the next iteration of the loop.
            while let Some(result) = select! {
                biased;
                _ = cancellation_token.cancelled() => None,
                result = subscriber_stream.next() => result,
            } {
                let Ok((number_of_messages, buffer)) = result.inspect_err(|err| {
                    error!("Data Tunnel message stream of service '{service_name}' returned an intermittent error: {err:?}")
                }) else {
                    continue;
                };
                let messages: Box<dyn Iterator<Item = _>> = {
                    let all_messages = buffer
                        .chunks(u32_into_usize(message_size))
                        .take(u32_into_usize(number_of_messages));
                    if sub_sampling_interval == SubSamplingInterval::ZERO {
                        Box::new(all_messages)
                    } else {
                        // Only process the last (=newest) message.
                        Box::new(all_messages.rev().take(1))
                    }
                };

                info!("Received {number_of_messages} messages for service '{service_name}'.");
                for message_bytes in messages {
                    match Self::read_length_delimited_proto(message_bytes, &message_descriptor) {
                        Ok(msg) => {
                            (callback)(msg.clone());
                        }
                        Err(err) => error!(
                            "Failed to convert bytes to protobuf for topic '{service_name}': {err:?}"
                        ),
                    };
                }
            }
        });

        let data_tunnel_subscription =
            Box::new(DataTunnelSubscription { _subscription_drop_guard: subscription_drop_guard });

        Ok(CreateSubscriptionOutcome::Subscribed(data_tunnel_subscription))
    }
}

/// Subscriber struct impl that encapsulates the data required for reading messages.
impl DataTunnelSubscriber {
    /// Creates a new instance of the data tunnel subscriber.
    pub fn new(ctx: ContextRef) -> Self {
        Self { service_message_descriptors: HashMap::new().into(), ctx }
    }

    async fn discover_service(&self, service_name: &ServiceName) -> Option<ServiceUnitDefinition> {
        let sd = ServiceDiscoveryManager::new(self.ctx);
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
                        error!("Failed to query service discovery for {service_name}: {err:?}");
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
        let publisher_metadata =
            TelemetryPublisherMetadata::parse_from_bytes(app_metadata.as_slice()).with_context(
                || format!("Failed to parse publisher app metadata for service '{service_name}'."),
            )?;

        // Build the provided proto file descriptors.
        let file_descriptors = FileDescriptor::new_dynamic_fds(
            publisher_metadata.descriptor_protos,
            &WELL_KNOWN_FILE_DESCRIPTORS,
        )
        .map_err(|e| anyhow!("Invalid Descriptor Protos: {:?}", e))?;

        // Iterate through all `file_descriptors` and find `MessageDescriptor` for the given
        // `message_type_name`. `message_by_full_name()` function requires the name to start
        // with a dot. The function returns a clone of the `MessageDescriptor`, so we can
        // return it as-is without cloning again.
        file_descriptors
            .iter()
            .find_map(|fd| fd.message_by_full_name(&publisher_metadata.message_type_name))
            .ok_or(anyhow!(
                "Message Name {} not found in descriptor",
                publisher_metadata.message_type_name
            ))
    }

    fn read_length_delimited_proto(
        bytes: &[u8],
        message_descriptor: &MessageDescriptor,
    ) -> anyhow::Result<Box<dyn MessageDyn>> {
        let mut is = protobuf::CodedInputStream::from_bytes(bytes);
        let message_size = is
            .read_raw_varint32()
            .with_context(|| "Failed to read the size of the protobuf message.")?
            .into();
        is.push_limit(message_size)
            .with_context(|| "Failed to push the limit of the protobuf stream.")?;

        let message = message_descriptor
            .parse_from(&mut is)
            .with_context(|| "Failed to parse the protobuf messagw.")?;
        Ok(message)
    }
}

#[async_trait]
impl ServicePublisherSubscription for DataTunnelSubscription {
    async fn unsubscribe(self: Box<Self>) -> anyhow::Result<()> {
        // This drops `self` and therefore cancels the subscription via the `DropGuard`.
        Ok(())
    }
}

#[cfg(test)]
async fn make_context(fqin_extra: u8) -> ContextRef {
    use sdv_telemetry_comms_utils::wait_for_sdv_agents_and_create_context;
    use tokio::sync::OnceCell;

    static CONTEXT_REF: OnceCell<ContextRef> = OnceCell::const_new();
    *CONTEXT_REF
        .get_or_init(|| async {
            // Tests may run in multiple parallel processes, therefore we need to ensure that the FQINs
            // created by the processes do not conflict with each other. This is achieved by embedding
            // the process ID into the FQIN.
            let fqin = ServiceFqin::from_str(&format!(
                "vm:com.google.sdv.telemetry.tests.DataTunnelSubscriberTest{}{}/instance",
                std::process::id(),
                fqin_extra
            ))
            .unwrap();
            // The FQIN of the created context is actually silently updated with the correct VM
            // name, so we must not rely on `fqin`, but instead query it from the Context via
            // `ctx.get_self_fqin()`.
            wait_for_sdv_agents_and_create_context(fqin).await
        })
        .await
}

#[cfg(all(test, unit_tests))]
mod tests {
    use super::*;

    use googletest::prelude::*;
    use sdv_telemetry_test_utils::parameterized_test;

    const VALID_SERVICE_NAME: &str =
        "com.google.sdv.telemetry.tests.DataTunnelSubscriberTest#Topic|default";
    const INVALID_SERVICE_NAME: &str = "lalala";

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_metadata_invalid_service_name_success() {
        let ctx = make_context(0).await;
        let subscriber = DataTunnelSubscriber::new(ctx);
        assert!(subscriber.get_metadata(INVALID_SERVICE_NAME).await.unwrap().is_none());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_message_provider_invalid_service_name_success() {
        let ctx = make_context(0).await;
        let subscriber = DataTunnelSubscriber::new(ctx);
        assert!(subscriber.get_message_provider(INVALID_SERVICE_NAME).unwrap().is_none());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn create_subscription_invalid_service_name_success() {
        let ctx = make_context(0).await;
        let subscriber = DataTunnelSubscriber::new(ctx);
        assert!(matches!(
            subscriber
                .create_subscription(
                    INVALID_SERVICE_NAME,
                    None,
                    &SubSamplingInterval::ZERO,
                    Box::new(|_| {})
                )
                .await
                .unwrap(),
            CreateSubscriptionOutcome::NotRegistered(_)
        ));
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_message_provider_success() {
        let ctx = make_context(0).await;
        let subscriber = DataTunnelSubscriber::new(ctx);
        assert!(subscriber.get_message_provider(VALID_SERVICE_NAME).unwrap().is_none());
    }

    parameterized_test!(
        service_name_parser,

        |input, expected| {
            assert_that!(ServiceName::parse(input), expected);
        },

        unsupported_service_name_fails: (
            "hello",
            err(anything()),
        ),
        multiple_pipes_fails: (
            "vm:com.google.sdv.telemetry.tests.Blah/instance|the-unit-name|another-thing",
            err(anything()),
        ),
        fqin_fails: (
            "vm:com.google.sdv.telemetry.tests.Blah/instance",
            err(anything()),
        ),
        fqin_with_unit_name_success: (
            "vm:com.google.sdv.telemetry.tests.Blah/instance|the-unit-name",
            ok(
                eq(
                    ServiceName::FqinBased{
                        fqin: ServiceFqin::from_str("vm:com.google.sdv.telemetry.tests.Blah/instance").unwrap(),
                        unit_name: "the-unit-name".to_owned()
                    }
                )
            ),
        ),
        unit_type_without_unit_name_success: (
            "com.google.sdv.telemetry.tests.Blah#Blubb",
            ok(
                eq(
                    ServiceName::UnitTypeBased{
                        unit_type: UnitType::from_str("com.google.sdv.telemetry.tests.Blah#Blubb").unwrap(),
                        unit_name: None,
                    }
                )
            ),
        ),
        unit_type_with_unit_name_success: (
            "com.google.sdv.telemetry.tests.Blah#Blubb|the-unit-name",
            ok(
                eq(
                    ServiceName::UnitTypeBased{
                        unit_type: UnitType::from_str("com.google.sdv.telemetry.tests.Blah#Blubb").unwrap(),
                        unit_name: Some("the-unit-name".to_owned())
                    }
                )
            ),
        ),
    );
}

#[cfg(all(test, integration_tests))]
mod tests {
    use super::*;

    use protobuf::reflect::ReflectValueRef;
    use protobuf::MessageFull;
    use sdv_comms::sd::PublisherUnitMetadata;
    use sdv_comms::sd::UnitMetadata;
    use sdv_comms::sd::UnitRegistrationArgs;
    use sdv_status_rs::SdvResult;
    use sdv_telemetry_test_proto::test_message::{self, TestMessage};
    use sdv_telemetry_test_utils::unique_dt_topic_name;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::time::Instant;

    const UNIT_NAME: &str = "default";

    fn make_unique_unit_type() -> UnitType {
        UnitType::from_str(&format!(
            "com.google.sdv.telemetry.tests.DataTunnelSubscriberIntegrationTest#{}",
            unique_dt_topic_name()
        ))
        .unwrap()
    }

    fn make_unique_service_name() -> String {
        format!("{}|{UNIT_NAME}", make_unique_unit_type())
    }

    async fn register_topic(
        ctx: ContextRef,
        service_name: &str,
        publisher_metadata: TelemetryPublisherMetadata,
    ) -> SdvResult<dt::Publisher> {
        let ServiceName::UnitTypeBased { unit_type, unit_name: Some(unit_name) } =
            ServiceName::parse(service_name).unwrap()
        else {
            unreachable!();
        };

        let mut unit_registration_args = UnitRegistrationArgs::new();
        unit_registration_args.set_unit_type(&unit_type);
        unit_registration_args.set_unit_name(&unit_name);

        let mut publisher_unit_metadata = PublisherUnitMetadata::new();
        publisher_unit_metadata.set_message_size(3);
        publisher_unit_metadata.set_message_count(2);
        unit_registration_args.set_unit_metadata(&UnitMetadata::Publisher(publisher_unit_metadata));

        let mut app_metadata = vec![];
        publisher_metadata.write_to_vec(&mut app_metadata).unwrap();
        unit_registration_args.set_application_metadata(&app_metadata);

        dt::Publisher::create(ctx, unit_registration_args).await
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

    fn get_int_field_value(message: Box<dyn MessageDyn>) -> i32 {
        let int_field_descriptor = message.descriptor_dyn().field_by_name("int_field").unwrap();
        match int_field_descriptor.get_singular(&*message) {
            Some(ReflectValueRef::I32(value)) => value,
            result => panic!("Unexpected value: {result:?}"),
        }
    }

    // Unit Type -> 0 matches
    #[sdv_telemetry_test_utils_macros::timeout(5_000)]
    #[tokio::test]
    async fn discover_service_by_unit_type_when_exactly_no_matching_service_exists_fails() {
        let ctx = make_context(0).await;
        let subscriber = DataTunnelSubscriber::new(ctx);

        let unit_type = make_unique_unit_type();

        let service_name =
            ServiceName::UnitTypeBased { unit_type: unit_type.clone(), unit_name: None };
        let result = subscriber.discover_service(&service_name).await;

        assert!(result.is_none(), "Unexpected result: {result:?}");
    }

    // Unit Type -> 1 match
    #[sdv_telemetry_test_utils_macros::timeout(5_000)]
    #[tokio::test]
    async fn discover_service_by_unit_type_when_exactly_one_matching_service_exists_success() {
        let ctx = make_context(0).await;
        let subscriber = DataTunnelSubscriber::new(ctx);

        let unit_type = make_unique_unit_type();
        let _publisher1 = register_topic(
            ctx,
            &format!("{unit_type}|{UNIT_NAME}"),
            TelemetryPublisherMetadata {
                descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
                message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let _publisher2 = register_topic(
            ctx,
            &format!("{}|another-unit-name", make_unique_unit_type()),
            TelemetryPublisherMetadata {
                descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
                message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let service_name =
            ServiceName::UnitTypeBased { unit_type: unit_type.clone(), unit_name: None };
        let result = subscriber.discover_service(&service_name).await;

        let service_unit_definition = result.unwrap();
        assert_eq!(service_unit_definition.get_unit_type(), unit_type);
        assert_eq!(service_unit_definition.get_unit_identifier().get_unit_name(), UNIT_NAME);
    }

    // Unit Type -> 2 matches
    #[sdv_telemetry_test_utils_macros::timeout(5_000)]
    #[tokio::test]
    async fn discover_service_by_unit_type_when_two_matching_services_exist_fail() {
        let (ctx1, ctx2, ctx3) =
            (make_context(1).await, make_context(2).await, make_context(3).await);
        let subscriber = DataTunnelSubscriber::new(ctx1);

        let unit_type = make_unique_unit_type();
        let _publisher1 = register_topic(
            ctx2,
            &format!("{unit_type}|first-unit-name"),
            TelemetryPublisherMetadata {
                descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
                message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let _publisher2 = register_topic(
            ctx3,
            &format!("{unit_type}|second-unit-name"),
            TelemetryPublisherMetadata {
                descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
                message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let service_name =
            ServiceName::UnitTypeBased { unit_type: unit_type.clone(), unit_name: None };
        let result = subscriber.discover_service(&service_name).await;

        assert!(result.is_none(), "Unexpected result: {result:?}");
    }

    // Unit Type + Unit Name -> 0 matches
    #[sdv_telemetry_test_utils_macros::timeout(5_000)]
    #[tokio::test]
    async fn discover_service_by_unit_type_and_unit_name_when_no_matching_service_exists_fails() {
        let ctx = make_context(0).await;
        let subscriber = DataTunnelSubscriber::new(ctx);

        let unit_type = make_unique_unit_type();

        let service_name = ServiceName::UnitTypeBased {
            unit_type: unit_type.clone(),
            unit_name: Some(UNIT_NAME.to_owned()),
        };
        let result = subscriber.discover_service(&service_name).await;

        assert!(result.is_none(), "Unexpected result: {result:?}");
    }

    // Unit Type + Unit Name -> 1 match
    #[sdv_telemetry_test_utils_macros::timeout(5_000)]
    #[tokio::test]
    async fn discover_service_by_unit_type_and_unit_name_when_exactly_one_matching_service_exists_success(
    ) {
        let ctx = make_context(0).await;
        let subscriber = DataTunnelSubscriber::new(ctx);

        let unit_type = make_unique_unit_type();
        let _publisher1 = register_topic(
            ctx,
            &format!("{unit_type}|{UNIT_NAME}"),
            TelemetryPublisherMetadata {
                descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
                message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let _publisher2 = register_topic(
            ctx,
            &format!("{unit_type}|another-unit-name"),
            TelemetryPublisherMetadata {
                descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
                message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let service_name = ServiceName::UnitTypeBased {
            unit_type: unit_type.clone(),
            unit_name: Some(UNIT_NAME.to_owned()),
        };
        let result = subscriber.discover_service(&service_name).await;

        let service_unit_definition = result.unwrap();
        assert_eq!(service_unit_definition.get_unit_type(), unit_type);
        assert_eq!(service_unit_definition.get_unit_identifier().get_unit_name(), UNIT_NAME);
    }

    /// This madness forks the current process, and creates a publisher in the child process. It
    /// then returns the child process' PID as well as the FQIN of the publisher of the child
    /// process.
    ///
    /// This function is _not_ async due to the warnings around the use of `fork` [1], which sound
    /// pretty scary, so this should be called before setting up an async Tokio environment.
    ///
    /// [1]: https://docs.rs/nix/latest/nix/unistd/fn.fork.html#safety
    fn fork_and_create_publisher_then_sleep(
        fqin_extra: u8,
        service_name: String,
    ) -> (nix::unistd::Pid, ServiceFqin) {
        use nix::unistd::{
            fork, pipe as nix_pipe, read as nix_read, write as nix_write, ForkResult,
        };
        use std::os::fd::AsRawFd;

        // This will be used to send the FQIN from the child process to the parent process.
        let (rx, tx) = nix_pipe().unwrap();

        // SAFETY: Should be safe in tests.
        match unsafe { fork() }.unwrap() {
            ForkResult::Parent { child: child_pid, .. } => {
                drop(tx); // Close the writing end of the pipe.

                let mut child_fqin = vec![0; 1000];
                let read_count = nix_read(rx.as_raw_fd(), &mut child_fqin).unwrap();
                child_fqin.resize(read_count, 0);

                let child_fqin =
                    ServiceFqin::from_str(std::str::from_utf8(&child_fqin).unwrap()).unwrap();

                (child_pid, child_fqin)
            }
            ForkResult::Child => {
                drop(rx); // Close the reading end of the pipe.

                tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap().block_on(
                    async {
                        let ctx = make_context(fqin_extra).await;

                        let _publisher = register_topic(
                            ctx,
                            &service_name,
                            TelemetryPublisherMetadata {
                                descriptor_protos: vec![test_message::file_descriptor()
                                    .proto()
                                    .clone()],
                                message_type_name: format!(
                                    ".{}",
                                    TestMessage::descriptor().full_name()
                                ),
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap();

                        nix_write(tx, format!("{}", ctx.get_self_fqin()).as_bytes()).unwrap();

                        // TODO: Ideally, the parent would signal us when we can terminate, but for
                        // now, we just sleep for a long-enough duration.
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    },
                );
                std::process::exit(0);
            }
        }
    }

    // Unit Type + Unit Name -> 2 matches
    #[test]
    fn discover_service_by_unit_type_and_unit_name_when_two_matching_service_exists_fails() {
        use nix::sys::wait::waitpid;

        let unit_type = make_unique_unit_type();
        // We must create these services in separate processes, because it is impossible to create
        // two DT topics with the same unit name in the same process (even if we specified different
        // FQINs).
        let (child_pid1, _) =
            fork_and_create_publisher_then_sleep(1, format!("{unit_type}|{UNIT_NAME}"));
        let (child_pid2, _) =
            fork_and_create_publisher_then_sleep(2, format!("{unit_type}|{UNIT_NAME}"));

        tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap().block_on(async {
            let ctx = make_context(0).await;
            let subscriber = DataTunnelSubscriber::new(ctx);

            let service_name = ServiceName::UnitTypeBased {
                unit_type: unit_type.clone(),
                unit_name: Some(UNIT_NAME.to_owned()),
            };
            let result = subscriber.discover_service(&service_name).await;

            assert!(result.is_none(), "Unexpected result: {result:?}");
        });

        waitpid(child_pid1, None).unwrap();
        waitpid(child_pid2, None).unwrap();
    }

    // FQIN + Unit Name -> 0 matches
    #[test]
    fn discover_service_by_fqin_when_no_service_with_unit_name_exists_fails() {
        use nix::sys::wait::waitpid;

        let unit_type = make_unique_unit_type();
        let (child_pid, child_fqin) =
            fork_and_create_publisher_then_sleep(1, format!("{unit_type}|different-name"));

        tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap().block_on(async {
            let ctx = make_context(2).await;
            let subscriber = DataTunnelSubscriber::new(ctx);

            let service_name =
                ServiceName::FqinBased { fqin: child_fqin, unit_name: UNIT_NAME.to_owned() };
            let result = subscriber.discover_service(&service_name).await;

            assert!(result.is_none(), "Unexpected result: {result:?}");
        });
        waitpid(child_pid, None).unwrap();
    }

    // FQIN + Unit Name -> 1 match
    #[test]
    fn discover_service_by_fqin_when_exactly_one_service_with_unit_name_exists_success() {
        use nix::sys::wait::waitpid;

        let unit_type = make_unique_unit_type();
        let (child_pid, child_fqin) =
            fork_and_create_publisher_then_sleep(1, format!("{unit_type}|{UNIT_NAME}"));

        tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap().block_on(async {
            let ctx = make_context(2).await;
            let subscriber = DataTunnelSubscriber::new(ctx);

            let service_name =
                ServiceName::FqinBased { fqin: child_fqin, unit_name: UNIT_NAME.to_owned() };
            let result = subscriber.discover_service(&service_name).await;

            let service_unit_definition = result.unwrap();
            assert_eq!(service_unit_definition.get_unit_type(), unit_type);
            assert_eq!(service_unit_definition.get_unit_identifier().get_unit_name(), UNIT_NAME);
        });
        waitpid(child_pid, None).unwrap();
    }

    // FQIN + Unit Name -> 2 matches
    #[sdv_telemetry_test_utils_macros::timeout(5_000)]
    #[tokio::test]
    async fn discover_service_by_fqin_when_two_services_with_unit_name_exist_fails() {
        let ctx = make_context(0).await;

        let _publisher1 = register_topic(
            ctx,
            &format!("{}|{UNIT_NAME}", make_unique_unit_type()),
            TelemetryPublisherMetadata {
                descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
                message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        )
        .await;

        let publisher2 = register_topic(
            ctx,
            &format!("{}|{UNIT_NAME}", make_unique_unit_type()),
            TelemetryPublisherMetadata {
                descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
                message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
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

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_metadata_topic_not_registered_returns_none() {
        let ctx = make_context(0).await;

        let service_name = make_unique_service_name();

        let subscriber = DataTunnelSubscriber::new(ctx);
        // The service should not exist yet, therefore metadata should be `None`.
        assert!(subscriber.get_metadata(&service_name).await.unwrap().is_none());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_metadata_success() {
        let ctx = make_context(0).await;

        let service_name = make_unique_service_name();

        let _publisher = register_topic(
            ctx,
            &service_name,
            TelemetryPublisherMetadata {
                descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
                message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let subscriber = DataTunnelSubscriber::new(ctx);
        let publisher_metadata = subscriber.get_metadata(&service_name).await.unwrap().unwrap();
        assert!(!publisher_metadata.supports_get);
        assert_eq!(publisher_metadata.message_descriptor.name(), "TestMessage");
        assert_eq!(
            publisher_metadata.message_descriptor.proto(),
            TestMessage::descriptor().proto()
        );
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn create_subscription_when_topic_does_not_exist_failure() {
        let ctx = make_context(0).await;

        let service_name = make_unique_service_name();

        let subscriber = DataTunnelSubscriber::new(ctx);
        let outcome = subscriber
            .create_subscription(
                &service_name,
                None,
                &SubSamplingInterval::ZERO,
                Box::new(|_| {
                    unreachable!();
                }),
            )
            .await
            .unwrap();
        assert!(matches!(outcome, CreateSubscriptionOutcome::NotRegistered(_)));
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn create_subscription_success() {
        let ctx = make_context(0).await;

        let service_name = make_unique_service_name();

        let _publisher = register_topic(
            ctx,
            &service_name,
            TelemetryPublisherMetadata {
                descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
                message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let subscriber = DataTunnelSubscriber::new(ctx);
        let outcome = subscriber
            .create_subscription(
                &service_name,
                None,
                &SubSamplingInterval::ZERO,
                Box::new(|_| {
                    unreachable!();
                }),
            )
            .await
            .unwrap();
        assert!(matches!(outcome, CreateSubscriptionOutcome::Subscribed(_)));
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn create_multiple_subscription_success() {
        let ctx = make_context(0).await;

        let service_name = make_unique_service_name();

        let _publisher = register_topic(
            ctx,
            &service_name,
            TelemetryPublisherMetadata {
                descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
                message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let subscriber = DataTunnelSubscriber::new(ctx);
        let outcome1 = subscriber
            .create_subscription(
                &service_name,
                None,
                &SubSamplingInterval::ZERO,
                Box::new(|_| {
                    unreachable!();
                }),
            )
            .await
            .unwrap();

        let outcome2 = subscriber
            .create_subscription(
                &service_name,
                None,
                &SubSamplingInterval::ZERO,
                Box::new(|_| {
                    unreachable!();
                }),
            )
            .await
            .unwrap();
        assert!(matches!(outcome1, CreateSubscriptionOutcome::Subscribed(_)));
        assert!(matches!(outcome2, CreateSubscriptionOutcome::Subscribed(_)));
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn create_repeated_subscriptions_and_read_messages_success() {
        let ctx = make_context(0).await;

        let service_name = make_unique_service_name();

        let publisher = register_topic(
            ctx,
            &service_name,
            TelemetryPublisherMetadata {
                descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
                message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let subscriber = DataTunnelSubscriber::new(ctx);

        let (tx1, mut rx1) = mpsc::unbounded_channel::<Box<dyn MessageDyn>>();
        let outcome1 = subscriber
            .create_subscription(
                &service_name,
                None,
                &SubSamplingInterval::ZERO,
                Box::new(move |message| tx1.send(message).unwrap()),
            )
            .await
            .unwrap();

        let (tx2, mut rx2) = mpsc::unbounded_channel::<Box<dyn MessageDyn>>();
        let outcome2 = subscriber
            .create_subscription(
                &service_name,
                None,
                &SubSamplingInterval::ZERO,
                Box::new(move |message| tx2.send(message).unwrap()),
            )
            .await
            .unwrap();

        write_message(&publisher, TestMessage { int_field: 1, ..Default::default() });
        write_message(&publisher, TestMessage { int_field: 2, ..Default::default() });

        assert!(matches!(outcome1, CreateSubscriptionOutcome::Subscribed(_)));
        assert!(matches!(outcome2, CreateSubscriptionOutcome::Subscribed(_)));

        assert_eq!(get_int_field_value(rx1.recv().await.unwrap()), 1);
        assert_eq!(get_int_field_value(rx1.recv().await.unwrap()), 2);

        assert_eq!(get_int_field_value(rx2.recv().await.unwrap()), 1);
        assert_eq!(get_int_field_value(rx2.recv().await.unwrap()), 2);
    }

    #[sdv_telemetry_test_utils_macros::timeout(10_000)]
    // Force a multi-threaded runtime for this test, to ensure that the notification timestamps are
    // more accurate, even when one of the threads is stuck in synchronous FFI code.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn create_subscription_and_read_messages_with_sub_sampling_interval_success() {
        let ctx = make_context(0).await;

        let service_name = make_unique_service_name();
        let notification_interval = SubSamplingInterval::try_from(Duration::from_secs(2)).unwrap();

        let publisher = register_topic(
            ctx,
            &service_name,
            TelemetryPublisherMetadata {
                descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
                message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let subscriber = DataTunnelSubscriber::new(ctx);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let outcome = subscriber
            .create_subscription(
                &service_name,
                None,
                &notification_interval,
                Box::new(move |message| tx.send((Instant::now(), message)).unwrap()),
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

        assert_eq!(get_int_field_value(message1), 1);
        // Message 2 should be silently discarded.
        assert_eq!(get_int_field_value(message3), 3);
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
}
