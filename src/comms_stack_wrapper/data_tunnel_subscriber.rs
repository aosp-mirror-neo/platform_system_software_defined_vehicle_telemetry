// Copyright 2023 Google LLC

//! Telemetry subscription logic for DT.

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use futures::future::{self, BoxFuture};
use futures::StreamExt;
use log::info;
use protobuf::reflect::MessageDescriptor;
use protobuf::well_known_types::any::Any;
use protobuf::MessageDyn;
use sdv_comms::dt;
use sdv_comms::id::UnitIdentifier;
use sdv_comms::sd::ServiceUnitDefinition;
use sdv_comms::ContextRef;
use sdv_config_provider::vsidl_provider_api::VsidlConfigProviderV1;
use sdv_telemetry_service_publisher_accessor::{
    CreateSubscriptionOutcome, FetchLastMessageOutcome, GetMessageProviderOutcome,
    ServicePublisherAccessorError, ServicePublisherMessageProvider, ServicePublisherSubscription,
    SubSamplingInterval,
};
use std::iter::Iterator;
use std::pin::pin;
use tokio::select;
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::data_tunnel_message_stream::create_message_stream;

/// Struct for storing all data tunnel subscriptions that implements `ServicePublisherManager`.
pub struct DataTunnelSubscriber {
    ctx: future::Shared<BoxFuture<'static, ContextRef>>,
}

/// Struct for subscription of DT topic.
struct DataTunnelSubscription {
    _subscription_drop_guard: DropGuard,
}

/// Subscriber struct impl that encapsulates the data required for reading messages.
impl DataTunnelSubscriber {
    /// Creates a new instance of the data tunnel subscriber.
    pub(super) fn new(ctx: future::Shared<BoxFuture<'static, ContextRef>>) -> Self {
        Self { ctx }
    }

    pub(super) async fn create_subscription(
        &self,
        unit_identifier: UnitIdentifier,
        fetch_last_message: bool,
        message_descriptor: MessageDescriptor,
        sub_sampling_interval: &SubSamplingInterval,
        mut new_message_callback: Box<
            dyn FnMut(Result<Box<dyn MessageDyn>, ServicePublisherAccessorError>) + Send + Sync,
        >,
        cancellation_callback: Box<dyn FnOnce() + Send + Sync>,
    ) -> Result<CreateSubscriptionOutcome, ServicePublisherAccessorError> {
        let message_stream = create_message_stream(
            self.ctx.clone().await,
            unit_identifier.clone(),
            sub_sampling_interval.to_owned(),
            fetch_last_message,
        )
        .await
        .map_err(|err| ServicePublisherAccessorError::FailedToSubscribe(err.into()))?;

        let cancellation_token = CancellationToken::new();
        let subscription_drop_guard = cancellation_token.clone().drop_guard();
        let sub_sampling_interval = sub_sampling_interval.to_owned();
        tokio::spawn(async move {
            let mut message_stream = pin!(message_stream);
            // This loop runs infinitely until the cancellation token fires. In case of an error
            // while message parsing, the data is lost, as data was retrieved successfully from the
            // data tunnel. In case of data tunnel errors, data is not lost, and can be fetched in
            // the next iteration of the loop.
            loop {
                match select! {
                    biased;
                    _ = cancellation_token.cancelled() => None,
                    result = message_stream.next() => Some(result),
                } {
                    None => {
                        // Subscription is cancelled on the Telemetry Service side.
                        return;
                    }
                    Some(None) => {
                        // Subscription is cancelled on the publisher side.
                        (cancellation_callback)();
                        return;
                    }
                    Some(Some(result)) => {
                        let messages = match result {
                            Ok(result) => result,
                            Err(err) => {
                                (new_message_callback)(Err(
                                    ServicePublisherAccessorError::MessageRetrievalFailed(err),
                                ));
                                continue;
                            }
                        };

                        info!(
                            "Received {} messages from service unit '{}'.",
                            messages.iter().len(),
                            Self::to_debug_string(&unit_identifier)
                        );

                        let messages: Box<dyn Iterator<Item = _>> = {
                            if sub_sampling_interval == SubSamplingInterval::ZERO {
                                Box::new(messages.iter())
                            } else {
                                // Only process the last (=newest) message.
                                Box::new(messages.iter().rev().take(1))
                            }
                        };

                        for message_bytes in messages {
                            let message = Self::read_length_delimited_proto(
                                message_bytes,
                                &message_descriptor,
                            )
                            .map_err(ServicePublisherAccessorError::MessageParsingFailed);
                            (new_message_callback)(message);
                        }
                    }
                }
            }
        });

        let data_tunnel_subscription =
            Box::new(DataTunnelSubscription { _subscription_drop_guard: subscription_drop_guard });

        Ok(CreateSubscriptionOutcome::Subscribed {
            subscription: data_tunnel_subscription,
            fetch_last_message_outcome: FetchLastMessageOutcome::new_supported(fetch_last_message),
        })
    }

    pub fn get_message_provider(
        &self,
        unit_identifier: UnitIdentifier,
        message_descriptor: MessageDescriptor,
    ) -> Result<GetMessageProviderOutcome, ServicePublisherAccessorError> {
        struct ServicePublisherMessageProviderImpl {
            ctx: future::Shared<BoxFuture<'static, ContextRef>>,
            unit_identifier: UnitIdentifier,
            message_descriptor: MessageDescriptor,
        }

        #[async_trait]
        impl ServicePublisherMessageProvider for ServicePublisherMessageProviderImpl {
            async fn get_latest_message(
                &self,
                _configuration: Option<&Any>,
            ) -> Result<Option<Box<dyn MessageDyn>>, ServicePublisherAccessorError> {
                let message_bytes = dt::InstantReader::get_last_message(
                    self.ctx.clone().await,
                    &self.unit_identifier,
                )
                .await
                .map_err(|err| ServicePublisherAccessorError::MessageRetrievalFailed(err.into()))?;

                if message_bytes.is_empty() {
                    return Ok(None);
                }

                Ok(Some(
                    DataTunnelSubscriber::read_length_delimited_proto(
                        &message_bytes,
                        &self.message_descriptor,
                    )
                    .map_err(ServicePublisherAccessorError::MessageParsingFailed)?,
                ))
            }
        }

        Ok(GetMessageProviderOutcome::Success(Box::new(ServicePublisherMessageProviderImpl {
            ctx: self.ctx.clone(),
            unit_identifier,
            message_descriptor,
        })))
    }

    pub(super) async fn get_message_descriptor(
        vsidl_provider: &dyn VsidlConfigProviderV1,
        service_unit_definition: &ServiceUnitDefinition,
    ) -> anyhow::Result<MessageDescriptor> {
        let descriptor = vsidl_provider
            .get_publication_descriptor(
                service_unit_definition.get_service_bundle_identity().get_fqin(),
                &service_unit_definition.get_unit_type(),
            )
            .await
            .ok_or_else(|| {
                anyhow!(
                    "VSIDL provider was unable to provide publication descriptor for {}|{}",
                    service_unit_definition.get_service_bundle_identity().get_fqin(),
                    service_unit_definition.get_unit_type()
                )
            })?;

        Ok(descriptor.message_descriptor)
    }

    pub(crate) fn read_length_delimited_proto(
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
            .with_context(|| "Failed to parse the protobuf message.")?;
        Ok(message)
    }

    fn to_debug_string(unit_identifier: &UnitIdentifier) -> String {
        format!("{}|{}", unit_identifier.get_sid(), unit_identifier.get_unit_name())
    }
}

#[async_trait]
impl ServicePublisherSubscription for DataTunnelSubscription {
    async fn unsubscribe(self: Box<Self>) {
        // This drops `self` and therefore cancels the subscription via the `DropGuard`.
    }
}

#[cfg(all(test, integration_tests))]
mod tests {
    use super::*;

    use googletest::prelude::*;
    use protobuf::well_known_types::wrappers::Int32Value;
    use protobuf::MessageFull;
    use sdv_config_provider::vsidl_provider_api::{
        MockVsidlConfigProviderV1, PublicationDescriptor, SizeMetadata,
    };
    use sdv_proto_reflection_rs::message_metadata::MessageMetadata;

    use crate::test_utils;

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_message_descriptor_success() {
        // Get ServiceUnitDefinition
        let ctx = test_utils::make_context(0).await;

        let unit_type = test_utils::make_unique_unit_type();
        let _publisher = test_utils::register_topic(
            ctx,
            &format!("{}|{}", unit_type, test_utils::UNIT_NAME),
            MessageMetadata::default(),
        )
        .await
        .unwrap();

        let service_unit_definition = test_utils::discover_service(ctx, &unit_type).await;

        // Create and set mocked VSIDL Provider
        let mut mock_vsidl_provider = MockVsidlConfigProviderV1::new();
        let expected_descriptor = Int32Value::descriptor();
        let publication_descriptor = PublicationDescriptor {
            message_descriptor: expected_descriptor.clone(),
            size_metadata: SizeMetadata::default(),
        };
        mock_vsidl_provider
            .expect_get_publication_descriptor()
            .times(1)
            .returning(move |_, _| Some(publication_descriptor.clone()));

        // Get Message Descriptor
        let result = DataTunnelSubscriber::get_message_descriptor(
            &mock_vsidl_provider,
            &service_unit_definition,
        )
        .await;

        // Assert
        assert_that!(result, ok(eq(&expected_descriptor)));
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_message_descriptor_failure() {
        // Get ServiceUnitDefinition
        let ctx = test_utils::make_context(0).await;
        let unit_type = test_utils::make_unique_unit_type();
        let _publisher = test_utils::register_topic(
            ctx,
            &format!("{}|{}", unit_type, test_utils::UNIT_NAME),
            MessageMetadata::default(),
        )
        .await
        .unwrap();

        let service_unit_definition = test_utils::discover_service(ctx, &unit_type).await;

        // Create and set mocked VSIDL Provider
        let mut mock_vsidl_provider = MockVsidlConfigProviderV1::new();
        mock_vsidl_provider.expect_get_publication_descriptor().times(1).returning(|_, _| None);

        // Get Message Descriptor
        let result = DataTunnelSubscriber::get_message_descriptor(
            &mock_vsidl_provider,
            &service_unit_definition,
        )
        .await;

        assert_that!(
            result,
            err(displays_as(contains_substring(
                "VSIDL provider was unable to provide publication descriptor"
            )))
        );
    }
}
