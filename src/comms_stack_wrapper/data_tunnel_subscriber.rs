// Copyright 2023 Google LLC

//! Telemetry subscription logic for DT.

use anyhow::Context;
use async_trait::async_trait;
use futures::future::{self, BoxFuture};
use futures::StreamExt;
use log::{error, info};
use protobuf::reflect::MessageDescriptor;
use protobuf::well_known_types::any::Any;
use protobuf::MessageDyn;
use sdv_comms::dt;
use sdv_comms::id::UnitIdentifier;
use sdv_comms::ContextRef;
use sdv_telemetry::service_publisher_accessor::{
    CreateSubscriptionOutcome, GetMessageProviderOutcome, ServicePublisherMessageProvider,
    ServicePublisherSubscription,
};
use sdv_telemetry::sub_sampling_interval::SubSamplingInterval;
use sdv_telemetry_utils::conversions::u32_into_usize;
use std::iter::Iterator;
use tokio::select;
use tokio_util::sync::{CancellationToken, DropGuard};

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
        sub_sampling_interval: &SubSamplingInterval,
        message_descriptor: MessageDescriptor,
        mut callback: Box<dyn FnMut(Box<dyn MessageDyn>) + Send + Sync>,
    ) -> anyhow::Result<CreateSubscriptionOutcome> {
        let mut subscriber_stream =
            dt::SubscriberStream::create(
                self.ctx.clone().await,
                &unit_identifier,
                sub_sampling_interval.as_duration(),
                dt::SubscriberOptions { fetch_last_message: false }
            )
            .await
            .with_context(
                || format!(
                    "Failed to subscribe to unit identifier {} with sub-sampling interval {sub_sampling_interval:?}.",
                    Self::to_debug_string(&unit_identifier))
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
                    error!(
                        "Data Tunnel message stream of service unit '{}' returned an intermittent error: {err:?}",
                        Self::to_debug_string(&unit_identifier))
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

                info!(
                    "Received {number_of_messages} messages from service unit '{}'.",
                    Self::to_debug_string(&unit_identifier)
                );
                for message_bytes in messages {
                    match Self::read_length_delimited_proto(message_bytes, &message_descriptor) {
                        Ok(msg) => {
                            (callback)(msg);
                        }
                        Err(err) => error!(
                            "Failed to convert bytes to protobuf for service unit '{}': {err:?}",
                            Self::to_debug_string(&unit_identifier)
                        ),
                    };
                }
            }
        });

        let data_tunnel_subscription =
            Box::new(DataTunnelSubscription { _subscription_drop_guard: subscription_drop_guard });

        Ok(CreateSubscriptionOutcome::Subscribed(data_tunnel_subscription))
    }

    pub fn get_message_provider(
        &self,
        unit_identifier: UnitIdentifier,
        message_descriptor: MessageDescriptor,
    ) -> anyhow::Result<GetMessageProviderOutcome> {
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
            ) -> anyhow::Result<Option<Box<dyn MessageDyn>>> {
                let Ok(message_bytes) = dt::InstantReader::get_last_message(
                    self.ctx.clone().await,
                    &self.unit_identifier,
                )
                .await
                else {
                    // TODO - b/380258890: It is not currently possible for us to distinguish
                    // between no message being available and the call failing. Therefore, we treat
                    // any error as no message being available for now.
                    return Ok(None);
                };

                Ok(Some(DataTunnelSubscriber::read_length_delimited_proto(
                    &message_bytes,
                    &self.message_descriptor,
                )?))
            }
        }

        Ok(GetMessageProviderOutcome::Success(Box::new(ServicePublisherMessageProviderImpl {
            ctx: self.ctx.clone(),
            unit_identifier,
            message_descriptor,
        })))
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
            .with_context(|| "Failed to parse the protobuf message.")?;
        Ok(message)
    }

    fn to_debug_string(unit_identifier: &UnitIdentifier) -> String {
        format!("{}|{}", unit_identifier.get_sid(), unit_identifier.get_unit_name())
    }
}

#[async_trait]
impl ServicePublisherSubscription for DataTunnelSubscription {
    async fn unsubscribe(self: Box<Self>) -> anyhow::Result<()> {
        // This drops `self` and therefore cancels the subscription via the `DropGuard`.
        Ok(())
    }
}
