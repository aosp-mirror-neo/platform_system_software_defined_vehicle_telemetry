// Copyright 2025 Google LLC

use async_stream::stream;
use futures::{
    future::{self, OptionFuture},
    stream::empty as stream_empty,
    stream::once as stream_once,
    Stream, StreamExt, TryStreamExt,
};
use sdv_comms::dt;
use sdv_comms::id::UnitIdentifier;
use sdv_comms::ContextRef;
use sdv_status_rs::{SdvResult, SdvStatus};
use sdv_telemetry_service_publisher_accessor::SubSamplingInterval;
use sdv_telemetry_utils::conversions::u32_into_usize;

pub(crate) struct Messages {
    message_count: usize,
    message_size: usize,
    buffer: Vec<u8>,
}

impl Messages {
    pub(crate) fn iter(
        &self,
    ) -> impl DoubleEndedIterator<Item = &[u8]> + ExactSizeIterator<Item = &[u8]> {
        self.buffer.chunks_exact(self.message_size).take(self.message_count)
    }
}

/// Creates a new `Stream` that produces `Messages` for each set of messages received from the
/// provided `unit_identifier`.
///
/// If `fetch_last_message` is `true`, we fetch the last message using `dt::InstantReader` before
/// subscribing to new messages. We cannot use the `fetch_last_message` field of
/// `dt::SubscriberOptions`, because that option only works for same-VM messages (see
/// b/425992794#comment15, b/427624375, b/441473091, b/441498744).
pub(crate) async fn create_message_stream(
    ctx: ContextRef,
    unit_identifier: UnitIdentifier,
    sub_sampling_interval: SubSamplingInterval,
    fetch_last_message: bool,
) -> SdvResult<impl Stream<Item = anyhow::Result<Messages>>> {
    // Important: Fetch **and await** the last message _before_ creating the `SubscriberStream`
    // (otherwise the `SubscriberStream` might return the last message as well!).
    let maybe_last_message = OptionFuture::from(
        fetch_last_message.then(|| dt::InstantReader::get_last_message(ctx, &unit_identifier)),
    )
    .await;

    let mut subscriber_stream = dt::SubscriberStream::create(
        ctx,
        &unit_identifier,
        sub_sampling_interval.as_duration(),
        dt::SubscriberOptions {
            // IMPORTANT: This MUST be set to `false`, even if `fetch_last_message` is `true`!
            fetch_last_message: false,
        },
    )
    .await?;

    let message_size =
        u32_into_usize(subscriber_stream.get_publisher_unit_metadata().get_message_size());

    let maybe_last_message = maybe_last_message.and_then(|result| {
        result.map_or_else(
            |err| {
                Some(Err(anyhow::Error::from(err)
                    .context("Failed to fetch initial message from Data Tunnel message stream")))
            },
            |buffer| {
                if buffer.is_empty() {
                    None
                } else {
                    Some(Ok(Messages { message_count: 1, message_size, buffer }))
                }
            },
        )
    });
    let last_message_stream = maybe_last_message.map_or_else(
        || stream_empty().boxed(),
        |result| stream_once(future::ready(result)).boxed(),
    );

    let subscriber_stream = stream! {
        while let Some(result) = subscriber_stream.next().await {
            yield result.or_else(|err| {
                // This error indicates an overflow in the FMQ ring buffer. FMQ then recovers
                // the read pointer, so a subsequent read is likely to succeed.
                //
                // For sub-sampling subscribers that read less frequently than the publisher
                // writes, the initial read is expected to overflow. Retrying the read is
                // acceptable in this case, as these subscribers only need to retain the most
                // recent value.
                if sub_sampling_interval != SubSamplingInterval::ZERO {
                    subscriber_stream.read_all_available_messages()
                } else {
                    Err(err)
                }
            });
        }
    }
    .map_ok(move |(message_count, buffer)| Messages {
        message_count: u32_into_usize(message_count),
        message_size,
        buffer,
    })
    .map_err(|err: SdvStatus| {
        anyhow::Error::from(err)
            .context("Data Tunnel message stream produced an intermittent error")
    });

    Ok(last_message_stream.chain(subscriber_stream))
}

#[cfg(all(test, integration_tests))]
mod integration_tests {
    use super::*;

    use protobuf::well_known_types::wrappers::Int64Value;
    use protobuf::{Message, MessageFull};
    use sdv_proto_reflection_rs::message_metadata::MessageMetadata;
    use std::pin::pin;
    use std::thread;
    use tokio::time::{timeout_at, Duration, Instant};

    use crate::data_tunnel_publisher::DataTunnelPublisher;
    use crate::data_tunnel_subscriber::DataTunnelSubscriber;
    use crate::test_utils;

    #[sdv_telemetry_test_utils::timeout(10_000)]
    #[tokio::test]
    async fn no_duplicate_message_for_frequent_publisher_with_fetch_last_message_set_to_true() {
        const DURATION: Duration = Duration::from_secs(3);

        let ctx = test_utils::make_context(0).await;
        let unit_type = test_utils::make_unique_unit_type();

        let publisher = DataTunnelPublisher::register_topic(
            ctx,
            &unit_type,
            test_utils::UNIT_NAME,
            1000000,
            10,
            MessageMetadata {
                file_descriptors: vec![Int64Value::descriptor().file_descriptor_proto().to_owned()],
                message_full_name: format!(".{}", Int64Value::descriptor().full_name()),
                ..Default::default()
            }
            .write_to_bytes()
            .unwrap(),
        )
        .await
        .unwrap();

        let unit_identifier =
            test_utils::discover_service(ctx, &unit_type).await.get_unit_identifier();

        let start = Instant::now();
        thread::spawn(move || {
            let mut value = 0;
            while start.elapsed() < DURATION {
                publisher
                    .publish_length_delimited_proto(&Int64Value { value, ..Default::default() })
                    .unwrap();
                value += 1;
            }
        });

        let message_stream = create_message_stream(
            ctx,
            unit_identifier,
            SubSamplingInterval::ZERO,
            // Important: Set fetch_last_message to `true`.
            true,
        )
        .await
        .unwrap();
        let mut message_stream = pin!(message_stream);

        let mut i = 0;
        let mut last_value = None;
        while let Ok(Some(messages)) = timeout_at(start + DURATION, message_stream.next()).await {
            for message_bytes in messages.unwrap().iter() {
                let value = DataTunnelSubscriber::read_length_delimited_proto(
                    message_bytes,
                    &Int64Value::descriptor(),
                )
                .unwrap()
                .downcast_ref::<Int64Value>()
                .unwrap()
                .value;
                i += 1;

                if i == 1 {
                    // Nothing to do for the first received value (this value is received via
                    // `dt::InstantReader`).
                } else if i == 2 {
                    // The second value must be bigger than the first value. There can be a gap,
                    // because some time passes between the first value being received via
                    // `dt::InstantReader` and the second value being received via
                    // `dt::SubscriberStream`.
                    assert!(
                        value > last_value.unwrap(),
                        "Unexpected value received: last_value = {last_value:?}, value = {value}"
                    );
                } else {
                    // All other values must be consecutive (i.e., increase by exactly 1).
                    assert!(
                        value == last_value.unwrap() + 1,
                        "Unexpected value received: last_value = {last_value:?}, value = {value}"
                    );
                }
                last_value = Some(value);
            }
        }

        // Check that the test actually did something :)
        assert!(i > 1000, "Too few messages were processed, got: {i:?}")
    }
}
