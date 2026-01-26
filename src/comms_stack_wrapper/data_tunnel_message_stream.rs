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

use async_stream::stream;
use futures::{Stream, StreamExt, TryStreamExt};
use sdv_comms::dt;
use sdv_comms::id::UnitIdentifier;
use sdv_comms::ContextRef;
use sdv_status_rs::{SdvResult, SdvStatus};
use sdv_telemetry_service_publisher_accessor::SubSamplingInterval;
use sdv_telemetry_utils::conversions::u32_into_usize;

#[cfg_attr(test, derive(Debug))]
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
pub(crate) async fn create_message_stream(
    ctx: ContextRef,
    unit_identifier: UnitIdentifier,
    sub_sampling_interval: SubSamplingInterval,
    fetch_last_message: bool,
) -> SdvResult<impl Stream<Item = anyhow::Result<Messages>>> {
    let mut subscriber_stream = dt::SubscriberStream::create(
        ctx,
        &unit_identifier,
        sub_sampling_interval.as_duration(),
        dt::SubscriberOptions { fetch_last_message },
    )
    .await?;

    let message_size =
        u32_into_usize(subscriber_stream.get_publisher_unit_metadata().get_message_size());

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

    Ok(subscriber_stream)
}

#[cfg(all(test, integration_tests))]
mod integration_tests {
    use super::*;

    use googletest::assert_that;
    use googletest::matchers::{container_eq, elements_are, eq, ok, pat};
    use protobuf::well_known_types::empty::Empty;
    use protobuf::well_known_types::wrappers::Int64Value;
    use protobuf::{Message, MessageFull};
    use sdv_proto_reflection_rs::message_metadata::MessageMetadata;
    use std::pin::pin;
    use std::thread;
    use tokio::time::{sleep, timeout_at, Duration, Instant};

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

                assert!(
                    last_value.is_none_or(
                        // All values must be consecutive (i.e., increase by exactly 1).
                        |last_value| value == last_value + 1
                    ),
                    "Unexpected value received: last_value = {last_value:?}, value = {value}"
                );
                last_value = Some(value);
            }
        }

        // Check that the test actually did something :)
        assert!(i > 1000, "Too few messages were processed, got: {i:?}")
    }

    #[sdv_telemetry_test_utils::timeout(10_000)]
    #[tokio::test]
    async fn correctly_handles_no_last_message_when_creating_subscription() {
        const MESSAGE_SIZE: u32 = 10;

        let ctx = test_utils::make_context(0).await;
        let unit_type = test_utils::make_unique_unit_type();

        let publisher = DataTunnelPublisher::register_topic(
            ctx,
            &unit_type,
            test_utils::UNIT_NAME,
            10,
            MESSAGE_SIZE,
            MessageMetadata {
                file_descriptors: vec![Empty::descriptor().file_descriptor_proto().to_owned()],
                message_full_name: format!(".{}", Empty::descriptor().full_name()),
                ..Default::default()
            }
            .write_to_bytes()
            .unwrap(),
        )
        .await
        .unwrap();

        let unit_identifier =
            test_utils::discover_service(ctx, &unit_type).await.get_unit_identifier();

        let message_stream = create_message_stream(
            ctx,
            unit_identifier,
            SubSamplingInterval::ZERO,
            // Important: Set fetch_last_message to `true`.
            true,
        )
        .await
        .unwrap();
        let message_stream = pin!(message_stream);

        // Give the message stream some time to fetch the latest (non-existing) message.
        sleep(Duration::from_secs(1)).await;

        // Drop the publisher so that the message stream ends.
        drop(publisher);

        let messages = message_stream.collect::<Vec<_>>().await;
        assert_that!(&messages, elements_are!());
    }

    #[sdv_telemetry_test_utils::timeout(10_000)]
    #[tokio::test]
    async fn correctly_handles_empty_messages() {
        const MESSAGE_SIZE: u32 = 10;

        let ctx = test_utils::make_context(0).await;
        let unit_type = test_utils::make_unique_unit_type();

        let publisher = DataTunnelPublisher::register_topic(
            ctx,
            &unit_type,
            test_utils::UNIT_NAME,
            10,
            MESSAGE_SIZE,
            MessageMetadata {
                file_descriptors: vec![Empty::descriptor().file_descriptor_proto().to_owned()],
                message_full_name: format!(".{}", Empty::descriptor().full_name()),
                ..Default::default()
            }
            .write_to_bytes()
            .unwrap(),
        )
        .await
        .unwrap();

        // Send the first message before the message stream is created.
        publisher.publish_length_delimited_proto(&Empty::default()).unwrap();

        let unit_identifier =
            test_utils::discover_service(ctx, &unit_type).await.get_unit_identifier();

        let message_stream = create_message_stream(
            ctx,
            unit_identifier,
            SubSamplingInterval::ZERO,
            // Important: Set fetch_last_message to `true`.
            true,
        )
        .await
        .unwrap();
        let message_stream = pin!(message_stream);

        // Give the message stream some time to fetch the latest message.
        sleep(Duration::from_secs(1)).await;

        // Send another message.
        publisher.publish_length_delimited_proto(&Empty::default()).unwrap();

        // Drop the publisher so that the message stream ends.
        drop(publisher);

        let messages = message_stream.collect::<Vec<_>>().await;
        assert_that!(
            &messages,
            elements_are!(ok(pat!(&Messages {
                message_count: eq(2),
                message_size: eq(u32_into_usize(MESSAGE_SIZE)),
                buffer: ref container_eq(vec![0; 2 * u32_into_usize(MESSAGE_SIZE)]),
            })))
        );
    }
}
