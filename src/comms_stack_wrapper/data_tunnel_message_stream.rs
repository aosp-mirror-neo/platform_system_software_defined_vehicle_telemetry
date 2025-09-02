// Copyright 2025 Google LLC

use async_stream::stream;
use futures::{Stream, StreamExt, TryStreamExt};
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
