// Copyright 2023 Google LLC

//! Telemetry publishing logic for DT.

use crate::data_tunnel_api_client;
use anyhow::Context;
use sdv_dt::{TopicHandle, TopicTypedMetadata};

/// Struct for data publisher of DT, to simplify sample usage.
pub struct DataTunnelPublisher {
    topic_handle: TopicHandle,
}

/// Publisher struct impl that encapsulates the data required for publishing.
impl DataTunnelPublisher {
    /// Register a new data tunnel topic. Returns `Self`, which can be used to publish messages.
    ///
    /// # Arguments
    ///
    /// * `topic_name` - The name of the topic of the data tunnel.
    /// * `message_count` - The size of the data tunnel queue, i.e., how many messages it can hold.
    /// * `message_size` - The size of each message in bytes. It cannot be changed during the
    ///                    topic's lifetime.
    /// * `metadata` - Topic Metadata.
    pub fn register_topic(
        topic_name: &str,
        message_count: usize,
        message_size: usize,
        metadata: Vec<u8>,
    ) -> anyhow::Result<DataTunnelPublisher> {
        cxx::let_cxx_string!(topic_name = topic_name);
        let topic_handle = data_tunnel_api_client::register_topic_with_metadata(
            &topic_name,
            &TopicTypedMetadata { message_count_: message_count, message_size_: message_size },
            metadata,
        )
        .with_context(|| format!("Failed to register topic {topic_name} with metadata."))?;
        Ok(DataTunnelPublisher { topic_handle })
    }

    /// Publish a single message to the data tunnel.
    ///
    /// # Arguments
    ///
    /// * `message_bytes` - The bytes that should be written to the data tunnel. Their length must
    ///                     be equal to the topic's configured message size.
    ///
    /// # Safety
    /// The size of `message_bytes` should be equal to the message size set in register topic call
    /// multiplied by the number_of_messages.
    pub fn publish(&self, message_bytes: &[u8]) -> anyhow::Result<()> {
        // TODO: This variable should probably be used -> fixing in follow-up CL.
        let _message_count = data_tunnel_api_client::available_to_write(self.topic_handle)
            .with_context(|| "Failed to query how many messages are available to write.")?;

        // SAFETY: `message_bytes` contains no more than `self.message_size` bytes since we are
        // sending a single message of size <= `self.message_size`, and `DataTunnelPublisher`
        // has been created by `register_topic` which called `RegisterTopic` with
        // `self.message_size`.
        unsafe { data_tunnel_api_client::write_message(self.topic_handle, message_bytes) }
            .with_context(|| "Failed to write message.")?;

        Ok(())
    }
}
