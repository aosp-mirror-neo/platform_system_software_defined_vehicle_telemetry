// Copyright 2023 Google LLC

//! Telemetry publishing logic for data tunnels.

use crate::data_tunnel_api_client;
use anyhow::{anyhow, Context};

/// Represents the data publisher side of a data tunnel.
pub struct DataTunnelPublisher {
    topic_descriptor: data_tunnel_api_client::TopicDescriptor,
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
        let topic_descriptor = data_tunnel_api_client::register_topic_with_metadata(
            &topic_name,
            sdv_dt::TopicTypedMetadata {
                message_count_: message_count,
                message_size_: message_size,
            },
            metadata,
        )
        .with_context(|| format!("Failed to register topic {topic_name} with metadata."))?;
        Ok(DataTunnelPublisher { topic_descriptor })
    }

    /// Publish a single message to the data tunnel.
    ///
    /// # Arguments
    ///
    /// * `message_bytes` - The bytes that should be written to the data tunnel. Their length must
    ///                     be equal to the topic's configured message size.
    pub fn publish_bytes(&self, message_bytes: &[u8]) -> anyhow::Result<()> {
        let message_count = data_tunnel_api_client::available_to_write(&self.topic_descriptor)
            .with_context(|| "Failed to query how many messages are available to write.")?;
        if message_count == 0 {
            return Err(anyhow!("No messages available to write."));
        }

        data_tunnel_api_client::write_message(&self.topic_descriptor, message_bytes)
            .with_context(|| "Failed to write bytes.")?;
        Ok(())
    }

    /// Serialize and publish a protobuf message to the data tunnel.
    ///
    /// # Arguments
    ///
    /// * `message` - The protobuf message that should be written to the data tunnel. It's
    ///               serialized form, including a varint to represent its length, must fit the
    ///               topic's configured message size, otherwise an error is returned.
    pub fn publish_length_delimited_proto<M: protobuf::Message>(
        &self,
        message: &M,
    ) -> anyhow::Result<()> {
        let max_message_size = self.topic_descriptor.dt_metadata.message_size_;

        let mut raw_message = Vec::<u8>::with_capacity(max_message_size);
        // Prepend the message length to the serialized message, so that the reader knows how many
        // bytes to read.
        message
            .write_length_delimited_to_vec(&mut raw_message)
            .with_context(|| "Failed to serialize protobuf message.")?;
        if raw_message.len() > max_message_size {
            return Err(anyhow!(
                "Message is too long: {} bytes (max: {} bytes)",
                raw_message.len(),
                max_message_size
            ));
        }
        raw_message.resize(max_message_size, 0);
        self.publish_bytes(&raw_message)
    }
}
