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

#[cfg(all(test, integration_tests))]
mod tests {
    use super::*;

    use protobuf::Message;
    use sdv_telemetry_test_proto::test_message::TestMessage;
    use sdv_telemetry_test_utils::unique_dt_topic_name;

    #[test]
    fn register_topic_success() {
        let topic_name = unique_dt_topic_name();
        let publisher =
            DataTunnelPublisher::register_topic(&topic_name, 10, 100, vec![1, 2, 3]).unwrap();

        cxx::let_cxx_string!(topic_name = topic_name);

        assert_eq!(publisher.topic_descriptor.dt_metadata.message_count_, 10);
        assert_eq!(publisher.topic_descriptor.dt_metadata.message_size_, 100);
        assert!(data_tunnel_api_client::topic_exists(&topic_name).unwrap());
        assert_eq!(
            data_tunnel_api_client::get_topic_metadata(&topic_name).unwrap().as_slice(),
            &[1, 2, 3]
        );
    }

    #[test]
    fn publish_bytes_success() {
        let topic_name = unique_dt_topic_name();
        let publisher = DataTunnelPublisher::register_topic(&topic_name, 10, 3, vec![]).unwrap();

        cxx::let_cxx_string!(topic_name = topic_name);
        let subscription_descriptor = data_tunnel_api_client::subscribe(&topic_name).unwrap();

        publisher.publish_bytes(&[1, 1, 1]).unwrap();
        publisher.publish_bytes(&[2, 2, 2]).unwrap();

        assert_eq!(data_tunnel_api_client::available_to_read(&subscription_descriptor).unwrap(), 2);
        let mut buffer = vec![0u8; 6];
        data_tunnel_api_client::read_messages(&subscription_descriptor, &mut buffer, 2).unwrap();
        assert_eq!(buffer, vec![1, 1, 1, 2, 2, 2]);
    }

    #[test]
    fn publish_bytes_overflow() {
        let topic_name = unique_dt_topic_name();
        let publisher = DataTunnelPublisher::register_topic(&topic_name, 2, 3, vec![]).unwrap();

        cxx::let_cxx_string!(topic_name = topic_name);
        let subscription_descriptor = data_tunnel_api_client::subscribe(&topic_name).unwrap();

        publisher.publish_bytes(&[1, 1, 1]).unwrap();
        publisher.publish_bytes(&[2, 2, 2]).unwrap();
        // This line produces an overflow. However, the publisher is not notified of overflows and
        // can just continue publishing.
        publisher.publish_bytes(&[3, 3, 3]).unwrap();
        publisher.publish_bytes(&[4, 4, 4]).unwrap();

        // The subscriber should be notified of the overflow.
        assert_eq!(
            data_tunnel_api_client::available_to_read(&subscription_descriptor).unwrap_err(),
            sdv_dt::Result::Overflow
        );
        let result = data_tunnel_api_client::read_messages(&subscription_descriptor, &mut [], 0);
        assert!(
            matches!(result, Err(data_tunnel_api_client::ReadMessagesError::DataTunnelError(ref err))
                if *err == sdv_dt::Result::Overflow),
            "Expected overflow error, got {result:?}"
        );
        // The change in FMQ (aosp/3119872) that alters the behaviour of the reading pointer reset
        // in case of the buffer overwrite now preserve half of the queue
        assert_eq!(data_tunnel_api_client::available_to_read(&subscription_descriptor).unwrap(), 1);
    }

    #[test]
    fn publish_length_delimited_proto_success() {
        let topic_name = unique_dt_topic_name();
        let publisher = DataTunnelPublisher::register_topic(&topic_name, 2, 30, vec![]).unwrap();

        cxx::let_cxx_string!(topic_name = topic_name);
        let subscription_descriptor = data_tunnel_api_client::subscribe(&topic_name).unwrap();

        let proto = TestMessage { string_field: "test".to_owned(), ..Default::default() };
        publisher.publish_length_delimited_proto(&proto).unwrap();

        assert_eq!(data_tunnel_api_client::available_to_read(&subscription_descriptor).unwrap(), 1);
        let mut buffer = vec![0u8; 30];
        data_tunnel_api_client::read_messages(&subscription_descriptor, &mut buffer, 1).unwrap();

        let mut expected = proto.write_length_delimited_to_bytes().unwrap();
        // `buffer` should start with the length delimited byte representation of the protobuf, and
        // then be padded with zeros.
        assert_eq!(buffer[..expected.len()], expected);
        expected.resize(30, 0);
        assert_eq!(buffer, expected);
    }

    #[test]
    fn publish_length_delimited_proto_too_long_failure() {
        let topic_name = unique_dt_topic_name();
        let publisher = DataTunnelPublisher::register_topic(&topic_name, 2, 30, vec![]).unwrap();

        cxx::let_cxx_string!(topic_name = topic_name);
        let subscription_descriptor = data_tunnel_api_client::subscribe(&topic_name).unwrap();

        let proto = TestMessage {
            string_field: "this is a very long string that should make the proto bigger than the maximum message size".to_owned(),
            ..Default::default()
        };
        assert!(publisher.publish_length_delimited_proto(&proto).is_err());

        assert_eq!(data_tunnel_api_client::available_to_read(&subscription_descriptor).unwrap(), 0);
    }
}
