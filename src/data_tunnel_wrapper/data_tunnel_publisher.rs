// Copyright 2023 Google LLC

//! Telemetry publishing logic for data tunnels.

use anyhow::{anyhow, Context};
use sdv_comms::dt;
use sdv_comms::sd::{PublisherUnitMetadata, UnitMetadata, UnitRegistrationArgs, UnitType};
use sdv_comms::ContextRef;
use sdv_telemetry_utils::conversions::u32_into_usize;

/// The old, deprecated implementation of a data tunnel publisher for the DT API v0.
pub type DeprecatedDataTunnelPublisher =
    crate::deprecated_data_tunnel_publisher::DeprecatedDataTunnelPublisher;

/// Represents the data publisher side of a data tunnel.
pub struct DataTunnelPublisher {
    dt_publisher: dt::Publisher,
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
    pub async fn register_topic(
        ctx: ContextRef,
        unit_type: &UnitType,
        unit_name: &str,
        message_count: u32,
        message_size: u32,
        metadata: Vec<u8>,
    ) -> anyhow::Result<DataTunnelPublisher> {
        let mut unit_registration_args = UnitRegistrationArgs::new();
        unit_registration_args.set_unit_type(unit_type);
        unit_registration_args.set_unit_name(unit_name);

        let mut publisher_unit_metadata = PublisherUnitMetadata::new();
        publisher_unit_metadata.set_message_size(message_size);
        publisher_unit_metadata.set_message_count(message_count);
        unit_registration_args.set_unit_metadata(&UnitMetadata::Publisher(publisher_unit_metadata));

        unit_registration_args.set_application_metadata(&metadata);

        let dt_publisher = dt::Publisher::create(ctx, unit_registration_args)
            .await
            .with_context(|| format!("Failed to register topic {unit_type} with metadata."))?;

        Ok(DataTunnelPublisher { dt_publisher })
    }

    /// Publish a single message to the data tunnel.
    ///
    /// # Arguments
    ///
    /// * `message_bytes` - The bytes that should be written to the data tunnel. Their length must
    ///                     be equal to the topic's configured message size.
    pub fn publish_bytes(&self, message_bytes: &[u8]) -> anyhow::Result<()> {
        let topic_message_size =
            u32_into_usize(self.dt_publisher.get_publisher_unit_metadata().get_message_size());
        if message_bytes.len() != topic_message_size {
            return Err(anyhow!(
                "Message size ({}) does not match configured message size for topic ({topic_message_size})", message_bytes.len()
            ));
        }
        self.dt_publisher
            .write_messages(message_bytes)
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
        let max_message_size =
            u32_into_usize(self.dt_publisher.get_publisher_unit_metadata().get_message_size());

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

    use futures::stream::StreamExt;
    use protobuf::Message;
    use sdv_comms::id::ServiceFqin;
    use sdv_comms::sd::{ServiceDiscoveryManager, ServiceUnitDefinition, UnitType};
    use sdv_status_rs::SdvStatusCode;
    use sdv_telemetry_comms_utils::wait_for_service_discovery_ready_and_create_context;
    use sdv_telemetry_test_proto::test_message::TestMessage;
    use sdv_telemetry_test_utils::unique_dt_topic_name;
    use std::str::FromStr;
    use tokio::sync::OnceCell;

    const UNIT_NAME: &str = "default";

    async fn make_context() -> ContextRef {
        static CONTEXT_REF: OnceCell<ContextRef> = OnceCell::const_new();
        *CONTEXT_REF
            .get_or_init(|| async {
                // Tests may run in multiple parallel processes, therefore we need to ensure that the
                // FQINs created by the processes do not conflict with each other. This is achieved by
                // embedding the process ID into the FQIN.
                let fqin = ServiceFqin::from_str(&format!(
                    "vm:com.google.sdv.telemetry.tests.DataTunnelSubscriberTest{}/instance",
                    std::process::id()
                ))
                .unwrap();

                wait_for_service_discovery_ready_and_create_context(fqin).await
            })
            .await
    }

    fn make_unique_unit_type() -> UnitType {
        UnitType::from_str(&format!(
            "com.google.sdv.telemetry.tests.DataTunnelPublisherTest#{}",
            unique_dt_topic_name()
        ))
        .unwrap()
    }

    async fn discover_service(ctx: ContextRef, unit_type: &UnitType) -> ServiceUnitDefinition {
        let sd = ServiceDiscoveryManager::new(ctx);
        let service_units = sd.list_units_by_type(unit_type).collect::<Vec<_>>().await;
        assert_eq!(service_units.len(), 1);
        service_units[0].clone()
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn register_topic_success() {
        let ctx = make_context().await;
        let unit_type = make_unique_unit_type();

        let publisher =
            DataTunnelPublisher::register_topic(ctx, &unit_type, UNIT_NAME, 10, 100, vec![1, 2, 3])
                .await
                .unwrap();

        let service_unit_definition = discover_service(ctx, &unit_type).await;

        assert_eq!(publisher.dt_publisher.get_publisher_unit_metadata().get_message_count(), 10);
        assert_eq!(publisher.dt_publisher.get_publisher_unit_metadata().get_message_size(), 100);

        assert_eq!(service_unit_definition.get_application_metadata().as_slice(), &[1, 2, 3]);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn publish_bytes_success() {
        let ctx = make_context().await;
        let unit_type = make_unique_unit_type();

        let publisher =
            DataTunnelPublisher::register_topic(ctx, &unit_type, UNIT_NAME, 10, 3, vec![])
                .await
                .unwrap();

        let service_unit_definition = discover_service(ctx, &unit_type).await;
        let subscriber = dt::Subscriber::create(
            ctx,
            &service_unit_definition.get_unit_identifier(),
            dt::SubscriberOptions { fetch_last_message: false },
        )
        .await
        .unwrap();

        publisher.publish_bytes(&[1, 1, 1]).unwrap();
        publisher.publish_bytes(&[2, 2, 2]).unwrap();

        assert_eq!(subscriber.available_to_read().unwrap(), 2);
        let mut buffer = vec![0u8; 6];
        subscriber.read_messages(&mut buffer).unwrap();
        assert_eq!(buffer, vec![1, 1, 1, 2, 2, 2]);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn publish_bytes_overflow() {
        let ctx = make_context().await;
        let unit_type = make_unique_unit_type();

        let publisher =
            DataTunnelPublisher::register_topic(ctx, &unit_type, UNIT_NAME, 2, 3, vec![])
                .await
                .unwrap();

        let service_unit_definition = discover_service(ctx, &unit_type).await;
        let subscriber = dt::Subscriber::create(
            ctx,
            &service_unit_definition.get_unit_identifier(),
            dt::SubscriberOptions { fetch_last_message: false },
        )
        .await
        .unwrap();

        publisher.publish_bytes(&[1, 1, 1]).unwrap();
        publisher.publish_bytes(&[2, 2, 2]).unwrap();
        // This line produces an overflow. However, the publisher is not notified of overflows and
        // can just continue publishing.
        publisher.publish_bytes(&[3, 3, 3]).unwrap();
        publisher.publish_bytes(&[4, 4, 4]).unwrap();

        // The subscriber should be notified of the overflow.
        assert_eq!(subscriber.available_to_read().unwrap(), 4); // TODO: Why 4? Should this not be an overflow error?
        assert_eq!(
            subscriber.read_messages(&mut []).unwrap_err().status_code(),
            SdvStatusCode::Unknown // TODO: Why is this not an overflow error?
        );

        // The change in FMQ (aosp/3119872) that alters the behaviour of the reading pointer reset
        // in case of the buffer overwrite now preserve half of the queue
        assert_eq!(subscriber.available_to_read().unwrap(), 1);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn publish_length_delimited_proto_success() {
        let ctx = make_context().await;
        let unit_type = make_unique_unit_type();

        let publisher =
            DataTunnelPublisher::register_topic(ctx, &unit_type, UNIT_NAME, 2, 30, vec![])
                .await
                .unwrap();

        let service_unit_definition = discover_service(ctx, &unit_type).await;
        let subscriber = dt::Subscriber::create(
            ctx,
            &service_unit_definition.get_unit_identifier(),
            dt::SubscriberOptions { fetch_last_message: false },
        )
        .await
        .unwrap();

        let proto = TestMessage { string_field: "test".to_owned(), ..Default::default() };
        publisher.publish_length_delimited_proto(&proto).unwrap();

        assert_eq!(subscriber.available_to_read().unwrap(), 1);
        let mut buffer = vec![0u8; 30];
        subscriber.read_messages(&mut buffer).unwrap();

        let mut expected = proto.write_length_delimited_to_bytes().unwrap();
        // `buffer` should start with the length delimited byte representation of the protobuf, and
        // then be padded with zeros.
        assert_eq!(buffer[..expected.len()], expected);
        expected.resize(30, 0);
        assert_eq!(buffer, expected);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn publish_length_delimited_proto_too_long_failure() {
        let ctx = make_context().await;
        let unit_type = make_unique_unit_type();

        let publisher =
            DataTunnelPublisher::register_topic(ctx, &unit_type, UNIT_NAME, 2, 30, vec![])
                .await
                .unwrap();

        let service_unit_definition = discover_service(ctx, &unit_type).await;
        let subscriber = dt::Subscriber::create(
            ctx,
            &service_unit_definition.get_unit_identifier(),
            dt::SubscriberOptions { fetch_last_message: false },
        )
        .await
        .unwrap();

        let proto = TestMessage {
            string_field: "this is a very long string that should make the proto bigger than the maximum message size".to_owned(),
            ..Default::default()
        };
        assert!(publisher.publish_length_delimited_proto(&proto).is_err());

        assert_eq!(subscriber.available_to_read().unwrap(), 0);
    }
}
