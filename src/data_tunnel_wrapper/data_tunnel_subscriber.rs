// Copyright 2023 Google LLC

//! Telemetry subscription logic for DT.

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use log::{error, info, trace};
use protobuf::reflect::{FileDescriptor, MessageDescriptor};
use protobuf::{Message, MessageDyn};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

use sdv_telemetry::service_publisher_accessor::{
    CreateSubscriptionOutcome, ServicePublisherMessageProvider, ServicePublisherMetadata,
    ServicePublisherSubscription,
};
use sdv_telemetry::ServicePublisherAccessor;
use sdv_telemetry_utils::descriptor_utils::WELL_KNOWN_FILE_DESCRIPTORS;
use sdvtelemetry_metadata_proto_rust::publisher_metadata::TelemetryPublisherMetadata;

use crate::data_tunnel_api_client::{
    self, DataTunnelError, ReadMessagesError, SubscriptionDescriptor,
};

// Notification interval between two consecutive messages received from Data Tunnel.
const NOTIFICATION_INTERVAL: u32 = 0;

/// Struct for storing all data tunnel subscriptions that implements `ServicePublisherManager`.
pub struct DataTunnelSubscriber {
    /// Hashmap to store topic names and their `MessageDescriptor`s.
    topic_message_descriptors: Mutex<HashMap<String, MessageDescriptor>>,
}

/// Struct for subscription of DT topic.
pub struct DataTunnelSubscription {
    _subscription_descriptor: Arc<SubscriptionDescriptor>,
}

impl Default for DataTunnelSubscriber {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ServicePublisherAccessor for DataTunnelSubscriber {
    fn get_metadata(&self, topic_name: &str) -> anyhow::Result<Option<ServicePublisherMetadata>> {
        let message_descriptor = match self
            .topic_message_descriptors
            .lock()
            .unwrap()
            .entry(topic_name.to_owned())
        {
            Entry::Occupied(o) => o.get().clone(),
            Entry::Vacant(v) => {
                cxx::let_cxx_string!(cxx_topic_name = topic_name);
                let topic_exists = data_tunnel_api_client::topic_exists(&cxx_topic_name)
                    .with_context(|| {
                        format!("Failed to check whether topic {topic_name} exists.")
                    })?;
                if !topic_exists {
                    return Ok(None);
                }

                let publisher_metadata =
                    data_tunnel_api_client::get_topic_metadata(&cxx_topic_name).with_context(
                        || format!("Failed to get publisher metadata for topic {topic_name}."),
                    )?;
                let publisher_metadata =
                    TelemetryPublisherMetadata::parse_from_bytes(publisher_metadata.as_slice())
                        .with_context(|| {
                            format!("Failed to parse publisher metadata for topic {topic_name}.")
                        })?;

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
                let message_descriptor = file_descriptors
                    .iter()
                    .find_map(|fd| fd.message_by_full_name(&publisher_metadata.message_type_name))
                    .ok_or(anyhow!(
                        "Message Name {} not found in descriptor",
                        publisher_metadata.message_type_name
                    ))?;

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
        topic_name: &str,
        _configuration: Option<&[u8]>,
        mut callback: Box<dyn FnMut(Box<dyn MessageDyn>) + Send + Sync>,
    ) -> anyhow::Result<CreateSubscriptionOutcome> {
        info!("Subscribing to data tunnel topic {}", topic_name);

        cxx::let_cxx_string!(cxx_topic_name = topic_name);
        let topic_exists = data_tunnel_api_client::topic_exists(&cxx_topic_name)
            .with_context(|| format!("Failed to check whether topic {topic_name} exists."))?;
        if !topic_exists {
            return Ok(CreateSubscriptionOutcome::NotRegistered(callback));
        }

        let subscription_descriptor = data_tunnel_api_client::subscribe(&cxx_topic_name)
            .map(Arc::new)
            .with_context(|| format!("Failed to subscribe to data tunnel topic {topic_name}"))?;

        let notify = Arc::new(Notify::new());
        data_tunnel_api_client::set_notification_handler(
            &subscription_descriptor,
            Arc::clone(&notify),
            NOTIFICATION_INTERVAL,
        )
        .with_context(|| {
            format!("Failed to set notification handler for data tunnel topic {topic_name}")
        })?;

        let sdv_dt::TopicTypedMetadata {
            message_count_: message_count,
            message_size_: message_size,
        } = subscription_descriptor.dt_metadata;
        let mut buffer = vec![0u8; message_count * message_size];

        // Discard all existing data after subscribing to the topic. We do not want to trigger on
        // old data (b/344644795).
        match Self::read(&subscription_descriptor, topic_name, &mut buffer) {
            Ok(number_of_messages) => {
                info!("Discarded {number_of_messages} old messages for topic {topic_name}.")
            }
            Err(err) => error!("Could not read old messages for topic {topic_name}: {err:?}"),
        };

        let message_descriptor = self
            .get_metadata(topic_name)?
            .ok_or_else(|| anyhow!("Metadata not found for topic: {}", topic_name))?
            .message_descriptor;

        let topic_name = topic_name.to_owned();
        let subscription_descriptor_clone = Arc::clone(&subscription_descriptor);
        // Tokio task for fetching data from data tunnel and pushing it to telemetry service.
        tokio::spawn(async move {
            // This loop runs infinitely until the telemetry service stops. In case of an error
            // while message parsing, the data is lost, as data was retrieved successfully from the
            // data tunnel. In case of data tunnel errors, data is not lost, and can be fetched in
            // the next iteration of the loop.
            //
            // TODO(b/329515273): Add logic to stop the loop once data tunnel provides support for
            // unsubscribing.
            loop {
                // Wait for the next message(s) to arrive.
                notify.notified().await;

                let number_of_messages =
                    match Self::read(&subscription_descriptor_clone, &topic_name, &mut buffer) {
                        Ok(number_of_messages) => number_of_messages,
                        Err(err) => {
                            error!("Could not read messages for topic {topic_name}: {err:?}");
                            continue;
                        }
                    };

                info!("Received {number_of_messages} messages for topic {topic_name}.");
                for message_bytes in buffer.chunks(message_size).take(number_of_messages) {
                    match Self::read_length_delimited_proto(message_bytes, &message_descriptor) {
                        Ok(msg) => (callback)(msg),
                        Err(err) => error!(
                            "Failed to convert bytes to protobuf for topic {topic_name}: {err:?}"
                        ),
                    };
                }
            }
        });

        Ok(CreateSubscriptionOutcome::Subscribed(Box::new(DataTunnelSubscription {
            _subscription_descriptor: subscription_descriptor,
        })))
    }
}

/// Subscriber struct impl that encapsulates the data required for reading messages.
impl DataTunnelSubscriber {
    /// Creates a new instance of the data tunnel subscriber.
    pub fn new() -> Self {
        Self { topic_message_descriptors: HashMap::new().into() }
    }

    /// Read all available messages from the data tunnel and write them to the `buffer`.
    /// Returns how many messages were read into the buffer.
    fn read(
        subscription_descriptor: &SubscriptionDescriptor,
        topic_name: &str,
        buffer: &mut [u8],
    ) -> anyhow::Result<usize> {
        let read_message_count =
            data_tunnel_api_client::available_to_read(subscription_descriptor).or_else(|err| -> anyhow::Result<usize> {
                if err != sdv_dt::Result::Overflow {
                    return Err(err.into());
                }
                trace!("Data tunnel overflow for topic {topic_name} occurred.");

                // We have to invoke `ReadMessages()` after an Overflow error to reset the Overflow
                // flag.
                const NUM_MESSAGES: usize = 0;
                match data_tunnel_api_client::read_messages(subscription_descriptor, &mut [], NUM_MESSAGES) {
                    Err(ReadMessagesError::DataTunnelError(DataTunnelError(sdv_dt::Result::Overflow))) => {}
                    // Any other result, including `Ok`, is unexpected, and should be treated as an
                    // error.
                    Err(err) => return Err(
                        anyhow!("Failed to read message after data tunnel overflow for topic {topic_name}: {err:?}")
                    ),
                    Ok(_) => return Err(
                        anyhow!("Successfully read message after data tunnel overflow for topic {topic_name}, but expected an error.")
                    )
                };
                Ok(data_tunnel_api_client::available_to_read(subscription_descriptor)?)
            })
            .with_context(|| {
                format!("Failed to query how many messages are available to read for data tunnel topic {topic_name}")
            })?;

        if read_message_count > 0 {
            data_tunnel_api_client::read_messages(subscription_descriptor, buffer, read_message_count)
                .with_context(|| {
                    format!("Failed to read {read_message_count} messages for data tunnel topic {topic_name}")
                })?;
        }

        Ok(read_message_count)
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
        // This logic will be replaced once the data tunnel has support for unsubscribe.
        Ok(())
    }
}

#[cfg(all(test, unit_tests))]
mod tests {
    use super::*;

    #[test]
    fn get_message_provider_success() -> anyhow::Result<()> {
        let subscriber = DataTunnelSubscriber::new();
        assert!(subscriber.get_message_provider("service-name")?.is_none());

        Ok(())
    }
}

#[cfg(all(test, integration_tests))]
mod tests {
    use protobuf::reflect::ReflectValueRef;
    use protobuf::MessageFull;
    use sdv_dt::TopicTypedMetadata;
    use sdv_telemetry_test_proto::test_message;
    use sdv_telemetry_test_proto::test_message::TestMessage;
    use tokio::sync::mpsc;

    use super::*;
    use crate::data_tunnel_api_client::TopicDescriptor;

    // TODO(b/343140446): We should clean up data tunnel topics.
    fn unique_topic_name() -> String {
        format!("Telemetry.Test.Topic.{}", rand::random::<u128>())
    }

    fn register_topic(
        topic_name: &str,
        publisher_metadata: TelemetryPublisherMetadata,
    ) -> TopicDescriptor {
        let mut metadata = vec![];
        publisher_metadata.write_to_vec(&mut metadata).unwrap();
        cxx::let_cxx_string!(cxx_topic_name = &topic_name);
        data_tunnel_api_client::register_topic_with_metadata(
            &cxx_topic_name,
            TopicTypedMetadata { message_count_: 2, message_size_: 3 },
            metadata,
        )
        .unwrap()
    }

    fn write_message(topic_descriptor: &TopicDescriptor, message: TestMessage) {
        let mut bytes = vec![];
        message.write_length_delimited_to_vec(&mut bytes).unwrap();
        data_tunnel_api_client::write_message(topic_descriptor, &bytes).unwrap();
    }

    fn get_int_field_value(message: Box<dyn MessageDyn>) -> i32 {
        let int_field_descriptor = message.descriptor_dyn().field_by_name("int_field").unwrap();
        match int_field_descriptor.get_singular(&*message) {
            Some(ReflectValueRef::I32(value)) => value,
            result => panic!("Unexpected value: {result:?}"),
        }
    }

    #[test]
    fn get_metadata_topic_not_registered_returns_none() {
        let topic_name = unique_topic_name();

        let subscriber = DataTunnelSubscriber::new();
        // The topic should not exist yet, therefore metadata should be `None`.
        assert!(subscriber.get_metadata(&topic_name).unwrap().is_none());
    }

    #[test]
    fn get_metadata_success() {
        let topic_name = unique_topic_name();

        register_topic(
            &topic_name,
            TelemetryPublisherMetadata {
                descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
                message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        );

        let subscriber = DataTunnelSubscriber::new();
        let publisher_metadata = subscriber.get_metadata(&topic_name).unwrap().unwrap();
        assert!(!publisher_metadata.supports_get);
        assert_eq!(publisher_metadata.message_descriptor.name(), "TestMessage");
        assert_eq!(
            publisher_metadata.message_descriptor.proto(),
            TestMessage::descriptor().proto()
        );
    }

    #[tokio::test]
    async fn create_subscription_when_topic_does_not_exist_failure() {
        let topic_name = unique_topic_name();

        let subscriber = DataTunnelSubscriber::new();
        let outcome = subscriber
            .create_subscription(
                &topic_name,
                None,
                Box::new(|_| {
                    unreachable!();
                }),
            )
            .await
            .unwrap();
        assert!(matches!(outcome, CreateSubscriptionOutcome::NotRegistered(_)));
    }

    #[tokio::test]
    async fn create_subscription_success() {
        let topic_name = unique_topic_name();

        register_topic(
            &topic_name,
            TelemetryPublisherMetadata {
                descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
                message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        );

        let subscriber = DataTunnelSubscriber::new();
        let outcome = subscriber
            .create_subscription(
                &topic_name,
                None,
                Box::new(|_| {
                    unreachable!();
                }),
            )
            .await
            .unwrap();
        assert!(matches!(outcome, CreateSubscriptionOutcome::Subscribed(_)));
    }

    #[tokio::test]
    async fn create_subscription_read_messages_success() {
        let topic_name = unique_topic_name();

        let topic_descriptor = register_topic(
            &topic_name,
            TelemetryPublisherMetadata {
                descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
                message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        );

        // Write a message before the subscription is created, to verify that it will not be
        // received but ignored - we do not want to trigger on old data (b/344644795).
        write_message(&topic_descriptor, TestMessage { int_field: 1, ..Default::default() });

        let subscriber = DataTunnelSubscriber::new();
        let (tx, mut rx) = mpsc::unbounded_channel::<Box<dyn MessageDyn>>();
        let outcome = subscriber
            .create_subscription(
                &topic_name,
                None,
                Box::new(move |message| tx.send(message).unwrap()),
            )
            .await
            .unwrap();
        assert!(matches!(outcome, CreateSubscriptionOutcome::Subscribed(_)));

        write_message(&topic_descriptor, TestMessage { int_field: 2, ..Default::default() });
        assert_eq!(get_int_field_value(rx.recv().await.unwrap()), 2);

        write_message(&topic_descriptor, TestMessage { int_field: 3, ..Default::default() });
        write_message(&topic_descriptor, TestMessage { int_field: 4, ..Default::default() });
        assert_eq!(get_int_field_value(rx.recv().await.unwrap()), 3);
        assert_eq!(get_int_field_value(rx.recv().await.unwrap()), 4);
    }
}
