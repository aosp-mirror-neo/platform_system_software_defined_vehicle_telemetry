// Copyright 2023 Google LLC

//! Telemetry subscription logic for DT.

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use log::{error, info, trace};
use parking_lot::Mutex;
use protobuf::reflect::{FileDescriptor, MessageDescriptor};
use protobuf::well_known_types::any::Any;
use protobuf::{Message, MessageDyn};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Notify;

use sdv_telemetry::service_publisher_accessor::{
    CreateSubscriptionOutcome, ServicePublisherMessageProvider, ServicePublisherMetadata,
    ServicePublisherSubscription, SubSamplingInterval,
};
use sdv_telemetry::ServicePublisherAccessor;
use sdv_telemetry_metadata_proto_rust::publisher_metadata::TelemetryPublisherMetadata;
use sdv_telemetry_utils::descriptor_utils::WELL_KNOWN_FILE_DESCRIPTORS;

use crate::data_tunnel_api_client::{
    self, DataTunnelError, ReadMessagesError, SubscriptionDescriptor,
};

type Callback = Arc<Mutex<dyn FnMut(Box<dyn MessageDyn>) + Send + Sync>>;

/// Struct for storing all data tunnel subscriptions that implements `ServicePublisherManager`.
pub struct DataTunnelSubscriber {
    /// Hashmap to store topic names and their `MessageDescriptor`s.
    topic_message_descriptors: Mutex<HashMap<String, MessageDescriptor>>,
    /// Hashmap to store topic names and their `DataTunnelSubscription`s.
    /// TODO: b/349242614: Remove temporary workaround once unsubscribe is supported
    topic_subscriptions: Mutex<HashMap<String, Arc<SubscriptionDescriptor>>>,
    /// Hashmap to store topic names and their callbacks.
    /// TODO: b/349242614: Remove temporary workaround once unsubscribe is supported
    topic_callbacks: Arc<Mutex<HashMap<String, Vec<Callback>>>>,
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
        _configuration: Option<&Any>,
        sub_sampling_interval: &SubSamplingInterval,
        callback: Box<dyn FnMut(Box<dyn MessageDyn>) + Send + Sync>,
    ) -> anyhow::Result<CreateSubscriptionOutcome> {
        info!("Subscribing to data tunnel topic {}", topic_name);

        match self.topic_subscriptions.lock().entry(topic_name.to_owned()) {
            Entry::Occupied(o) => {
                // TODO: b/335448653 - We may need to update the sub-sampling interval here.

                let mut callbacks = self.topic_callbacks.lock();

                match callbacks.entry(topic_name.to_owned()) {
                    Entry::Occupied(mut entry) => {
                        let arc_callback = Arc::new(Mutex::new(callback));
                        entry.get_mut().push(arc_callback);
                    }
                    Entry::Vacant(_) => {
                        panic!("Data Tunnel Callbacks missing for {topic_name}");
                    }
                }

                return Ok(CreateSubscriptionOutcome::Subscribed(Box::new(
                    DataTunnelSubscription { _subscription_descriptor: Arc::clone(o.get()) },
                )));
            }
            Entry::Vacant(v) => {
                cxx::let_cxx_string!(cxx_topic_name = topic_name);
                let topic_exists = data_tunnel_api_client::topic_exists(&cxx_topic_name)
                    .with_context(|| {
                        format!("Failed to check whether topic {topic_name} exists.")
                    })?;
                if !topic_exists {
                    return Ok(CreateSubscriptionOutcome::NotRegistered(callback));
                }

                let subscription_descriptor =
                    data_tunnel_api_client::subscribe(&cxx_topic_name).map(Arc::new).with_context(
                        || format!("Failed to subscribe to data tunnel topic {topic_name}"),
                    )?;

                let notify = Arc::new(Notify::new());
                data_tunnel_api_client::set_notification_handler(
                    &subscription_descriptor,
                    Arc::clone(&notify),
                    sub_sampling_interval,
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
                    Err(err) => {
                        error!("Could not read old messages for topic {topic_name}: {err:?}")
                    }
                };

                let message_descriptor = self
                    .get_metadata(topic_name)?
                    .ok_or_else(|| anyhow!("Metadata not found for topic: {}", topic_name))?
                    .message_descriptor;

                {
                    let callback = Arc::new(Mutex::new(callback));
                    let mut callbacks = self.topic_callbacks.lock();
                    callbacks.entry(topic_name.to_owned()).or_default().push(callback);
                }

                let topic_name = topic_name.to_owned();
                let subscription_descriptor_clone = Arc::clone(&subscription_descriptor);
                let topic_callbacks_clone = Arc::clone(&self.topic_callbacks);
                let sub_sampling_interval_clone = sub_sampling_interval.to_owned();
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

                        let number_of_messages = match Self::read(
                            &subscription_descriptor_clone,
                            &topic_name,
                            &mut buffer,
                        ) {
                            Ok(number_of_messages) => number_of_messages,
                            Err(err) => {
                                error!("Could not read messages for topic {topic_name}: {err:?}");
                                continue;
                            }
                        };

                        let messages: Box<dyn std::iter::Iterator<Item = _>> = {
                            let all_messages = buffer.chunks(message_size).take(number_of_messages);
                            if sub_sampling_interval_clone == SubSamplingInterval::ZERO {
                                Box::new(all_messages)
                            } else {
                                // Only process the last (=newest) message.
                                Box::new(all_messages.rev().take(1))
                            }
                        };

                        info!("Received {number_of_messages} messages for topic {topic_name}.");
                        for message_bytes in messages {
                            match Self::read_length_delimited_proto(message_bytes, &message_descriptor) {
                                Ok(msg) => {
                                    let mut callbacks = topic_callbacks_clone.lock();
                                    if let Some(callback_list) = callbacks.get_mut(&topic_name) {
                                        for callback in callback_list.iter_mut() {
                                            let mut callback = callback.lock();
                                            (callback)(msg.clone());
                                        }
                                    } else {
                                        error!("No callbacks found for topic: {}", topic_name);
                                    }
                                },
                                Err(err) => error!(
                                    "Failed to convert bytes to protobuf for topic {topic_name}: {err:?}"
                                ),
                            };
                        }
                    }
                });

                v.insert(Arc::clone(&subscription_descriptor));
                let data_tunnel_subscription = Box::new(DataTunnelSubscription {
                    _subscription_descriptor: subscription_descriptor,
                });

                Ok(CreateSubscriptionOutcome::Subscribed(data_tunnel_subscription))
            }
        }
    }
}

/// Subscriber struct impl that encapsulates the data required for reading messages.
impl DataTunnelSubscriber {
    /// Creates a new instance of the data tunnel subscriber.
    pub fn new() -> Self {
        Self {
            topic_message_descriptors: HashMap::new().into(),
            topic_subscriptions: HashMap::new().into(),
            topic_callbacks: Arc::new(Mutex::new(HashMap::new())),
        }
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
    use super::*;

    use protobuf::reflect::ReflectValueRef;
    use protobuf::MessageFull;
    use sdv_dt::TopicTypedMetadata;
    use sdv_telemetry_test_proto::test_message;
    use sdv_telemetry_test_proto::test_message::TestMessage;
    use sdv_telemetry_test_utils::unique_dt_topic_name;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::time::Instant;

    use crate::data_tunnel_api_client::TopicDescriptor;

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
        let topic_name = unique_dt_topic_name();

        let subscriber = DataTunnelSubscriber::new();
        // The topic should not exist yet, therefore metadata should be `None`.
        assert!(subscriber.get_metadata(&topic_name).unwrap().is_none());
    }

    #[test]
    fn get_metadata_success() {
        let topic_name = unique_dt_topic_name();

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

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn create_subscription_when_topic_does_not_exist_failure() {
        let topic_name = unique_dt_topic_name();

        let subscriber = DataTunnelSubscriber::new();
        let outcome = subscriber
            .create_subscription(
                &topic_name,
                None,
                &SubSamplingInterval::ZERO,
                Box::new(|_| {
                    unreachable!();
                }),
            )
            .await
            .unwrap();
        assert!(matches!(outcome, CreateSubscriptionOutcome::NotRegistered(_)));
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn create_subscription_success() {
        let topic_name = unique_dt_topic_name();

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
                &SubSamplingInterval::ZERO,
                Box::new(|_| {
                    unreachable!();
                }),
            )
            .await
            .unwrap();
        assert!(matches!(outcome, CreateSubscriptionOutcome::Subscribed(_)));
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn create_multiple_subscription_success() {
        let topic_name = unique_dt_topic_name();

        register_topic(
            &topic_name,
            TelemetryPublisherMetadata {
                descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
                message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        );

        let subscriber = DataTunnelSubscriber::new();
        let outcome1 = subscriber
            .create_subscription(
                &topic_name,
                None,
                &SubSamplingInterval::ZERO,
                Box::new(|_| {
                    unreachable!();
                }),
            )
            .await
            .unwrap();

        let outcome2 = subscriber
            .create_subscription(
                &topic_name,
                None,
                &SubSamplingInterval::ZERO,
                Box::new(|_| {
                    unreachable!();
                }),
            )
            .await
            .unwrap();
        assert!(matches!(outcome1, CreateSubscriptionOutcome::Subscribed(_)));
        assert!(matches!(outcome2, CreateSubscriptionOutcome::Subscribed(_)));
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn create_repeated_subscriptions_and_read_messages_success() {
        let topic_name = unique_dt_topic_name();

        let topic_descriptor = register_topic(
            &topic_name,
            TelemetryPublisherMetadata {
                descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
                message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        );

        let subscriber = DataTunnelSubscriber::new();

        let (tx1, mut rx1) = mpsc::unbounded_channel::<Box<dyn MessageDyn>>();
        let outcome1 = subscriber
            .create_subscription(
                &topic_name,
                None,
                &SubSamplingInterval::ZERO,
                Box::new(move |message| tx1.send(message).unwrap()),
            )
            .await
            .unwrap();

        let (tx2, mut rx2) = mpsc::unbounded_channel::<Box<dyn MessageDyn>>();
        let outcome2 = subscriber
            .create_subscription(
                &topic_name,
                None,
                &SubSamplingInterval::ZERO,
                Box::new(move |message| tx2.send(message).unwrap()),
            )
            .await
            .unwrap();

        write_message(&topic_descriptor, TestMessage { int_field: 1, ..Default::default() });
        write_message(&topic_descriptor, TestMessage { int_field: 2, ..Default::default() });

        assert!(matches!(outcome1, CreateSubscriptionOutcome::Subscribed(_)));
        assert!(matches!(outcome2, CreateSubscriptionOutcome::Subscribed(_)));

        assert_eq!(get_int_field_value(rx1.recv().await.unwrap()), 1);
        assert_eq!(get_int_field_value(rx1.recv().await.unwrap()), 2);

        assert_eq!(get_int_field_value(rx2.recv().await.unwrap()), 1);
        assert_eq!(get_int_field_value(rx2.recv().await.unwrap()), 2);
    }

    #[sdv_telemetry_test_utils_macros::timeout(10_000)]
    // Force a multi-threaded runtime for this test, to ensure that the notification timestamps are
    // more accurate, even when one of the threads is stuck in synchronous FFI code.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn create_subscription_and_read_messages_with_sub_sampling_interval_success() {
        let topic_name = unique_dt_topic_name();
        let notification_interval = SubSamplingInterval::try_from(Duration::from_secs(2)).unwrap();

        let topic_descriptor = register_topic(
            &topic_name,
            TelemetryPublisherMetadata {
                descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
                message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        );

        let subscriber = DataTunnelSubscriber::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let outcome = subscriber
            .create_subscription(
                &topic_name,
                None,
                &notification_interval,
                Box::new(move |message| tx.send((Instant::now(), message)).unwrap()),
            )
            .await
            .unwrap();
        assert!(matches!(outcome, CreateSubscriptionOutcome::Subscribed(_)));

        let message1_sent_at = Instant::now();
        write_message(&topic_descriptor, TestMessage { int_field: 1, ..Default::default() });
        let (message1_received_at, message1) = rx.recv().await.unwrap();

        write_message(&topic_descriptor, TestMessage { int_field: 2, ..Default::default() });
        write_message(&topic_descriptor, TestMessage { int_field: 3, ..Default::default() });
        let (message3_received_at, message3) = rx.recv().await.unwrap();

        assert_eq!(get_int_field_value(message1), 1);
        // Message 2 should be silently discarded.
        assert_eq!(get_int_field_value(message3), 3);
        assert!(
            matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
            "The channel should be empty."
        );

        {
            let elapsed = message1_received_at.duration_since(message1_sent_at);
            // The first notification should arrive almost instantly, but to avoid flakiness, add
            // 20% of leeway.
            let max_elapsed = notification_interval.as_duration().mul_f64(0.2);
            assert!(elapsed <= max_elapsed, "Elapsed time before notification should be at most {max_elapsed:?}, but was {elapsed:?}");
        }
        {
            let elapsed = message3_received_at.duration_since(message1_received_at);
            // The second notification should arrive `notification_interval` after the first
            // notification. To avoid flakiness, add 20% of leeway.
            let min_elapsed = notification_interval.as_duration().mul_f64(0.8);
            assert!(elapsed >= min_elapsed, "Elapsed time before notification should be at least {min_elapsed:?}, but was {elapsed:?}");
        }
    }
}
