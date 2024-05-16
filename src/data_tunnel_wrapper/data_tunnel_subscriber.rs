// Copyright 2023 Google LLC

//! Telemetry subscription logic for DT.

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use log::{error, info};
use protobuf::reflect::{FileDescriptor, MessageDescriptor};
use protobuf::{Message, MessageDyn};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

use sdv_dt::{Result as SdvResult, SubscriptionHandle, TopicTypedMetadata};
use sdv_telemetry::service_publisher_accessor::{
    CreateSubscriptionOutcome, ServicePublisherMessageProvider, ServicePublisherMetadata,
    ServicePublisherSubscription,
};
use sdv_telemetry::ServicePublisherAccessor;
use sdv_telemetry_utils::descriptor_utils::WELL_KNOWN_FILE_DESCRIPTORS;
use sdvtelemetry_metadata_proto_rust::publisher_metadata::TelemetryPublisherMetadata;

use crate::data_tunnel_api_client;

// Notification interval between two consecutive messages received from Data Tunnel.
const NOTIFICATION_INTERVAL: u32 = 0;

/// Struct for storing all data tunnel subscriptions that implements `ServicePublisherManager`.
pub struct DataTunnelSubscriber {
    /// Hashmap to store topic names and their `MessageDescriptor`s.
    data_tunnel_topic_message_descriptors: Arc<Mutex<HashMap<String, MessageDescriptor>>>,
}

/// Struct for subscription of DT topic.
pub struct DataTunnelSubscription {
    _subscription_handle: SubscriptionHandle,
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
            .data_tunnel_topic_message_descriptors
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

                let publisher_metadata = data_tunnel_api_client::get_topic_metadata(
                    &cxx_topic_name,
                )
                .with_context(|| format!("Failed to get metadata for topic {topic_name}."))?;
                let publisher_metadata =
                    TelemetryPublisherMetadata::parse_from_bytes(publisher_metadata.as_slice())
                        .with_context(|| {
                            format!("Failed to parse metadata for topic {topic_name}.")
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

        let (
            subscription_handle,
            TopicTypedMetadata { message_count_: message_count, message_size_: message_size },
        ) = data_tunnel_api_client::subscribe(&cxx_topic_name)
            .with_context(|| format!("Failed to subscribe to data tunnel topic {topic_name}"))?;

        // Setting notification handler
        let notify = Arc::new(Notify::new());

        data_tunnel_api_client::set_notification_handler(
            subscription_handle,
            Arc::clone(&notify),
            NOTIFICATION_INTERVAL,
        )
        .with_context(|| {
            format!("Failed to set notification handler for data tunnel topic {topic_name}")
        })?;

        let message_descriptor = self
            .get_metadata(topic_name)?
            .ok_or_else(|| anyhow!("Metadata not found for topic: {}", topic_name))?
            .message_descriptor;

        let topic_name_clone = topic_name.to_owned();

        // Tokio task for fetching data from data tunnel and pushing it to telemetry service.
        tokio::spawn(async move {
            let size_of_buffer = message_count * message_size;
            let mut buffer = vec![0u8; size_of_buffer];
            let mut is_first_message = true;

            // This loop runs infintely until telemetry service stops. In case of error in message
            // parsing, the data is lost, as data was retrieved successfully from data tunnel. In
            // case of data tunnel errors, data is not lost, and can be fetched in the next
            // iteration of the loop. Logic to stop the loop will be added once data tunnel provides
            // support for unsubscribe calls
            loop {
                notify.notified().await;
                match Self::read(
                    subscription_handle,
                    &topic_name_clone,
                    &mut buffer,
                    is_first_message,
                ) {
                    Ok(0) => {
                        info!("No new messages.");
                    }
                    Ok(number_of_messages) => {
                        info!("Received {:?} messages", number_of_messages);
                        for message_bytes in buffer.chunks(message_size).take(number_of_messages) {
                            let mut is = protobuf::CodedInputStream::from_bytes(message_bytes);
                            // Get number of bytes that need to be read
                            let size = is.read_raw_varint32().unwrap().into();
                            if let Err(e) = is.push_limit(size) {
                                error!("Error when pushing the limit: {}", e);
                                return;
                            }

                            match message_descriptor.parse_from(&mut is) {
                                Ok(msg) => {
                                    (callback)(msg);
                                }
                                Err(e) => {
                                    error!("Error when parsing the message: {}", e);
                                }
                            };
                        }
                    }
                    Err(result) => {
                        error!("Could not read message result={:?}", result)
                    }
                };

                is_first_message = false;
            }
        });

        Ok(CreateSubscriptionOutcome::Subscribed(Box::new(DataTunnelSubscription {
            _subscription_handle: subscription_handle,
        })))
    }
}

/// Subscriber struct impl that encapsulates the data required for reading messages.
impl DataTunnelSubscriber {
    /// Creates a new instance of the struct.
    pub fn new() -> Self {
        // Create struct to interact with Data Tunnel Apis
        Self { data_tunnel_topic_message_descriptors: Arc::new(Mutex::new(HashMap::new())) }
    }

    /// Read messages using Data tunnel and writes data to the input buffer.
    /// If the buffer is smaller than the number of messages, it does a partial write.
    /// Returns how many messages was read into the buffer.
    fn read(
        subscription_handle: SubscriptionHandle,
        topic_name: &str,
        buffer: &mut [u8],
        is_first_message: bool,
    ) -> anyhow::Result<usize> {
        let read_message_count =
            data_tunnel_api_client::available_to_read(subscription_handle).or_else(|err| {
                if err != SdvResult::Overflow {
                    return Err(err);
                }
                const NUM_MESSAGES: usize = 1;
                // We have to invoke `ReadMessages()` after an Overflow error to reset the Overflow flag.
                // SAFETY: Buffer always has enough size to fit a single message.
                let result = unsafe { data_tunnel_api_client::read_messages(subscription_handle, buffer, NUM_MESSAGES) };
                if let Err(err) = result {
                    if err == SdvResult::Overflow {
                        return Err(err);
                    }
                }
                data_tunnel_api_client::available_to_read(subscription_handle)
            })
            .with_context(||
                format!("Failed to query how many messages are available to read for data tunnel topic {topic_name}"))?;

        if read_message_count > 0 {
            // SAFETY: `buffer` points to a buffer of size at least `read_message_count` *
            // `message_size` bytes, where `message_size` can be fetched from metadata argument in
            // `SubscribeToTopic()` method.
            let result = unsafe { data_tunnel_api_client::read_messages(subscription_handle, buffer, read_message_count) }
                .with_context(|| {
                    format!("Failed to read {read_message_count} messages for data tunnel topic {topic_name}")
                });
            if let Err(err) = result {
                // The first `ReadMessage()` invocation could return error if the first message in
                // the queue was overwritten.
                return if is_first_message { Ok(0) } else { Err(err) };
            }
        }

        Ok(read_message_count)
    }
}

#[async_trait]
impl ServicePublisherSubscription for DataTunnelSubscription {
    async fn unsubscribe(self: Box<Self>) -> anyhow::Result<()> {
        // This logic will be replaced once the data tunnel has support for unsubscribe.
        Ok(())
    }
}
