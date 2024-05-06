// Copyright 2023 Google LLC

//! Telemetry subscription logic for DT.

use anyhow::anyhow;
use async_trait::async_trait;
use cxx::CxxVector;
use log::{error, info};
use protobuf::reflect::{FileDescriptor, MessageDescriptor};
use protobuf::MessageDyn;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::pin::Pin;
use std::result::Result;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

use sdv_dt::{
    c_void, Result as SdvResult, SubscriptionHandle, SubscriptionNotificationType,
    TopicTypedMetadata,
};
use sdv_telemetry::service_publisher_accessor::{
    CreateSubscriptionOutcome, ServicePublisherSubscription,
};
use sdv_telemetry::ServicePublisherAccessor;
use sdv_telemetry_utils::descriptor_utils::WELL_KNOWN_FILE_DESCRIPTORS;
use sdvtelemetry_metadata_proto_rust::publisher_metadata::TelemetryPublisherMetadata;

// Notification interval betwween two consecutive messages received from Data Tunnel.
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

/// Callback that handles data received from the data tunnel.
///
/// # Safety
///
/// * `notification_type` - Points to an enum SubscriptionNotificationType defined in data tunnel.
///
/// * `user_data` - Points to a valid `mpsc::Notify` originally sent to the caller during the
///                 `SetNotificationHandler` api call and was originally allocated using a global
///                 allocator using Box::new().
extern "C" fn data_tunnel_callback(
    notification_type: SubscriptionNotificationType,
    user_data: *mut c_void,
) {
    // SAFETY: safe to use as the user_data passed by the caller was originally allocated using a
    // global allocator using Arc::new(). For more info check
    // https://doc.rust-lang.org/std/boxed/index.html#memory-layout
    // Convert pointer to reference but do not take ownership
    let notify = unsafe { &*(user_data as *const Notify) };

    if notification_type == SubscriptionNotificationType::DataAvailable {
        notify.notify_one();
    } else {
        error!("Unexpected notification type: {:?}", notification_type);
    }
}

impl Default for DataTunnelSubscriber {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ServicePublisherAccessor for DataTunnelSubscriber {
    /// Checks if topic with the given name exists in the data tunnel.
    fn get_message_descriptor(
        &self,
        topic_name: &str,
    ) -> Result<Option<MessageDescriptor>, anyhow::Error> {
        let message_descriptor = match self
            .data_tunnel_topic_message_descriptors
            .lock()
            .unwrap()
            .entry(topic_name.to_owned())
        {
            Entry::Occupied(o) => o.get().clone(),
            Entry::Vacant(v) => {
                cxx::let_cxx_string!(cxx_topic_name = topic_name);
                let mut topic_exists = true;
                let result = sdv_dt::TopicExists(&cxx_topic_name, &mut topic_exists);
                if result != SdvResult::Success {
                    return Err(anyhow!("Data Tunnel Result: {:?}", result));
                };
                if !topic_exists {
                    return Ok(None);
                }

                let mut cxx_metadata_vec = CxxVector::<u8>::new();
                let pinned_metadata = cxx_metadata_vec.pin_mut();
                let result = sdv_dt::GetTopicMetadata(&cxx_topic_name, pinned_metadata);
                if result != SdvResult::Success {
                    return Err(anyhow!("Data Tunnel Result: {:?}", result));
                };

                let publisher_metadata: TelemetryPublisherMetadata =
                    protobuf::Message::parse_from_bytes(cxx_metadata_vec.as_slice())
                        .map_err(|e| anyhow!("Error in parsing Metadata: {:?}", e))?;

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

        Ok(Some(message_descriptor))
    }

    async fn create_subscription(
        &self,
        topic_name: &str,
        _configuration: Option<&[u8]>,
        mut callback: Box<dyn FnMut(Box<dyn MessageDyn>) + Send + Sync>,
    ) -> anyhow::Result<CreateSubscriptionOutcome> {
        info!("Subscribing to data tunnel topic {}", topic_name);

        cxx::let_cxx_string!(cxx_topic_name = topic_name);
        let mut topic_exists = true;
        let result = sdv_dt::TopicExists(&cxx_topic_name, &mut topic_exists);
        if result != SdvResult::Success {
            return Err(anyhow!("Data Tunnel Result: {:?}", result));
        };
        if !topic_exists {
            return Ok(CreateSubscriptionOutcome::NotAvailable(callback));
        }

        let mut subscribe_meta_data = TopicTypedMetadata { message_count_: 0, message_size_: 0 };
        let metadata: Pin<&mut TopicTypedMetadata> = Pin::new(&mut subscribe_meta_data);
        let mut subscription_handle = SubscriptionHandle::default();
        let result = sdv_dt::SubscribeToTopic(&cxx_topic_name, metadata, &mut subscription_handle);
        if result != SdvResult::Success {
            return Err(anyhow!("Data Tunnel Result: {:?}", result));
        };

        // Setting notification handler
        let notify = Arc::new(Notify::new());

        // SAFETY: `user_data` must point to valid data for `data_tunnel_callback` to use.
        let result = unsafe {
            sdv_dt::SetNotificationHandler(
                subscription_handle,
                sdv_dt::SubscriptionNotificationCallback(data_tunnel_callback),
                Arc::into_raw(notify.clone()) as *mut c_void,
                NOTIFICATION_INTERVAL,
            )
        };

        if result != SdvResult::Success {
            return Err(anyhow!("Data Tunnel Result: {:?}", result));
        };

        let message_size = subscribe_meta_data.message_size_;
        let message_count = subscribe_meta_data.message_count_;

        let message_descriptor = self
            .get_message_descriptor(topic_name)?
            .ok_or_else(|| anyhow!("Metadata not found for topic: {}", topic_name))?;
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
        let mut read_message_count = 0usize;
        let mut result = sdv_dt::AvailableToRead(subscription_handle, &mut read_message_count);

        // We have to invoke `ReadMessages()` after an Overflow error to reset the Overflow flag.
        if result == sdv_dt::Result::Overflow {
            // SAFETY: Buffer always has enough size to fit a single message.
            if unsafe {
                sdv_dt::ReadMessages(subscription_handle, buffer.as_mut_ptr() as *mut c_void, 1)
            } == sdv_dt::Result::Overflow
            {
                return Err(anyhow!("Error: {:?}", result));
            }
            result = sdv_dt::AvailableToRead(subscription_handle, &mut read_message_count);
        }

        if result != SdvResult::Success {
            return Err(anyhow!("Error when calling AvailableToRead: {:?}", result));
        };

        if read_message_count == 0 {
            return Ok(0);
        }

        // SAFETY: `buffer` points to a buffer of size at least `message_count` * `message_size`
        // bytes, where `message_size` and `message_count` can be fetched from metadata argument in
        // `SubscribeToTopic()` method.
        let result = unsafe {
            sdv_dt::ReadMessages(
                subscription_handle,
                buffer.as_mut_ptr() as *mut c_void,
                read_message_count,
            )
        };

        match result {
            SdvResult::Success => Ok(read_message_count),
            _ => {
                // The first `ReadMessage()` invocation could return error if the first message in
                // the queue was overwritten.
                if is_first_message {
                    Ok(0usize)
                } else {
                    Err(anyhow!("Unable to read data from topic: {}", topic_name))
                }
            }
        }
    }
}

#[async_trait]
impl ServicePublisherSubscription for DataTunnelSubscription {
    async fn unsubscribe(self: Box<Self>) -> Result<(), anyhow::Error> {
        // This logic will be replaced once the data tunnel has support for unsubscribe.
        Ok(())
    }
}
