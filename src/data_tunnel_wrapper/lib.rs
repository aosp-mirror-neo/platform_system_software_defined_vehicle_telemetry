// Copyright 2023 Google LLC

//! Sample Wrapper for DT.

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
    c_void, Result as SdvResult, SubscriptionHandle, SubscriptionNotificationType, TopicHandle,
    TopicTypedMetadata,
};
use sdv_telemetry::ServicePublisherAccessor;
use sdv_telemetry_utils::descriptor_utils::WELL_KNOWN_FILE_DESCRIPTORS;
use sdvtelemetry_metadata_proto_rust::publisher_metadata::TelemetryPublisherMetadata;

// Notification interval betwween two consecutive messages received from Data Tunnel
const NOTIFICATION_INTERVAL: u32 = 0;

/// Callback that handles data received from data tunnel.
///
/// # Safety
///
/// * `notification_type` points to an enum SubscriptionNotificationType defined in data tunnel
///
/// * `user_data` points to a valid `mpsc::Notify` originally sent to the caller during
///    the SetNotificationHandler api call and was originally allocated using a
///    global allocator using Box::new()
extern "C" fn data_tunnel_callback(
    notification_type: SubscriptionNotificationType,
    user_data: *mut c_void,
) {
    // SAFETY: safe to use as the user_data passed by the caller was originally allocated using a
    // global allocator using Box::new(). For more info check
    // https://doc.rust-lang.org/std/boxed/index.html#memory-layout
    let notify = unsafe { Arc::from_raw(user_data as *const Notify) };

    if notification_type == SubscriptionNotificationType::DataAvailable {
        notify.notify_one();
    } else {
        error!("Unexpected notification type: {:?}", notification_type);
    }
}

/// Struct for data publisher of DT, to simplify sample usage.
pub struct DataTunnelPublisher {
    topic_handle: TopicHandle,
}

/// Struct for subscription of DT topic
pub struct DataTunnelSubscription {
    subscription_handle: SubscriptionHandle,
    first_message: bool,
    _subscribe_meta_data: TopicTypedMetadata,
}

/// Struct for storing all data tunnel subscriptions that implements ServicePublisherManager
pub struct DataTunnelWrapperImpl {
    /// Hashmap to store topic names and their subscriptions
    data_tunnel_subscriptions: Arc<Mutex<HashMap<String, DataTunnelSubscription>>>,
    /// Hashmap to store topic names and their MessageDescriptors
    data_tunnel_topic_message_descriptors: Arc<Mutex<HashMap<String, MessageDescriptor>>>,
}

/// Publisher struct impl that encapsulates the data required for publishing.
impl DataTunnelPublisher {
    /// Data tunnel rust publish messages sample.
    /// message_bytes: the messages that should be sent using DT in bytes. The message_bytes size should be equal
    /// to the message size set in register topic call multiplied by the number_of_messages.
    pub fn publish(&self, message_bytes: &[u8]) -> Result<(), SdvResult> {
        let mut message_count = 0usize;
        let result = sdv_dt::AvailableToWrite(self.topic_handle, &mut message_count);
        if result != SdvResult::Success {
            return Err(result);
        }

        let result =
            // Safety:
            // `message_bytes` contains no more than `self.message_size` bytes since we are sending
            // a single message of size <= self.message_size, and the `DataTunnelPublisher` has been created
            // by register_topic which called RegisterTopic with `self.message_size`
            unsafe { sdv_dt::WriteMessages(self.topic_handle, message_bytes.as_ptr() as *const c_void, 1) };
        match result {
            SdvResult::Success => Ok(()),
            _ => Err(result),
        }
    }
}

impl Default for DataTunnelWrapperImpl {
    fn default() -> Self {
        Self::new()
    }
}

/// Subscriber struct impl that encapsulates the data required for reading messages.
impl DataTunnelWrapperImpl {
    /// Creates a new instance of the struct.
    pub fn new() -> Self {
        // Create struct to interact with Data Tunnel Apis
        Self {
            data_tunnel_subscriptions: Arc::new(Mutex::new(HashMap::new())),
            data_tunnel_topic_message_descriptors: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Read messages using Data tunnel and writes data to the input buffer.
    /// If the buffer is smaller than the number of messages, it does a partial write.
    /// Returns how many messages was read into the buffer.
    fn read(
        data_tunnel_subscriptions: Arc<Mutex<HashMap<String, DataTunnelSubscription>>>,
        topic_name: &str,
        buffer: &mut [u8],
    ) -> anyhow::Result<usize> {
        let mut subscriptions_lock = data_tunnel_subscriptions.lock().unwrap();
        let Some(subscription) = subscriptions_lock.get_mut(topic_name) else {
            return Err(anyhow!("Subscription not found for topic {}", topic_name));
        };

        let mut read_message_count = 0usize;
        let mut result =
            sdv_dt::AvailableToRead(subscription.subscription_handle, &mut read_message_count);

        // We have to invoke ReadMessages() after Overflow error to reset the Overflow flag
        if result == sdv_dt::Result::Overflow {
            // SAFETY: buffer always has enough size to fit a single message.
            if unsafe {
                sdv_dt::ReadMessages(
                    subscription.subscription_handle,
                    buffer.as_mut_ptr() as *mut c_void,
                    1,
                )
            } == sdv_dt::Result::Overflow
            {
                return Err(anyhow!("Error: {:?}", result));
            }
            result =
                sdv_dt::AvailableToRead(subscription.subscription_handle, &mut read_message_count);
        }

        if result != SdvResult::Success {
            return Err(anyhow!("Error when calling AvailableToRead: {:?}", result));
        };

        if read_message_count == 0 {
            return Ok(0);
        }

        // Safety:
        // `buffer` points to a buffer of size at least `message_count` * `message_size`
        // bytes, where `message_size` and `message_count` can be fetched from metadata argument
        // in `SubscribeToTopic()` method.
        let result = unsafe {
            sdv_dt::ReadMessages(
                subscription.subscription_handle,
                buffer.as_mut_ptr() as *mut c_void,
                read_message_count,
            )
        };

        let answer = match result {
            SdvResult::Success => Ok(read_message_count),
            _ => {
                // The first ReadMessage() invocation could return error if the first
                // message in the queue was overwritten
                if subscription.first_message {
                    Ok(0usize)
                } else {
                    Err(anyhow!("Unable to read data from topic: {}", topic_name))
                }
            }
        };

        subscription.first_message = false;
        answer
    }
}

#[async_trait]
impl ServicePublisherAccessor for DataTunnelWrapperImpl {
    /// Subscribe to a Data Tunnel topic sample.
    /// The method returns a struct containing the topic information and the subscription topic handle.
    /// Returns a DT reader implementation that can be used to read messages.
    async fn try_subscribe(
        &self,
        topic_name: &str,
        callback: Arc<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>,
        _configuration: Option<&[u8]>,
    ) -> Result<bool, anyhow::Error> {
        info!("Subscribing to data tunnel topic {}", topic_name);

        let (message_size, message_count, notification_receiver) =
            match self.data_tunnel_subscriptions.lock().unwrap().entry(topic_name.to_owned()) {
                Entry::Occupied(_) => {
                    return Err(anyhow!("Already subscribed to the given topic: {}", topic_name));
                }
                Entry::Vacant(v) => {
                    cxx::let_cxx_string!(cxx_topic_name = topic_name);
                    let mut topic_exists = true;
                    let result = sdv_dt::TopicExists(&cxx_topic_name, &mut topic_exists);
                    if result != SdvResult::Success {
                        return Err(anyhow!("Data Tunnel Result: {:?}", result));
                    };
                    if !topic_exists {
                        return Ok(false);
                    }

                    let mut subscribe_meta_data =
                        TopicTypedMetadata { message_count_: 0, message_size_: 0 };
                    let metadata: Pin<&mut TopicTypedMetadata> = Pin::new(&mut subscribe_meta_data);
                    let mut subscription_handle = SubscriptionHandle::default();
                    let result = sdv_dt::SubscribeToTopic(
                        &cxx_topic_name,
                        metadata,
                        &mut subscription_handle,
                    );
                    if result != SdvResult::Success {
                        return Err(anyhow!("Data Tunnel Result: {:?}", result));
                    };

                    // Setting notification handler
                    let notify = Arc::new(Notify::new());

                    // Safety: "user_data" must point to valid data for "data_tunnel_callback" to use.
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

                    let size = subscribe_meta_data.message_size_;
                    let count = subscribe_meta_data.message_count_;

                    v.insert(DataTunnelSubscription {
                        subscription_handle,
                        _subscribe_meta_data: subscribe_meta_data,
                        first_message: true,
                    });

                    (size, count, notify)
                }
            };

        let message_descriptor = self
            .get_message_descriptor(topic_name)?
            .ok_or_else(|| anyhow!("Metadata not found for topic: {}", topic_name))?;
        let topic_name_clone = topic_name.to_owned();
        let data_tunnel_subscriptions = self.data_tunnel_subscriptions.clone();
        // Tokio task for fetching data from data tunnel and pushing it to telemetry service
        tokio::spawn(async move {
            let size_of_buffer = message_count * message_size;
            let mut buffer = vec![0u8; size_of_buffer];
            // This loop runs infintely until telemetry service stops. In case of error in message parsing,
            // the data is lost, as data was retrieved successfully from data tunnel.
            // In case of data tunnel errors, data is not lost, and can be fetched in the next iteration of the loop.
            // Logic to stop the loop will be added once data tunnel provides support for unsubscribe calls
            loop {
                notification_receiver.notified().await;
                match Self::read(data_tunnel_subscriptions.clone(), &topic_name_clone, &mut buffer)
                {
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
            }
        });

        Ok(true)
    }

    /// Unsubscribes from the specified topic and service name.
    async fn unsubscribe(
        &self,
        _topic_name: &str,
        _configuration: Option<&[u8]>,
    ) -> Result<(), anyhow::Error> {
        // This logic will be replaced once data tunnel has support for unsubscribe
        Ok(())
    }

    /// Checks if topic with given name exists in data tunnel
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

                // Build the provided proto file descriptors
                let file_descriptors = FileDescriptor::new_dynamic_fds(
                    publisher_metadata.descriptor_protos,
                    &WELL_KNOWN_FILE_DESCRIPTORS,
                )
                .map_err(|e| anyhow!("Invalid Descriptor Protos: {:?}", e))?;

                // iterate through all file_descriptors, find MessageDescriptor for the given message_type
                // message_by_full_name() method requires the name to start with dot, it returns a clone
                // of the MessageDescriptor, so we can return it as-is without cloning again
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
}

/// Data tunnel rust sample to  a register topic.
/// topic_name is the name of the topic for Data tunnel.
/// queue_size is the size of the data tunnel queue.
/// message_size is the size of the message in bytes being transferred and cannot be changed in the topic lifetime.
/// Returns a DT publisher implementation that can be used to write messages.
pub fn register_topic(
    topic_name: &str,
    queue_size: usize,
    message_size: usize,
    metadata: Vec<u8>,
) -> Result<DataTunnelPublisher, SdvResult> {
    cxx::let_cxx_string!(topic_name = topic_name);

    let mut publish_topic_handle = TopicHandle::default();
    let register_meta_data =
        TopicTypedMetadata { message_count_: queue_size, message_size_: message_size };

    let mut cxx_metadata_vec = CxxVector::<u8>::new();
    for elem in metadata {
        cxx_metadata_vec.pin_mut().push(elem);
    }
    let result = sdv_dt::RegisterTopicWithMetadata(
        &topic_name,
        &register_meta_data,
        &cxx_metadata_vec,
        &mut publish_topic_handle,
    );
    match result {
        SdvResult::Success => Ok(DataTunnelPublisher { topic_handle: publish_topic_handle }),
        _ => Err(result),
    }
}
