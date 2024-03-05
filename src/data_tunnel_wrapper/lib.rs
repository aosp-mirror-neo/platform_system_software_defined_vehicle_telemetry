// Copyright 2023 Google LLC

//! Sample Wrapper for DT.

use anyhow::anyhow;
use async_trait::async_trait;
use log::{error, info};
use once_cell::sync::OnceCell;
use protobuf::reflect::MessageDescriptor;
use protobuf::MessageDyn;
use protobuf::MessageFull;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::pin::Pin;
use std::result::Result;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};

use sample_publisher_proto::sample_publisher;
use sample_publisher_proto::sample_publisher::SampleMessage;
use sdv_dt::{c_void, Result as SdvResult, TopicTypedMetadata};
use sdv_telemetry::{ServicePublisherManager, ServicePublisherManagerSubscribeError};

// Static store of Message Descriptors for testing purposes. This logic will be removed once
// Data Tunnel adds support for Message Descriptor
static MESSAGE_DESC_STORE: OnceCell<HashMap<String, MessageDescriptor>> = OnceCell::new();

const TOPIC_NAME: &str = "DataTunnelTopic";
/// Struct for data publisher of DT, to simplify sample usage.
pub struct DataTunnelPublisher {
    topic_handle: u32,
}

/// Struct for subscription of DT topic
pub struct DataTunnelSubscription {
    subscribe_handle: u32,
    first_message: bool,
    _subscribe_meta_data: TopicTypedMetadata,
}

/// Struct for storing all data tunnel subscriptions that implements ServicePublisherManager
pub struct DataTunnelWrapperImpl {
    /// Hashmap to store topic names and their subscriptions
    data_tunnel_subscriptions: Arc<Mutex<HashMap<String, DataTunnelSubscription>>>,
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
        DataTunnelWrapperImpl { data_tunnel_subscriptions: Arc::new(Mutex::new(HashMap::new())) }
    }

    /// Read messages using Data tunnel and writes data to the input buffer.
    /// If the buffer is smaller than the number of messages, it does a partial write.
    /// Returns how many messages was read into the buffer.
    async fn read(
        data_tunnel_subscriptions: Arc<Mutex<HashMap<String, DataTunnelSubscription>>>,
        topic_name: &str,
        buffer: &mut Vec<u8>,
    ) -> anyhow::Result<usize> {
        if let Some(subscription) = data_tunnel_subscriptions.lock().unwrap().get_mut(topic_name) {
            let mut read_message_count = 0usize;
            let result =
                sdv_dt::AvailableToRead(subscription.subscribe_handle, &mut read_message_count);
            if result != SdvResult::Success {
                return Err(anyhow!("Error: {:?}", result));
            };

            if read_message_count == 0 {
                return Ok(0);
            }

            // Safety:
            // `buffer` must point to a buffer of size at least `message_count` * `message_size`
            // bytes, where `message_size` and `message_count` can be fetched from metadata argument
            // in `SubscribeToTopic()` method. This always true because the buffer allocated is of
            // size 100 * `message_size`
            let result = unsafe {
                sdv_dt::ReadMessages(
                    subscription.subscribe_handle,
                    buffer.as_mut_ptr() as *mut c_void,
                    read_message_count,
                )
            };

            let answer: anyhow::Result<usize> = match result {
                SdvResult::Success => Ok(read_message_count),
                _ => {
                    // The first ReadMessage() invocation could return error if the first
                    // message in the queue was overwritten
                    if subscription.first_message {
                        Ok(0)
                    } else {
                        Err(anyhow!("Unable to read data from topic: {}", topic_name))
                    }
                }
            };

            subscription.first_message = false;
            answer
        } else {
            Err(ServicePublisherManagerSubscribeError::TopicNotFound {
                service_name: "data_tunnel".to_string(),
                topic_name: topic_name.to_string(),
            }
            .into())
        }
    }
}

#[async_trait]
impl ServicePublisherManager for DataTunnelWrapperImpl {
    /// Subscribe to a Data Tunnel topic sample.
    /// The method returns a struct containing the topic information and the subscription topic handle.
    /// Returns a DT reader implementation that can be used to read messages.
    async fn try_subscribe(
        &self,
        service_name: &str,
        topic: &str,
        callback: Arc<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>,
        _configuration: Option<&[u8]>,
    ) -> Result<bool, ServicePublisherManagerSubscribeError> {
        info!("Subscribing to data tunnel topic {}", topic);

        let message_size =
            match self.data_tunnel_subscriptions.lock().unwrap().entry(topic.to_string()) {
                Entry::Occupied(_) => {
                    return Err(ServicePublisherManagerSubscribeError::TopicSubscriptionsExists {
                        topic_name: topic.to_string(),
                    })
                }
                Entry::Vacant(v) => {
                    cxx::let_cxx_string!(topic_name = topic);
                    let mut subscribe_handle = 0u32;
                    let mut topic_exists = true;

                    let mut subscribe_meta_data =
                        TopicTypedMetadata { message_count_: 0, message_size_: 0 };

                    let metadata: Pin<&mut TopicTypedMetadata> = Pin::new(&mut subscribe_meta_data);

                    // Check if topic exists
                    let result = sdv_dt::TopicExists(&topic_name, &mut topic_exists);
                    if result != SdvResult::Success {
                        return Err(ServicePublisherManagerSubscribeError::SubscribeFailed {
                            error: anyhow!("Data Tunnel Result: {:?}", result),
                        });
                    };

                    if !topic_exists {
                        return Ok(false);
                    }

                    // Subscribing to data tunnel topic
                    let result =
                        sdv_dt::SubscribeToTopic(&topic_name, metadata, &mut subscribe_handle);
                    if result != SdvResult::Success {
                        return Err(ServicePublisherManagerSubscribeError::SubscribeFailed {
                            error: anyhow!("Data Tunnel Result: {:?}", result),
                        });
                    };

                    let size = subscribe_meta_data.message_size_;

                    v.insert(DataTunnelSubscription {
                        subscribe_handle,
                        _subscribe_meta_data: subscribe_meta_data,
                        first_message: true,
                    });

                    size
                }
            };

        let message_descriptor = self.get_message_descriptor(service_name, topic)?;
        let topic_clone = topic.to_owned();
        let data_tunnel_subscriptions = self.data_tunnel_subscriptions.clone();
        // Tokio task for fetching data from data tunnel and pushing it to telemetry service
        tokio::spawn(async move {
            let size_of_buffer = 100 * message_size;
            let mut buffer = vec![0u8; size_of_buffer];
            loop {
                match Self::read(data_tunnel_subscriptions.clone(), &topic_clone, &mut buffer).await
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
                            let _ = is.push_limit(size);

                            let message = match message_descriptor.parse_from(&mut is) {
                                Ok(msg) => msg,
                                Err(e) => {
                                    panic!("Error in parsing message {}", e);
                                }
                            };
                            (callback)(message);
                        }
                    }
                    Err(result) => {
                        error!("Could not read message result={:?}", result)
                    }
                };

                // This logic will be replaced once data tunnel has support for notifications
                sleep(Duration::from_millis(500)).await;
            }
        });

        Ok(true)
    }

    /// Unsubscribes from the specified topic and service name.
    async fn unsubscribe(
        &self,
        _service_name: &str,
        _topic_name: &str,
        _configuration: Option<&[u8]>,
    ) -> Result<(), ServicePublisherManagerSubscribeError> {
        // This logic will be replaced once data tunnel has support for unsubscribe
        Ok(())
    }

    /// Checks if topic with given name exists in data tunnel
    fn get_message_descriptor(
        &self,
        service_name: &str,
        topic_name: &str,
    ) -> Result<MessageDescriptor, ServicePublisherManagerSubscribeError> {
        let map = MESSAGE_DESC_STORE.get_or_init(|| {
            let mut m = HashMap::new();

            // Inserting sample topic for testing purposes
            // Multiple topics and their corresponding Message Descriptors can be similarly added
            let file_descriptor = sample_publisher::file_descriptor();
            let message_type_name = format!(".{}", SampleMessage::descriptor().full_name());
            let msg_descriptor = file_descriptor.message_by_full_name(&message_type_name).unwrap();
            m.insert(TOPIC_NAME.to_string(), msg_descriptor);
            m
        });

        // Always returns true since the topic name is hardcoded
        if let Some(msg_descriptor) = map.get(topic_name) {
            Ok(msg_descriptor.clone())
        } else {
            Err(ServicePublisherManagerSubscribeError::TopicNotFound {
                service_name: service_name.to_string(),
                topic_name: topic_name.to_string(),
            })
        }
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
) -> Result<DataTunnelPublisher, SdvResult> {
    cxx::let_cxx_string!(topic_name = topic_name);
    let publish_topic_handle: &mut u32 = &mut 0;
    let register_meta_data =
        TopicTypedMetadata { message_count_: queue_size, message_size_: message_size };
    let result = sdv_dt::RegisterTopic(&topic_name, &register_meta_data, publish_topic_handle);
    match result {
        SdvResult::Success => Ok(DataTunnelPublisher { topic_handle: *publish_topic_handle }),
        _ => Err(result),
    }
}
