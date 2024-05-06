// Copyright 2023 Google LLC

//! Telemetry publishing logic for DT.

use cxx::CxxVector;
use sdv_dt::{c_void, Result as SdvResult, TopicHandle, TopicTypedMetadata};

/// Struct for data publisher of DT, to simplify sample usage.
pub struct DataTunnelPublisher {
    topic_handle: TopicHandle,
}

/// Data tunnel rust sample to register a topic. Returns a DT publisher implementation that can be
///  used to write messages.
///
/// # Arguments
///
/// * `topic_name` - The name of the topic for data tunnel.
/// * `queue_size` - The size of the data tunnel queue.
/// * `message_size` - The size of the message in bytes being transferred and cannot be changed in
///                    the topic lifetime.
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

/// Publisher struct impl that encapsulates the data required for publishing.
impl DataTunnelPublisher {
    /// Data tunnel rust publish messages sample.
    ///
    /// # Arguments
    ///
    /// * `message_bytes` - The messages that should be sent using DT in bytes.
    ///
    /// # Safety
    /// The size of `message_bytes` should be equal to the message size set in register topic call
    /// multiplied by the number_of_messages.
    pub fn publish(&self, message_bytes: &[u8]) -> Result<(), SdvResult> {
        let mut message_count = 0usize;
        let result = sdv_dt::AvailableToWrite(self.topic_handle, &mut message_count);
        if result != SdvResult::Success {
            return Err(result);
        }

        let result =
            // SAFETY: `message_bytes` contains no more than `self.message_size` bytes since we are
            // sending a single message of size <= `self.message_size`, and `DataTunnelPublisher`
            // has been created by `register_topic` which called `RegisterTopic` with
            // `self.message_size`.
            unsafe {
                sdv_dt::WriteMessages(self.topic_handle, message_bytes.as_ptr() as *const c_void, 1)
            };
        match result {
            SdvResult::Success => Ok(()),
            _ => Err(result),
        }
    }
}
