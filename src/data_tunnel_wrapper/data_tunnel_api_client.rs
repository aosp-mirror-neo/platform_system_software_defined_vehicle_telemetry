// Copyright 2024 Google LLC

//! A thin wrapper around the data tunnel API that provides more ergonomic types and function
//! signatures.

use std::pin::Pin;
use std::sync::Arc;

use cxx::{CxxString, CxxVector, UniquePtr};
use log::error;
use sdv_dt::{c_void, SubscriptionNotificationType, TopicTypedMetadata};
use tokio::sync::Notify;

/// The main purpose of this wrapper is to implement `std::error::Error` through `thiserror::Error`.
#[derive(thiserror::Error, Debug)]
#[error("{0:?}")]
#[repr(transparent)]
pub struct DataTunnelError(pub(super) sdv_dt::Result);

impl PartialEq<sdv_dt::Result> for DataTunnelError {
    fn eq(&self, other: &sdv_dt::Result) -> bool {
        &self.0 == other
    }
}

fn into_result(result: sdv_dt::Result) -> Result<(), DataTunnelError> {
    match result {
        sdv_dt::Result::Success => Ok(()),
        error => Err(DataTunnelError(error)),
    }
}

#[derive(thiserror::Error, Debug)]
pub enum WriteMessageError {
    #[error("Message size ({message_size}) does not match configured message size for topic ({topic_message_size})")]
    InvalidMessageSize { message_size: usize, topic_message_size: usize },
    #[error("DataTunnelError: {0}")]
    DataTunnelError(#[from] DataTunnelError),
}

#[derive(thiserror::Error, Debug)]
pub enum ReadMessagesError {
    #[error("Buffer size ({actual_buffer_size}) is insufficient to read {message_count} messages of size {message_size} for topic. Minimum required buffer size is {min_buffer_size}.")]
    InsufficientBufferSize {
        actual_buffer_size: usize,
        message_count: usize,
        message_size: usize,
        min_buffer_size: usize,
    },
    #[error("DataTunnelError: {0}")]
    DataTunnelError(#[from] DataTunnelError),
}

/// A wrapper around a data tunnel topic handle, that also includes the topic's metadata.
pub(super) struct TopicDescriptor {
    dt_handle: sdv_dt::TopicHandle,
    pub(super) dt_metadata: TopicTypedMetadata,
}

/// A wrapper around a data tunnel subscription handle, that also includes the topic's metadata.
pub(super) struct SubscriptionDescriptor {
    dt_handle: sdv_dt::SubscriptionHandle,
    pub(super) dt_metadata: TopicTypedMetadata,
}

/// Checks whether the provided data tunnel topic exists.
pub(super) fn topic_exists(topic_name: &CxxString) -> Result<bool, DataTunnelError> {
    let mut topic_exists = true;
    into_result(sdv_dt::TopicExists(topic_name, &mut topic_exists))?;
    Ok(topic_exists)
}

/// Returns the metadata for the provided data tunnel topic.
pub(super) fn get_topic_metadata(
    topic_name: &CxxString,
) -> Result<UniquePtr<CxxVector<u8>>, DataTunnelError> {
    let mut metadata = CxxVector::<u8>::new();
    into_result(sdv_dt::GetTopicMetadata(topic_name, metadata.pin_mut()))?;
    Ok(metadata)
}

/// Register a new data tunnel topic with the metadata.
pub(super) fn register_topic_with_metadata(
    topic_name: &CxxString,
    topic_metadata: TopicTypedMetadata,
    app_metadata: Vec<u8>,
) -> Result<TopicDescriptor, DataTunnelError> {
    let mut dt_handle = sdv_dt::TopicHandle::default();

    let app_metadata = {
        let mut vec = CxxVector::<u8>::new();
        for elem in app_metadata {
            vec.pin_mut().push(elem);
        }
        vec
    };
    into_result(sdv_dt::RegisterTopicWithMetadata(
        topic_name,
        &topic_metadata,
        &app_metadata,
        &mut dt_handle,
    ))?;
    Ok(TopicDescriptor { dt_handle, dt_metadata: topic_metadata })
}

/// Returns the number of messages available to write to the provided data tunnel topic.
pub(super) fn available_to_write(
    topic_descriptor: &TopicDescriptor,
) -> Result<usize, DataTunnelError> {
    let mut count = 0;
    into_result(sdv_dt::AvailableToWrite(topic_descriptor.dt_handle, &mut count))?;
    Ok(count)
}

/// Writes a message to the provided data tunnel topic.
///
/// # Arguments
/// * `topic_descriptor` - The data tunnel topic descriptor.
/// * `message` - The message to write. Its size must match the topic's configured message size.
pub(super) fn write_message(
    topic_descriptor: &TopicDescriptor,
    message: &[u8],
) -> Result<(), WriteMessageError> {
    if message.len() != topic_descriptor.dt_metadata.message_size_ {
        return Err(WriteMessageError::InvalidMessageSize {
            message_size: message.len(),
            topic_message_size: topic_descriptor.dt_metadata.message_size_,
        });
    }

    const NUM_MESSAGES: usize = 1;
    into_result(
        // SAFETY: `message` is exactly as long as the topic's configured message size.
        unsafe {
            sdv_dt::WriteMessages(
                topic_descriptor.dt_handle,
                message.as_ptr() as *const c_void,
                NUM_MESSAGES,
            )
        },
    )?;

    Ok(())
}

/// Creates a subscription to the provided data tunnel topic.
///
/// # Arguments
/// * `topic_name` - The data tunnel topic to subscribe to.
pub(super) fn subscribe(topic_name: &CxxString) -> Result<SubscriptionDescriptor, DataTunnelError> {
    let mut topic_metadata = TopicTypedMetadata { message_count_: 0, message_size_: 0 };
    let mut dt_handle = sdv_dt::SubscriptionHandle::default();
    into_result(sdv_dt::SubscribeToTopic(
        topic_name,
        Pin::new(&mut topic_metadata),
        &mut dt_handle,
    ))?;
    Ok(SubscriptionDescriptor { dt_handle, dt_metadata: topic_metadata })
}

/// Returns the number of messages available to read to the provided data tunnel topic.
pub(super) fn available_to_read(
    subscription_descriptor: &SubscriptionDescriptor,
) -> Result<usize, DataTunnelError> {
    let mut count = 0;
    into_result(sdv_dt::AvailableToRead(subscription_descriptor.dt_handle, &mut count))?;
    Ok(count)
}

/// Reads `count` messages from the provided data tunnel subscription into the provided `buffer`.
/// The provided `buffer` must be big enough to hold `count` messages, given the subscription's
/// topic's configured message size, otherwise an error will be returned.
///
/// # Arguments
/// * `subscription_descriptor` - The data tunnel subscription descriptor.
/// * `buffer` - The buffer to read messages into. Its size must be big enough to hold `count`
///              messages of the subscription's topic's configured message size.
/// * `count` - The number of messages to read.
pub(super) fn read_messages(
    subscription_descriptor: &SubscriptionDescriptor,
    buffer: &mut [u8],
    count: usize,
) -> Result<(), ReadMessagesError> {
    let min_buffer_size = subscription_descriptor.dt_metadata.message_size_ * count;
    if buffer.len() < min_buffer_size {
        return Err(ReadMessagesError::InsufficientBufferSize {
            actual_buffer_size: buffer.len(),
            message_count: count,
            message_size: subscription_descriptor.dt_metadata.message_size_,
            min_buffer_size,
        });
    }

    into_result(
        // SAFETY: `buffer` points to a buffer of sufficient size to hold `count` messages, given
        // the subscription's topic's configured message size.
        unsafe {
            sdv_dt::ReadMessages(
                subscription_descriptor.dt_handle,
                buffer.as_mut_ptr() as *mut c_void,
                count,
            )
        },
    )?;

    Ok(())
}

/// Callback that handles notifications received from the data tunnel.
///
/// * `notification_type` - The type of notification received.
/// * `user_data` - User data originally sent to the caller during the `SetNotificationHandler` API
///                 call.
///
/// # Safety
///
/// `user_data` must point to the `Notify` object originally sent to the caller during the
/// `SetNotificationHandler` API call.
unsafe extern "C" fn on_notification_callback(
    notification_type: SubscriptionNotificationType,
    user_data: *mut c_void,
) {
    if let SubscriptionNotificationType::DataAvailable = notification_type {
        // SAFETY: safe to use as the user_data passed by the caller was originally allocated using
        // a global allocator using Arc::new(&Notify).
        //
        // Convert the pointer to a reference but do not take back ownership (using
        // `Arc::from_raw`), so that further notifications can be received.
        let notify = unsafe { &*(user_data as *const Notify) };
        notify.notify_one()
    } else {
        error!("Unexpected notification type: {notification_type:?}");
    }
}

pub(super) fn set_notification_handler(
    subscription_descriptor: &SubscriptionDescriptor,
    notify: Arc<Notify>,
    notification_interval: u32,
) -> Result<(), DataTunnelError> {
    // TODO(b/329515273): For now, we intentionally leak the `Arc` `notify_ptr` points to. This is
    // because it needs to be referenced in `on_notification_callback`, and there is currently no
    // way to unsubscribe from a data tunnel - therefore it needs to live on forever. Once the data
    // tunnel supports unsubscribing, we need to re-claim the memory using `Arc::from_raw` when
    // unsubscribing.
    let notify_ptr: *const Notify = Arc::into_raw(notify);
    into_result(
        // SAFETY: `notify_ptr` points to a valid `Notify*`, as expected by the
        // `data_tunnel_callback`, and remains valid forever.
        //
        // TODO(b/329515273): Update this comment once we support unsubscribing.
        unsafe {
            sdv_dt::SetNotificationHandler(
                subscription_descriptor.dt_handle,
                sdv_dt::SubscriptionNotificationCallback(on_notification_callback),
                notify_ptr as *mut c_void,
                notification_interval,
            )
        },
    )
}

#[cfg(all(test, unit_tests))]
mod tests {
    use super::*;

    #[test]
    fn sdv_dt_result_as_data_tunnel_error() {
        let result = sdv_dt::Result::Success;
        assert!(matches!(into_result(result), Ok(())));

        let result = sdv_dt::Result::Overflow;
        assert!(matches!(into_result(result), Err(DataTunnelError(sdv_dt::Result::Overflow))));
        assert_eq!(into_result(result).unwrap_err(), sdv_dt::Result::Overflow);
    }

    #[test]
    fn data_tunnel_error_display_message() {
        assert_eq!(format!("{}", DataTunnelError(sdv_dt::Result::Overflow)), "Overflow");
    }
}

#[cfg(all(test, integration_tests))]
mod tests {
    use super::*;

    use sdv_telemetry_test_utils::unique_dt_topic_name;

    fn create_topic(
        topic_name: &CxxString,
        message_count: usize,
        message_size: usize,
    ) -> TopicDescriptor {
        register_topic_with_metadata(
            topic_name,
            TopicTypedMetadata { message_count_: message_count, message_size_: message_size },
            vec![],
        )
        .unwrap()
    }

    #[test]
    fn topic_does_not_exist_success() {
        let topic_name = unique_dt_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);

        let result = topic_exists(&topic_name);

        assert!(matches!(result, Ok(false)), "Expected Ok(false), got {result:?}");
    }

    #[test]
    fn topic_exists_success() {
        let topic_name = unique_dt_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);
        let _topic_descriptor = create_topic(&topic_name, 10, 10);

        let result = topic_exists(&topic_name);

        assert!(matches!(result, Ok(true)), "Expected Ok(true), got {result:?}");
    }

    #[test]
    fn get_topic_metadata_failure() {
        let topic_name = unique_dt_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);

        let result = get_topic_metadata(&topic_name);

        assert_eq!(result.unwrap_err(), sdv_dt::Result::Failure);
    }

    #[test]
    fn get_topic_metadata_success() {
        let topic_name = unique_dt_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);
        register_topic_with_metadata(
            &topic_name,
            TopicTypedMetadata { message_count_: 10, message_size_: 10 },
            vec![1, 2, 3, 4, 5],
        )
        .unwrap();

        let result = get_topic_metadata(&topic_name);

        assert_eq!(result.unwrap().as_slice(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn register_topic_with_metadata_success() {
        let topic_name = unique_dt_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);

        let topic_descriptor = register_topic_with_metadata(
            &topic_name,
            TopicTypedMetadata { message_count_: 3, message_size_: 10 },
            vec![1, 2, 3, 4, 5],
        )
        .unwrap();

        assert!(topic_exists(&topic_name).unwrap());
        assert_eq!(get_topic_metadata(&topic_name).unwrap().as_slice(), &[1, 2, 3, 4, 5]);
        assert_eq!(available_to_write(&topic_descriptor).unwrap(), 3);
    }

    #[test]
    fn available_to_write_success() {
        let topic_name = unique_dt_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);
        let topic_descriptor = create_topic(&topic_name, 10, 4);

        let result = available_to_write(&topic_descriptor);

        assert!(matches!(result, Ok(10)), "Expected Ok(10), got {result:?}");
    }

    #[test]
    fn write_message_too_short_failure() {
        let topic_name = unique_dt_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);
        let topic_descriptor = create_topic(&topic_name, 10, 4);

        let result = write_message(&topic_descriptor, &[1, 2, 3]);

        assert!(
            matches!(result, Err(WriteMessageError::InvalidMessageSize { .. })),
            "Expected InvalidMessageSize error, got {result:?}"
        );
    }

    #[test]
    fn write_message_too_long_failure() {
        let topic_name = unique_dt_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);
        let topic_descriptor = create_topic(&topic_name, 10, 4);

        let result = write_message(&topic_descriptor, &[1, 2, 3, 4, 5]);

        assert!(
            matches!(result, Err(WriteMessageError::InvalidMessageSize { .. })),
            "Expected InvalidMessageSize error, got {result:?}"
        );
    }

    #[test]
    fn write_message_success() {
        let topic_name = unique_dt_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);
        let topic_descriptor = create_topic(&topic_name, 10, 4);

        let result = write_message(&topic_descriptor, &[1, 2, 3, 4]);

        assert!(result.is_ok());
    }

    #[test]
    fn subscribe_to_topic_that_does_not_exist_fails() {
        let topic_name = unique_dt_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);

        let result = subscribe(&topic_name);

        // TODO(b/339033786): `Debug` is not implemented for `SubscriptionDescriptor`.
        assert_eq!(result.err().unwrap(), sdv_dt::Result::Failure);
    }

    #[test]
    fn subscribe_success() {
        let topic_name = unique_dt_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);
        let _topic_descriptor = create_topic(&topic_name, 10, 4);

        let result = subscribe(&topic_name);

        let subscription_descriptor = result.unwrap();
        assert_eq!(subscription_descriptor.dt_metadata.message_count_, 10);
        assert_eq!(subscription_descriptor.dt_metadata.message_size_, 4);
    }

    #[test]
    fn available_to_read_0_messages_available() {
        let topic_name = unique_dt_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);
        let _topic_descriptor = create_topic(&topic_name, 2, 4);
        let subscription_descriptor = subscribe(&topic_name).unwrap();

        let result = available_to_read(&subscription_descriptor);

        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn available_to_read_2_messages_available() {
        let topic_name = unique_dt_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);
        let topic_descriptor = create_topic(&topic_name, 2, 4);
        let subscription_descriptor = subscribe(&topic_name).unwrap();

        write_message(&topic_descriptor, &[1, 2, 3, 4]).unwrap();
        write_message(&topic_descriptor, &[1, 2, 3, 4]).unwrap();
        let result = available_to_read(&subscription_descriptor);

        assert_eq!(result.unwrap(), 2);
    }

    #[test]
    fn available_to_read_overflow() {
        let topic_name = unique_dt_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);
        let topic_descriptor = create_topic(&topic_name, 2, 4);
        let subscription_descriptor = subscribe(&topic_name).unwrap();

        write_message(&topic_descriptor, &[1, 2, 3, 4]).unwrap();
        write_message(&topic_descriptor, &[1, 2, 3, 4]).unwrap();
        write_message(&topic_descriptor, &[1, 2, 3, 4]).unwrap();

        assert_eq!(
            available_to_read(&subscription_descriptor).unwrap_err(),
            sdv_dt::Result::Overflow
        );
        // The overflow flag should not be cleared, even on repeated calls.
        assert_eq!(
            available_to_read(&subscription_descriptor).unwrap_err(),
            sdv_dt::Result::Overflow
        );
    }

    #[test]
    fn read_messages_buffer_too_short_failure() {
        let topic_name = unique_dt_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);
        let _topic_descriptor = create_topic(&topic_name, 2, 4);
        let subscription_descriptor = subscribe(&topic_name).unwrap();

        let result = read_messages(&subscription_descriptor, &mut [1, 2], 1);

        assert!(
            matches!(result, Err(ReadMessagesError::InsufficientBufferSize { .. })),
            "Expected InsufficientBufferSize error, got {result:?}"
        );
    }

    #[test]
    fn read_zero_messages_when_no_messages_available_success() {
        let topic_name = unique_dt_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);
        let _topic_descriptor = create_topic(&topic_name, 2, 4);
        let subscription_descriptor = subscribe(&topic_name).unwrap();

        let mut buffer = vec![1, 2, 3, 4];
        read_messages(&subscription_descriptor, &mut buffer, 0).unwrap();

        assert_eq!(buffer, vec![1, 2, 3, 4], "The buffer should not be modified.");
    }

    #[test]
    fn read_one_message_when_no_messages_available_failure() {
        let topic_name = unique_dt_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);
        let _topic_descriptor = create_topic(&topic_name, 2, 4);
        let subscription_descriptor = subscribe(&topic_name).unwrap();

        let mut buffer = vec![0u8; 4];
        let result = read_messages(&subscription_descriptor, &mut buffer, 1);

        assert!(
            matches!(result, Err(ReadMessagesError::DataTunnelError(ref err)) if *err == sdv_dt::Result::Failure),
            "Expected failure error, got {result:?}"
        );
    }

    #[test]
    fn read_one_message_success() {
        let topic_name = unique_dt_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);
        let topic_descriptor = create_topic(&topic_name, 2, 4);
        let subscription_descriptor = subscribe(&topic_name).unwrap();

        write_message(&topic_descriptor, &[1, 1, 1, 1]).unwrap();

        assert_eq!(available_to_read(&subscription_descriptor).unwrap(), 1);
        let mut buffer = vec![0u8; 4];
        read_messages(&subscription_descriptor, &mut buffer, 1).unwrap();
        assert_eq!(buffer, vec![1, 1, 1, 1]);
        assert_eq!(available_to_read(&subscription_descriptor).unwrap(), 0);
    }

    #[test]
    fn read_two_messages_together_success() {
        let topic_name = unique_dt_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);
        let topic_descriptor = create_topic(&topic_name, 2, 4);
        let subscription_descriptor = subscribe(&topic_name).unwrap();

        write_message(&topic_descriptor, &[2, 2, 2, 2]).unwrap();
        write_message(&topic_descriptor, &[3, 3, 3, 3]).unwrap();

        assert_eq!(available_to_read(&subscription_descriptor).unwrap(), 2);
        let mut buffer = vec![0u8; 8];
        read_messages(&subscription_descriptor, &mut buffer, 2).unwrap();
        assert_eq!(buffer, vec![2, 2, 2, 2, 3, 3, 3, 3]);
        assert_eq!(available_to_read(&subscription_descriptor).unwrap(), 0);
    }

    #[test]
    fn read_two_messages_separately_success() {
        let topic_name = unique_dt_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);
        let topic_descriptor = create_topic(&topic_name, 2, 4);
        let subscription_descriptor = subscribe(&topic_name).unwrap();

        write_message(&topic_descriptor, &[4, 4, 4, 4]).unwrap();
        write_message(&topic_descriptor, &[5, 5, 5, 5]).unwrap();

        assert_eq!(available_to_read(&subscription_descriptor).unwrap(), 2);
        let mut buffer = vec![0u8; 4];
        read_messages(&subscription_descriptor, &mut buffer, 1).unwrap();
        assert_eq!(buffer, vec![4, 4, 4, 4]);

        assert_eq!(available_to_read(&subscription_descriptor).unwrap(), 1);
        read_messages(&subscription_descriptor, &mut buffer, 1).unwrap();
        assert_eq!(buffer, vec![5, 5, 5, 5]);
        assert_eq!(available_to_read(&subscription_descriptor).unwrap(), 0);
    }

    #[test]
    fn read_messages_subscribe_after_messages_already_written_success() {
        let topic_name = unique_dt_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);
        let topic_descriptor = create_topic(&topic_name, 2, 4);

        write_message(&topic_descriptor, &[1, 1, 1, 1]).unwrap();
        write_message(&topic_descriptor, &[2, 2, 2, 2]).unwrap();

        let subscription_descriptor = subscribe(&topic_name).unwrap();

        assert_eq!(available_to_read(&subscription_descriptor).unwrap(), 2);
        let mut buffer = vec![0u8; 8];
        read_messages(&subscription_descriptor, &mut buffer, 2).unwrap();
        assert_eq!(buffer, vec![1, 1, 1, 1, 2, 2, 2, 2]);
        assert_eq!(available_to_read(&subscription_descriptor).unwrap(), 0);
    }

    #[test]
    fn read_messages_subscribe_after_overflow_success() {
        let topic_name = unique_dt_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);
        let topic_descriptor = create_topic(&topic_name, 2, 4);

        write_message(&topic_descriptor, &[1, 1, 1, 1]).unwrap();
        write_message(&topic_descriptor, &[2, 2, 2, 2]).unwrap();
        write_message(&topic_descriptor, &[3, 3, 3, 3]).unwrap(); // <- Overflow

        let subscription_descriptor = subscribe(&topic_name).unwrap();

        // Detect the overflow and clear the overflow bit.
        assert_eq!(
            available_to_read(&subscription_descriptor).unwrap_err(),
            sdv_dt::Result::Overflow
        );
        let result = read_messages(&subscription_descriptor, &mut [], 0);
        assert!(
            matches!(result, Err(ReadMessagesError::DataTunnelError(ref err)) if *err == sdv_dt::Result::Overflow),
            "Expected overflow error, got {result:?}"
        );

        // The change in FMQ (aosp/3119872) that alters the behaviour of the reading pointer reset
        // in case of the buffer overwrite now preserve half of the queue
        // The queue size is 2 messages. Half of the buffer is 1 message
        let mut buffer = vec![0u8; 4];
        assert_eq!(available_to_read(&subscription_descriptor).unwrap(), 1);
        read_messages(&subscription_descriptor, &mut buffer, 1).unwrap();
        // We still should be able to read the last message before overwrite
        assert_eq!(buffer, vec![3, 3, 3, 3]);

        // New messages can be read.
        write_message(&topic_descriptor, &[4, 4, 4, 4]).unwrap();

        assert_eq!(available_to_read(&subscription_descriptor).unwrap(), 1);
        read_messages(&subscription_descriptor, &mut buffer, 1).unwrap();
        assert_eq!(buffer, vec![4, 4, 4, 4]);
        assert_eq!(available_to_read(&subscription_descriptor).unwrap(), 0);
    }

    #[test]
    fn read_messages_after_overflow_success() {
        let topic_name = unique_dt_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);
        let topic_descriptor = create_topic(&topic_name, 2, 4);
        let subscription_descriptor = subscribe(&topic_name).unwrap();

        write_message(&topic_descriptor, &[1, 1, 1, 1]).unwrap();
        assert_eq!(available_to_read(&subscription_descriptor).unwrap(), 1);
        write_message(&topic_descriptor, &[2, 2, 2, 2]).unwrap();
        assert_eq!(available_to_read(&subscription_descriptor).unwrap(), 2);
        write_message(&topic_descriptor, &[3, 3, 3, 3]).unwrap(); // <- Overflow
        assert_eq!(
            available_to_read(&subscription_descriptor).unwrap_err(),
            sdv_dt::Result::Overflow
        );

        // Clear the overflow bit.
        let result = read_messages(&subscription_descriptor, &mut [], 0);
        assert!(
            matches!(result, Err(ReadMessagesError::DataTunnelError(ref err)) if *err == sdv_dt::Result::Overflow),
            "Expected overflow error, got {result:?}"
        );

        // The change in FMQ (aosp/3119872) that alters the behaviour of the reading pointer reset
        // in case of the buffer overwrite now preserve half of the queue
        // The queue size is 2 messages. Half of the buffer is 1 message
        let mut buffer = vec![0u8; 4];
        assert_eq!(available_to_read(&subscription_descriptor).unwrap(), 1);
        read_messages(&subscription_descriptor, &mut buffer, 1).unwrap();
        // We still should be able to read the last message before overwrite
        assert_eq!(buffer, vec![3, 3, 3, 3]);

        // New messages can be read.
        write_message(&topic_descriptor, &[4, 4, 4, 4]).unwrap();
        assert_eq!(available_to_read(&subscription_descriptor).unwrap(), 1);
        read_messages(&subscription_descriptor, &mut buffer, 1).unwrap();
        assert_eq!(buffer, vec![4, 4, 4, 4]);
        assert_eq!(available_to_read(&subscription_descriptor).unwrap(), 0);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn set_notification_handler_success() {
        let topic_name = unique_dt_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);
        let topic_descriptor = create_topic(&topic_name, 2, 4);
        let subscription_descriptor = subscribe(&topic_name).unwrap();

        let notify = Arc::new(Notify::new());
        set_notification_handler(&subscription_descriptor, Arc::clone(&notify), 0).unwrap();

        write_message(&topic_descriptor, &[1, 1, 1, 1]).unwrap();
        notify.notified().await;
        assert_eq!(available_to_read(&subscription_descriptor).unwrap(), 1);

        write_message(&topic_descriptor, &[2, 2, 2, 2]).unwrap();
        notify.notified().await;
        assert_eq!(available_to_read(&subscription_descriptor).unwrap(), 2);
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn set_notification_handler_ingores_overflow() {
        let topic_name = unique_dt_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);
        let topic_descriptor = create_topic(&topic_name, 2, 4);
        let subscription_descriptor = subscribe(&topic_name).unwrap();

        let notify = Arc::new(Notify::new());
        set_notification_handler(&subscription_descriptor, Arc::clone(&notify), 0).unwrap();

        write_message(&topic_descriptor, &[1, 1, 1, 1]).unwrap();
        notify.notified().await;

        write_message(&topic_descriptor, &[2, 2, 2, 2]).unwrap();
        notify.notified().await;

        write_message(&topic_descriptor, &[3, 3, 3, 3]).unwrap(); // <- Overflow
        notify.notified().await;
        assert_eq!(
            available_to_read(&subscription_descriptor).unwrap_err(),
            sdv_dt::Result::Overflow
        );

        write_message(&topic_descriptor, &[4, 4, 4, 4]).unwrap();
        notify.notified().await;
        assert_eq!(
            available_to_read(&subscription_descriptor).unwrap_err(),
            sdv_dt::Result::Overflow
        );
    }
}
