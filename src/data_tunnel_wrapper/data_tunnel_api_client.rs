// Copyright 2024 Google LLC

//! A thin wrapper around the data tunnel API that provides more ergonomic types and function
//! signatures.

use std::pin::Pin;
use std::sync::Arc;

use cxx::{CxxString, CxxVector, UniquePtr};
use log::error;
use sdv_dt::{c_void, SubscriptionHandle, SubscriptionNotificationType, TopicTypedMetadata};
use tokio::sync::Notify;

/// The main purpose of this wrapper is to implement `std::error::Error` through `thiserror::Error`.
#[derive(thiserror::Error, Debug)]
#[error("{0:?}")]
#[repr(transparent)]
pub struct DataTunnelError(sdv_dt::Result);

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

/// A wrapper around a data tunnel topic handle, that also includes the topic's metadata.
pub(super) struct TopicDescriptor {
    dt_handle: sdv_dt::TopicHandle,
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

pub(super) fn subscribe(
    topic_name: &CxxString,
) -> Result<(SubscriptionHandle, TopicTypedMetadata), DataTunnelError> {
    let mut topic_metadata = TopicTypedMetadata { message_count_: 0, message_size_: 0 };
    let mut subscription_handle = SubscriptionHandle::default();
    into_result(sdv_dt::SubscribeToTopic(
        topic_name,
        Pin::new(&mut topic_metadata),
        &mut subscription_handle,
    ))?;
    Ok((subscription_handle, topic_metadata))
}

pub(super) fn available_to_read(
    subscription_handle: SubscriptionHandle,
) -> Result<usize, DataTunnelError> {
    let mut count = 0;
    into_result(sdv_dt::AvailableToRead(subscription_handle, &mut count))?;
    Ok(count)
}

/// # Safety
///
/// The provided `buffer` must be big enough to hold `count` messages, given the subscription's
/// topic's configured message size.
pub(super) unsafe fn read_messages(
    subscription_handle: SubscriptionHandle,
    buffer: &mut [u8],
    count: usize,
) -> Result<(), DataTunnelError> {
    into_result(
        // SAFETY: `buffer` points to a buffer of sufficient size to hold `count` messages, given
        // the subscription's topic's configured message size.
        unsafe {
            sdv_dt::ReadMessages(subscription_handle, buffer.as_mut_ptr() as *mut c_void, count)
        },
    )
}

/// Callback that handles notifications received from the data tunnel.
///
/// * `notification_type` - The type of notification received.
/// * `user_data` - User data originally sent to the caller during the `SetNotificationHandler` API
///                 call.
///
/// TODO(b/343435803): This function should be marked as unsafe.
///
/// # Safety
///
/// `user_data` must point to the `Notify` object originally sent to the caller during the
/// `SetNotificationHandler` API call.
extern "C" fn on_notification_callback(
    notification_type: SubscriptionNotificationType,
    user_data: *mut c_void,
) {
    // SAFETY: safe to use as the user_data passed by the caller was originally allocated using a
    // global allocator using Arc::new().
    //
    // Convert the pointer to a reference but do not take back ownership (using `Arc::from_raw`), so
    // that further notifications can be received.
    let notify = unsafe { &*(user_data as *const Notify) };

    if notification_type == SubscriptionNotificationType::DataAvailable {
        notify.notify_one();
    } else {
        error!("Unexpected notification type: {notification_type:?}");
    }
}

pub(super) fn set_notification_handler(
    subscription_handle: SubscriptionHandle,
    notify: Arc<Notify>,
    notification_interval: u32,
) -> Result<(), DataTunnelError> {
    // TODO(b/329515273): For now, we intentionally leak the `Arc` `notify_ptr` points to. This is
    // because it needs to be referenced in `data_tunnel_callback`, and there is currently no way to
    // unsubscribe from a data tunnel - therefore it needs to live on forever. Once the data tunnel
    // supports unsubscribing, we need to re-claim the memory using `Arc::from_raw` when
    // unsubscribing.
    let notify_ptr: *const Notify = Arc::into_raw(notify);
    into_result(
        // SAFETY: `notify_ptr` must point to valid data for `data_tunnel_callback` to use and
        // remain valid during its lifetime.
        unsafe {
            sdv_dt::SetNotificationHandler(
                subscription_handle,
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

    // TODO(b/343140446): We should clean up data tunnel topics.
    fn unique_topic_name() -> String {
        format!("Telemetry.Test.Topic.{}", rand::random::<u128>())
    }

    #[test]
    fn topic_does_not_exist_success() {
        let topic_name = unique_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);

        assert!(!topic_exists(&topic_name).unwrap());
    }

    #[test]
    fn topic_exists_success() {
        let topic_name = unique_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);

        register_topic_with_metadata(
            &topic_name,
            TopicTypedMetadata { message_count_: 10, message_size_: 10 },
            vec![],
        )
        .unwrap();

        assert!(topic_exists(&topic_name).unwrap());
    }

    #[test]
    fn get_topic_metadata_failure() {
        let topic_name = unique_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);

        let err = get_topic_metadata(&topic_name).unwrap_err();

        assert_eq!(err, sdv_dt::Result::Failure);
    }

    #[test]
    fn get_topic_metadata_success() {
        let topic_name = unique_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);

        register_topic_with_metadata(
            &topic_name,
            TopicTypedMetadata { message_count_: 10, message_size_: 10 },
            vec![1, 2, 3, 4, 5],
        )
        .unwrap();

        assert_eq!(get_topic_metadata(&topic_name).unwrap().as_slice(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn register_topic_with_metadata_success() {
        let topic_name = unique_topic_name();
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
        let topic_name = unique_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);

        let topic_descriptor = register_topic_with_metadata(
            &topic_name,
            TopicTypedMetadata { message_count_: 3, message_size_: 10 },
            vec![1, 2, 3, 4, 5],
        )
        .unwrap();

        assert_eq!(available_to_write(&topic_descriptor).unwrap(), 3);
    }

    #[test]
    fn write_message_too_long_failure() {
        let topic_name = unique_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);

        let topic_descriptor = register_topic_with_metadata(
            &topic_name,
            TopicTypedMetadata { message_count_: 10, message_size_: 4 },
            vec![],
        )
        .unwrap();

        // Try sending a message that is too short.
        let result = write_message(&topic_descriptor, &[1, 2, 3]);
        assert!(
            matches!(result, Err(WriteMessageError::InvalidMessageSize { .. })),
            "Expected InvalidMessageSize error, got {result:?}"
        );
    }

    #[test]
    fn write_message_too_short_failure() {
        let topic_name = unique_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);

        let topic_descriptor = register_topic_with_metadata(
            &topic_name,
            TopicTypedMetadata { message_count_: 10, message_size_: 4 },
            vec![],
        )
        .unwrap();

        // Try sending a message that is too long.
        let result = write_message(&topic_descriptor, &[1, 2, 3, 4, 5]);
        assert!(
            matches!(result, Err(WriteMessageError::InvalidMessageSize { .. })),
            "Expected InvalidMessageSize error, got {result:?}"
        );
    }

    #[test]
    fn write_message_success() {
        let topic_name = unique_topic_name();
        cxx::let_cxx_string!(topic_name = topic_name);

        let topic_descriptor = register_topic_with_metadata(
            &topic_name,
            TopicTypedMetadata { message_count_: 10, message_size_: 4 },
            vec![],
        )
        .unwrap();

        write_message(&topic_descriptor, &[1, 2, 3, 4]).unwrap();
    }

    // TODO: Add tests for the subscriber-side of the API.
}
