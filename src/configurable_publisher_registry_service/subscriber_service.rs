// Copyright 2023 Google LLC

use async_trait::async_trait;
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{ConfigurablePublisherStatusCodes::ConfigurablePublisherStatusCodes as StatusCodes, ISubscriber::ISubscriberAsyncServer};
use parking_lot::Mutex;
use protobuf::{reflect::MessageDescriptor, MessageDyn};
use sdv_telemetry_service_publisher_accessor::ServicePublisherAccessorError;

/// Implementation of ISubscriber interface that calls associated callback on a new message.
pub struct SubscriberService {
    message_descriptor: MessageDescriptor,
    new_message_callback: Mutex<
        Box<dyn FnMut(Result<Box<dyn MessageDyn>, ServicePublisherAccessorError>) + Send + Sync>,
    >,
}

impl binder::Interface for SubscriberService {}

#[async_trait]
impl ISubscriberAsyncServer for SubscriberService {
    async fn sendData(&self, data: &[u8]) -> binder::Result<()> {
        self.message_descriptor.parse_from_bytes(data).map_or_else(
            |err| {
                let binder_msg = format!("Unable to parse message bytes: {:?}", err);
                (self.new_message_callback.lock())(Err(
                    ServicePublisherAccessorError::MessageParsingFailed(err.into()),
                ));
                Err(binder::Status::new_service_specific_error_str(
                    StatusCodes::SEND_DATA_DESERIALIZATION_FAILED.0,
                    Some(binder_msg),
                ))
            },
            |msg| {
                (self.new_message_callback.lock())(Ok(msg));
                Ok(())
            },
        )
    }
}

impl SubscriberService {
    pub fn new(
        message_descriptor: MessageDescriptor,
        new_message_callback: Box<
            dyn FnMut(Result<Box<dyn MessageDyn>, ServicePublisherAccessorError>) + Send + Sync,
        >,
    ) -> Self {
        Self { message_descriptor, new_message_callback: new_message_callback.into() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use protobuf::well_known_types::wrappers::Int32Value;
    use protobuf::{Message, MessageFull};
    use sdv_telemetry_test_utils::oneshot_callback;

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscriber_wrapper_send_data_invalid_message_fails() {
        let (tx, rx) = oneshot_callback();
        let wrapper = SubscriberService::new(Int32Value::descriptor(), Box::new(tx));

        let err_binder = wrapper.sendData(&[0xFF]).await.unwrap_err();
        let err_callback = rx.await.unwrap_err();

        assert_eq!(
            err_binder.service_specific_error(),
            StatusCodes::SEND_DATA_DESERIALIZATION_FAILED.0,
            "Unexpected error: {err_binder:?}"
        );
        assert!(
            matches!(err_callback, ServicePublisherAccessorError::MessageParsingFailed(_)),
            "Unexpected error: {err_callback:?}"
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscriber_wrapper_send_data_success() {
        let (tx, rx) = oneshot_callback();
        let wrapper = SubscriberService::new(Int32Value::descriptor(), Box::new(tx));
        let value = Box::new(Int32Value { value: 42, ..Default::default() });

        wrapper.sendData(&value.write_to_bytes().unwrap()).await.unwrap();
        let recv_val = rx.await.unwrap().downcast_box::<Int32Value>().unwrap();

        assert_eq!(recv_val, value);
    }
}
