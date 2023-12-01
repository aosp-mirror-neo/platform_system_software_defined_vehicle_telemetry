// Copyright 2023 Google LLC. All Rights Reserved.

#![allow(missing_docs)]
use android_sdv_telemetry_configurable_publisher::aidl::android::sdv::telemetry::configurable_publisher::ISubscriber::ISubscriberAsyncServer;
use android_sdv_telemetry_configurable_publisher::binder::{Interface, Result as BinderResult};
use async_trait::async_trait;
use binder::Status;
use protobuf::{reflect::MessageDescriptor, MessageDyn};

/// Implementation of ISubscriber interface that calls associated callback on a new message.
pub struct SubscriberService {
    message_descriptor: MessageDescriptor,
    callback: Box<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>,
}

impl Interface for SubscriberService {}

#[async_trait]
impl ISubscriberAsyncServer for SubscriberService {
    async fn sendData(&self, data: &[u8]) -> BinderResult<()> {
        let message = self.message_descriptor.parse_from_bytes(data).map_err(|err| {
            Status::new_service_specific_error_str(
                -1,
                Some(format!("Unable to parse message bytes: {:?}", err)),
            )
        })?;
        (self.callback)(message);
        Ok(())
    }
}

impl SubscriberService {
    pub fn new(
        message_descriptor: MessageDescriptor,
        callback: Box<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>,
    ) -> Self {
        SubscriberService { message_descriptor, callback }
    }
}

#[cfg(test)]
mod tests {
    use protobuf::{well_known_types::wrappers::Int32Value, Message, MessageFull};
    use std::sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    };

    use super::*;

    #[tokio::test]
    async fn subscriber_wrapper_send_data_invalid_message_fails() {
        let wrapper = SubscriberService::new(Int32Value::descriptor(), Box::new(|_| {}));

        assert!(wrapper.sendData(&[0xFF]).await.err().is_some());
    }

    #[tokio::test]
    async fn subscriber_wrapper_send_data_success() {
        let num_calls = Arc::new(AtomicU32::new(0));
        let num_calls_clone = num_calls.clone();
        let wrapper = SubscriberService::new(
            Int32Value::descriptor(),
            Box::new(move |_| {
                num_calls_clone.fetch_add(1, Ordering::SeqCst);
            }),
        );

        wrapper.sendData(&Int32Value::new().write_to_bytes().unwrap()).await.unwrap();

        assert_eq!(num_calls.load(Ordering::SeqCst), 1);
    }
}
