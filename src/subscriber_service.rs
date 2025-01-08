// Copyright 2023 Google LLC

use async_trait::async_trait;
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::ISubscriber::ISubscriberAsyncServer;
use parking_lot::Mutex;
use protobuf::{reflect::MessageDescriptor, MessageDyn};
use sdv_telemetry::metrics_config_runtime_error::ServiceRuntimeError;

/// Implementation of ISubscriber interface that calls associated callback on a new message.
pub struct SubscriberService {
    message_descriptor: MessageDescriptor,
    new_message_callback:
        Mutex<Box<dyn FnMut(Result<Box<dyn MessageDyn>, ServiceRuntimeError>) + Send + Sync>>,
}

impl binder::Interface for SubscriberService {}

#[async_trait]
impl ISubscriberAsyncServer for SubscriberService {
    #[allow(non_snake_case)]
    async fn sendData(&self, data: &[u8]) -> binder::Result<()> {
        self.message_descriptor.parse_from_bytes(data).map_or_else(
            |err| {
                let binder_msg = format!("Unable to parse message bytes: {:?}", err);
                (self.new_message_callback.lock())(Err(ServiceRuntimeError::MessageParsingError(
                    err.into(),
                )));
                Err(binder::Status::new_service_specific_error_str(-1, Some(binder_msg)))
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
            dyn FnMut(Result<Box<dyn MessageDyn>, ServiceRuntimeError>) + Send + Sync,
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
    use tokio::sync::oneshot;

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscriber_wrapper_send_data_invalid_message_fails() {
        let (tx, rx) = oneshot::channel();
        let mut maybe_tx = Some(tx);
        let wrapper = SubscriberService::new(
            Int32Value::descriptor(),
            Box::new(move |res| {
                maybe_tx.take().expect("Callback should only be called once").send(res).unwrap()
            }),
        );

        let err_binder = wrapper.sendData(&[0xFF]).await.unwrap_err();
        let err_callback = rx.await.unwrap().unwrap_err();

        assert_eq!(err_binder.service_specific_error(), -1, "Unexpected error: {err_binder:?}");
        assert!(
            matches!(err_callback, ServiceRuntimeError::MessageParsingError(_)),
            "Unexpected error: {err_callback:?}"
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscriber_wrapper_send_data_success() {
        let (tx, rx) = oneshot::channel();
        let mut maybe_tx = Some(tx);
        let wrapper = SubscriberService::new(
            Int32Value::descriptor(),
            Box::new(move |res| {
                maybe_tx.take().expect("Callback should only be called once").send(res).unwrap()
            }),
        );
        let value = Box::new(Int32Value { value: 42, ..Default::default() });

        wrapper.sendData(&value.write_to_bytes().unwrap()).await.unwrap();
        let recv_val = rx.await.unwrap().unwrap().downcast_box::<Int32Value>().unwrap();

        assert_eq!(recv_val, value);
    }
}
