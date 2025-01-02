// Copyright 2023 Google LLC

use async_trait::async_trait;
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::ISubscriber::ISubscriberAsyncServer;
use protobuf::{reflect::MessageDescriptor, MessageDyn};
use tokio::sync::mpsc;

/// Implementation of ISubscriber interface that calls associated callback on a new message.
pub struct SubscriberService {
    message_descriptor: MessageDescriptor,
    tx: mpsc::Sender<Box<dyn MessageDyn>>,
}

impl binder::Interface for SubscriberService {}

#[async_trait]
impl ISubscriberAsyncServer for SubscriberService {
    #[allow(non_snake_case)]
    async fn sendData(&self, data: &[u8]) -> binder::Result<()> {
        let message = self.message_descriptor.parse_from_bytes(data).map_err(|err| {
            binder::Status::new_service_specific_error_str(
                -1,
                Some(format!("Unable to parse message bytes: {:?}", err)),
            )
        })?;

        let res = self.tx.send(message).await;

        if res.is_err() {
            return Err(binder::Status::new_service_specific_error_str(
                -1,
                Some(format!("Unable to send message using tokio channel: {:?}", res)),
            ));
        }

        Ok(())
    }
}

impl SubscriberService {
    pub fn new(
        message_descriptor: MessageDescriptor,
        tx: mpsc::Sender<Box<dyn MessageDyn>>,
    ) -> Self {
        Self { message_descriptor, tx }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use protobuf::{well_known_types::wrappers::Int32Value, Message, MessageFull};

    // Arbitrarily chosen buffer capacity
    const TOKIO_CHANNEL_CAPACITY: usize = 32;

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscriber_wrapper_send_data_invalid_message_fails() {
        let (tx, _) = mpsc::channel(TOKIO_CHANNEL_CAPACITY);
        let wrapper = SubscriberService::new(Int32Value::descriptor(), tx);

        assert!(wrapper.sendData(&[0xFF]).await.err().is_some());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscriber_wrapper_send_data_success() {
        let (tx, mut rx) = mpsc::channel(TOKIO_CHANNEL_CAPACITY);
        let wrapper = SubscriberService::new(Int32Value::descriptor(), tx);

        let val = Int32Value::new().write_to_bytes().unwrap();
        wrapper.sendData(&val).await.unwrap();

        let send_val: Box<Int32Value> =
            Int32Value::descriptor().parse_from_bytes(&val).unwrap().downcast_box().unwrap();

        let recv_msg = rx.recv().await.unwrap();
        let recv_val: Box<Int32Value> = recv_msg.downcast_box().unwrap();

        assert_eq!(recv_val, send_val);
    }
}
