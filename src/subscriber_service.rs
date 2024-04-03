// Copyright 2023 Google LLC. All Rights Reserved.

#![allow(missing_docs)]
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::ISubscriber::ISubscriber;
use google_sdv_telemetry_configurable_publisher::binder::{Interface, Result as BinderResult};
use binder::Status;
use protobuf::{reflect::MessageDescriptor, MessageDyn};
use tokio::sync::mpsc;

/// Implementation of ISubscriber interface that calls associated callback on a new message.
pub struct SubscriberService {
    message_descriptor: MessageDescriptor,
    tx: mpsc::Sender<Box<dyn MessageDyn>>,
}

impl Interface for SubscriberService {}

impl ISubscriber for SubscriberService {
    fn sendData(&self, data: &[u8]) -> BinderResult<()> {
        let message = self.message_descriptor.parse_from_bytes(data).map_err(|err| {
            Status::new_service_specific_error_str(
                -1,
                Some(format!("Unable to parse message bytes: {:?}", err)),
            )
        })?;

        let res = self.tx.blocking_send(message);

        if res.is_err() {
            return Err(Status::new_service_specific_error_str(
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
    use protobuf::{well_known_types::wrappers::Int32Value, Message, MessageFull};

    use super::*;

    // Arbitrarily chosen buffer cpaacity
    const TOKIO_CHANNEL_CAPACITY: usize = 32;

    #[test]
    fn subscriber_wrapper_send_data_invalid_message_fails() {
        let (tx, _) = mpsc::channel(TOKIO_CHANNEL_CAPACITY);
        let wrapper = SubscriberService::new(Int32Value::descriptor(), tx);

        assert!(wrapper.sendData(&[0xFF]).err().is_some());
    }

    #[test]
    fn subscriber_wrapper_send_data_success() {
        let (tx, mut rx) = mpsc::channel(TOKIO_CHANNEL_CAPACITY);
        let wrapper = SubscriberService::new(Int32Value::descriptor(), tx);

        let val = Int32Value::new().write_to_bytes().unwrap();
        wrapper.sendData(&val).unwrap();

        let send_val: Box<Int32Value> =
            Int32Value::descriptor().parse_from_bytes(&val).unwrap().downcast_box().unwrap();

        let recv_msg = rx.blocking_recv().unwrap();
        let recv_val: Box<Int32Value> = recv_msg.downcast_box().unwrap();

        assert_eq!(recv_val, send_val);
    }
}
