// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{ConfigurablePublisherStatusCodes::ConfigurablePublisherStatusCodes as StatusCodes, ISubscriber::ISubscriberAsyncServer};
use parking_lot::Mutex;
use log::debug;
use protobuf::reflect::MessageDescriptor;
use sdv_telemetry_service_publisher_accessor::{NewMessageCallback, ServicePublisherAccessorError};

/// Implementation of ISubscriber interface that calls associated callback on a new message.
pub struct SubscriberService {
    service_name: String,
    message_descriptor: MessageDescriptor,
    new_message_callback: Mutex<NewMessageCallback>,
}

impl binder::Interface for SubscriberService {}

#[async_trait]
impl ISubscriberAsyncServer for SubscriberService {
    async fn sendData(&self, data: &[u8]) -> binder::Result<()> {
        debug!(
            "Received 1 message from configurable publisher registry service '{}'.",
            self.service_name,
        );

        self.message_descriptor.parse_from_bytes(data).map_or_else(
            |err| {
                let binder_msg = format!("Unable to parse message bytes: {err:?}");
                (self.new_message_callback.lock())(Err(
                    ServicePublisherAccessorError::MessageParsingFailed(err.into()),
                ));
                Err(binder::Status::new_service_specific_error_str(
                    StatusCodes::SEND_DATA_DESERIALIZATION_FAILED.0,
                    Some(binder_msg),
                ))
            },
            |msg| {
                (self.new_message_callback.lock())(Ok((msg, None)));
                Ok(())
            },
        )
    }
}

impl SubscriberService {
    pub fn new(
        service_name: String,
        message_descriptor: MessageDescriptor,
        new_message_callback: NewMessageCallback,
    ) -> Self {
        Self { service_name, message_descriptor, new_message_callback: new_message_callback.into() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use protobuf::well_known_types::wrappers::Int32Value;
    use protobuf::{Message, MessageFull};
    use sdv_telemetry_test_utils::assert_matches;
    use sdv_telemetry_test_utils::oneshot_callback;

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscriber_wrapper_send_data_invalid_message_fails() {
        let (tx, rx) = oneshot_callback();
        let wrapper = SubscriberService::new(
            "service_name".to_owned(),
            Int32Value::descriptor(),
            Box::new(tx),
        );

        let err_binder = wrapper.sendData(&[0xFF]).await.unwrap_err();
        let err_callback = rx.await.unwrap_err();

        assert_eq!(
            err_binder.service_specific_error(),
            StatusCodes::SEND_DATA_DESERIALIZATION_FAILED.0,
            "Unexpected error: {err_binder:?}"
        );
        assert_matches!(err_callback, ServicePublisherAccessorError::MessageParsingFailed(_));
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn subscriber_wrapper_send_data_success() {
        let (tx, rx) = oneshot_callback();
        let wrapper = SubscriberService::new(
            "service_name".to_owned(),
            Int32Value::descriptor(),
            Box::new(tx),
        );
        let value = Box::new(Int32Value { value: 42, ..Default::default() });

        wrapper.sendData(&value.write_to_bytes().unwrap()).await.unwrap();
        let recv_val = rx.await.unwrap().0.downcast_box::<Int32Value>().unwrap();

        assert_eq!(recv_val, value);
    }
}
