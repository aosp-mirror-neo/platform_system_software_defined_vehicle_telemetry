// Copyright 2023 Google LLC. All Rights Reserved.

#![allow(missing_docs)]
use android_sdv_telemetry_configurable_publisher::aidl::android::sdv::telemetry::configurable_publisher::ISubscriber::ISubscriber;
use android_sdv_telemetry_configurable_publisher::binder::{Interface, Result as BinderResult};
use binder::Status;
use protobuf::{reflect::MessageDescriptor, MessageDyn};

/// Interface used to send data from a registered publisher to its subscriber
pub trait Subscriber: Send + Sync {
    /// sends data to instance of Subscriber
    fn send_data(&self, data: Box<dyn MessageDyn>);
}

/// Wrapper from Subscriber and implements ISubscriber interface
pub struct SubscriberWrapper {
    message_descriptor: MessageDescriptor,
    subscriber: Box<dyn Subscriber>,
}

impl Interface for SubscriberWrapper {}

impl ISubscriber for SubscriberWrapper {
    fn sendData(&self, data: &[u8]) -> BinderResult<()> {
        let message = self.message_descriptor.parse_from_bytes(data).map_err(|err| {
            Status::new_service_specific_error_str(
                -1,
                Some(format!("Unable to parse message bytes: {:?}", err)),
            )
        })?;
        self.subscriber.send_data(message);
        Ok(())
    }
}

impl SubscriberWrapper {
    pub fn new(message_descriptor: MessageDescriptor, subscriber: Box<dyn Subscriber>) -> Self {
        SubscriberWrapper { message_descriptor, subscriber }
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

    #[derive(Clone)]
    struct MockSubscriber {
        num_calls: Arc<AtomicU32>,
    }

    impl Subscriber for MockSubscriber {
        fn send_data(&self, _data: Box<dyn MessageDyn>) {
            self.num_calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl MockSubscriber {
        fn new() -> Self {
            MockSubscriber { num_calls: AtomicU32::new(0).into() }
        }

        fn get_num_calls(&self) -> u32 {
            self.num_calls.load(Ordering::SeqCst)
        }
    }

    #[test]
    fn subscriber_wrapper_send_data_invalid_message_fails() {
        let wrapper =
            SubscriberWrapper::new(Int32Value::descriptor(), Box::new(MockSubscriber::new()));

        assert!(wrapper.sendData(&[0xFF]).err().is_some());
    }

    #[test]
    fn subscriber_wrapper_send_data_success() {
        let subscriber = MockSubscriber::new();
        let wrapper =
            SubscriberWrapper::new(Int32Value::descriptor(), Box::new(subscriber.clone()));

        wrapper.sendData(&Int32Value::new().write_to_bytes().unwrap()).unwrap();

        assert_eq!(subscriber.get_num_calls(), 1);
    }
}
