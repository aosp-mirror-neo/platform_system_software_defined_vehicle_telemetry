// Copyright 2025 Google LLC
#![allow(dead_code)]

use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{
    ConfigurablePublisherStatusCodes::ConfigurablePublisherStatusCodes as StatusCodes,
    ISubscriber::ISubscriber};

#[derive(thiserror::Error, Debug)]
pub enum SubscriberError {
    #[error("Unable to deserialize the data sent to the Subscriber: {error:?}")]
    DeserializationFailed { error: binder::Status },
    #[error("An unknown error occurred while sending data: {error:?}")]
    UnknownError { error: binder::Status },
}

pub trait Subscriber: Send + Sync {
    // Sends data to the subscriber. The data format is already known by the subscriber.
    fn send_data(&self, data: &[u8]) -> Result<(), SubscriberError>;
}

pub(crate) struct SubscriberImpl {
    subscriber_aidl: binder::Strong<dyn ISubscriber>,
}

impl SubscriberImpl {
    pub(crate) fn new(subscriber_aidl: binder::Strong<dyn ISubscriber>) -> Self {
        Self { subscriber_aidl }
    }
}

impl Subscriber for SubscriberImpl {
    fn send_data(&self, data: &[u8]) -> Result<(), SubscriberError> {
        self.subscriber_aidl.sendData(data).map_err(|error| {
            let status_code = error.service_specific_error();

            if status_code == StatusCodes::SEND_DATA_DESERIALIZATION_FAILED.0 {
                SubscriberError::DeserializationFailed { error }
            } else {
                SubscriberError::UnknownError { error }
            }
        })
    }
}
