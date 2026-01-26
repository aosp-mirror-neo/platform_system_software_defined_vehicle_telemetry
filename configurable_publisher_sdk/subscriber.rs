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

use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{
    ConfigurablePublisherStatusCodes::ConfigurablePublisherStatusCodes as StatusCodes,
    ISubscriber::ISubscriber};

/// Errors returned by objects that implement the Subscriber trait
#[derive(thiserror::Error, Debug)]
pub enum SubscriberError {
    /// Failed to deserialize data received by the Subscriber.
    #[error("Unable to deserialize the data sent to the Subscriber: {error:?}")]
    DeserializationFailed {
        /// The binder status indicating the cause of the deserialization failure.
        error: binder::Status,
    },

    /// An unspecified error occurred while the Subscriber was processing or sending data.
    #[error("An unknown error occurred while sending data: {error:?}")]
    UnknownError {
        /// The binder status providing more details about the unknown error.
        error: binder::Status,
    },
}

/// This trait is passed to a configurable publisher when creating a new subscription
pub trait Subscriber: Send + Sync {
    /// Sends data to the subscriber. The data format is already known by the subscriber.
    fn send_data(&self, data: &[u8]) -> Result<(), SubscriberError>;
}

/// Wrapper for ISubscriber that is passed to configurable publisher
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
