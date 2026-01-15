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
    IConfigurablePublisher::IConfigurablePublisher, ISubscriber::ISubscriber};

use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisherSubscription::{BnConfigurablePublisherSubscription, IConfigurablePublisherSubscription};

use crate::configurable_publisher_subscription::{
    ConfigurablePublisherSubscription, ConfigurablePublisherSubscriptionAidlImpl,
};
use crate::subscriber::{Subscriber, SubscriberImpl};
use binder::{BinderFeatures, Interface, Result as BinderResult, Status, Strong};
use std::ffi::CString;
use std::sync::Arc;

/// Errors returned by objects that implement the ConfigurablePublisher trait
#[derive(thiserror::Error, Debug)]
pub enum ConfigurablePublisherError {
    /// Failed to get the latest message; none found for the configuration.
    #[error("No message found for the given configuration")]
    GetLatestMessageFailed,

    /// Failed to create a new subscription.
    #[error("Create Subcription Failed")]
    CreateSubscriptionFailed,

    /// An unexpected or unspecified error occurred.
    #[error("An unknown error occurred")]
    UnknownError,
}

fn to_binder_error(error: ConfigurablePublisherError) -> Status {
    let (service_specific_code, error_message_str) = match error {
        ConfigurablePublisherError::GetLatestMessageFailed => {
            (StatusCodes::GET_LATEST_MESSAGE_FAILED.0, error.to_string())
        }
        ConfigurablePublisherError::CreateSubscriptionFailed => {
            (StatusCodes::CREATE_SUBSCRIPTION_FAILED.0, error.to_string())
        }
        ConfigurablePublisherError::UnknownError => {
            (StatusCodes::UNKNOWN_ERROR.0, error.to_string())
        }
    };

    let c_string_message = CString::new(error_message_str.as_str()).unwrap_or_else(|_| {
        panic!(
            "Original message ('{error_message_str}') contained null bytes. This is unrecoverable."
        )
    });

    Status::new_service_specific_error(service_specific_code, Some(&c_string_message))
}

/// Implements the IConfigurablePublisher interface and stores the ConfigurablePublisher trait object
/// passed by the client
pub(crate) struct ConfigurablePublisherAidlImpl {
    configurable_publisher: Arc<dyn ConfigurablePublisher>,
}

impl Interface for ConfigurablePublisherAidlImpl {}

impl IConfigurablePublisher for ConfigurablePublisherAidlImpl {
    fn createSubscription(
        &self,
        subscriber: &Strong<dyn ISubscriber>,
        config: Option<&[u8]>,
    ) -> BinderResult<Strong<dyn IConfigurablePublisherSubscription>> {
        let subscription_impl = Box::new(SubscriberImpl::new(subscriber.clone()));
        let configurable_publisher_subscription: Box<dyn ConfigurablePublisherSubscription> =
            match self.configurable_publisher.create_subscription(subscription_impl, config) {
                Ok(subscription_arc) => subscription_arc,
                Err(publisher_error) => {
                    return Err(to_binder_error(publisher_error));
                }
            };
        let configurable_publisher_subscription_aidl =
            ConfigurablePublisherSubscriptionAidlImpl::new(configurable_publisher_subscription);
        Ok(BnConfigurablePublisherSubscription::new_binder(
            configurable_publisher_subscription_aidl,
            BinderFeatures::default(),
        ))
    }
    fn getLatestMessage(&self, configuration: Option<&[u8]>) -> BinderResult<Option<Vec<u8>>> {
        self.configurable_publisher.get_latest_message(configuration).map_err(to_binder_error)
    }
}

impl ConfigurablePublisherAidlImpl {
    pub fn new(configurable_publisher: Arc<dyn ConfigurablePublisher>) -> Self {
        ConfigurablePublisherAidlImpl { configurable_publisher }
    }
}

/// The trait passed by clients when they register a new configurable publisher
pub trait ConfigurablePublisher: Send + Sync {
    /// Subscribes to a registered publisher by passing a Subscriber trait object
    fn create_subscription(
        &self,
        subscriber: Box<dyn Subscriber>,
        configuration: Option<&[u8]>,
    ) -> Result<Box<dyn ConfigurablePublisherSubscription>, ConfigurablePublisherError>;

    /// Provides the latest message for the provided configuration
    fn get_latest_message(
        &self,
        configuration: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, ConfigurablePublisherError>;
}
