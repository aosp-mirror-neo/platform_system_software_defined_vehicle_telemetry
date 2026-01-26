/*
 * Copyright (C) 2026 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#![cfg(test)]

use async_trait::async_trait;
use parking_lot::Mutex;
use protobuf::{reflect::MessageDescriptor, well_known_types::any::Any, MessageDyn};
use sdv_telemetry_service_publisher_accessor::{
    AvailabilityCallback, AvailabilityCallbackRegistration, CancellationCallback,
    CreateSubscriptionOutcome, FetchLastMessageOutcome, GetMessageProviderOutcome,
    NewMessageCallback, ServicePublisherAccessor, ServicePublisherAccessorError,
    ServicePublisherMessageProvider, ServicePublisherMetadata, ServicePublisherSubscription,
    SubSamplingInterval,
};
use sdv_telemetry_utils::collection_extension::HashMapExtension;
use std::collections::HashMap;
use std::sync::{Arc, Weak};

pub(crate) struct MockServicePublisherAccessor {
    /// Shared mock data.
    data: Arc<Mutex<MockServicePublisherAccessorData>>,
    /// Indicates whether to return Some availability callback registration or None.
    return_availability_callback_registration: bool,
}

struct MockServicePublisherAccessorData {
    /// The map of subscribed callbacks, keyed by service name and sub-sampling interval.
    new_message_callbacks: HashMap<
        (String, SubSamplingInterval),
        Box<dyn FnMut(Result<Box<dyn MessageDyn>, ServicePublisherAccessorError>) + Send + Sync>,
    >,
    /// Last passed cancellation callback.
    cancellation_callback: Option<CancellationCallback>,
    /// The last configuration message that was passed to create_subscription.
    last_configuration: Option<Any>,
    /// When set, the descriptor will be returned on the next call to get_metadata.
    next_descriptor: Option<MessageDescriptor>,
    /// When set, the error will be returned on the next call to the adapter.
    next_error: Option<anyhow::Error>,
    /// When set, the message will be returned on the next call to get_latest_message.
    next_latest_message: Option<Box<dyn MessageDyn>>,
    /// When set to true, the next call to create_subscription or get_message_provider will return a
    /// not found result.
    next_not_available: bool,
    /// When set to true, the next call to get_message_provider will return a get not supported
    /// result.
    next_get_not_supported: bool,
    /// The number of times get latest message function of the mock was called.
    num_get_message_provider_calls: u32,
    /// The number of times get metadata function of the mock was called.
    num_get_metadata_calls: u32,
    /// The number of times subscribe function of the mock was called.
    num_subscribe_calls: u32,
    /// The number of times unsubscribe function of the mock was called.
    num_unsubscribe_calls: u32,
    /// The number of times register_availability_callback function of the mock was called.
    num_register_availability_callback_calls: u32,
    /// The number of times availability callback unregister function of the mock was called.
    num_unregister_availability_callback_calls: u32,
}

struct MockServicePublisherMessageProvider {
    data: Weak<Mutex<MockServicePublisherAccessorData>>,
}

struct MockServicePublisherSubscription {
    data: Weak<Mutex<MockServicePublisherAccessorData>>,
    service_name: String,
    sub_sampling_interval: SubSamplingInterval,
}

struct MockAvailabilityCallbackRegistration {
    data: Weak<Mutex<MockServicePublisherAccessorData>>,
}

#[async_trait]
impl ServicePublisherAccessor for MockServicePublisherAccessor {
    async fn get_metadata(
        &self,
        _service_name: &str,
    ) -> Result<Option<ServicePublisherMetadata>, ServicePublisherAccessorError> {
        let mut data = self.data.lock();
        data.num_get_metadata_calls += 1;
        data.check_next_error().map_err(ServicePublisherAccessorError::MetadataParsingFailed)?;

        let metadata = data
            .next_descriptor
            .take()
            .map(|message_descriptor| ServicePublisherMetadata { message_descriptor });
        Ok(metadata)
    }

    async fn get_message_provider(
        &self,
        _service_name: &str,
    ) -> Result<GetMessageProviderOutcome, ServicePublisherAccessorError> {
        let mut data = self.data.lock();
        data.num_get_message_provider_calls += 1;
        data.check_next_error().map_err(ServicePublisherAccessorError::MetadataParsingFailed)?;

        if data.next_not_available {
            data.next_not_available = false;
            return Ok(GetMessageProviderOutcome::NotRegistered);
        }
        if data.next_get_not_supported {
            data.next_get_not_supported = false;
            return Ok(GetMessageProviderOutcome::GetNotSupported);
        }

        Ok(GetMessageProviderOutcome::Success(Box::new(MockServicePublisherMessageProvider {
            data: Arc::downgrade(&self.data),
        })))
    }

    async fn create_subscription<'a>(
        &'a self,
        service_name: &'a str,
        configuration: Option<&'a Any>,
        _fetch_last_message: bool,
        sub_sampling_interval: &'a SubSamplingInterval,
        new_message_callback: NewMessageCallback,
        cancellation_callback: CancellationCallback,
    ) -> Result<CreateSubscriptionOutcome, ServicePublisherAccessorError> {
        let mut data = self.data.lock();
        data.last_configuration = configuration.cloned();
        data.num_subscribe_calls += 1;
        data.check_next_error().map_err(ServicePublisherAccessorError::MetadataParsingFailed)?;

        if data.next_not_available {
            data.next_not_available = false;
            return Ok(CreateSubscriptionOutcome::NotRegistered {
                new_message_callback,
                cancellation_callback,
            });
        }

        if data
            .new_message_callbacks
            .try_insert_value(
                (service_name.to_owned(), sub_sampling_interval.to_owned()),
                new_message_callback,
            )
            .is_err()
        {
            panic!("Callback is already set for service name {service_name}");
        }

        data.cancellation_callback.replace(cancellation_callback);

        let subscription = MockServicePublisherSubscription {
            service_name: service_name.to_owned(),
            data: Arc::downgrade(&self.data),
            sub_sampling_interval: sub_sampling_interval.to_owned(),
        };
        Ok(CreateSubscriptionOutcome::Subscribed {
            subscription: Box::new(subscription),
            fetch_last_message_outcome: FetchLastMessageOutcome::NotSupported,
        })
    }

    async fn register_availability_callback(
        &self,
        _service_name: &str,
        _callback: &AvailabilityCallback,
    ) -> anyhow::Result<Option<Box<dyn AvailabilityCallbackRegistration>>> {
        let mut data = self.data.lock();
        data.num_register_availability_callback_calls += 1;
        data.check_next_error()?;
        if self.return_availability_callback_registration {
            Ok(Some(Box::new(MockAvailabilityCallbackRegistration {
                data: Arc::downgrade(&self.data),
            })))
        } else {
            Ok(None)
        }
    }
}

impl MockServicePublisherAccessor {
    pub(crate) fn new() -> Self {
        let data = Arc::new(Mutex::new(MockServicePublisherAccessorData {
            cancellation_callback: None,
            new_message_callbacks: HashMap::new(),
            last_configuration: None,
            next_descriptor: None,
            next_error: None,
            next_latest_message: None,
            next_not_available: false,
            next_get_not_supported: false,
            num_get_message_provider_calls: 0,
            num_get_metadata_calls: 0,
            num_subscribe_calls: 0,
            num_unsubscribe_calls: 0,
            num_register_availability_callback_calls: 0,
            num_unregister_availability_callback_calls: 0,
        }));
        Self { data, return_availability_callback_registration: false }
    }

    pub(crate) fn get_num_get_message_provider_calls(&self) -> u32 {
        self.data.lock().num_get_message_provider_calls
    }

    pub(crate) fn get_num_get_metadata_calls(&self) -> u32 {
        self.data.lock().num_get_metadata_calls
    }

    pub(crate) fn get_num_subscribe_calls(&self) -> u32 {
        self.data.lock().num_subscribe_calls
    }

    pub(crate) fn get_num_unsubscribe_calls(&self) -> u32 {
        self.data.lock().num_unsubscribe_calls
    }

    pub(crate) fn get_num_register_availability_callback_calls(&self) -> u32 {
        self.data.lock().num_register_availability_callback_calls
    }

    pub(crate) fn get_num_unregister_availability_callback_calls(&self) -> u32 {
        self.data.lock().num_unregister_availability_callback_calls
    }

    pub(crate) fn notify_cancellation(&self) {
        self.data.lock().cancellation_callback.take().unwrap()();
    }

    pub(crate) fn notify_subscriber(
        &self,
        service_name: &str,
        sub_sampling_interval: &SubSamplingInterval,
        message: Result<Box<dyn MessageDyn>, ServicePublisherAccessorError>,
    ) {
        let mut data_lock = self.data.lock();
        data_lock
            .new_message_callbacks
            .get_mut(&(service_name.to_owned(), sub_sampling_interval.to_owned()))
            .unwrap()(message)
    }

    pub(crate) fn set_next_error(&self, error: anyhow::Error) {
        self.data.lock().next_error.replace(error);
    }

    pub(crate) fn take_last_configuration(&self) -> Option<Any> {
        self.data.lock().last_configuration.take()
    }

    pub(crate) fn with_availability_callback(mut self) -> Self {
        self.return_availability_callback_registration = true;
        self
    }

    pub(crate) fn with_latest_message(self, message: Box<dyn MessageDyn>) -> Self {
        self.data.lock().next_latest_message.replace(message);
        self
    }

    pub(crate) fn with_next_descriptor(self, descriptor: MessageDescriptor) -> Self {
        self.data.lock().next_descriptor.replace(descriptor);
        self
    }

    pub(crate) fn with_next_error(self, error: anyhow::Error) -> Self {
        self.data.lock().next_error.replace(error);
        self
    }

    pub(crate) fn with_next_not_available(self) -> Self {
        self.data.lock().next_not_available = true;
        self
    }

    pub(crate) fn with_next_get_not_supported(self) -> Self {
        self.data.lock().next_get_not_supported = true;
        self
    }
}

impl MockServicePublisherAccessorData {
    /// Return next error if it's set or Ok otherwise.
    fn check_next_error(&mut self) -> anyhow::Result<()> {
        self.next_error.take().map_or_else(|| Ok(()), Err)
    }
}

#[async_trait]
impl AvailabilityCallbackRegistration for MockAvailabilityCallbackRegistration {
    async fn unregister(self: Box<Self>) {
        let data_reference = self.data.upgrade().unwrap();
        data_reference.lock().num_unregister_availability_callback_calls += 1;
    }
}

#[async_trait]
impl ServicePublisherMessageProvider for MockServicePublisherMessageProvider {
    async fn get_latest_message(
        &self,
        _configuration: Option<&Any>,
    ) -> Result<Option<Box<dyn MessageDyn>>, ServicePublisherAccessorError> {
        let data_reference = self.data.upgrade().unwrap();
        let mut data = data_reference.lock();
        data.check_next_error().map_err(ServicePublisherAccessorError::MessageParsingFailed)?;

        Ok(data.next_latest_message.take())
    }
}

#[async_trait]
impl ServicePublisherSubscription for MockServicePublisherSubscription {
    async fn unsubscribe(self: Box<Self>) {
        let data_reference = self.data.upgrade().unwrap();
        let mut data = data_reference.lock();
        data.num_unsubscribe_calls += 1;

        let _ = data
            .new_message_callbacks
            .remove(&(self.service_name, self.sub_sampling_interval))
            .unwrap();
    }
}
