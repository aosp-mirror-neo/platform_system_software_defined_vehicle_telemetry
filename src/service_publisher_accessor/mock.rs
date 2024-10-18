// Copyright 2023 Google LLC

use super::*;

use async_trait::async_trait;
use parking_lot::Mutex;
use protobuf::{reflect::MessageDescriptor, MessageDyn};
use sdv_telemetry_utils::collection_extension::HashMapExtension;
use std::collections::HashMap;
use std::sync::{Arc, Weak};

pub(crate) struct MockServicePublisherAccessor {
    data: Arc<Mutex<MockServicePublisherAccessorData>>,
}

struct MockServicePublisherAccessorData {
    /// The map of subscribed callbacks, keyed by service name.
    callbacks: HashMap<String, Box<dyn FnMut(Box<dyn MessageDyn>) + Send + Sync>>,
    /// The last configuration message that was passed to create_subscription.
    last_configuration: Option<Any>,
    /// When set, the descriptor will be returned on the next call to get_metadata.
    next_descriptor: Option<MessageDescriptor>,
    /// When set, the error will be returned on the next call to the adapter.
    next_error: Option<anyhow::Error>,
    /// When set, the message will be returned on the next call to get_latest_message.
    next_latest_message: Option<Box<dyn MessageDyn>>,
    /// When set to true, the next call to create_subscription will return a not found result.
    next_not_available: bool,
    /// The number of times get latest message function of the mock was called.
    num_get_message_provider_calls: u32,
    /// The number of times get metadata function of the mock was called.
    num_get_metadata_calls: u32,
    /// The number of times subscribe function of the mock was called.
    num_subscribe_calls: u32,
    /// The number of times unsubscribe function of the mock was called.
    num_unsubscribe_calls: u32,
}

struct MockServicePublisherMessageProvider {
    data: Weak<Mutex<MockServicePublisherAccessorData>>,
}

struct MockServicePublisherSubscription {
    data: Weak<Mutex<MockServicePublisherAccessorData>>,
    service_name: String,
}

#[async_trait]
impl ServicePublisherAccessor for MockServicePublisherAccessor {
    async fn get_metadata(
        &self,
        _service_name: &str,
    ) -> anyhow::Result<Option<ServicePublisherMetadata>> {
        let mut data = self.data.lock();
        data.num_get_metadata_calls += 1;
        data.check_next_error()?;

        let metadata = data.next_descriptor.take().map(|message_descriptor| {
            ServicePublisherMetadata { supports_get: false, message_descriptor }
        });
        Ok(metadata)
    }

    fn get_message_provider(
        &self,
        _service_name: &str,
    ) -> anyhow::Result<Option<Box<dyn ServicePublisherMessageProvider>>> {
        let mut data = self.data.lock();
        data.num_get_message_provider_calls += 1;
        data.check_next_error()?;

        if data.next_not_available {
            data.next_not_available = false;
            return Ok(None);
        }

        Ok(Some(Box::new(MockServicePublisherMessageProvider { data: Arc::downgrade(&self.data) })))
    }

    async fn create_subscription(
        &self,
        service_name: &str,
        configuration: Option<&Any>,
        _sub_sampling_interval: &SubSamplingInterval,
        callback: Box<dyn FnMut(Box<dyn MessageDyn>) + Send + Sync>,
    ) -> anyhow::Result<CreateSubscriptionOutcome> {
        let mut data = self.data.lock();
        data.last_configuration = configuration.cloned();
        data.num_subscribe_calls += 1;
        data.check_next_error()?;

        if data.next_not_available {
            data.next_not_available = false;
            return Ok(CreateSubscriptionOutcome::NotRegistered(callback));
        }

        if data.callbacks.try_insert_value(service_name.to_owned(), callback).is_err() {
            panic!("Callback is already set for service name {}", service_name);
        }

        let subscription = MockServicePublisherSubscription {
            service_name: service_name.to_owned(),
            data: Arc::downgrade(&self.data),
        };
        Ok(CreateSubscriptionOutcome::Subscribed(Box::new(subscription)))
    }
}

impl MockServicePublisherAccessor {
    pub(crate) fn new() -> Self {
        let data = Arc::new(Mutex::new(MockServicePublisherAccessorData {
            callbacks: HashMap::new(),
            last_configuration: None,
            next_descriptor: None,
            next_error: None,
            next_latest_message: None,
            next_not_available: false,
            num_get_message_provider_calls: 0,
            num_get_metadata_calls: 0,
            num_subscribe_calls: 0,
            num_unsubscribe_calls: 0,
        }));
        Self { data }
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

    pub(crate) fn notify_subscriber(&self, service_name: &str, message: Box<dyn MessageDyn>) {
        let mut data_lock = self.data.lock();
        data_lock.callbacks.get_mut(service_name).unwrap()(message);
    }

    pub(crate) fn set_next_error(&self, error: anyhow::Error) {
        self.data.lock().next_error.replace(error);
    }

    pub(crate) fn take_last_configuration(&self) -> Option<Any> {
        self.data.lock().last_configuration.take()
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
}

impl MockServicePublisherAccessorData {
    /// Return next error if it's set or Ok otherwise.
    fn check_next_error(&mut self) -> anyhow::Result<()> {
        self.next_error.take().map_or_else(|| Ok(()), Err)
    }
}

#[async_trait]
impl ServicePublisherMessageProvider for MockServicePublisherMessageProvider {
    async fn get_latest_message(
        &self,
        _configuration: Option<&Any>,
    ) -> anyhow::Result<Option<Box<dyn MessageDyn>>> {
        let data_reference = self.data.upgrade().unwrap();
        let mut data = data_reference.lock();
        data.check_next_error()?;

        Ok(data.next_latest_message.take())
    }
}

#[async_trait]
impl ServicePublisherSubscription for MockServicePublisherSubscription {
    async fn unsubscribe(self: Box<Self>) -> anyhow::Result<()> {
        let data_reference = self.data.upgrade().unwrap();
        let mut data = data_reference.lock();
        data.num_unsubscribe_calls += 1;
        data.check_next_error()?;

        let _ = data.callbacks.remove(&self.service_name).unwrap();
        Ok(())
    }
}
