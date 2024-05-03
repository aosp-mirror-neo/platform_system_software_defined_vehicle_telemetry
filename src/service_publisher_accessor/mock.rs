// Copyright 2023 Google LLC. All Rights Reserved.

use async_trait::async_trait;
use protobuf::{reflect::MessageDescriptor, MessageDyn};
use sdv_telemetry_utils::collection_extension::HashMapExtension;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use super::*;

pub(crate) struct MockServicePublisherAccessor {
    data: Arc<Mutex<MockServicePublisherAccessorData>>,
}

struct MockServicePublisherAccessorData {
    /// When set, the descriptor will be returned on the next call to get_message_descriptor.
    next_descriptor: Option<MessageDescriptor>,
    /// When set, the error will be returned on the next call to the adapter.
    next_error: Option<anyhow::Error>,
    /// The number of times get message descriptor function of the mock was called.
    num_get_message_descriptor_calls: u32,
    /// The number of times subscribe function of the mock was called.
    num_subscribe_calls: u32,
    /// The number of times unsubscribe function of the mock was called.
    num_unsubscribe_calls: u32,
    /// The map of subscribed callbacks, keyed by service name.
    callbacks: HashMap<String, Box<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>>,
}

struct MockServicePublisherSubscription {
    data: Weak<Mutex<MockServicePublisherAccessorData>>,
    service_name: String,
}

#[async_trait]
impl ServicePublisherAccessor for MockServicePublisherAccessor {
    fn get_message_descriptor(
        &self,
        _service_name: &str,
    ) -> Result<Option<MessageDescriptor>, anyhow::Error> {
        let mut data = self.data.lock().unwrap();
        data.num_get_message_descriptor_calls += 1;
        data.check_next_error()?;

        Ok(data.next_descriptor.take())
    }

    async fn create_subscription(
        &self,
        service_name: &str,
        _configuration: Option<&[u8]>,
        callback: Box<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>,
    ) -> anyhow::Result<CreateSubscriptionOutcome> {
        let mut data = self.data.lock().unwrap();
        data.num_subscribe_calls += 1;
        data.check_next_error()?;

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
            next_descriptor: None,
            next_error: None,
            num_subscribe_calls: 0,
            num_unsubscribe_calls: 0,
            num_get_message_descriptor_calls: 0,
            callbacks: HashMap::new(),
        }));
        Self { data }
    }

    pub(crate) fn get_num_subscribe_calls(&self) -> u32 {
        self.data.lock().unwrap().num_subscribe_calls
    }

    pub(crate) fn get_num_unsubscribe_calls(&self) -> u32 {
        self.data.lock().unwrap().num_unsubscribe_calls
    }

    pub(crate) fn get_num_get_message_descriptor_calls(&self) -> u32 {
        self.data.lock().unwrap().num_get_message_descriptor_calls
    }

    pub(crate) fn notify_subscriber(&self, service_name: &str, message: Box<dyn MessageDyn>) {
        let data_lock = self.data.lock().unwrap();
        data_lock.callbacks.get(service_name).unwrap()(message);
    }

    pub(crate) fn with_next_descriptor(self, descriptor: MessageDescriptor) -> Self {
        self.data.lock().unwrap().next_descriptor.replace(descriptor);
        self
    }

    pub(crate) fn with_next_error(self, error: anyhow::Error) -> Self {
        self.data.lock().unwrap().next_error.replace(error);
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
impl ServicePublisherSubscription for MockServicePublisherSubscription {
    async fn unsubscribe(self: Box<Self>) -> Result<(), anyhow::Error> {
        let data_reference = self.data.upgrade().unwrap();
        let mut data = data_reference.lock().unwrap();
        data.num_unsubscribe_calls += 1;
        data.check_next_error()?;

        let _ = data.callbacks.remove(&self.service_name).unwrap();
        Ok(())
    }
}
