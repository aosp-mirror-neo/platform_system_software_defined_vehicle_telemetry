// Copyright 2023 Google LLC. All Rights Reserved.

use async_trait::async_trait;
use protobuf::{reflect::MessageDescriptor, MessageDyn};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

use crate::{ConfigurablePublisherRegistry, ConfigurablePublisherRegistrySubscribeError};

pub(crate) struct MockConfigurablePublisherRegistry {
    /// When set, the descriptor will be returned on the next call to get_message_descriptor.
    pub next_descriptor: Mutex<Option<MessageDescriptor>>,
    /// When set, the error will be returned on the next call to subscribe.
    pub next_error: Mutex<Option<ConfigurablePublisherRegistrySubscribeError>>,
    /// The number of times subscribe function of the mock was called.
    num_subscribe_calls: AtomicU32,
    /// The map of provided subscriber functions.
    subscribers: Mutex<HashMap<(String, String), Box<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>>>,
}

#[async_trait]
impl ConfigurablePublisherRegistry for MockConfigurablePublisherRegistry {
    fn get_message_descriptor(
        &self,
        _service_name: String,
        _topic_name: String,
    ) -> Option<MessageDescriptor> {
        self.next_descriptor.lock().unwrap().take()
    }

    async fn subscribe(
        &self,
        service_name: String,
        topic_name: String,
        subscriber: Box<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>,
        _configuration: Option<&[u8]>,
    ) -> Result<(), ConfigurablePublisherRegistrySubscribeError> {
        self.num_subscribe_calls.fetch_add(1, Ordering::SeqCst);

        if let Some(err) = self.next_error.lock().unwrap().take() {
            return Err(err);
        }

        self.subscribers.lock().unwrap().insert((service_name, topic_name), subscriber);
        Ok(())
    }
}

impl Default for MockConfigurablePublisherRegistry {
    fn default() -> Self {
        Self::new(None)
    }
}

impl MockConfigurablePublisherRegistry {
    pub(crate) fn new_with_descriptor(descriptor: MessageDescriptor) -> Self {
        Self::new(Some(descriptor))
    }

    pub(crate) fn new(descriptor: Option<MessageDescriptor>) -> Self {
        MockConfigurablePublisherRegistry {
            next_descriptor: descriptor.into(),
            next_error: None.into(),
            num_subscribe_calls: AtomicU32::new(0),
            subscribers: HashMap::new().into(),
        }
    }

    pub(crate) fn get_num_subscribe_calls(&self) -> u32 {
        self.num_subscribe_calls.load(Ordering::SeqCst)
    }

    pub(crate) fn notify_subscriber(
        &self,
        service_name: String,
        topic_name: String,
        message: Box<dyn MessageDyn>,
    ) {
        (self.subscribers.lock().unwrap().get(&(service_name, topic_name)).as_ref().unwrap())(
            message,
        );
    }
}
