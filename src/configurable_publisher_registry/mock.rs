// Copyright 2023 Google LLC. All Rights Reserved.

use async_trait::async_trait;
use binder::Status;
use protobuf::{reflect::MessageDescriptor, MessageDyn};
use std::collections::hash_map::Entry;
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
    /// The number of times subscribe function of the mock was called.
    num_unsubscribe_calls: AtomicU32,
    /// The map of provided subscriber functions.
    subscribers: Mutex<HashMap<(String, String), Box<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>>>,
}

#[async_trait]
impl ConfigurablePublisherRegistry for MockConfigurablePublisherRegistry {
    fn get_message_descriptor(
        &self,
        service_name: &str,
        _topic_name: &str,
    ) -> Result<MessageDescriptor, ConfigurablePublisherRegistrySubscribeError> {
        self.next_descriptor.lock().unwrap().take().ok_or_else(|| {
            ConfigurablePublisherRegistrySubscribeError::ServiceNotFound {
                service_name: service_name.to_owned(),
            }
        })
    }

    async fn subscribe(
        &self,
        service_name: &str,
        topic_name: &str,
        subscriber: Box<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>,
        _configuration: Option<&[u8]>,
    ) -> Result<(), ConfigurablePublisherRegistrySubscribeError> {
        self.num_subscribe_calls.fetch_add(1, Ordering::SeqCst);

        if let Some(err) = self.next_error.lock().unwrap().take() {
            return Err(err);
        }

        self.subscribers
            .lock()
            .unwrap()
            .insert((service_name.to_owned(), topic_name.to_owned()), subscriber);
        Ok(())
    }

    async fn unsubscribe(
        &self,
        service_name: &str,
        topic_name: &str,
        _configuration: Option<&[u8]>,
    ) -> Result<(), ConfigurablePublisherRegistrySubscribeError> {
        self.num_unsubscribe_calls.fetch_add(1, Ordering::SeqCst);

        match self
            .subscribers
            .lock()
            .unwrap()
            .entry((service_name.to_string(), topic_name.to_string()))
        {
            Entry::Occupied(o) => {
                let _ = o.remove_entry();
            }
            Entry::Vacant(_) => {
                return Err(ConfigurablePublisherRegistrySubscribeError::UnsubscribeFailed {
                    error: Status::new_service_specific_error_str(-1, Some("Publisher not found")),
                })
            }
        }

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
            num_unsubscribe_calls: AtomicU32::new(0),
            subscribers: HashMap::new().into(),
        }
    }

    pub(crate) fn get_num_subscribe_calls(&self) -> u32 {
        self.num_subscribe_calls.load(Ordering::SeqCst)
    }

    pub(crate) fn get_num_unsubscribe_calls(&self) -> u32 {
        self.num_unsubscribe_calls.load(Ordering::SeqCst)
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
