// Copyright 2023 Google LLC. All Rights Reserved.

use anyhow::anyhow;
use async_trait::async_trait;
use protobuf::{reflect::MessageDescriptor, MessageDyn};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use crate::service_publisher_accessor::ServicePublisherAccessor;

pub(crate) struct MockServicePublisherAccessor {
    /// When set, the descriptor will be returned on the next call to get_message_descriptor.
    pub next_descriptor: Mutex<Option<MessageDescriptor>>,
    /// When set, the error will be returned on the next call to get_message_descriptor or subscribe.
    pub next_error: Mutex<Option<anyhow::Error>>,
    /// The number of times subscribe function of the mock was called.
    num_subscribe_calls: AtomicU32,
    /// The number of times subscribe function of the mock was called.
    num_unsubscribe_calls: AtomicU32,
    /// The number of times get message descriptor function of the mock was called.
    num_get_message_descriptor_calls: AtomicU32,
    /// The map of provided subscriber functions.
    subscribers: Mutex<HashMap<String, Arc<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>>>,
}

#[async_trait]
impl ServicePublisherAccessor for MockServicePublisherAccessor {
    fn get_message_descriptor(
        &self,
        _service_name: &str,
    ) -> Result<Option<MessageDescriptor>, anyhow::Error> {
        self.num_get_message_descriptor_calls.fetch_add(1, Ordering::SeqCst);

        if let Some(err) = self.next_error.lock().unwrap().take() {
            return Err(err);
        }

        Ok(self.next_descriptor.lock().unwrap().take())
    }

    async fn try_subscribe(
        &self,
        service_name: &str,
        subscriber: Arc<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>,
        _configuration: Option<&[u8]>,
    ) -> Result<bool, anyhow::Error> {
        self.num_subscribe_calls.fetch_add(1, Ordering::SeqCst);

        if let Some(err) = self.next_error.lock().unwrap().take() {
            return Err(err);
        }

        self.subscribers.lock().unwrap().insert(service_name.to_owned(), subscriber.clone());
        Ok(true)
    }

    async fn unsubscribe(
        &self,
        service_name: &str,
        _configuration: Option<&[u8]>,
    ) -> Result<(), anyhow::Error> {
        self.num_unsubscribe_calls.fetch_add(1, Ordering::SeqCst);
        match self.subscribers.lock().unwrap().entry(service_name.to_owned()) {
            Entry::Occupied(o) => {
                let _ = o.remove_entry();
            }
            Entry::Vacant(_) => return Err(anyhow!("Publisher Not Found")),
        }

        Ok(())
    }
}

impl Default for MockServicePublisherAccessor {
    fn default() -> Self {
        Self::new(None)
    }
}

impl MockServicePublisherAccessor {
    pub(crate) fn new_with_descriptor(descriptor: MessageDescriptor) -> Self {
        Self::new(Some(descriptor))
    }

    pub(crate) fn new(descriptor: Option<MessageDescriptor>) -> Self {
        Self {
            next_descriptor: descriptor.into(),
            next_error: None.into(),
            num_subscribe_calls: AtomicU32::new(0),
            num_unsubscribe_calls: AtomicU32::new(0),
            num_get_message_descriptor_calls: AtomicU32::new(0),
            subscribers: HashMap::new().into(),
        }
    }

    pub(crate) fn get_num_subscribe_calls(&self) -> u32 {
        self.num_subscribe_calls.load(Ordering::SeqCst)
    }

    pub(crate) fn get_num_unsubscribe_calls(&self) -> u32 {
        self.num_unsubscribe_calls.load(Ordering::SeqCst)
    }

    pub(crate) fn get_num_get_message_descriptor_calls(&self) -> u32 {
        self.num_get_message_descriptor_calls.load(Ordering::SeqCst)
    }

    pub(crate) fn notify_subscriber(&self, service_name: &str, message: Box<dyn MessageDyn>) {
        (self.subscribers.lock().unwrap().get(service_name).as_ref().unwrap())(message);
    }
}
