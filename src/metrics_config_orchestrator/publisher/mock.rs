// Copyright 2023 Google LLC. All Rights Reserved.

use async_trait::async_trait;
use protobuf::MessageDyn;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::Mutex as AsyncMutex;

use super::{
    Publisher, PublisherActivateError, PublisherDeactivateError, PublisherSubscribeError,
    PublisherUnsubscribeError,
};

pub(crate) struct MockPublisher {
    pub expected_trigger_names: HashSet<String>,
    pub latest_message: Option<Box<dyn MessageDyn>>,

    name: String,
    activate_callback: Option<Box<dyn Fn(&str) + Send + Sync>>,
    new_message_callback:
        AsyncMutex<Option<Box<dyn for<'a> FnMut(&'a dyn MessageDyn) + Send + Sync>>>,
    subscribe_callback: Option<Box<dyn Fn() -> Option<PublisherSubscribeError> + Send + Sync>>,
    unsubscribe_callback: Option<Box<dyn Fn() -> Option<PublisherUnsubscribeError> + Send + Sync>>,
}

#[async_trait]
impl Publisher for MockPublisher {
    fn get_name(&self) -> &str {
        &self.name
    }

    fn get_latest_message(&self) -> Option<Box<dyn MessageDyn>> {
        self.latest_message.clone()
    }

    async fn activate(self: Arc<Self>) -> Result<(), PublisherActivateError> {
        if let Some(callback) = &self.activate_callback {
            callback(&self.name);
        }

        Ok(())
    }

    async fn deactivate(&self) -> Result<(), PublisherDeactivateError> {
        Ok(())
    }

    async fn subscribe(
        &self,
        data_trigger_name: &str,
        new_message_callback: Box<dyn for<'a> FnMut(&'a dyn MessageDyn) + Send + Sync>,
    ) -> Result<(), PublisherSubscribeError> {
        assert!(self.expected_trigger_names.contains(data_trigger_name));
        self.new_message_callback.lock().await.replace(new_message_callback);

        if let Some(callback) = &self.subscribe_callback {
            if let Some(error) = callback() {
                return Err(error);
            }
        }

        Ok(())
    }

    async fn unsubscribe(&self, data_trigger_name: &str) -> Result<(), PublisherUnsubscribeError> {
        assert!(self.expected_trigger_names.contains(data_trigger_name));

        if let Some(callback) = &self.unsubscribe_callback {
            if let Some(error) = callback() {
                return Err(error);
            }
        }

        Ok(())
    }
}

impl MockPublisher {
    pub(crate) fn new(name: String) -> Self {
        MockPublisher {
            expected_trigger_names: HashSet::new(),
            latest_message: None,
            name,
            activate_callback: None,
            new_message_callback: None.into(),
            subscribe_callback: None,
            unsubscribe_callback: None,
        }
    }

    pub(crate) async fn fire_new_message(&self, message: &dyn MessageDyn) {
        self.new_message_callback.lock().await.as_mut().unwrap()(message);
    }

    pub(crate) fn set_activate_callback(
        &mut self,
        callback: impl Fn(&str) + Send + Sync + 'static,
    ) {
        self.activate_callback.replace(Box::new(callback));
    }

    pub(crate) fn set_subscribe_callback(
        &mut self,
        callback: impl Fn() -> Option<PublisherSubscribeError> + Send + Sync + 'static,
    ) {
        self.subscribe_callback.replace(Box::new(callback));
    }

    pub(crate) fn set_unsubscribe_callback(
        &mut self,
        callback: impl Fn() -> Option<PublisherUnsubscribeError> + Send + Sync + 'static,
    ) {
        self.unsubscribe_callback.replace(Box::new(callback));
    }
}
