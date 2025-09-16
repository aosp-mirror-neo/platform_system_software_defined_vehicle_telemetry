// Copyright 2024 Google LLC

use binder::{BinderFeatures, Interface, Result as BinderResult, Strong};
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{ISubscriber::ISubscriber, IConfigurablePublisher::IConfigurablePublisher, IConfigurablePublisherSubscription::{IConfigurablePublisherSubscription, BnConfigurablePublisherSubscription}};
use parking_lot::Mutex;
use sdv_telemetry_utils::atomic::SequenceNumber;
use std::{collections::HashMap, sync::Arc};

use crate::subscription::Subscription;

pub(crate) struct PublisherWrapper {
    pub(crate) publisher: Arc<Publisher>,
}

impl Interface for PublisherWrapper {}

impl IConfigurablePublisher for PublisherWrapper {
    fn createSubscription(
        &self,
        subscriber: &Strong<dyn ISubscriber>,
        config: Option<&[u8]>,
    ) -> BinderResult<Strong<dyn IConfigurablePublisherSubscription>> {
        self.publisher.create_subscription(subscriber, config)
    }

    fn getLatestMessage(&self, config: Option<&[u8]>) -> BinderResult<Option<Vec<u8>>> {
        self.publisher.get_latest_message(config)
    }
}

#[derive(Debug)]
pub(crate) struct Publisher {
    next_subscription_id: SequenceNumber,
    subscribers: Mutex<HashMap<u64, Strong<dyn ISubscriber>>>,
    latest_message: Mutex<Option<Vec<u8>>>,
}

impl Publisher {
    pub(crate) fn new() -> Self {
        Self {
            next_subscription_id: SequenceNumber::new(),
            subscribers: HashMap::new().into(),
            latest_message: None.into(),
        }
    }

    pub(crate) fn publish(&self, message: &[u8]) {
        *self.latest_message.lock() = Some(message.to_owned());
        for subscriber in self.subscribers.lock().values() {
            let _ = subscriber.sendData(message);
        }
    }

    fn create_subscription(
        self: &Arc<Self>,
        subscriber: &Strong<dyn ISubscriber>,
        _config: Option<&[u8]>,
    ) -> BinderResult<Strong<dyn IConfigurablePublisherSubscription>> {
        let subscription_id = self.next_subscription_id.next();

        self.subscribers.lock().insert(subscription_id, subscriber.clone());

        let weak_self = Arc::downgrade(self);
        let subscription = Subscription::new(move || {
            if let Some(strong_self) = weak_self.upgrade() {
                strong_self.subscribers.lock().remove(&subscription_id);
            }
        });

        Ok(BnConfigurablePublisherSubscription::new_binder(subscription, BinderFeatures::default()))
    }

    fn get_latest_message(&self, _config: Option<&[u8]>) -> BinderResult<Option<Vec<u8>>> {
        Ok(self.latest_message.lock().clone())
    }
}
