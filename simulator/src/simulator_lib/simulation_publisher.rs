// Copyright 2024 Google LLC

use binder::{BinderFeatures, Interface, Result as BinderResult, Strong};
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{ISubscriber::ISubscriber, IConfigurablePublisher::IConfigurablePublisher, IConfigurablePublisherSubscription::{IConfigurablePublisherSubscription, BnConfigurablePublisherSubscription}};
use parking_lot::Mutex;
use sdv_telemetry_utils::atomic::SequenceNumber;
use std::{collections::HashMap, sync::Arc, sync::Weak};

use crate::{
    publisher_config::ParsedDataEntry, publisher_config::PublisherConfig,
    simulation_publisher_subscription::SimulationPublisherSubscription,
};

pub struct SimulationPublisherWrapper {
    pub publisher: Arc<SimulationPublisher>,
}

impl Interface for SimulationPublisherWrapper {}

impl IConfigurablePublisher for SimulationPublisherWrapper {
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

pub struct SimulationPublisher {
    pub config: PublisherConfig,

    next_subscription_id: SequenceNumber,
    subscribers: Mutex<HashMap<u64, Strong<dyn ISubscriber>>>,
    latest_message: Mutex<Option<Vec<u8>>>,
}

impl SimulationPublisher {
    pub fn new(config: PublisherConfig) -> Self {
        Self {
            config,
            next_subscription_id: SequenceNumber::new(),
            subscribers: HashMap::new().into(),
            latest_message: None.into(),
        }
    }

    pub fn start_publishing(self: &Arc<Self>) {
        let weak_self = Arc::downgrade(self);
        let data = self.config.parsed_data.clone();
        let loop_data = self.config.loop_data;

        tokio::spawn(async move {
            loop {
                if Self::publish_data_once(&weak_self, &data).await.is_err() {
                    break;
                }

                if !loop_data {
                    break;
                }
            }
        });
    }

    async fn publish_data_once(
        weak_self: &Weak<Self>,
        data: &Vec<ParsedDataEntry>,
    ) -> Result<(), ()> {
        for data_entry in data {
            let Some(strong_self) = weak_self.upgrade() else {
                return Err(());
            };

            tokio::time::sleep(data_entry.delay).await;

            *strong_self.latest_message.lock() = Some(data_entry.message_bytes.clone());
            for subscriber in strong_self.subscribers.lock().values() {
                let _ = subscriber.sendData(&data_entry.message_bytes);
            }
        }

        Ok(())
    }

    fn create_subscription(
        self: &Arc<Self>,
        subscriber: &Strong<dyn ISubscriber>,
        _config: Option<&[u8]>,
    ) -> BinderResult<Strong<dyn IConfigurablePublisherSubscription>> {
        let subscription_id = self.next_subscription_id.next();

        self.subscribers.lock().insert(subscription_id, subscriber.clone());

        let weak_self = Arc::downgrade(self);
        let subscription = SimulationPublisherSubscription::new(move || {
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
