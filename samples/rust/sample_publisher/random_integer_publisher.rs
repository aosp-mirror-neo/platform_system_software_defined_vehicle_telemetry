// Copyright 2023 Google LLC

use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{ISubscriber::ISubscriber, IConfigurablePublisherSubscription::{IConfigurablePublisherSubscription, BnConfigurablePublisherSubscription}};
use google_sdv_telemetry_configurable_publisher::binder::{BinderFeatures, Strong, Result as BinderResult};

use crate::random_integer_publisher_subscription::RandomIntegerPublisherSubscription;
use sample_publisher_proto::sample_publisher::SampleMessage;

use log::{error, info};
use protobuf::{Error, Message};
use rand::Rng;
use std::sync::{Arc, Mutex};
use tokio::{sync::Notify, time::Duration};

pub struct RandomIntegerPublisher {
    subscriber: Arc<Mutex<Option<Strong<dyn ISubscriber>>>>,
    notify_subscribed: Arc<Notify>,
}

impl RandomIntegerPublisher {
    pub fn new(notify_subscribed: Arc<Notify>) -> Self {
        Self { subscriber: Mutex::new(None).into(), notify_subscribed }
    }

    pub fn create_subscription(
        &self,
        subscriber: &Strong<dyn ISubscriber>,
        _config: Option<&[u8]>,
    ) -> BinderResult<Strong<dyn IConfigurablePublisherSubscription>> {
        let mut guard = self.subscriber.lock().unwrap();
        *guard = Some(subscriber.clone());
        self.notify_subscribed.notify_one();

        let subs = RandomIntegerPublisherSubscription::new(Arc::downgrade(&self.subscriber));
        let binder_subscription =
            BnConfigurablePublisherSubscription::new_binder(subs, BinderFeatures::default());

        Ok(binder_subscription)
    }

    // Publishes messages at random intervals until there is no longer a subscriber.
    pub async fn publish_data_randomly(
        &self,
        min_interval: Duration,
        max_interval: Duration,
    ) -> Result<(), Error> {
        let interval_range = min_interval..max_interval;
        let mut rng = rand::thread_rng();

        loop {
            match &*self.subscriber.lock().unwrap() {
                Some(subscriber) => {
                    // Generate random data and send to subscriber.
                    let random_value: i32 = rng.gen();
                    let mut message = SampleMessage::new();
                    message.sample_data = random_value;
                    info!("Sending message to subscriber: {}", &message);

                    if let Err(err) = subscriber.sendData(&message.write_to_bytes()?) {
                        error!("Error while sending message to subscriber: {:?}", err);
                    }
                }
                None => {
                    // Stop publishing data if we no longer have a subscriber.
                    break;
                }
            };

            tokio::time::sleep(if interval_range.is_empty() {
                interval_range.start
            } else {
                rng.gen_range(min_interval..max_interval)
            })
            .await;
        }

        Ok(())
    }
}
