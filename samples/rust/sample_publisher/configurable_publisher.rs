// Copyright 2023 Google LLC. All Rights Reserved.

use android_sdv_telemetry_configurable_publisher::aidl::android::sdv::telemetry::configurable_publisher::ISubscriber::ISubscriber;
use android_sdv_telemetry_configurable_publisher::binder::Strong;
use configurable_publisher_proto::sample_publisher::SampleMessage;

use log::{error, info};
use protobuf::Message;
use rand::{rngs::SmallRng, Rng, SeedableRng};
use std::sync::{Arc, Mutex};
use tokio::{sync::Notify, time::Duration};

pub struct ConfigurablePublisher {
    subscriber: Mutex<Option<Strong<dyn ISubscriber>>>,
    notify_subscribed: Arc<Notify>,
}

impl ConfigurablePublisher {
    pub fn new(notify_subscribed: Arc<Notify>) -> Self {
        ConfigurablePublisher { subscriber: Mutex::new(None), notify_subscribed }
    }

    pub fn subscribe(
        &self,
        subscriber: &Strong<dyn ISubscriber>,
        _config: Option<&[u8]>,
    ) -> Result<(), String> {
        let mut guard = self.subscriber.lock().unwrap();
        *guard = Some(subscriber.clone());
        self.notify_subscribed.notify_one();

        Ok(())
    }

    // Publishes messages at random intervals until the map is cleared.
    pub async fn publish_data_randomly(&self, min_interval_ms: u64, max_interval_ms: u64) {
        let mut small_rng = SmallRng::from_entropy();

        loop {
            // Verify that the map hasn't been cleared.
            if self.subscriber.lock().unwrap().is_none() {
                self.notify_subscribed.notify_one();
                return;
            }

            // If there is an interval, sleep randomly in within bounds, otherwise sleep fixed duration.
            tokio::time::sleep(Duration::from_millis(if min_interval_ms != max_interval_ms {
                small_rng.gen_range(min_interval_ms..max_interval_ms)
            } else {
                min_interval_ms
            }))
            .await;

            // Generate random data and send to subscribers.
            let mut rng = rand::thread_rng();
            let random_value: i32 = rng.gen(); // Generate a random i32

            if let Some(subscriber) = &*self.subscriber.lock().unwrap() {
                info!("Sending data to subscriber {}", random_value);
                let mut message = SampleMessage::new();
                message.sample_data = random_value;
                let res = subscriber.sendData(&message.write_to_bytes().unwrap());

                if res.is_err() {
                    error!("Error in sending data {:?}", res);
                }
            }
        }
    }
}
