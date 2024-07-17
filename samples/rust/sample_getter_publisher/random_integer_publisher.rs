// Copyright 2024 Google LLC

mod subscription;

use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisherSubscription::{IConfigurablePublisherSubscription, BnConfigurablePublisherSubscription};
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::ISubscriber::ISubscriber;
use log::{info, error};
use protobuf::Message;
use sample_publisher_proto::sample_publisher::SampleMessage;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use subscription::RandomIntegerPublisherSubscription;

pub struct RandomIntegerPublisher {
    next_subscription_id: AtomicU32,
    latest_message: Mutex<Option<SampleMessage>>,
    subscribers: Mutex<HashMap<u32, binder::Strong<dyn ISubscriber>>>,
}

impl RandomIntegerPublisher {
    pub fn new() -> Self {
        Self {
            next_subscription_id: AtomicU32::new(1),
            latest_message: None.into(),
            subscribers: HashMap::new().into(),
        }
    }

    pub async fn activate(self: &Arc<Self>) {
        loop {
            let message =
                SampleMessage { sample_data: rand::random::<u8>().into(), ..Default::default() };
            info!("Generated message: {message}");
            let message_bytes =
                message.write_to_bytes().expect("Message should be successfully serialized");
            self.latest_message.lock().unwrap().replace(message);

            for subscriber in self.subscribers.lock().unwrap().values() {
                if let Err(err) = subscriber.sendData(&message_bytes) {
                    error!("Error while sending message to subscriber: {err:?}");
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    }

    pub fn create_subscription(
        self: &Arc<Self>,
        subscriber: &binder::Strong<dyn ISubscriber>,
        _config: Option<&[u8]>,
    ) -> binder::Strong<dyn IConfigurablePublisherSubscription> {
        let subscription_id = self.next_subscription_id.fetch_add(1, Ordering::SeqCst);
        let subscription =
            RandomIntegerPublisherSubscription::new(Arc::downgrade(self), subscription_id);
        self.subscribers.lock().unwrap().insert(subscription_id, subscriber.clone());
        BnConfigurablePublisherSubscription::new_binder(
            subscription,
            binder::BinderFeatures::default(),
        )
    }

    pub fn get_latest_message(&self) -> Option<Vec<u8>> {
        let latest_message = self.latest_message.lock().unwrap();
        if let Some(msg) = &*latest_message {
            info!("Reporting latest message {msg}");
            Some(msg.write_to_bytes().expect("Message should be successfully serialized"))
        } else {
            info!("Reporting no latest message");
            None
        }
    }

    fn remove_subscription(&self, subscription_id: u32) {
        self.subscribers.lock().unwrap().remove(&subscription_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::ISubscriber::{BnSubscriber, ISubscriber};
    use std::sync::Arc;
    use tokio::sync::mpsc;

    #[derive(Clone)]
    struct MockSubscriber {
        tx: mpsc::UnboundedSender<Vec<u8>>,
    }

    impl binder::Interface for MockSubscriber {}

    impl ISubscriber for MockSubscriber {
        fn sendData(&self, data: &[u8]) -> binder::Result<()> {
            self.tx.send(data.to_vec()).unwrap();
            Ok(())
        }
    }

    impl MockSubscriber {
        fn new(tx: mpsc::UnboundedSender<Vec<u8>>) -> Self {
            Self { tx }
        }
    }

    #[tokio::test]
    async fn activate_success() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mock_subscriber = MockSubscriber::new(tx);
        let bn_subscriber =
            BnSubscriber::new_binder(mock_subscriber.clone(), binder::BinderFeatures::default());
        let publisher = Arc::new(RandomIntegerPublisher::new());
        let publisher_clone = Arc::clone(&publisher);

        publisher.create_subscription(&bn_subscriber, None);
        tokio::spawn(async move {
            publisher_clone.activate().await;
        });

        let recv_msg = SampleMessage::parse_from_bytes(&rx.recv().await.unwrap()).unwrap();
        let latest_msg =
            SampleMessage::parse_from_bytes(&publisher.get_latest_message().unwrap()).unwrap();
        assert_eq!(recv_msg.sample_data, latest_msg.sample_data);
    }
}
