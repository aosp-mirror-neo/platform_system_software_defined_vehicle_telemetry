// Copyright 2024 Google LLC

mod subscription;

use log::{error, info};
use protobuf::Message;
use sample_publisher_proto::sample_publisher::SampleMessage;
use sdv_telemetry_utils::atomic::{SequenceNumber, SequenceNumberType};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use sdv_configurable_publisher_rust_wrapper::configurable_publisher::{
    ConfigurablePublisher, ConfigurablePublisherError,
};
use sdv_configurable_publisher_rust_wrapper::configurable_publisher_subscription::ConfigurablePublisherSubscription;
use sdv_configurable_publisher_rust_wrapper::subscriber::Subscriber;
use subscription::RandomIntegerPublisherSubscription;

pub struct RandomIntegerPublisher {
    next_subscription_id: SequenceNumber,
    latest_message: Mutex<Option<SampleMessage>>,
    subscribers: Mutex<HashMap<SequenceNumberType, Box<dyn Subscriber>>>,
    weak_self: Weak<RandomIntegerPublisher>,
}

impl RandomIntegerPublisher {
    pub fn new() -> Arc<Self> {
        Arc::new_cyclic(|weak_self| RandomIntegerPublisher {
            next_subscription_id: SequenceNumber::new(),
            latest_message: None.into(),
            subscribers: HashMap::new().into(),
            weak_self: Weak::clone(weak_self),
        })
    }

    pub async fn activate(self: &Arc<Self>) {
        loop {
            let message =
                SampleMessage { sample_data: rand::random::<u8>().into(), ..Default::default() };
            info!("Generated message: {message}");
            let message_bytes =
                message.write_to_bytes().expect("Message should be successfully serialized");
            self.latest_message.lock().unwrap().replace(message);

            {
                let guard = self.subscribers.lock().unwrap();
                for subscriber in guard.values() {
                    if let Err(err) = subscriber.send_data(&message_bytes) {
                        error!("Error while sending message to subscriber: {err:?}");
                    }
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }
    }

    fn remove_subscription(&self, subscription_id: SequenceNumberType) {
        self.subscribers.lock().unwrap().remove(&subscription_id);
    }
}

impl ConfigurablePublisher for RandomIntegerPublisher {
    fn create_subscription(
        &self,
        subscriber: Box<dyn Subscriber>,
        _config: Option<&[u8]>,
    ) -> Result<Box<dyn ConfigurablePublisherSubscription>, ConfigurablePublisherError> {
        let subscription_id = self.next_subscription_id.next();
        self.subscribers.lock().unwrap().insert(subscription_id, subscriber);
        Ok(Box::new(RandomIntegerPublisherSubscription::new(
            Weak::clone(&self.weak_self),
            subscription_id,
        )))
    }

    fn get_latest_message(
        &self,
        _configuration: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, ConfigurablePublisherError> {
        let latest_message = self.latest_message.lock().unwrap();
        if let Some(msg) = &*latest_message {
            info!("Reporting latest message {msg}");
            match msg.write_to_bytes() {
                Ok(bytes) => Ok(Some(bytes)),
                Err(_) => Err(ConfigurablePublisherError::GetLatestMessageFailed),
            }
        } else {
            info!("Reporting no latest message");
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use sdv_configurable_publisher_rust_wrapper::subscriber::SubscriberError;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    #[derive(Clone)]
    struct MockSubscriber {
        tx: mpsc::UnboundedSender<Vec<u8>>,
    }

    impl Subscriber for MockSubscriber {
        fn send_data(&self, data: &[u8]) -> Result<(), SubscriberError> {
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
        let mock_subscriber = Box::new(MockSubscriber::new(tx));
        let publisher = Arc::new(RandomIntegerPublisher::new());
        let publisher_clone = Arc::clone(&publisher);

        let _ = publisher.create_subscription(mock_subscriber, None);
        tokio::spawn(async move {
            publisher_clone.activate().await;
        });

        let recv_msg = SampleMessage::parse_from_bytes(&rx.recv().await.unwrap()).unwrap();
        let latest_msg =
            SampleMessage::parse_from_bytes(&publisher.get_latest_message(None).unwrap().unwrap())
                .unwrap();
        assert_eq!(recv_msg.sample_data, latest_msg.sample_data);
    }

    #[tokio::test]
    async fn get_latest_message_no_message() {
        let publisher = RandomIntegerPublisher::new();
        let latest_msg = publisher.get_latest_message(None).unwrap();
        assert!(latest_msg.is_none());
    }
}
