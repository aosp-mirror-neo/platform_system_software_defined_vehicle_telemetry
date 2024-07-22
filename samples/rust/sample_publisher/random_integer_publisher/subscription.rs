// Copyright 2024 Google LLC

use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisherSubscription::IConfigurablePublisherSubscription;
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::ISubscriber::{ISubscriber, ISubscriberAsync};
use log::{debug, error, info};
use parking_lot::Mutex;
use protobuf::Message;
use sample_publisher_proto::sample_publisher::SampleMessage;
use std::sync::{Arc, Weak};
use tokio::time::Duration;

pub struct RandomIntegerPublisherSubscription {
    data: Arc<RandomIntegerPublisherSubscriptionData>,
}

struct RandomIntegerPublisherSubscriptionData {
    is_active: Mutex<bool>,
}

impl binder::Interface for RandomIntegerPublisherSubscription {}

impl IConfigurablePublisherSubscription for RandomIntegerPublisherSubscription {
    fn unsubscribe(&self) -> binder::Result<()> {
        info!("Stopping data publishing");
        *self.data.is_active.lock() = false;
        Ok(())
    }
}

impl RandomIntegerPublisherSubscription {
    pub fn new(subscriber: &binder::Strong<dyn ISubscriber>) -> Self {
        let data = Arc::new(RandomIntegerPublisherSubscriptionData { is_active: true.into() });
        let data_reference = Arc::downgrade(&data);
        let async_subscriber = subscriber.clone().into_async();
        tokio::spawn(Self::send_data(data_reference, async_subscriber));

        Self { data }
    }

    async fn send_data(
        data_reference: Weak<RandomIntegerPublisherSubscriptionData>,
        async_subscriber: binder::Strong<dyn ISubscriberAsync<binder_tokio::Tokio>>,
    ) {
        info!("Subscription started, publishing data");

        loop {
            let Some(data) = data_reference.upgrade() else {
                info!("Data pointer is dropped, finishing");
                break;
            };

            if !*data.is_active.lock() {
                info!("Subscription is deactivated, finishing");
                break;
            }

            // Generate random data and send to subscriber.
            let random_value = rand::random::<u8>().into();
            let mut message = SampleMessage::new();
            message.sample_data = random_value;
            debug!("Sending message to subscriber: {}", &message);

            if let Err(err) = async_subscriber
                .sendData(
                    &message.write_to_bytes().expect("Message should be successfully serialized"),
                )
                .await
            {
                error!("Error while sending message to subscriber: {:?}", err);
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
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
    async fn new_and_unsubscribe_success() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mock_subscriber = MockSubscriber::new(tx);
        let bn_subscriber =
            BnSubscriber::new_binder(mock_subscriber.clone(), binder::BinderFeatures::default());

        let subscription = Arc::new(RandomIntegerPublisherSubscription::new(&bn_subscriber));
        SampleMessage::parse_from_bytes(&rx.recv().await.unwrap()).unwrap();
        subscription.unsubscribe().unwrap();

        assert!(!*subscription.data.is_active.lock());
    }
}
