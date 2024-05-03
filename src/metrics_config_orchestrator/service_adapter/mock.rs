// Copyright 2023 Google LLC. All Rights Reserved.

use std::future;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;
use tokio::sync::Notify;

use super::*;

pub(crate) struct MockServiceAdapter {
    // Block subscribe call to keep publisher in the Subscribing state.
    pub(crate) block_subscribe: bool,
    // Block unsubscribe call to keep publisher in the Unsubscribing state.
    pub(crate) block_unsubscribe: bool,
    // The last configuration message passed to subscribe.
    pub(crate) latest_configuration: Mutex<Option<Any>>,
    // Notify that's triggered when subscribe is blocked.
    pub(crate) notify_subscribing: Arc<Notify>,
    // Notify that's triggered when unsubscribe is blocked.
    pub(crate) notify_unsubscribing: Arc<Notify>,
    num_unsubscribe_calls: AtomicU32,
    new_message_callback:
        AsyncMutex<Option<Box<dyn for<'a> FnMut(&'a dyn MessageDyn) + Send + Sync>>>,
}

#[async_trait]
impl ServiceAdapter for MockServiceAdapter {
    async fn subscribe(
        &self,
        _service_name: &str,
        configuration: &Option<Any>,
        new_message_callback: Box<dyn for<'a> FnMut(&'a dyn MessageDyn) + Send + Sync>,
    ) -> Result<ServiceSubscription, ServiceAdapterSubscribeError> {
        self.new_message_callback.lock().await.replace(new_message_callback);
        if let Some(configuration) = configuration {
            self.latest_configuration.lock().unwrap().replace(configuration.clone());
        }
        if self.block_subscribe {
            self.notify_subscribing.notify_one();
            future::pending::<()>().await;
        }
        Ok(ServiceSubscription {
            subscription_type: ServiceSubscriptionType::Shared {
                service_name: String::default(),
                subscription_id: u32::MAX,
            },
        })
    }

    async fn unsubscribe(
        &self,
        _subscription: ServiceSubscription,
    ) -> Result<(), ServiceAdapterUnsubscribeError> {
        self.num_unsubscribe_calls.fetch_add(1, Ordering::Release);
        if self.block_unsubscribe {
            self.notify_unsubscribing.notify_one();
            future::pending::<()>().await;
        }
        Ok(())
    }
}

impl MockServiceAdapter {
    pub(crate) fn new() -> Self {
        Self {
            block_subscribe: false,
            block_unsubscribe: false,
            latest_configuration: None.into(),
            notify_subscribing: Notify::new().into(),
            notify_unsubscribing: Notify::new().into(),
            new_message_callback: None.into(),
            num_unsubscribe_calls: AtomicU32::new(0),
        }
    }

    pub(crate) fn get_num_unsubscribe_calls(&self) -> u32 {
        self.num_unsubscribe_calls.load(Ordering::Acquire)
    }

    pub(crate) async fn publish_new_message(&self, message: &dyn MessageDyn) {
        self.new_message_callback.lock().await.as_mut().unwrap()(message);
    }
}
