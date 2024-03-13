// Copyright 2023 Google LLC. All Rights Reserved.

use std::future;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::sync::Notify;

use super::*;

pub(crate) struct MockServiceAdapter {
    // Block subscribe call to keep publisher in the Subscribing state.
    pub(crate) block_subscribe: bool,
    // Block unsubscribe call to keep publisher in the Unsubscribing state.
    pub(crate) block_unsubscribe: bool,
    num_unsubscribe_calls: AtomicU32,
    new_message_callback:
        AsyncMutex<Option<Box<dyn for<'a> FnMut(&'a dyn MessageDyn) + Send + Sync>>>,
    pub(crate) notify_subscribing: Arc<Notify>,
    pub(crate) notify_unsubscribing: Arc<Notify>,
}

#[async_trait]
impl ServiceAdapter for MockServiceAdapter {
    async fn subscribe(
        &self,
        _service_name: &str,
        _topic_name: &str,
        new_message_callback: Box<dyn for<'a> FnMut(&'a dyn MessageDyn) + Send + Sync>,
    ) -> Result<ServiceSubscription, ServiceAdapterSubscribeError> {
        self.new_message_callback.lock().await.replace(new_message_callback);
        if self.block_subscribe {
            self.notify_subscribing.notify_one();
            future::pending::<()>().await;
        }
        Ok(ServiceSubscription {
            service_name: String::default(),
            subscription_id: u32::MAX,
            topic_name: String::default(),
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
            new_message_callback: None.into(),
            notify_subscribing: Notify::new().into(),
            notify_unsubscribing: Notify::new().into(),
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
