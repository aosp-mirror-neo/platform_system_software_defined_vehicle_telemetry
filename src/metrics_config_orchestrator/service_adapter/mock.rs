// Copyright 2023 Google LLC

use super::*;

use parking_lot::Mutex;
use std::future;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::sync::Notify;

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
    next_error: Mutex<Option<anyhow::Error>>,
    next_message_provider: Mutex<Option<MockServiceMessageProvider>>,
    num_get_message_provider_calls: AtomicU32,
    num_subscribe_calls: AtomicU32,
    num_unsubscribe_calls: AtomicU32,
    new_message_callback:
        AsyncMutex<Option<Box<dyn for<'a> FnMut(&'a dyn MessageDyn) + Send + Sync>>>,
}

pub(crate) struct MockServiceMessageProvider {
    message_provider: Option<Box<dyn Fn() -> Box<dyn MessageDyn> + Send + Sync>>,
    next_error: Mutex<Option<anyhow::Error>>,
}

#[async_trait]
impl ServiceAdapter for MockServiceAdapter {
    fn get_message_provider(
        &self,
        _service_name: &str,
    ) -> anyhow::Result<Option<Box<dyn ServiceMessageProvider>>> {
        self.num_get_message_provider_calls.fetch_add(1, Ordering::Release);

        if let Some(error) = self.next_error.lock().take() {
            return Err(error);
        }

        if let Some(message_provider) = self.next_message_provider.lock().take() {
            Ok(Some(Box::new(message_provider)))
        } else {
            Ok(None)
        }
    }

    async fn subscribe(
        &self,
        _service_name: &str,
        configuration: Option<&Any>,
        new_message_callback: Box<dyn for<'a> FnMut(&'a dyn MessageDyn) + Send + Sync>,
    ) -> anyhow::Result<Option<ServiceSubscription>> {
        self.num_subscribe_calls.fetch_add(1, Ordering::Release);
        self.new_message_callback.lock().await.replace(new_message_callback);
        if let Some(configuration) = configuration {
            self.latest_configuration.lock().replace(configuration.clone());
        }

        if let Some(error) = self.next_error.lock().take() {
            return Err(error);
        }

        if self.block_subscribe {
            self.notify_subscribing.notify_one();
            future::pending::<()>().await;
        }

        Ok(Some(ServiceSubscription {
            subscription_type: ServiceSubscriptionType::Shared {
                service_name: String::default(),
                subscription_id: u32::MAX,
            },
        }))
    }

    async fn unsubscribe(&self, _subscription: ServiceSubscription) -> anyhow::Result<()> {
        self.num_unsubscribe_calls.fetch_add(1, Ordering::Release);

        if let Some(error) = self.next_error.lock().take() {
            return Err(error);
        }

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
            next_error: None.into(),
            next_message_provider: None.into(),
            new_message_callback: None.into(),
            notify_subscribing: Notify::new().into(),
            notify_unsubscribing: Notify::new().into(),
            num_get_message_provider_calls: 0.into(),
            num_subscribe_calls: 0.into(),
            num_unsubscribe_calls: 0.into(),
        }
    }

    pub(crate) fn get_num_get_message_provider_calls(&self) -> u32 {
        self.num_get_message_provider_calls.load(Ordering::Acquire)
    }

    pub(crate) fn get_num_subscribe_calls(&self) -> u32 {
        self.num_subscribe_calls.load(Ordering::Acquire)
    }

    pub(crate) fn get_num_unsubscribe_calls(&self) -> u32 {
        self.num_unsubscribe_calls.load(Ordering::Acquire)
    }

    pub(crate) async fn publish_new_message(&self, message: &dyn MessageDyn) {
        self.new_message_callback.lock().await.as_mut().unwrap()(message);
    }

    pub(crate) fn with_next_error(self, error: anyhow::Error) -> Self {
        self.next_error.lock().replace(error);
        self
    }

    pub(crate) fn with_next_message_provider(self, provider: MockServiceMessageProvider) -> Self {
        self.next_message_provider.lock().replace(provider);
        self
    }
}

#[async_trait]
impl ServiceMessageProvider for MockServiceMessageProvider {
    async fn get_latest_message(
        &self,
        _configuration: Option<&Any>,
    ) -> anyhow::Result<Option<Box<dyn MessageDyn>>> {
        if let Some(err) = self.next_error.lock().take() {
            Err(err)
        } else if let Some(message_provider) = &self.message_provider {
            Ok(Some(message_provider()))
        } else {
            Ok(None)
        }
    }
}

impl MockServiceMessageProvider {
    pub(crate) fn new() -> Self {
        Self { message_provider: None, next_error: None.into() }
    }

    pub(crate) fn set_message_provider(
        &mut self,
        message_provider: impl Fn() -> Box<dyn MessageDyn> + Send + Sync + 'static,
    ) {
        self.message_provider = Some(Box::new(message_provider));
    }

    pub(crate) fn with_next_error(self, error: anyhow::Error) -> Self {
        self.next_error.lock().replace(error);
        self
    }
}
