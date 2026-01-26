// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;

use parking_lot::Mutex;
use std::future;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::sync::{oneshot, Notify};

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
    availability_callback: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    cancellation_callback: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
    message_provider_returned_sender: Mutex<mpsc::UnboundedSender<()>>,
    message_provider_returned_receiver: AsyncMutex<mpsc::UnboundedReceiver<()>>,
    next_error: Mutex<Option<anyhow::Error>>,
    next_register_availability_callback_error: Mutex<Option<anyhow::Error>>,
    next_message_provider: Mutex<Option<MockServiceMessageProvider>>,
    next_message_provider_get_not_supported: Mutex<bool>,
    num_get_message_provider_calls: AtomicU32,
    num_subscribe_calls: AtomicU32,
    num_unsubscribe_calls: AtomicU32,
    num_register_availability_callback_calls: AtomicU32,
    num_unregister_availability_callback_calls: Arc<AtomicU32>,
    new_message_callback:
        AsyncMutex<Option<Box<dyn for<'a> FnMut(&'a Arc<dyn MessageDyn>) + Send + Sync>>>,
    return_availability_callback_registration: bool,
    successful_subscribe_sender: Mutex<Option<oneshot::Sender<()>>>,
}

pub(crate) struct MockServiceMessageProvider {
    message_provider: Option<Box<dyn Fn() -> Box<dyn MessageDyn> + Send + Sync>>,
    next_error: Mutex<Option<anyhow::Error>>,
}

pub(crate) struct MockAvailabilityCallbackRegistration {
    num_unregister_availability_callback_calls: Arc<AtomicU32>,
}

#[async_trait]
impl ServiceAdapter for MockServiceAdapter {
    async fn get_message_provider(
        &self,
        _service_name: &str,
    ) -> Result<GetMessageProviderOutcome, ServicePublisherAccessorError> {
        self.num_get_message_provider_calls.fetch_add(1, Ordering::AcqRel);

        let sender = self.message_provider_returned_sender.lock();
        let _ = sender.send(());

        if let Some(error) = self.next_error.lock().take() {
            return Err(ServicePublisherAccessorError::MetadataParsingFailed(error));
        }

        let mut get_not_supported = self.next_message_provider_get_not_supported.lock();
        if *get_not_supported {
            *get_not_supported = false;
            return Ok(GetMessageProviderOutcome::GetNotSupported);
        }

        if let Some(message_provider) = self.next_message_provider.lock().take() {
            Ok(GetMessageProviderOutcome::Success(Box::new(message_provider)))
        } else {
            Ok(GetMessageProviderOutcome::NotRegistered)
        }
    }

    async fn subscribe(
        self: Arc<Self>,
        _service_name: &str,
        configuration: Option<&Any>,
        _fetch_last_message: bool,
        _sub_sampling_interval: &SubSamplingInterval,
        new_message_callback: Box<dyn for<'a> FnMut(&'a Arc<dyn MessageDyn>) + Send + Sync>,
        cancellation_callback: Box<dyn FnOnce() + Send + Sync>,
    ) -> Result<Option<ServiceSubscription>, ServicePublisherAccessorError> {
        self.num_subscribe_calls.fetch_add(1, Ordering::AcqRel);

        if let Some(configuration) = configuration {
            self.latest_configuration.lock().replace(configuration.clone());
        }

        if let Some(error) = self.next_error.lock().take() {
            return Err(ServicePublisherAccessorError::MetadataParsingFailed(error));
        }

        if self.block_subscribe {
            self.notify_subscribing.notify_one();
            future::pending::<()>().await;
        }

        let maybe_sender = self.successful_subscribe_sender.lock().take();
        if let Some(sender) = maybe_sender {
            self.new_message_callback.lock().await.replace(new_message_callback);
            self.cancellation_callback.lock().replace(cancellation_callback);
            let _ = sender.send(());
            Ok(Some(ServiceSubscription {
                subscription_type: ServiceSubscriptionType::Shared {
                    service_name: String::default(),
                    subscription_id: SequenceNumberType::MAX,
                    sub_sampling_interval: SubSamplingInterval::MAX,
                    fetch_last_message_outcome: FetchLastMessageOutcome::NotSupported,
                },
            }))
        } else {
            Ok(None)
        }
    }

    async fn unsubscribe(&self, _subscription: ServiceSubscription) {
        self.num_unsubscribe_calls.fetch_add(1, Ordering::AcqRel);

        if self.block_unsubscribe {
            self.notify_unsubscribing.notify_one();
            future::pending::<()>().await;
        }
    }

    async fn register_availability_callback(
        &self,
        _service_name: &str,
        callback: Box<dyn Fn() + Send + Sync>,
    ) -> anyhow::Result<Option<Box<dyn AvailabilityCallbackRegistration>>> {
        if let Some(error) = self.next_register_availability_callback_error.lock().take() {
            return Err(error);
        }
        self.num_register_availability_callback_calls.fetch_add(1, Ordering::AcqRel);
        self.availability_callback.lock().replace(callback);
        if self.return_availability_callback_registration {
            Ok(Some(Box::new(MockAvailabilityCallbackRegistration {
                num_unregister_availability_callback_calls: Arc::clone(
                    &self.num_unregister_availability_callback_calls,
                ),
            })))
        } else {
            Ok(None)
        }
    }
}

impl MockServiceAdapter {
    pub(crate) fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            availability_callback: None.into(),
            block_subscribe: false,
            block_unsubscribe: false,
            cancellation_callback: None.into(),
            successful_subscribe_sender: None.into(),
            latest_configuration: None.into(),
            message_provider_returned_sender: tx.into(),
            message_provider_returned_receiver: rx.into(),
            next_error: None.into(),
            next_register_availability_callback_error: None.into(),
            next_message_provider: None.into(),
            next_message_provider_get_not_supported: false.into(),
            new_message_callback: None.into(),
            notify_subscribing: Notify::new().into(),
            notify_unsubscribing: Notify::new().into(),
            num_get_message_provider_calls: 0.into(),
            num_subscribe_calls: 0.into(),
            num_unsubscribe_calls: 0.into(),
            num_register_availability_callback_calls: 0.into(),
            num_unregister_availability_callback_calls: Arc::new(0.into()),
            return_availability_callback_registration: false,
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

    pub(crate) fn get_num_register_availability_callback_calls(&self) -> u32 {
        self.num_register_availability_callback_calls.load(Ordering::Acquire)
    }

    pub(crate) fn get_num_unregister_availability_callback_calls(&self) -> u32 {
        self.num_unregister_availability_callback_calls.load(Ordering::Acquire)
    }

    pub(crate) fn call_availability_callback(&self) {
        self.availability_callback.lock().as_ref().unwrap()();
    }

    pub(crate) async fn wait_message_provider_returned(&self) {
        let mut rx = self.message_provider_returned_receiver.lock().await;
        rx.recv().await.unwrap()
    }

    pub(crate) fn set_next_message_provider(&self, provider: MockServiceMessageProvider) {
        self.next_message_provider.lock().replace(provider);
    }

    pub(crate) fn set_next_subscribe_successful(&self) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        self.successful_subscribe_sender.lock().replace(tx);
        rx
    }

    pub(crate) fn notify_cancelled(&self) {
        self.cancellation_callback.lock().take().unwrap()();
    }

    pub(crate) async fn publish_new_message(&self, message: &Arc<dyn MessageDyn>) {
        self.new_message_callback.lock().await.as_mut().unwrap()(message);
    }

    pub(crate) fn with_availability_callback(mut self) -> Self {
        self.return_availability_callback_registration = true;
        self
    }

    pub(crate) fn with_next_error(self, error: anyhow::Error) -> Self {
        self.next_error.lock().replace(error);
        self
    }

    pub(crate) fn set_next_error(&self, error: anyhow::Error) {
        self.next_error.lock().replace(error);
    }

    pub(crate) fn with_next_register_availability_callback_error(
        self,
        error: anyhow::Error,
    ) -> Self {
        self.next_register_availability_callback_error.lock().replace(error);
        self
    }

    pub(crate) fn with_next_message_provider_get_not_supported(self) -> Self {
        *self.next_message_provider_get_not_supported.lock() = true;
        self
    }
}

#[async_trait]
impl ServiceMessageProvider for MockServiceMessageProvider {
    async fn get_latest_message(
        &self,
        _configuration: Option<&Any>,
    ) -> Result<Option<Box<dyn MessageDyn>>, ServicePublisherAccessorError> {
        if let Some(err) = self.next_error.lock().take() {
            Err(ServicePublisherAccessorError::MessageParsingFailed(err))
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

#[async_trait]
impl AvailabilityCallbackRegistration for MockAvailabilityCallbackRegistration {
    async fn unregister(self: Box<Self>) {
        self.num_unregister_availability_callback_calls.fetch_add(1, Ordering::AcqRel);
    }
}
