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

use futures::stream;
use futures::stream::BoxStream;
use futures::StreamExt;
use log::error;
use parking_lot::Mutex;
use std::sync::Arc;
use binder::Interface;
use binder::Strong;
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisher::IConfigurablePublisher;
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::ISubscriber::ISubscriber;
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisherSubscription::IConfigurablePublisherSubscription;
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisherSubscription::BnConfigurablePublisherSubscription;
use binder::BinderFeatures;
use binder::IntoBinderResult;
use tokio_util::sync::CancellationToken;

use crate::codec::Codec;
use crate::PublisherMetadata;

/// Trait implemented by publishers.
pub trait Publisher: Send + 'static
where
    // The message type must be `Send`, because it is used inside of `BoxStream`, which requires it
    // to be `Send.`
    <Self::MessageCodec as Codec>::T: Send,
{
    /// The codec used to encode/decode messages.
    type MessageCodec: Codec;
    /// The codec used to encode/decode configs.
    type ConfigCodec: Codec;

    /// Get the publisher's metadata
    fn get_metadata(&mut self) -> PublisherMetadata;

    /// Subscribe to the publisher's messages.
    fn subscribe(
        &mut self,
        _config: Option<<Self::ConfigCodec as Codec>::T>,
    ) -> BoxStream<'static, <Self::MessageCodec as Codec>::T> {
        stream::pending().boxed()
    }

    /// Get the publisher's current message, if available.
    fn get_on_demand(
        &mut self,
        _config: Option<<Self::ConfigCodec as Codec>::T>,
    ) -> Option<<Self::MessageCodec as Codec>::T> {
        None
    }
}

pub(crate) struct PublisherBinder<P: Publisher> {
    publisher: Arc<Mutex<P>>,
}

impl<P: Publisher> PublisherBinder<P> {
    pub(crate) fn new(publisher: P) -> Self {
        Self { publisher: Arc::new(Mutex::new(publisher)) }
    }
}

impl<P: Publisher> Interface for PublisherBinder<P> {}

impl<P: Publisher> IConfigurablePublisher for PublisherBinder<P> {
    fn createSubscription(
        &self,
        subscriber: &Strong<dyn ISubscriber>,
        config: Option<&[u8]>,
    ) -> binder::Result<Strong<dyn IConfigurablePublisherSubscription>> {
        let config = config
            .map(P::ConfigCodec::decode)
            .transpose()
            .or_binder_exception(binder::ExceptionCode::ILLEGAL_ARGUMENT)?;

        let mut publisher_mutex_guard = self.publisher.lock();
        let stream = publisher_mutex_guard.subscribe(config);
        drop(publisher_mutex_guard);

        let subscriber = Strong::clone(subscriber);
        let cancellation_token = CancellationToken::new();
        let cancellation_token_clone = cancellation_token.clone();
        tokio::spawn(async move {
            stream
                .take_until(cancellation_token_clone.cancelled())
                .for_each(|message| async {
                    let Ok(message) = P::MessageCodec::encode(message)
                        .inspect_err(|err| error!("Failed to encode message: {err:?}"))
                    else {
                        return;
                    };

                    let Ok(()) = subscriber
                        .sendData(&message)
                        .inspect_err(|err| error!("Failed to publish message: {err:?}"))
                    else {
                        return;
                    };
                })
                .await
        });

        struct SubscriptionBinder {
            cancellation_token: CancellationToken,
        }

        impl binder::Interface for SubscriptionBinder {}

        impl IConfigurablePublisherSubscription for SubscriptionBinder {
            fn unsubscribe(&self) -> binder::Result<()> {
                self.cancellation_token.cancel();
                Ok(())
            }
        }

        Ok(BnConfigurablePublisherSubscription::new_binder(
            SubscriptionBinder { cancellation_token },
            BinderFeatures::default(),
        ))
    }

    fn getLatestMessage(&self, config: Option<&[u8]>) -> binder::Result<Option<Vec<u8>>> {
        let config = config
            .map(P::ConfigCodec::decode)
            .transpose()
            .or_binder_exception(binder::ExceptionCode::ILLEGAL_ARGUMENT)?;

        let mut publisher_mutex_guard = self.publisher.lock();
        let message = publisher_mutex_guard.get_on_demand(config);
        drop(publisher_mutex_guard);

        let message = message
            .map(P::MessageCodec::encode)
            .transpose()
            .or_binder_exception(binder::ExceptionCode::SERVICE_SPECIFIC)?;
        Ok(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::stream::BoxStream;
    use futures::StreamExt;
    use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisherRegistry::MockIConfigurablePublisherRegistry;
    use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::ISubscriber::BnSubscriber;
    use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::ISubscriber::MockISubscriber;
    use googletest::expect_that;
    use googletest::matchers::container_eq;
    use googletest::matchers::eq;
    use googletest::matchers::ok;
    use googletest::matchers::some;
    use mockall::mock;
    use mockall::predicate as m;
    use mockall::Sequence;
    use protobuf::MessageFull;
    use protobuf::well_known_types::wrappers::Int32Value;
    use protobuf::well_known_types::wrappers::StringValue;
    use sdv_telemetry_test_utils::oneshot_callback;
    use std::sync::LazyLock;
    use tokio::task::yield_now;

    use crate::codec::ProtobufCodec;
    use crate::PublisherName;
    use crate::Registry;

    static METADATA: LazyLock<PublisherMetadata> = LazyLock::new(|| PublisherMetadata {
        name: "test_publisher".parse::<PublisherName>().unwrap(),
        file_descriptor_set: vec![],
        message_full_name: StringValue::descriptor().full_name().to_owned(),
        supports_on_demand_fetching: true,
    });

    mock! {
        PublisherImpl {}

        impl Publisher for PublisherImpl {
            type MessageCodec = ProtobufCodec<StringValue>;
            type ConfigCodec = ProtobufCodec<Int32Value>;

            fn get_metadata(&mut self) -> PublisherMetadata;
            fn subscribe(&mut self, config: Option<Int32Value>) -> BoxStream<'static, StringValue>;
            fn get_on_demand(&mut self, config: Option<Int32Value>) -> Option<StringValue>;
        }
    }

    #[sdv_telemetry_test_utils::timeout]
    #[googletest::test]
    #[tokio::test]
    async fn get_on_demand_success() {
        let mut publisher = MockPublisherImpl::new();
        publisher.expect_get_on_demand().with(m::eq(None)).return_once(|_config| {
            Some(StringValue { value: "test".to_owned(), ..Default::default() })
        });

        let publisher = PublisherBinder::new(publisher);

        let result = publisher.getLatestMessage(None);

        expect_that!(result, ok(some(container_eq(b"\x0a\x04test".to_owned()))));
    }

    #[sdv_telemetry_test_utils::timeout]
    #[googletest::test]
    #[tokio::test]
    async fn subscribe_success() {
        let mut seq = Sequence::new();

        let mut publisher = MockPublisherImpl::new();
        publisher.expect_subscribe().with(m::eq(None)).return_once(|_config| {
            stream::iter(vec![
                StringValue { value: "a".to_owned(), ..Default::default() },
                StringValue { value: "b".to_owned(), ..Default::default() },
                StringValue { value: "c".to_owned(), ..Default::default() },
            ])
            .boxed()
        });

        let mut subscriber = MockISubscriber::new();
        subscriber
            .expect_sendData()
            .once()
            .in_sequence(&mut seq)
            .with(m::eq(b"\x0a\x01a".to_owned()))
            .return_once(|_message| Ok(()));
        subscriber
            .expect_sendData()
            .once()
            .in_sequence(&mut seq)
            .with(m::eq(b"\x0a\x01b".to_owned()))
            .return_once(|_message| Ok(()));
        subscriber
            .expect_sendData()
            .once()
            .in_sequence(&mut seq)
            .with(m::eq(b"\x0a\x01c".to_owned()))
            .return_once(|_message| Ok(()));

        let publisher = PublisherBinder::new(publisher);
        let subscriber = BnSubscriber::new_binder(subscriber, BinderFeatures::default());

        let subscription = publisher.createSubscription(&subscriber, None);
        expect_that!(&subscription, ok(googletest::matchers::anything()));

        // Give it some time to process the message stream.
        yield_now().await;
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    #[googletest::test]
    async fn handles_multiple_subscriptions_and_on_demand_calls() {
        let (tx, rx) = oneshot_callback();
        let mut registry_mock = MockIConfigurablePublisherRegistry::new();
        registry_mock
            .expect_registerPublisher()
            .once()
            .withf(|publisher_info, _publisher_binder| {
                publisher_info.serviceName == METADATA.name.0
            })
            .return_once(move |_, publisher_binder| {
                tx(Strong::clone(publisher_binder));
                Ok(())
            });
        registry_mock
            .expect_unregisterPublisher()
            .once()
            .with(m::eq("test_publisher"))
            .return_once(|_| Ok(()));
        let mut registry = Registry::new_mocked(registry_mock);

        let mut publisher = MockPublisherImpl::new();
        publisher.expect_get_metadata().return_once(|| METADATA.clone());

        // Pairs of config -> expected messages.
        let subscribe_calls = [
            (None, vec![StringValue { value: "0".to_owned(), ..Default::default() }]),
            (None, vec![]),
            (
                Some(Int32Value { value: 7, ..Default::default() }),
                vec![
                    StringValue { value: "1".to_owned(), ..Default::default() },
                    StringValue { value: "2".to_owned(), ..Default::default() },
                    StringValue { value: "3".to_owned(), ..Default::default() },
                ],
            ),
            (Some(Int32Value { value: 7, ..Default::default() }), vec![]),
        ];

        // Pairs of config -> expected message.
        let on_demand_calls = [
            (None, Some(StringValue { value: "0".to_owned(), ..Default::default() })),
            (None, None),
            (
                Some(Int32Value { value: 7, ..Default::default() }),
                Some(StringValue { value: "7".to_owned(), ..Default::default() }),
            ),
            (Some(Int32Value { value: 7, ..Default::default() }), None),
        ];

        for (config, messages) in subscribe_calls.clone().into_iter() {
            publisher
                .expect_subscribe()
                .once()
                .with(m::eq(config))
                .return_once(move |_config| stream::iter(messages).boxed());
        }

        for (config, message) in on_demand_calls.clone().into_iter() {
            publisher
                .expect_get_on_demand()
                .once()
                .with(m::eq(config))
                .return_once(move |_config| message);
        }

        let registration = registry.register(publisher).unwrap();
        let publisher_binder = rx.await;

        for (config, messages) in subscribe_calls.into_iter() {
            let mut subscriber = MockISubscriber::new();
            let mut seq = Sequence::new();
            for message in messages.into_iter() {
                subscriber
                    .expect_sendData()
                    .once()
                    .in_sequence(&mut seq)
                    .with(m::eq(ProtobufCodec::encode(message).unwrap()))
                    .return_once(|_message| Ok(()));
            }
            let subscriber = BnSubscriber::new_binder(subscriber, BinderFeatures::default());
            let _subscription = publisher_binder.createSubscription(
                &subscriber,
                config.map(|config| ProtobufCodec::encode(config).unwrap()).as_deref(),
            );
        }

        for (config, message) in on_demand_calls.into_iter() {
            let result = publisher_binder
                .getLatestMessage(
                    config.map(|config| ProtobufCodec::encode(config).unwrap()).as_deref(),
                )
                .map(|maybe_message| {
                    maybe_message.map(|message| ProtobufCodec::decode(&message).unwrap())
                });
            expect_that!(result, ok(eq(&message)));
        }

        // Give it some time to process the message streams.
        yield_now().await;

        expect_that!(registry.unregister(registration), ok(eq(&())));
    }
}
