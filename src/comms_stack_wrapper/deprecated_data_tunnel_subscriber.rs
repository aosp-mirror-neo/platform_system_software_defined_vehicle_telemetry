// Copyright 2023 Google LLC

//! Telemetry subscription logic for DT.

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use log::{error, info, trace};
use parking_lot::Mutex;
use protobuf::reflect::{FileDescriptor, MessageDescriptor};
use protobuf::well_known_types::any::Any;
use protobuf::{Message, MessageDyn};
use std::cell::LazyCell;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::iter::Iterator;
use std::sync::{Arc, Weak};
use tokio::select;
use tokio::sync::Notify;
use tokio::time::{sleep, Duration, Instant};
use tokio_util::sync::{CancellationToken, DropGuard};

use sdv_telemetry::service_publisher_accessor::ServicePublisherAccessor;
use sdv_telemetry::service_publisher_accessor::{
    AvailabilityCallbackRegistration, CreateSubscriptionOutcome, ServicePublisherMessageProvider,
    ServicePublisherMetadata, ServicePublisherSubscription,
};
use sdv_telemetry::sub_sampling_interval::SubSamplingInterval;
use sdv_telemetry_metadata_proto_rust::publisher_metadata::TelemetryPublisherMetadata;
use sdv_telemetry_utils::descriptor_utils::WELL_KNOWN_FILE_DESCRIPTORS;

use crate::data_tunnel_api_client::{
    self, DataTunnelError, ReadMessagesError, SubscriptionDescriptor,
};

type Callback = Mutex<Box<dyn FnMut(Box<dyn MessageDyn>) + Send + Sync>>;

type TopicSubscription = (
    Arc<SubscriptionDescriptor>,
    Arc<Notify>,
    SubSamplingInterval, // calculated sub-sampling interval
    Option<DropGuard>,   // `DropGuard` for a task to send a synthetic notification
    HashMap<
        SubSamplingInterval, // actually requested sub-sampling interval
        (
            // Weakly hold onto the set of callbacks for this (topic, sub_sampling_interval).
            Vec<Weak<Callback>>,
            Option<Instant>, // last time the callbacks were called
        ),
    >,
);

type TopicSubscriptions = Mutex<
    HashMap<
        String, // topic name
        TopicSubscription,
    >,
>;

/// Struct for storing all data tunnel subscriptions that implements `ServicePublisherManager`.
pub struct DeprecatedDataTunnelSubscriber {
    /// Hashmap to store topic names and their `MessageDescriptor`s.
    topic_message_descriptors: Mutex<HashMap<String, MessageDescriptor>>,
    /// Hashmap to store topic names and their associated subscription data.
    /// TODO: b/349242614: Remove temporary workaround once unsubscribe is supported
    topic_subscriptions: Arc<TopicSubscriptions>,
}

/// Struct for subscription of DT topic.
struct DataTunnelSubscription {
    _subscription_descriptor: Arc<SubscriptionDescriptor>,

    topic_name: String,
    sub_sampling_interval: SubSamplingInterval,
    // The callback associated with this subscription. `DeprecatedDataTunnelSubscriber` only holds a
    // weak reference to it, so dropping `Self` is equivalent to unsubscribing.
    callback: Arc<Callback>,
    topic_subscriptions: Weak<TopicSubscriptions>,
}

struct AvailabilityCallbackRegistrationImpl {}

impl Default for DeprecatedDataTunnelSubscriber {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ServicePublisherAccessor for DeprecatedDataTunnelSubscriber {
    async fn get_metadata(
        &self,
        topic_name: &str,
    ) -> anyhow::Result<Option<ServicePublisherMetadata>> {
        self.get_metadata_impl(topic_name)
    }

    async fn get_message_provider(
        &self,
        _service_name: &str,
    ) -> anyhow::Result<Option<Box<dyn ServicePublisherMessageProvider>>> {
        Ok(None)
    }

    async fn create_subscription(
        &self,
        topic_name: &str,
        _configuration: Option<&Any>,
        sub_sampling_interval: &SubSamplingInterval,
        callback: Box<dyn FnMut(Box<dyn MessageDyn>) + Send + Sync>,
    ) -> anyhow::Result<CreateSubscriptionOutcome> {
        info!("Subscribing to data tunnel topic {}", topic_name);

        match self.topic_subscriptions.lock().entry(topic_name.to_owned()) {
            Entry::Occupied(mut o) => {
                let (
                    subscription_descriptor,
                    notify,
                    stored_calculated_sub_sampling_interval,
                    _,
                    ref mut callback_map,
                ) = o.get_mut();

                let entry = callback_map.entry(sub_sampling_interval.to_owned()).or_default();
                let (stored_callbacks, _) = entry;

                // Check if the currently used sub-sampling interval needs adjustment now that
                // we have another callback with its own sub-sampling interval.
                let new_calculated_sub_sampling_interval =
                    stored_calculated_sub_sampling_interval.gcd_or_zero(sub_sampling_interval);
                if &new_calculated_sub_sampling_interval != stored_calculated_sub_sampling_interval
                {
                    data_tunnel_api_client::set_notification_handler(
                        subscription_descriptor,
                        Arc::clone(notify),
                        &new_calculated_sub_sampling_interval,
                    )
                    .with_context(|| {
                        format!("Failed to update sub-sampling interval for topic {topic_name}")
                    })?;
                }

                let callback = Arc::new(Mutex::new(callback));
                stored_callbacks.push(Arc::downgrade(&callback));

                return Ok(CreateSubscriptionOutcome::Subscribed(Box::new(
                    DataTunnelSubscription {
                        _subscription_descriptor: Arc::clone(subscription_descriptor),
                        topic_name: topic_name.to_owned(),
                        sub_sampling_interval: sub_sampling_interval.to_owned(),
                        callback,
                        topic_subscriptions: Arc::downgrade(&self.topic_subscriptions),
                    },
                )));
            }
            Entry::Vacant(v) => {
                cxx::let_cxx_string!(cxx_topic_name = topic_name);
                let topic_exists = data_tunnel_api_client::topic_exists(&cxx_topic_name)
                    .with_context(|| {
                        format!("Failed to check whether topic {topic_name} exists.")
                    })?;
                if !topic_exists {
                    return Ok(CreateSubscriptionOutcome::NotRegistered(callback));
                }

                let subscription_descriptor =
                    data_tunnel_api_client::subscribe(&cxx_topic_name).map(Arc::new).with_context(
                        || format!("Failed to subscribe to data tunnel topic {topic_name}"),
                    )?;

                let notify = Arc::new(Notify::new());
                data_tunnel_api_client::set_notification_handler(
                    &subscription_descriptor,
                    Arc::clone(&notify),
                    sub_sampling_interval,
                )
                .with_context(|| {
                    format!("Failed to set notification handler for data tunnel topic {topic_name}")
                })?;

                let sdv_dt::TopicTypedMetadata {
                    message_count_: message_count,
                    message_size_: message_size,
                } = subscription_descriptor.dt_metadata;
                let mut buffer = vec![0u8; message_count * message_size];

                // Discard all existing data after subscribing to the topic. We do not want to trigger on
                // old data (b/344644795).
                match Self::read(&subscription_descriptor, topic_name, &mut buffer) {
                    Ok(number_of_messages) => {
                        info!("Discarded {number_of_messages} old messages for topic {topic_name}.")
                    }
                    Err(err) => {
                        error!("Could not read old messages for topic {topic_name}: {err:?}")
                    }
                };

                let message_descriptor = self
                    .get_metadata_impl(topic_name)?
                    .ok_or_else(|| anyhow!("Metadata not found for topic: {}", topic_name))?
                    .message_descriptor;

                let subscription_descriptor_clone = Arc::clone(&subscription_descriptor);
                let topic_subscriptions_clone = Arc::clone(&self.topic_subscriptions);
                let notify_clone = Arc::clone(&notify);
                let topic_name_clone = topic_name.to_owned();
                // Tokio task for fetching data from data tunnel and pushing it to telemetry service.
                tokio::spawn(async move {
                    // This loop runs infinitely until the telemetry service stops. In case of an error
                    // while message parsing, the data is lost, as data was retrieved successfully from the
                    // data tunnel. In case of data tunnel errors, data is not lost, and can be fetched in
                    // the next iteration of the loop.
                    //
                    // TODO(b/329515273): Add logic to stop the loop once data tunnel provides support for
                    // unsubscribing.
                    loop {
                        // Wait for the next message(s) to arrive.
                        notify_clone.notified().await;

                        let number_of_messages = match Self::read(
                            &subscription_descriptor_clone,
                            &topic_name_clone,
                            &mut buffer,
                        ) {
                            Ok(number_of_messages) => number_of_messages,
                            Err(err) => {
                                error!(
                                    "Could not read messages for topic {topic_name_clone}: {err:?}"
                                );
                                continue;
                            }
                        };
                        info!(
                            "Received {number_of_messages} messages for topic {topic_name_clone}."
                        );

                        let mut topic_subscriptions_lock = topic_subscriptions_clone.lock();
                        let Some(topic_subscription) =
                            topic_subscriptions_lock.get_mut(&topic_name_clone)
                        else {
                            trace!("No more subscriber to topic: {topic_name_clone}");
                            continue;
                        };

                        let messages = buffer
                            .chunks(message_size)
                            .take(number_of_messages)
                            .map(|message| {
                                // By using `LazyCell`, we avoid unnecessarily parsing messages
                                // which may not end up being read.
                                LazyCell::new(||
                                    Self::read_length_delimited_proto(message, &message_descriptor)
                                        .inspect_err(|err| {
                                            error!("Failed to convert bytes to protobuf for topic {topic_name_clone}: {err:?}");
                                        })
                                        .ok()
                                )
                            })
                            .collect::<Vec<_>>();

                        Self::handle_messages(
                            topic_name_clone.clone(),
                            None,
                            topic_subscription,
                            &messages,
                            Arc::clone(&topic_subscriptions_clone),
                        );
                    }
                });

                let callback = Arc::new(Mutex::new(callback));
                let callback_map = HashMap::from([(
                    sub_sampling_interval.to_owned(),
                    (vec![Arc::downgrade(&callback)], None),
                )]);

                v.insert((
                    Arc::clone(&subscription_descriptor),
                    notify,
                    sub_sampling_interval.to_owned(),
                    None,
                    callback_map,
                ));

                let data_tunnel_subscription = Box::new(DataTunnelSubscription {
                    _subscription_descriptor: subscription_descriptor,
                    topic_name: topic_name.to_owned(),
                    sub_sampling_interval: sub_sampling_interval.to_owned(),
                    callback,
                    topic_subscriptions: Arc::downgrade(&self.topic_subscriptions),
                });

                Ok(CreateSubscriptionOutcome::Subscribed(data_tunnel_subscription))
            }
        }
    }

    async fn register_availability_callback(
        &self,
        _service_name: &str,
        _callback: &Arc<dyn Fn() + Send + Sync>,
    ) -> anyhow::Result<Option<Box<dyn AvailabilityCallbackRegistration>>> {
        // TODO(b/376937258): Implement availability notifications.
        Ok(Some(Box::new(AvailabilityCallbackRegistrationImpl {})))
    }
}

/// Subscriber struct impl that encapsulates the data required for reading messages.
impl DeprecatedDataTunnelSubscriber {
    /// Creates a new instance of the data tunnel subscriber.
    pub fn new() -> Self {
        Self {
            topic_message_descriptors: HashMap::new().into(),
            topic_subscriptions: Arc::new(HashMap::new().into()),
        }
    }

    fn get_metadata_impl(
        &self,
        topic_name: &str,
    ) -> anyhow::Result<Option<ServicePublisherMetadata>> {
        let message_descriptor = match self
            .topic_message_descriptors
            .lock()
            .entry(topic_name.to_owned())
        {
            Entry::Occupied(o) => o.get().clone(),
            Entry::Vacant(v) => {
                cxx::let_cxx_string!(cxx_topic_name = topic_name);
                let topic_exists = data_tunnel_api_client::topic_exists(&cxx_topic_name)
                    .with_context(|| {
                        format!("Failed to check whether topic {topic_name} exists.")
                    })?;
                if !topic_exists {
                    return Ok(None);
                }

                let publisher_metadata =
                    data_tunnel_api_client::get_topic_metadata(&cxx_topic_name).with_context(
                        || format!("Failed to get publisher metadata for topic {topic_name}."),
                    )?;
                let publisher_metadata =
                    TelemetryPublisherMetadata::parse_from_bytes(publisher_metadata.as_slice())
                        .with_context(|| {
                            format!("Failed to parse publisher metadata for topic {topic_name}.")
                        })?;

                // Build the provided proto file descriptors.
                let file_descriptors = FileDescriptor::new_dynamic_fds(
                    publisher_metadata.descriptor_protos,
                    &WELL_KNOWN_FILE_DESCRIPTORS,
                )
                .map_err(|e| anyhow!("Invalid Descriptor Protos: {:?}", e))?;

                // Iterate through all `file_descriptors` and find `MessageDescriptor` for the given
                // `message_type_name`. `message_by_full_name()` function requires the name to start
                // with a dot. The function returns a clone of the `MessageDescriptor`, so we can
                // return it as-is without cloning again.
                let message_descriptor = file_descriptors
                    .iter()
                    .find_map(|fd| fd.message_by_full_name(&publisher_metadata.message_type_name))
                    .ok_or(anyhow!(
                        "Message Name {} not found in descriptor",
                        publisher_metadata.message_type_name
                    ))?;

                v.insert(message_descriptor.clone());

                message_descriptor
            }
        };

        Ok(Some(ServicePublisherMetadata { supports_get: false, message_descriptor }))
    }

    /// Handles the provided messages for the given topic and subscription.
    ///
    /// `only_process_messages_for`, if `Some(_)` will only process the provided messages for
    /// callbacks registered for a sub-sampling interval specified in it.
    fn handle_messages<M>(
        topic_name: String,
        only_process_messages_for: Option<Vec<SubSamplingInterval>>,
        topic_subscription: &mut TopicSubscription,
        messages: &[M],
        topic_subscriptions: Arc<TopicSubscriptions>,
    ) where
        M: std::ops::Deref<Target = Option<Box<dyn protobuf::MessageDyn>>> + Sized,
    {
        let (_, _, _, ref mut drop_guard, ref mut callback_map) = topic_subscription;
        *drop_guard = None;

        struct SyntheticNotificationOptions {
            notify_after: Duration,
            only_process_messages_for: Vec<SubSamplingInterval>,
        }
        impl SyntheticNotificationOptions {
            fn combine(mut self, other: Self) -> Self {
                self.only_process_messages_for.extend(other.only_process_messages_for);
                self.notify_after = self.notify_after.min(other.notify_after);
                self
            }
        }

        let mut synthetic_notification_options: Option<SyntheticNotificationOptions> = None;
        let now = Instant::now();
        for (sub_sampling_interval, (callbacks, ref mut last_call)) in callback_map.iter_mut() {
            if only_process_messages_for.as_ref().is_some_and(|only_process_messages_for| {
                !only_process_messages_for.contains(sub_sampling_interval)
            }) {
                continue;
            }

            if callbacks.is_empty() {
                // If no more subscriber exists, skip all of the work.
                continue;
            }

            let outstanding_duration = last_call.map_or(Duration::ZERO, |last_call| {
                let duration_since_last_call = now.duration_since(last_call);
                sub_sampling_interval.as_duration().saturating_sub(duration_since_last_call)
            });

            if outstanding_duration.as_millis() >= 1 {
                assert_ne!(
                    sub_sampling_interval,
                    &SubSamplingInterval::ZERO,
                    "If there is an outstanding duration, the sub-sampling interval cannot be 0 - this can never happen."
                );

                let options = SyntheticNotificationOptions {
                    notify_after: outstanding_duration,
                    only_process_messages_for: vec![sub_sampling_interval.clone()],
                };

                synthetic_notification_options = Some(match synthetic_notification_options {
                    None => options,
                    Some(synthetic_notification_options) => {
                        synthetic_notification_options.combine(options)
                    }
                });
                continue;
            }

            let messages: Box<dyn Iterator<Item = _>> = {
                if sub_sampling_interval == &SubSamplingInterval::ZERO {
                    Box::new(messages.iter())
                } else {
                    // Only process the last (=newest) message.
                    Box::new(messages.iter().rev().take(1))
                }
            };

            for message in messages {
                if let Some(message) = message.deref() {
                    for callback in callbacks.iter() {
                        if let Some(callback) = callback.upgrade() {
                            (callback.lock())(message.clone());
                        }
                    }
                }
            }

            *last_call = Some(now);
        }

        let Some(synthetic_notification_options) = synthetic_notification_options else {
            return;
        };

        let last_message =
            messages.iter().rev().take(1).next().and_then(|message| message.deref().clone());
        let Some(message) = last_message else {
            return;
        };

        let cancellation_token = CancellationToken::new();
        *drop_guard = Some(cancellation_token.clone().drop_guard());

        let topic_subscriptions = Arc::clone(&topic_subscriptions);
        tokio::spawn(async move {
            select! {
                biased;
                _ = cancellation_token.cancelled() => return,
                _ = sleep(synthetic_notification_options.notify_after) => {},
            };

            let mut topic_subscriptions_lock = topic_subscriptions.lock();
            let Some(topic_subscription) = topic_subscriptions_lock.get_mut(&topic_name) else {
                trace!("No more subscriber to topic: {topic_name}");
                return;
            };

            Self::handle_messages(
                topic_name,
                Some(synthetic_notification_options.only_process_messages_for),
                topic_subscription,
                &[Box::new(Some(message))],
                Arc::clone(&topic_subscriptions),
            );
        });
    }

    /// Read all available messages from the data tunnel and write them to the `buffer`.
    /// Returns how many messages were read into the buffer.
    fn read(
        subscription_descriptor: &SubscriptionDescriptor,
        topic_name: &str,
        buffer: &mut [u8],
    ) -> anyhow::Result<usize> {
        let read_message_count =
            data_tunnel_api_client::available_to_read(subscription_descriptor).or_else(|err| -> anyhow::Result<usize> {
                if err != sdv_dt::Result::Overflow {
                    return Err(err.into());
                }
                trace!("Data tunnel overflow for topic {topic_name} occurred.");

                // We have to invoke `ReadMessages()` after an Overflow error to reset the Overflow
                // flag.
                const NUM_MESSAGES: usize = 0;
                match data_tunnel_api_client::read_messages(subscription_descriptor, &mut [], NUM_MESSAGES) {
                    Err(ReadMessagesError::DataTunnelError(DataTunnelError(sdv_dt::Result::Overflow))) => {}
                    // Any other result, including `Ok`, is unexpected, and should be treated as an
                    // error.
                    Err(err) => return Err(
                        anyhow!("Failed to read message after data tunnel overflow for topic {topic_name}: {err:?}")
                    ),
                    Ok(_) => return Err(
                        anyhow!("Successfully read message after data tunnel overflow for topic {topic_name}, but expected an error.")
                    )
                };
                Ok(data_tunnel_api_client::available_to_read(subscription_descriptor)?)
            })
            .with_context(|| {
                format!("Failed to query how many messages are available to read for data tunnel topic {topic_name}")
            })?;

        if read_message_count > 0 {
            data_tunnel_api_client::read_messages(subscription_descriptor, buffer, read_message_count)
                .with_context(|| {
                    format!("Failed to read {read_message_count} messages for data tunnel topic {topic_name}")
                })?;
        }

        Ok(read_message_count)
    }

    fn read_length_delimited_proto(
        bytes: &[u8],
        message_descriptor: &MessageDescriptor,
    ) -> anyhow::Result<Box<dyn MessageDyn>> {
        let mut is = protobuf::CodedInputStream::from_bytes(bytes);
        let message_size = is
            .read_raw_varint32()
            .with_context(|| "Failed to read the size of the protobuf message.")?
            .into();
        is.push_limit(message_size)
            .with_context(|| "Failed to push the limit of the protobuf stream.")?;

        let message = message_descriptor
            .parse_from(&mut is)
            .with_context(|| "Failed to parse the protobuf messagw.")?;
        Ok(message)
    }
}

#[async_trait]
impl ServicePublisherSubscription for DataTunnelSubscription {
    async fn unsubscribe(mut self: Box<Self>) -> anyhow::Result<()> {
        self.unsubscribe_impl()?;
        Ok(())
    }
}

impl Drop for DataTunnelSubscription {
    fn drop(&mut self) {
        if let Err(err) = self.unsubscribe_impl() {
            error!("Failed to unsubscribe from topic {}: {err:?}", self.topic_name)
        }
    }
}

impl DataTunnelSubscription {
    fn unsubscribe_impl(&mut self) -> anyhow::Result<()> {
        let Some(topic_subscriptions) = self.topic_subscriptions.upgrade() else {
            return Ok(());
        };
        // Reset pointer in `self` so that we do not unsubscribe twice if `unsubscribe_impl` is called again.
        self.topic_subscriptions = Weak::new();

        let mut topic_subscriptions_lock = topic_subscriptions.lock();
        let Entry::Occupied(mut topic_subscription) =
            topic_subscriptions_lock.entry(self.topic_name.clone())
        else {
            return Ok(());
        };

        let (
            subscription_descriptor,
            notify,
            ref mut stored_calculated_sub_sampling_interval,
            _,
            ref mut callbacks_by_sub_sampling_interval_map,
        ) = topic_subscription.get_mut();

        let Some((ref mut callbacks_for_sub_sampling_interval, _last_call)) =
            callbacks_by_sub_sampling_interval_map.get_mut(&self.sub_sampling_interval)
        else {
            return Ok(());
        };

        let Some(callback_index) = callbacks_for_sub_sampling_interval
            .iter()
            .position(|each| each.upgrade().is_some_and(|each| Arc::ptr_eq(&each, &self.callback)))
        else {
            return Ok(());
        };
        callbacks_for_sub_sampling_interval.swap_remove(callback_index);

        if callbacks_for_sub_sampling_interval.is_empty() {
            callbacks_by_sub_sampling_interval_map.remove(&self.sub_sampling_interval);
        }

        if callbacks_by_sub_sampling_interval_map.is_empty() {
            topic_subscription.remove();
            return Ok(());
        }

        let Some(new_calculated_sub_sampling_interval) =
            callbacks_by_sub_sampling_interval_map.keys().cloned().reduce(|a, b| a.gcd_or_zero(&b))
        else {
            return Ok(());
        };
        if &new_calculated_sub_sampling_interval != stored_calculated_sub_sampling_interval {
            data_tunnel_api_client::set_notification_handler(
                subscription_descriptor,
                Arc::clone(notify),
                &new_calculated_sub_sampling_interval,
            )?;
            *stored_calculated_sub_sampling_interval = new_calculated_sub_sampling_interval;
        }

        Ok(())
    }
}

#[async_trait]
impl AvailabilityCallbackRegistration for AvailabilityCallbackRegistrationImpl {
    async fn unregister(self: Box<Self>) {}
}

#[cfg(all(test, unit_tests))]
mod tests {
    use super::*;

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_message_provider_success() -> anyhow::Result<()> {
        let subscriber = DeprecatedDataTunnelSubscriber::new();
        assert!(subscriber.get_message_provider("service-name").await?.is_none());

        Ok(())
    }
}

#[cfg(all(test, integration_tests))]
mod tests {
    use super::*;

    use futures::future::{join_all, ready};
    use futures::FutureExt;
    use protobuf::reflect::ReflectValueRef;
    use protobuf::MessageFull;
    use sdv_dt::TopicTypedMetadata;
    use sdv_telemetry_test_proto::test_message;
    use sdv_telemetry_test_proto::test_message::TestMessage;
    use sdv_telemetry_test_utils::unique_dt_topic_name;
    use std::iter::zip;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::time::{interval, sleep, Instant};

    use crate::data_tunnel_api_client::TopicDescriptor;

    fn register_topic(
        topic_name: &str,
        publisher_metadata: TelemetryPublisherMetadata,
    ) -> TopicDescriptor {
        let mut metadata = vec![];
        publisher_metadata.write_to_vec(&mut metadata).unwrap();
        cxx::let_cxx_string!(cxx_topic_name = &topic_name);
        data_tunnel_api_client::register_topic_with_metadata(
            &cxx_topic_name,
            TopicTypedMetadata { message_count_: 2, message_size_: 30 },
            metadata,
        )
        .unwrap()
    }

    fn write_message(topic_descriptor: &TopicDescriptor, message: TestMessage) {
        let max_message_size = topic_descriptor.dt_metadata.message_size_;

        let mut bytes = Vec::<u8>::with_capacity(max_message_size);
        message.write_length_delimited_to_vec(&mut bytes).unwrap();

        assert!(
            bytes.len() <= max_message_size,
            "Message is too long: {} bytes (max: {} bytes)",
            bytes.len(),
            max_message_size
        );
        bytes.resize(max_message_size, 0);

        data_tunnel_api_client::write_message(topic_descriptor, &bytes).unwrap();
    }

    fn get_int_field_value(message: &dyn MessageDyn) -> i32 {
        let int_field_descriptor = message.descriptor_dyn().field_by_name("int_field").unwrap();
        match int_field_descriptor.get_singular(message) {
            Some(ReflectValueRef::I32(value)) => value,
            None => 0,
            result => panic!("Unexpected value: {result:?}"),
        }
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_metadata_topic_not_registered_returns_none() {
        let topic_name = unique_dt_topic_name();

        let subscriber = DeprecatedDataTunnelSubscriber::new();
        // The topic should not exist yet, therefore metadata should be `None`.
        assert!(subscriber.get_metadata(&topic_name).await.unwrap().is_none());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_metadata_success() {
        let topic_name = unique_dt_topic_name();

        register_topic(
            &topic_name,
            TelemetryPublisherMetadata {
                descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
                message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        );

        let subscriber = DeprecatedDataTunnelSubscriber::new();
        let publisher_metadata = subscriber.get_metadata(&topic_name).await.unwrap().unwrap();
        assert!(!publisher_metadata.supports_get);
        assert_eq!(publisher_metadata.message_descriptor.name(), "TestMessage");
        assert_eq!(
            publisher_metadata.message_descriptor.proto(),
            TestMessage::descriptor().proto()
        );
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn create_subscription_when_topic_does_not_exist_failure() {
        let topic_name = unique_dt_topic_name();

        let subscriber = DeprecatedDataTunnelSubscriber::new();
        let outcome = subscriber
            .create_subscription(
                &topic_name,
                None,
                &SubSamplingInterval::ZERO,
                Box::new(|_| {
                    unreachable!();
                }),
            )
            .await
            .unwrap();
        assert!(matches!(outcome, CreateSubscriptionOutcome::NotRegistered(_)));
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn create_subscription_success() {
        let topic_name = unique_dt_topic_name();

        register_topic(
            &topic_name,
            TelemetryPublisherMetadata {
                descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
                message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        );

        let subscriber = DeprecatedDataTunnelSubscriber::new();
        let outcome = subscriber
            .create_subscription(
                &topic_name,
                None,
                &SubSamplingInterval::ZERO,
                Box::new(|_| {
                    unreachable!();
                }),
            )
            .await
            .unwrap();
        assert!(matches!(outcome, CreateSubscriptionOutcome::Subscribed(_)));
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn create_multiple_subscription_success() {
        let topic_name = unique_dt_topic_name();

        register_topic(
            &topic_name,
            TelemetryPublisherMetadata {
                descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
                message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        );

        let subscriber = DeprecatedDataTunnelSubscriber::new();
        let outcome1 = subscriber
            .create_subscription(
                &topic_name,
                None,
                &SubSamplingInterval::ZERO,
                Box::new(|_| {
                    unreachable!();
                }),
            )
            .await
            .unwrap();

        let outcome2 = subscriber
            .create_subscription(
                &topic_name,
                None,
                &SubSamplingInterval::ZERO,
                Box::new(|_| {
                    unreachable!();
                }),
            )
            .await
            .unwrap();
        assert!(matches!(outcome1, CreateSubscriptionOutcome::Subscribed(_)));
        assert!(matches!(outcome2, CreateSubscriptionOutcome::Subscribed(_)));
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn create_repeated_subscriptions_and_read_messages_success() {
        let topic_name = unique_dt_topic_name();

        let topic_descriptor = register_topic(
            &topic_name,
            TelemetryPublisherMetadata {
                descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
                message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        );

        let subscriber = DeprecatedDataTunnelSubscriber::new();

        let (tx1, mut rx1) = mpsc::unbounded_channel::<Box<dyn MessageDyn>>();
        let outcome1 = subscriber
            .create_subscription(
                &topic_name,
                None,
                &SubSamplingInterval::ZERO,
                Box::new(move |message| tx1.send(message).unwrap()),
            )
            .await
            .unwrap();

        let (tx2, mut rx2) = mpsc::unbounded_channel::<Box<dyn MessageDyn>>();
        let outcome2 = subscriber
            .create_subscription(
                &topic_name,
                None,
                &SubSamplingInterval::ZERO,
                Box::new(move |message| tx2.send(message).unwrap()),
            )
            .await
            .unwrap();

        write_message(&topic_descriptor, TestMessage { int_field: 1, ..Default::default() });
        write_message(&topic_descriptor, TestMessage { int_field: 2, ..Default::default() });

        assert!(matches!(outcome1, CreateSubscriptionOutcome::Subscribed(_)));
        assert!(matches!(outcome2, CreateSubscriptionOutcome::Subscribed(_)));

        assert_eq!(get_int_field_value(&*rx1.recv().await.unwrap()), 1);
        assert_eq!(get_int_field_value(&*rx1.recv().await.unwrap()), 2);

        assert_eq!(get_int_field_value(&*rx2.recv().await.unwrap()), 1);
        assert_eq!(get_int_field_value(&*rx2.recv().await.unwrap()), 2);
    }

    #[sdv_telemetry_test_utils_macros::timeout(10_000)]
    // Force a multi-threaded runtime for this test, to ensure that the notification timestamps are
    // more accurate, even when one of the threads is stuck in synchronous FFI code.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn create_subscription_and_read_messages_with_sub_sampling_interval_success() {
        let topic_name = unique_dt_topic_name();
        let notification_interval = SubSamplingInterval::try_from(Duration::from_secs(2)).unwrap();

        let topic_descriptor = register_topic(
            &topic_name,
            TelemetryPublisherMetadata {
                descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
                message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        );

        let subscriber = DeprecatedDataTunnelSubscriber::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let outcome = subscriber
            .create_subscription(
                &topic_name,
                None,
                &notification_interval,
                Box::new(move |message| tx.send((Instant::now(), message)).unwrap()),
            )
            .await
            .unwrap();
        assert!(matches!(outcome, CreateSubscriptionOutcome::Subscribed(_)));

        let message1_sent_at = Instant::now();
        write_message(&topic_descriptor, TestMessage { int_field: 1, ..Default::default() });
        let (message1_received_at, message1) = rx.recv().await.unwrap();

        write_message(&topic_descriptor, TestMessage { int_field: 2, ..Default::default() });
        write_message(&topic_descriptor, TestMessage { int_field: 3, ..Default::default() });
        let (message3_received_at, message3) = rx.recv().await.unwrap();

        assert_eq!(get_int_field_value(&*message1), 1);
        // Message 2 should be silently discarded.
        assert_eq!(get_int_field_value(&*message3), 3);
        assert!(
            matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
            "The channel should be empty."
        );

        {
            let elapsed = message1_received_at.duration_since(message1_sent_at);
            // The first notification should arrive almost instantly, but to avoid flakiness, add
            // 20% of leeway.
            let max_elapsed = notification_interval.as_duration().mul_f64(0.2);
            assert!(elapsed <= max_elapsed, "Elapsed time before notification should be at most {max_elapsed:?}, but was {elapsed:?}");
        }
        {
            let elapsed = message3_received_at.duration_since(message1_received_at);
            // The second notification should arrive `notification_interval` after the first
            // notification. To avoid flakiness, add 20% of leeway.
            let min_elapsed = notification_interval.as_duration().mul_f64(0.8);
            assert!(elapsed >= min_elapsed, "Elapsed time before notification should be at least {min_elapsed:?}, but was {elapsed:?}");
        }
    }

    struct SubSamplingTestConfig {
        publishing_interval: Duration,
        message_count: u128,
        subscribers: Vec<SubSamplingTestSubscriberConfig>,
    }

    #[derive(Debug, Clone)]
    enum SubSamplingUnsubscribeKind {
        Drop,
        CallUnsubscribe,
    }

    impl Default for SubSamplingUnsubscribeKind {
        fn default() -> Self {
            Self::CallUnsubscribe
        }
    }

    struct SubSamplingTestSubscriberConfig {
        sub_sampling_interval: Duration,
        expected_result: Vec<(u128, u128)>,

        subscribe_at: Duration,
        unsubscribe_at: Option<(Duration, SubSamplingUnsubscribeKind)>,
    }

    async fn run_sub_sampling_test(config: SubSamplingTestConfig) {
        let mut subscriptions = config
            .subscribers
            .iter()
            .map(|subscriber_config| {
                let sub_sampling_interval =
                    SubSamplingInterval::try_from(subscriber_config.sub_sampling_interval).unwrap();

                let (tx, rx) = mpsc::unbounded_channel();
                (
                    sub_sampling_interval,
                    // This holds the `DataTunnelSubscription` once subscribed.
                    Arc::new(Mutex::new(None)),
                    Some(tx),
                    rx,
                    subscriber_config.subscribe_at,
                    subscriber_config.unsubscribe_at.clone(),
                )
            })
            .collect::<Vec<_>>();

        let want = config
            .subscribers
            .into_iter()
            .map(|subscriber| subscriber.expected_result)
            .collect::<Vec<_>>();

        let topic_name = unique_dt_topic_name();
        let topic_descriptor = register_topic(
            &topic_name,
            TelemetryPublisherMetadata {
                descriptor_protos: vec![test_message::file_descriptor().proto().clone()],
                message_type_name: format!(".{}", TestMessage::descriptor().full_name()),
                ..Default::default()
            },
        );
        let subscriber = Arc::new(DeprecatedDataTunnelSubscriber::new());

        // Create subscriptions for subscribers that should start at the beginning, and await them.
        join_all(subscriptions.iter_mut().map(
            |(sub_sampling_interval, stored_subscription, tx, _, subscribe_at, _)| {
                if subscribe_at != &Duration::ZERO {
                    return ready(()).boxed();
                }

                let subscriber = &subscriber;
                let topic_name = topic_name.clone();
                let tx = tx.take().unwrap();
                Box::pin(async move {
                    let outcome = subscriber
                        .create_subscription(
                            &topic_name,
                            None,
                            sub_sampling_interval,
                            Box::new(move |message| tx.send((Instant::now(), message)).unwrap()),
                        )
                        .await
                        .unwrap();

                    let CreateSubscriptionOutcome::Subscribed(subscription) = outcome else {
                        panic!("Unexpected outcome for subscription");
                    };
                    *stored_subscription.lock() = Some(subscription);
                })
            },
        ))
        .await;

        // Schedule tasks to subscribe for subscriptions that shouldn't start immediately.
        subscriptions.iter_mut().for_each(
            |(sub_sampling_interval, stored_subscription, tx, _, subscribe_at, _)| {
                if subscribe_at == &Duration::ZERO {
                    return;
                }

                let subscriber = Arc::downgrade(&subscriber);
                let topic_name = topic_name.clone();
                let tx = tx.take().unwrap();

                let sub_sampling_interval = sub_sampling_interval.clone();
                let subscribe_at = subscribe_at.to_owned();
                let stored_subscription = Arc::clone(stored_subscription);
                tokio::spawn(async move {
                    sleep(subscribe_at).await;
                    let outcome = subscriber
                        .upgrade()
                        .unwrap()
                        .create_subscription(
                            &topic_name,
                            None,
                            &sub_sampling_interval,
                            Box::new(move |message| tx.send((Instant::now(), message)).unwrap()),
                        )
                        .await
                        .unwrap();

                    let CreateSubscriptionOutcome::Subscribed(subscription) = outcome else {
                        panic!("Unexpected outcome for subscription");
                    };
                    *stored_subscription.lock() = Some(subscription);
                });
            },
        );

        // Schedule tasks to unsubscribe if necessary.
        subscriptions.iter().for_each(|(_, stored_subscription, _, _, _, unsubscribe_at)| {
            if let Some((unsubscribe_at, unsubscribe_kind)) = unsubscribe_at {
                let unsubscribe_at = unsubscribe_at.to_owned();
                let unsubscribe_kind = unsubscribe_kind.to_owned();
                let stored_subscription = Arc::clone(stored_subscription);
                tokio::spawn(async move {
                    sleep(unsubscribe_at).await;

                    let subscription = stored_subscription.lock().take().unwrap();
                    match unsubscribe_kind {
                        SubSamplingUnsubscribeKind::CallUnsubscribe => {
                            subscription.unsubscribe().await.unwrap();
                        }
                        SubSamplingUnsubscribeKind::Drop => {
                            drop(subscription);
                        }
                    }
                });
            }
        });

        let mut interval = interval(config.publishing_interval);
        let start = Instant::now();
        for i in 0..config.message_count {
            interval.tick().await;
            write_message(
                &topic_descriptor,
                TestMessage {
                    int_field: (i * interval.period().as_millis()).try_into().unwrap(),
                    ..Default::default()
                },
            );
        }
        interval.tick().await;

        let subscription_results = join_all(subscriptions.into_iter().map(
            |(_, _subscription, _, mut rx, _, _)| async move {
                let mut messages = vec![];
                loop {
                    let Ok(message) = rx.try_recv() else {
                        return messages;
                    };
                    messages.push(message);
                }
            },
        ))
        .await;

        let mut got = subscription_results.iter().map(|_| vec![]).collect::<Vec<_>>();

        for (subscription_result, got) in zip(subscription_results.iter(), got.iter_mut()) {
            for (received_at, message) in subscription_result {
                let received_at = received_at.duration_since(start).as_millis();
                let sent_at = get_int_field_value(&**message).try_into().unwrap();
                got.push((received_at, sent_at));
            }
        }

        // Print the result and the expected for debugging purposes.
        fn print_result(result: &[Vec<(u128, u128)>]) {
            for (i, values) in result.iter().enumerate() {
                eprintln!("Subscription {}", i);
                for (received_at, sent_at) in values.iter() {
                    eprintln!("{}: {}", received_at, sent_at);
                }
            }
        }
        eprintln!("Got");
        print_result(got.as_slice());
        eprintln!("Want");
        print_result(want.as_slice());

        use googletest::prelude::*;

        expect_that!(got.len(), eq(want.len()));
        for (got, want) in zip(got.into_iter(), want.into_iter()) {
            expect_that!(got.len(), eq(want.len()));

            // Ensure that all timestamps received are strictly sorted.
            let (got_received_ats, got_sent_ats): (Vec<_>, Vec<_>) = got.iter().cloned().unzip();
            expect_that!(
                got_received_ats,
                predicate(|values: &Vec<_>| values.windows(2).all(|w| w[0] < w[1]))
            );
            expect_that!(
                got_sent_ats,
                predicate(|values: &Vec<_>| values.windows(2).all(|w| w[0] < w[1]))
            );

            for ((got_received_at, got_sent_at), (want_received_at, want_sent_at)) in
                zip(got.into_iter(), want.into_iter())
            {
                expect_that!(got_received_at.abs_diff(want_received_at), lt(50));

                // In the worst-case, we may receive a message that is almost two publishing
                // intervals old.
                expect_that!(
                    got_sent_at,
                    any!(
                        eq(want_sent_at.saturating_sub(2 * config.publishing_interval.as_millis())),
                        eq(want_sent_at.saturating_sub(config.publishing_interval.as_millis())),
                        eq(want_sent_at)
                    )
                );
            }
        }
    }

    #[sdv_telemetry_test_utils_macros::timeout(30_000)]
    #[googletest::test]
    // Force a multi-threaded runtime for this test, to ensure that the notification timestamps are
    // more accurate, even when one of the threads is stuck in synchronous FFI code.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn read_messages_with_multiple_sub_sampling_intervals_success() {
        run_sub_sampling_test(SubSamplingTestConfig {
            publishing_interval: Duration::from_millis(400),
            message_count: 30,
            subscribers: vec![
                SubSamplingTestSubscriberConfig {
                    sub_sampling_interval: Duration::from_millis(1700),
                    expected_result: vec![
                        (800, 800),
                        (2500, 2400),
                        (4200, 4000),
                        (5900, 5600),
                        (7600, 7600),
                        (9300, 9200),
                        (11000, 10800),
                    ],
                    subscribe_at: Duration::from_millis(450),
                    unsubscribe_at: None,
                },
                SubSamplingTestSubscriberConfig {
                    sub_sampling_interval: Duration::from_secs(3),
                    expected_result: vec![(800, 800), (3800, 3600), (6800, 6800), (9800, 9600)],
                    subscribe_at: Duration::from_millis(450),
                    unsubscribe_at: None,
                },
                SubSamplingTestSubscriberConfig {
                    sub_sampling_interval: Duration::from_millis(5500),
                    expected_result: vec![(800, 800), (6300, 6000), (11800, 11600)],
                    subscribe_at: Duration::from_millis(450),
                    unsubscribe_at: None,
                },
            ],
        })
        .await;
    }

    #[sdv_telemetry_test_utils_macros::timeout(30_000)]
    #[googletest::test]
    // Force a multi-threaded runtime for this test, to ensure that the notification timestamps are
    // more accurate, even when one of the threads is stuck in synchronous FFI code.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn read_messages_with_multiple_sub_sampling_intervals_including_zero_success() {
        run_sub_sampling_test(SubSamplingTestConfig {
            publishing_interval: Duration::from_millis(1500),
            message_count: 8,
            subscribers: vec![
                // No sub-sampling, therefore all messages should be received immediately.
                SubSamplingTestSubscriberConfig {
                    sub_sampling_interval: Duration::ZERO,
                    expected_result: vec![
                        (0, 0),
                        (1500, 1500),
                        (3000, 3000),
                        (4500, 4500),
                        (6000, 6000),
                        (7500, 7500),
                        (9000, 9000),
                        (10500, 10500),
                    ],
                    subscribe_at: Duration::ZERO,
                    unsubscribe_at: None,
                },
                // With sub-sampling, therefore we should only receive the latest messages.
                SubSamplingTestSubscriberConfig {
                    sub_sampling_interval: Duration::from_secs(5),
                    expected_result: vec![(0, 0), (5000, 4500), (10000, 9000)],
                    subscribe_at: Duration::ZERO,
                    unsubscribe_at: None,
                },
            ],
        })
        .await;
    }

    #[sdv_telemetry_test_utils_macros::timeout(10_000)]
    #[googletest::test]
    // Force a multi-threaded runtime for this test, to ensure that the notification timestamps are
    // more accurate, even when one of the threads is stuck in synchronous FFI code.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn read_messages_with_sub_sampling_interval_and_unsubscribe_success() {
        run_sub_sampling_test(SubSamplingTestConfig {
            publishing_interval: Duration::from_millis(300),
            message_count: 10,
            subscribers: vec![SubSamplingTestSubscriberConfig {
                sub_sampling_interval: Duration::from_millis(800),
                expected_result: vec![(0, 0), (800, 600), (1600, 1500)],
                subscribe_at: Duration::ZERO,
                unsubscribe_at: Some((
                    Duration::from_millis(1700),
                    SubSamplingUnsubscribeKind::CallUnsubscribe,
                )),
            }],
        })
        .await;
    }

    #[sdv_telemetry_test_utils_macros::timeout(10_000)]
    #[googletest::test]
    // Force a multi-threaded runtime for this test, to ensure that the notification timestamps are
    // more accurate, even when one of the threads is stuck in synchronous FFI code.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn read_messages_with_sub_sampling_interval_and_drop_success() {
        run_sub_sampling_test(SubSamplingTestConfig {
            publishing_interval: Duration::from_millis(300),
            message_count: 10,
            subscribers: vec![SubSamplingTestSubscriberConfig {
                sub_sampling_interval: Duration::from_millis(800),
                expected_result: vec![(0, 0), (800, 600), (1600, 1500)],
                subscribe_at: Duration::ZERO,
                unsubscribe_at: Some((
                    Duration::from_millis(1700),
                    SubSamplingUnsubscribeKind::Drop,
                )),
            }],
        })
        .await;
    }

    #[sdv_telemetry_test_utils_macros::timeout(10_000)]
    #[googletest::test]
    // Force a multi-threaded runtime for this test, to ensure that the notification timestamps are
    // more accurate, even when one of the threads is stuck in synchronous FFI code.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn read_messages_with_multiple_sub_sampling_intervals_and_unsubscribe_success() {
        run_sub_sampling_test(SubSamplingTestConfig {
            publishing_interval: Duration::from_millis(300),
            message_count: 15,
            subscribers: vec![
                SubSamplingTestSubscriberConfig {
                    sub_sampling_interval: Duration::from_millis(1),
                    expected_result: vec![(0, 0), (300, 300), (600, 600)],
                    subscribe_at: Duration::ZERO,
                    unsubscribe_at: Some((
                        Duration::from_millis(750),
                        SubSamplingUnsubscribeKind::default(),
                    )),
                },
                SubSamplingTestSubscriberConfig {
                    sub_sampling_interval: Duration::from_millis(800),
                    expected_result: vec![
                        (0, 0),
                        // In between the above and the below message, at exactly 750ms, the
                        // notification interval is updated from 1ms to 800ms, because the other
                        // subscriber unsubscribed.
                        (800, 600),
                        // At 900ms, we receive the next message. After updating the notification
                        // interval, the next message is passed through immediately and only then is
                        // the notification interval restarted. Therefore, the next notification
                        // will now arrive at 900ms+800ms=1700ms, then at 2500ms, etc.
                        //
                        // The messages sent at 1200ms and 1500ms are never received, because they
                        // fall into the aforementioned notification interval of 800ms that was
                        // reset at 750ms and restarted at 900ms. This means that the next message
                        // that is actually received is the one sent at 1800ms. Hence, the next
                        // result contains the message sent at 900ms, which is the latest one we
                        // have been notified of at this point.
                        (1600, 900),
                        // The last notification was received at 1700ms, therefore the newest
                        // message is still the one sent at 1500ms.
                        (2400, 1500),
                        // The last notification was received at 2500ms, therefore the newest
                        // message is still the one sent at 2400ms.
                        (3200, 2400),
                    ],
                    subscribe_at: Duration::ZERO,
                    unsubscribe_at: Some((
                        Duration::from_millis(3300),
                        SubSamplingUnsubscribeKind::default(),
                    )),
                },
            ],
        })
        .await;
    }

    #[sdv_telemetry_test_utils_macros::timeout(10_000)]
    #[googletest::test]
    // Force a multi-threaded runtime for this test, to ensure that the notification timestamps are
    // more accurate, even when one of the threads is stuck in synchronous FFI code.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn subscribing_later_with_same_sub_sampling_interval_does_not_reset_last_call_success() {
        run_sub_sampling_test(SubSamplingTestConfig {
            publishing_interval: Duration::from_millis(200),
            message_count: 30,
            subscribers: vec![
                SubSamplingTestSubscriberConfig {
                    sub_sampling_interval: Duration::ZERO,
                    expected_result: (0..30).map(|i| (i * 200, i * 200)).collect(),
                    subscribe_at: Duration::ZERO,
                    unsubscribe_at: None,
                },
                SubSamplingTestSubscriberConfig {
                    sub_sampling_interval: Duration::from_millis(5100),
                    expected_result: vec![(0, 0), (5100, 5000)],
                    subscribe_at: Duration::ZERO,
                    unsubscribe_at: None,
                },
                // Even though this one subscribes much later, it should still receive the message
                // at 5000ms, just like the earlier subscriber above.
                SubSamplingTestSubscriberConfig {
                    sub_sampling_interval: Duration::from_millis(5100),
                    expected_result: vec![(5100, 5000)],
                    subscribe_at: Duration::from_millis(4000),
                    unsubscribe_at: None,
                },
            ],
        })
        .await;
    }

    #[sdv_telemetry_test_utils_macros::timeout(10_000)]
    #[googletest::test]
    // Force a multi-threaded runtime for this test, to ensure that the notification timestamps are
    // more accurate, even when one of the threads is stuck in synchronous FFI code.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sub_sampling_works_when_slower_than_publishing_interval() {
        run_sub_sampling_test(SubSamplingTestConfig {
            publishing_interval: Duration::from_millis(1000),
            message_count: 4,
            subscribers: vec![SubSamplingTestSubscriberConfig {
                sub_sampling_interval: Duration::from_millis(300),
                expected_result: vec![(0, 0), (1000, 1000), (2000, 2000), (3000, 3000)],
                subscribe_at: Duration::ZERO,
                unsubscribe_at: None,
            }],
        })
        .await;
    }

    #[sdv_telemetry_test_utils_macros::timeout(10_000)]
    #[googletest::test]
    // Force a multi-threaded runtime for this test, to ensure that the notification timestamps are
    // more accurate, even when one of the threads is stuck in synchronous FFI code.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sub_sampling_works_when_slower_than_publishing_interval_with_multiple_subscribers() {
        run_sub_sampling_test(SubSamplingTestConfig {
            publishing_interval: Duration::from_millis(1000),
            message_count: 4,
            subscribers: vec![
                SubSamplingTestSubscriberConfig {
                    sub_sampling_interval: Duration::from_millis(100),
                    expected_result: vec![(0, 0), (1000, 1000), (2000, 2000), (3000, 3000)],
                    subscribe_at: Duration::ZERO,
                    unsubscribe_at: None,
                },
                SubSamplingTestSubscriberConfig {
                    sub_sampling_interval: Duration::from_millis(300),
                    expected_result: vec![(0, 0), (1000, 1000), (2000, 2000), (3000, 3000)],
                    subscribe_at: Duration::ZERO,
                    unsubscribe_at: None,
                },
                SubSamplingTestSubscriberConfig {
                    sub_sampling_interval: Duration::from_millis(500),
                    expected_result: vec![(0, 0), (1000, 1000), (2000, 2000), (3000, 3000)],
                    subscribe_at: Duration::ZERO,
                    unsubscribe_at: None,
                },
                SubSamplingTestSubscriberConfig {
                    sub_sampling_interval: Duration::from_millis(700),
                    expected_result: vec![(0, 0), (1000, 1000), (2000, 2000), (3000, 3000)],
                    subscribe_at: Duration::ZERO,
                    unsubscribe_at: None,
                },
            ],
        })
        .await;
    }

    #[sdv_telemetry_test_utils_macros::timeout(10_000)]
    #[googletest::test]
    // Force a multi-threaded runtime for this test, to ensure that the notification timestamps are
    // more accurate, even when one of the threads is stuck in synchronous FFI code.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sub_sampling_works_when_re_subscribing_after_unsubscribing() {
        run_sub_sampling_test(SubSamplingTestConfig {
            publishing_interval: Duration::from_millis(200),
            message_count: 12,
            subscribers: vec![
                SubSamplingTestSubscriberConfig {
                    sub_sampling_interval: Duration::from_millis(550),
                    expected_result: vec![(0, 0), (550, 400), (1100, 1000)],
                    subscribe_at: Duration::ZERO,
                    unsubscribe_at: Some((
                        Duration::from_millis(1300),
                        SubSamplingUnsubscribeKind::default(),
                    )),
                },
                SubSamplingTestSubscriberConfig {
                    sub_sampling_interval: Duration::from_millis(550),
                    expected_result: vec![(1800, 1800), (2350, 2200)],
                    subscribe_at: Duration::from_millis(1700),
                    unsubscribe_at: None,
                },
            ],
        })
        .await;
    }
}
