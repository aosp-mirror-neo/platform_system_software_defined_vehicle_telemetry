// Copyright 2023 Google LLC

//! Provides access to Service Publishers.

// External dependencies.
use anyhow::anyhow;
use async_trait::async_trait;
use protobuf::reflect::MessageDescriptor;
use protobuf::well_known_types::any::Any;
use protobuf::MessageDyn;
use std::marker::{Send, Sync};
use std::time::Duration;

/// Wrapper around an integer representing a sub-sampling interval.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SubSamplingInterval {
    interval_ms: u32,
}

impl SubSamplingInterval {
    /// A sub-sampling interval of zero (= no sub-sampling).
    pub const ZERO: Self = Self { interval_ms: 0 };

    /// Returns the sub-sampling interval in milliseconds.
    pub fn as_millis(&self) -> u32 {
        self.interval_ms
    }

    /// Returns the sub-sampling interval as a `Duration`.
    pub fn as_duration(&self) -> Duration {
        Duration::from_millis(self.interval_ms.into())
    }
}

impl TryFrom<Duration> for SubSamplingInterval {
    type Error = anyhow::Error;

    fn try_from(interval: Duration) -> Result<Self, Self::Error> {
        let interval_ms = interval.as_millis().try_into().map_err(|_| anyhow!(
            "Duration {interval:?} is too big to convert it to a sub-sampling interval (must be 2^32 milliseconds or smaller)."
        ))?;
        Ok(SubSamplingInterval { interval_ms })
    }
}

/// Interface of an entity that provides access to Service Publishers.
#[async_trait]
pub trait ServicePublisherAccessor: Send + Sync {
    /// Returns metadata of the specified service publisher.
    /// Returns None if the service publisher is not registered.
    /// Returns error if the service publisher is registered, but metadata retrieval failed.
    async fn get_metadata(
        &self,
        service_name: &str,
    ) -> anyhow::Result<Option<ServicePublisherMetadata>>;

    /// Returns provider of messages from the publisher specified by the service name.
    /// Returns None if the service publisher is not registered
    /// or is not able to provide the latest message without subscription.
    fn get_message_provider(
        &self,
        service_name: &str,
    ) -> anyhow::Result<Option<Box<dyn ServicePublisherMessageProvider>>>;

    /// Attempts to create subscription to the new messages from the publisher
    /// specified by the service name with certain configuration, if provided.
    async fn create_subscription(
        &self,
        service_name: &str,
        configuration: Option<&Any>,
        sub_sampling_interval: &SubSamplingInterval,
        callback: Box<dyn FnMut(Box<dyn MessageDyn>) + Send + Sync>,
    ) -> anyhow::Result<CreateSubscriptionOutcome>;
}

/// Provides messages from a Service Publisher.
#[async_trait]
pub trait ServicePublisherMessageProvider: Send + Sync {
    /// Returns latest published message with certain configuration, if provided.
    async fn get_latest_message(
        &self,
        configuration: Option<&Any>,
    ) -> anyhow::Result<Option<Box<dyn MessageDyn>>>;
}

/// Service publisher metadata.
pub struct ServicePublisherMetadata {
    /// True if the service provides the latest message without subscription.
    pub supports_get: bool,
    /// Message descriptor of the root message type.
    pub message_descriptor: MessageDescriptor,
}

/// Successful result of subscription creation.
pub enum CreateSubscriptionOutcome {
    /// Service publisher is available and a subscription was successfully created.
    Subscribed(Box<dyn ServicePublisherSubscription>),
    /// The service with the specified service name is not registered.
    NotRegistered(Box<dyn FnMut(Box<dyn MessageDyn>) + Send + Sync>),
}

/// Interface of the Service Publisher subscription.
#[async_trait]
pub trait ServicePublisherSubscription: Send + Sync {
    /// Ends the underlying subscription.
    async fn unsubscribe(self: Box<Self>) -> anyhow::Result<()>;
}

impl CreateSubscriptionOutcome {
    /// Returns the contained Subscribed value, consuming the self value.
    /// Panics if the value is not Subscribed.
    pub fn unwrap(self) -> Box<dyn ServicePublisherSubscription> {
        match self {
            Self::Subscribed(subscription) => subscription,
            Self::NotRegistered(_) => panic!("Unexpected result: Not Registered"),
        }
    }
}

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sub_sampling_interval_zero_is_zero() {
        assert_eq!(SubSamplingInterval::ZERO.as_duration(), Duration::ZERO);
    }

    #[test]
    fn sub_sampling_interval_from_too_big_duration_fails() {
        let duration = Duration::MAX;

        let sub_sampling_interval = SubSamplingInterval::try_from(duration);

        assert!(sub_sampling_interval.is_err());
    }

    #[test]
    fn sub_sampling_interval_as_millis_success() {
        let duration = Duration::from_millis(3333);

        let sub_sampling_interval = SubSamplingInterval::try_from(duration).unwrap();

        assert_eq!(sub_sampling_interval.as_millis(), 3333);
    }

    #[test]
    fn sub_sampling_interval_as_duration_success() {
        let duration = Duration::from_millis(3333);

        let sub_sampling_interval = SubSamplingInterval::try_from(duration).unwrap();

        assert_eq!(sub_sampling_interval.as_duration(), duration);
    }
}
