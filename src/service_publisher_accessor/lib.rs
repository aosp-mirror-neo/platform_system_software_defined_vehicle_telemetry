// Copyright 2023 Google LLC

//! Provides access to Service Publishers.

mod sub_sampling_interval;

pub use sub_sampling_interval::SubSamplingInterval;

use async_trait::async_trait;
use protobuf::reflect::MessageDescriptor;
use protobuf::well_known_types::any::Any;
use protobuf::MessageDyn;
use sdv_status_rs::SdvStatus;
use std::fmt;
use std::sync::Arc;

/// Errors related to Service Publisher Accessor
#[derive(Debug, thiserror::Error)]
pub enum ServicePublisherAccessorError {
    /// Failed to parse message
    #[error("Failed to parse message: {0:#}")]
    MessageParsingFailed(anyhow::Error),

    /// Failed to parse metadata
    #[error("Failed to parse metadata: {0:#}")]
    MetadataParsingFailed(anyhow::Error),

    /// Failed to connect to RPC client
    #[error("Failed to connect to RPC client: {0}")]
    RpcFailedToConnect(SdvStatus),

    /// Subscription failure
    #[error("Failed to subscribe: {0:#}")]
    FailedToSubscribe(anyhow::Error),

    /// Message retrieval failed
    #[error("Failed to retrieve message: {0:#}")]
    MessageRetrievalFailed(anyhow::Error),

    /// Service is not supported
    #[error("Service is not supported by underlying data providers")]
    ServiceNotSupported,
}

/// Callback that is called when a new message is available.
pub type NewMessageCallback =
    Box<dyn FnMut(Result<Box<dyn MessageDyn>, ServicePublisherAccessorError>) + Send + Sync>;

/// Callback that is called when a subscription is cancelled/ended by the
/// underlying data provider.
pub type CancellationCallback = Box<dyn FnOnce() + Send + Sync>;

/// Callback that is called when a service becomes available.
pub type AvailabilityCallback = Arc<dyn Fn() + Send + Sync>;

/// Interface of an entity that provides access to Service Publishers.
#[mockall::automock]
#[async_trait]
pub trait ServicePublisherAccessor: Send + Sync {
    /// Returns metadata of the specified service publisher.
    /// Returns None if the service publisher is not registered.
    /// Returns error if the service publisher is registered, but metadata retrieval failed.
    async fn get_metadata(
        &self,
        service_name: &str,
    ) -> Result<Option<ServicePublisherMetadata>, ServicePublisherAccessorError>;

    /// Returns provider of messages from the publisher specified by the service name.
    /// Returns None if the service publisher is not registered
    /// or is not able to provide the latest message without subscription.
    async fn get_message_provider(
        &self,
        service_name: &str,
    ) -> Result<GetMessageProviderOutcome, ServicePublisherAccessorError>;

    /// Attempts to create subscription to the new messages from the publisher
    /// specified by the service name with certain configuration, if provided.
    async fn create_subscription<'a>(
        &'a self,
        service_name: &'a str,
        configuration: Option<&'a Any>,
        fetch_last_message: bool,
        sub_sampling_interval: &'a SubSamplingInterval,
        new_message_callback: NewMessageCallback,
        cancellation_callback: CancellationCallback,
    ) -> Result<CreateSubscriptionOutcome, ServicePublisherAccessorError>;

    /// Registers a callback that will be called when a service with the specified string
    /// service_name becomes available.
    async fn register_availability_callback(
        &self,
        service_name: &str,
        callback: &AvailabilityCallback,
    ) -> anyhow::Result<Option<Box<dyn AvailabilityCallbackRegistration>>>;
}

/// Interface of the availability callback registration.
#[async_trait]
pub trait AvailabilityCallbackRegistration: Send + Sync {
    /// Unregisters the callback.
    async fn unregister(self: Box<Self>);
}

/// Provides messages from a Service Publisher.
#[async_trait]
pub trait ServicePublisherMessageProvider: Send + Sync {
    /// Returns latest published message with certain configuration, if provided.
    async fn get_latest_message(
        &self,
        configuration: Option<&Any>,
    ) -> Result<Option<Box<dyn MessageDyn>>, ServicePublisherAccessorError>;
}

/// Service publisher metadata.
#[derive(Debug)]
pub struct ServicePublisherMetadata {
    /// Message descriptor of the root message type.
    pub message_descriptor: MessageDescriptor,
}

/// Represents the outcome of whether `fetch_last_message` was enabled, disabled, or is not
/// supported by the service publisher.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum FetchLastMessageOutcome {
    /// `fetch_last_message` is not supported by this service publisher.
    NotSupported,
    /// `fetch_last_message` is supported and enabled for this service publisher.
    Enabled,
    /// `fetch_last_message` is supported but disabled for this service publisher.
    Disabled,
}

impl FetchLastMessageOutcome {
    /// Creates an instance of `Self` that is either `Self::Enabled` or `Self::Disabled`.
    pub fn new_supported(enabled: bool) -> Self {
        if enabled {
            Self::Enabled
        } else {
            Self::Disabled
        }
    }
}

/// Successful result of subscription creation.
pub enum CreateSubscriptionOutcome {
    /// Service publisher is available and a subscription was successfully created.
    Subscribed {
        /// The subscription handle.
        subscription: Box<dyn ServicePublisherSubscription>,
        /// Whether the subscription supports `fetch_last_message` and whether it is enabled or not.
        fetch_last_message_outcome: FetchLastMessageOutcome,
    },
    /// The service with the specified service name is not registered.
    NotRegistered {
        /// New message callback passed to the create subscription call.
        new_message_callback: NewMessageCallback,
        /// Cancellation callback passed to the create subscription call.
        cancellation_callback: CancellationCallback,
    },
}

/// Successful result of getting a message provider.
pub enum GetMessageProviderOutcome {
    /// Service publisher is available and supports Get.
    Success(Box<dyn ServicePublisherMessageProvider>),
    /// Service publisher is available, but does not support Get.
    GetNotSupported,
    /// Service publisher is not available.
    NotRegistered,
}

impl fmt::Debug for GetMessageProviderOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Success(_) => "Success(<provider>)",
                Self::GetNotSupported => "GetNotSupported",
                Self::NotRegistered => "NotRegistered",
            }
        )
    }
}

/// Interface of the Service Publisher subscription.
#[mockall::automock]
#[async_trait]
pub trait ServicePublisherSubscription: Send + Sync {
    /// Ends the underlying subscription.
    async fn unsubscribe(self: Box<Self>);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fetch_last_message_outcome_new_supported() {
        assert_eq!(FetchLastMessageOutcome::new_supported(true), FetchLastMessageOutcome::Enabled);
        assert_eq!(
            FetchLastMessageOutcome::new_supported(false),
            FetchLastMessageOutcome::Disabled
        );
    }
}
