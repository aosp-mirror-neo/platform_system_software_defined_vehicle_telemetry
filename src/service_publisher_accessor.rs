// Copyright 2023 Google LLC

//! Provides access to Service Publishers.

// External dependencies.
use async_trait::async_trait;
use protobuf::reflect::MessageDescriptor;
use protobuf::well_known_types::any::Any;
use protobuf::MessageDyn;
use sdv_status_rs::SdvStatus;
use std::fmt;
use std::sync::Arc;

use crate::metrics_config_runtime_error::ServiceRuntimeError;
use crate::sub_sampling_interval::SubSamplingInterval;

/// get_metadata error.
#[derive(Debug, thiserror::Error)]
pub enum GetMetadataError {
    /// Failed to parse metadata.
    #[error("failed to parse metadata: {0:#}")]
    FailedToParseMetadata(anyhow::Error),
}

/// get_message_provider error.
#[derive(Debug, thiserror::Error)]
pub enum GetMessageProviderError {
    /// Failed to parse metadata.
    #[error("failed to parse metadata: {0:#}")]
    FailedToParseMetadata(anyhow::Error),
    /// Failed to connect to RPC client.
    #[error("failed to connect to RPC client: {0}")]
    RpcFailedToConnect(SdvStatus),
}

/// create_subscription error.
#[derive(Debug, thiserror::Error)]
pub enum CreateSubscriptionError {
    /// Failed to parse metadata.
    #[error("failed to parse metadata: {0:#}")]
    FailedToParseMetadata(anyhow::Error),
    /// Failed to connect to RPC client.
    #[error("failed to connect to RPC client: {0}")]
    RpcFailedToConnect(SdvStatus),
    /// Failed to subscribe.
    #[error("failed to subscribe: {0:#}")]
    FailedToSubscribe(anyhow::Error),
}

/// get_latest_message error.
#[derive(Debug, thiserror::Error)]
pub enum GetLatestMessageError {
    /// Failed to retrieve the message.
    #[error("failed to retrieve message: {0:#}")]
    FailedToRetrieveMessage(anyhow::Error),
    /// Failed to parse the message.
    #[error("failed to parse message: {0:#}")]
    MessageParsingError(anyhow::Error),
}

/// Errors that occur during service discovery but are more treated as warnings, since other service
/// publisher accessors might be able to find the service.
#[derive(Debug, thiserror::Error)]
pub enum ServiceDiscoveryError {
    /// More than one service matches the criteria.
    #[error("more than one service found, service_name is ambiguous")]
    AmbiguousServiceName,
    /// An internal error occurred while attempting to discover services.
    #[error("internal service discovery error: {0}")]
    InternalError(SdvStatus),
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
    ) -> (Result<Option<ServicePublisherMetadata>, GetMetadataError>, Vec<ServiceDiscoveryError>);

    /// Returns provider of messages from the publisher specified by the service name.
    /// Returns None if the service publisher is not registered
    /// or is not able to provide the latest message without subscription.
    async fn get_message_provider(
        &self,
        service_name: &str,
    ) -> (Result<GetMessageProviderOutcome, GetMessageProviderError>, Vec<ServiceDiscoveryError>);

    /// Attempts to create subscription to the new messages from the publisher
    /// specified by the service name with certain configuration, if provided.
    async fn create_subscription(
        &self,
        service_name: &str,
        configuration: Option<&Any>,
        sub_sampling_interval: &SubSamplingInterval,
        callback: Box<dyn FnMut(Result<Box<dyn MessageDyn>, ServiceRuntimeError>) + Send + Sync>,
    ) -> (Result<CreateSubscriptionOutcome, CreateSubscriptionError>, Vec<ServiceDiscoveryError>);

    /// Registers a callback that will be called when a service with the specified string
    /// service_name becomes available.
    async fn register_availability_callback(
        &self,
        service_name: &str,
        callback: &Arc<dyn Fn() + Send + Sync>,
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
    ) -> Result<Option<Box<dyn MessageDyn>>, GetLatestMessageError>;
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
    NotRegistered(Box<dyn FnMut(Result<Box<dyn MessageDyn>, ServiceRuntimeError>) + Send + Sync>),
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
