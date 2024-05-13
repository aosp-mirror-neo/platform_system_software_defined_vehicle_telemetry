// Copyright 2023 Google LLC. All Rights Reserved.

//! Provides access to Service Publishers.

// External dependencies.
use async_trait::async_trait;
use protobuf::reflect::MessageDescriptor;
use protobuf::MessageDyn;
use std::marker::{Send, Sync};

/// Interface of an entity that provides access to Service Publishers.
#[async_trait]
pub trait ServicePublisherAccessor: Send + Sync {
    /// Returns metadata of the specified service publisher.
    /// Returns None if the service publisher is not registered.
    /// Returns error if the service publisher is registered, but metadata retrieval failed.
    fn get_metadata(&self, service_name: &str) -> anyhow::Result<Option<ServicePublisherMetadata>>;

    /// Returns provider of messages from the publisher specified by the service name.
    fn get_message_provider(&self, service_name: &str)
        -> anyhow::Result<GetMessageProviderOutcome>;

    /// Attempts to create subscription to the new messages from the publisher
    /// specified by the service name with certain configuration, if provided.
    async fn create_subscription(
        &self,
        service_name: &str,
        configuration: Option<&[u8]>,
        callback: Box<dyn FnMut(Box<dyn MessageDyn>) + Send + Sync>,
    ) -> anyhow::Result<CreateSubscriptionOutcome>;
}

/// Provides messages from a Service Publisher.
pub trait ServicePublisherMessageProvider {
    /// Returns latest published message.
    fn get_latest_message(&self) -> Option<Box<dyn MessageDyn>>;
}

/// Service publisher metadata.
pub struct ServicePublisherMetadata {
    /// True if the service provides the latest message without subscription.
    pub supports_get: bool,
    /// Message descriptor of the root message type.
    pub message_descriptor: MessageDescriptor,
}

/// Successful result of GetMessageProvider function.
pub enum GetMessageProviderOutcome {
    /// Service publisher is available and a message provider was successfully created.
    Available(Box<dyn ServicePublisherMessageProvider>),
    /// The service with the specified service name is not available.
    NotAvailable,
    /// Get latest message operation is not supported by the accessor.
    NotSupported,
}

/// Successful result of subscription creation.
pub enum CreateSubscriptionOutcome {
    /// Service publisher is available and a subscription was successfully created.
    Subscribed(Box<dyn ServicePublisherSubscription>),
    /// The service with the specified service name is not available.
    NotAvailable(Box<dyn FnMut(Box<dyn MessageDyn>) + Send + Sync>),
}

/// Interface of the Service Publisher subscription.
#[async_trait]
pub trait ServicePublisherSubscription: Send + Sync {
    /// Ends the underlying subscription.
    async fn unsubscribe(self: Box<Self>) -> Result<(), anyhow::Error>;
}

impl GetMessageProviderOutcome {
    /// Returns the contained Available value, consuming the self value.
    /// Panics if the value is not Available.
    pub fn unwrap(self) -> Box<dyn ServicePublisherMessageProvider> {
        match self {
            Self::Available(provider) => provider,
            Self::NotAvailable => panic!("Unexpected result: Not Available"),
            Self::NotSupported => panic!("Unexpected result: Not Supported"),
        }
    }
}

impl CreateSubscriptionOutcome {
    /// Returns the contained Subscribed value, consuming the self value.
    /// Panics if the value is not Subscribed.
    pub fn unwrap(self) -> Box<dyn ServicePublisherSubscription> {
        match self {
            Self::Subscribed(subscription) => subscription,
            Self::NotAvailable(_) => panic!("Unexpected result: Not Available"),
        }
    }
}

#[cfg(test)]
pub(crate) mod mock;
