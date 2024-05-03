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
    /// Returns message descriptor of the specified service.
    /// Returns None if the service is not registered.
    /// Returns error if the service is registered, but metadata retrieval failed.
    fn get_message_descriptor(
        &self,
        service_name: &str,
    ) -> Result<Option<MessageDescriptor>, anyhow::Error>;

    /// Attempts to create subscription to the new messages from the publisher
    /// specified by the service name with certain configuration, if provided.
    async fn create_subscription(
        &self,
        service_name: &str,
        configuration: Option<&[u8]>,
        callback: Box<dyn FnMut(Box<dyn MessageDyn>) + Send + Sync>,
    ) -> anyhow::Result<CreateSubscriptionOutcome>;
}

/// Successful result of subscription creation.
pub enum CreateSubscriptionOutcome {
    /// Subscription was successfully created.
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

impl CreateSubscriptionOutcome {
    /// Returns the contained Subscribed value, consuming the self value.
    /// Panics if the value is not Subscribed.
    pub fn unwrap(self) -> Box<dyn ServicePublisherSubscription> {
        match self {
            CreateSubscriptionOutcome::Subscribed(subscription) => subscription,
            CreateSubscriptionOutcome::NotAvailable(_) => {
                panic!("Unexpected result: Not Available")
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod mock;
