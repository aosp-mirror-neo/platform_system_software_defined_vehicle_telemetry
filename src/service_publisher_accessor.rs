// Copyright 2023 Google LLC. All Rights Reserved.

//! Provides access to Service Publishers.

// External dependencies.
use async_trait::async_trait;
use protobuf::reflect::MessageDescriptor;
use protobuf::MessageDyn;
use std::marker::{Send, Sync};
use std::sync::Arc;

/// Interface of an entity that provides access to Service Publishers.
#[async_trait]
pub trait ServicePublisherAccessor: Send + Sync {
    /// Returns message descriptor of the specified topic published by the specified service.
    /// Returns None if the service and topic combination is not registered.
    /// Returns error if the service and topic combination is registered, but metadata retrieval failed.
    fn get_message_descriptor(
        &self,
        service_name: &str,
    ) -> Result<Option<MessageDescriptor>, anyhow::Error>;

    /// Subscribes specified callback to the new messages from the specified topic
    /// with certain configuration, if provided.
    async fn try_subscribe(
        &self,
        service_name: &str,
        callback: Arc<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>,
        configuration: Option<&[u8]>,
    ) -> Result<bool, anyhow::Error>;

    /// Unsubscribes from the specified topic and service name.
    async fn unsubscribe(
        &self,
        service_name: &str,
        configuration: Option<&[u8]>,
    ) -> Result<(), anyhow::Error>;
}

#[cfg(test)]
pub(crate) mod mock;
