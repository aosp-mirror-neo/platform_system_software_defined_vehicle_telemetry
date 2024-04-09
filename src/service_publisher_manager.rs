// Copyright 2023 Google LLC. All Rights Reserved.

#![allow(missing_docs)]

pub mod service_publisher_manager_impl;

// External dependencies.
use async_trait::async_trait;
use log::error;
use protobuf::reflect::MessageDescriptor;
use protobuf::MessageDyn;
use std::fmt::Debug;
use std::marker::{Send, Sync};
use std::sync::Arc;
use thiserror::Error;

/// Interface used to manage subscriptions to service publishers
#[async_trait]
pub trait ServicePublisherManager: Send + Sync {
    /// Returns message descriptor of the specified topic published by the specified service.
    /// Returns None if the service and topic combination is not registered.
    /// Returns error if the service and topic combination is registered, but metadata retrieval failed.
    fn get_message_descriptor(
        &self,
        service_name: &str,
        topic_name: &str,
    ) -> Result<Option<MessageDescriptor>, ServicePublisherManagerSubscribeError>;

    /// Subscribes specified callback to the new messages from the specified topic
    /// with certain configuration, if provided.
    async fn try_subscribe(
        &self,
        service_name: &str,
        topic_name: &str,
        callback: Arc<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>,
        configuration: Option<&[u8]>,
    ) -> Result<bool, ServicePublisherManagerSubscribeError>;

    /// Unsubscribes from the specified topic and service name.
    async fn unsubscribe(
        &self,
        service_name: &str,
        topic_name: &str,
        configuration: Option<&[u8]>,
    ) -> Result<(), ServicePublisherManagerSubscribeError>;
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ServicePublisherManagerSubscribeError {
    #[error("Unable to subscribe to the publisher: {error}")]
    SubscribeFailed { error: anyhow::Error },
    #[error("Unable to unsubscribe from the topic: {error}")]
    UnsubscribeFailed { error: anyhow::Error },
}

#[cfg(test)]
pub(crate) mod mock;
