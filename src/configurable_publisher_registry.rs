// Copyright 2023 Google LLC. All Rights Reserved.

#![allow(missing_docs)]

// External dependencies.
use async_trait::async_trait;
use log::error;
use protobuf::reflect::MessageDescriptor;
use protobuf::MessageDyn;
use std::fmt::Debug;
use std::marker::{Send, Sync};
use thiserror::Error;

/// Interface used to manage subscriptions to registered publishers
#[async_trait]
pub trait ConfigurablePublisherRegistry: Send + Sync {
    /// Returns message descriptor of the specified topic published by the specified service.
    fn get_message_descriptor(
        &self,
        service_name: &str,
        topic_name: &str,
    ) -> Result<MessageDescriptor, ConfigurablePublisherRegistrySubscribeError>;

    /// Subscribes specified callback to the new messages from the specified topic
    /// with certain configuration, if provided.
    async fn subscribe(
        &self,
        service_name: &str,
        topic_name: &str,
        callback: Box<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>,
        configuration: Option<&[u8]>,
    ) -> Result<(), ConfigurablePublisherRegistrySubscribeError>;

    /// Unsubscribes from the specified topic and service name.
    async fn unsubscribe(
        &self,
        service_name: &str,
        topic_name: &str,
        configuration: Option<&[u8]>,
    ) -> Result<(), ConfigurablePublisherRegistrySubscribeError>;
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ConfigurablePublisherRegistrySubscribeError {
    #[error("Publisher {service_name} is not registered")]
    ServiceNotFound { service_name: String },
    #[error("Topic {topic_name} is not registered for publisher {service_name}")]
    TopicNotFound { service_name: String, topic_name: String },
    #[error("Unable to subscribe to the topic: {error}")]
    SubscribeFailed { error: binder::Status },
    #[error("Unable to unsubscribe from the topic: {error}")]
    UnsubscribeFailed { error: binder::Status },
}

#[cfg(test)]
pub(crate) mod mock;
