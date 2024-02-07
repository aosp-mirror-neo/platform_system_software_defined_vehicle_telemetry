// Copyright 2023 Google LLC. All Rights Reserved.

#![allow(missing_docs)]

// External dependencies.
use async_trait::async_trait;
use log::error;
use protobuf::reflect::MessageDescriptor;
use protobuf::MessageDyn;
use std::fmt::Debug;
use std::marker::{Send, Sync};
use std::sync::Arc;
use thiserror::Error;

/// Interface used to manage subscriptions to data tunnel topics
#[async_trait]
pub trait DataTunnelWrapper: Send + Sync {
    /// Returns message descriptor of the specified topic
    fn get_message_descriptor(&self, topic_name: &str) -> Option<MessageDescriptor>;

    /// Subscribes specified callback to the new messages from the specified topic
    async fn subscribe(
        self: Arc<Self>,
        topic_name: &str,
        callback: Box<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>,
    ) -> Result<(), DataTunnelWrapperError>;

    /// Checks if topic with given name exists
    fn topic_exists(&self, topic_name: &str) -> bool;
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum DataTunnelWrapperError {
    #[error("Unable to read data from topic: {topic_name}")]
    ReadFailed { topic_name: String },
    #[error("Unable to subscribe to the topic: {topic_name}")]
    SubscribeFailed { topic_name: String },
    #[error("Already subscribed to the given topic: {topic_name}")]
    TopicSubscriptionsExists { topic_name: String },
    #[error("Topic {topic_name} is not registered with data tunnel")]
    TopicNotFound { topic_name: String },
}

#[cfg(test)]
pub(crate) mod mock;
