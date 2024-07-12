// Copyright 2023 Google LLC

use async_trait::async_trait;
use downcast_rs::DowncastSync;
use log::error;
use protobuf::MessageDyn;
use sdv_telemetry_utils::value_holder::ValueHolder;
use std::fmt::Debug;
use std::marker::{Send, Sync};
use std::sync::Arc;
use thiserror::Error;

#[cfg(test)]
use downcast_rs::impl_downcast;

use crate::expr::node::NodeEvaluationContext;

mod base;

pub(crate) mod aggregation;
pub(crate) mod service;

#[cfg(test)]
pub(crate) mod mock;

type CallbackFn = Box<dyn FnMut(&dyn MessageDyn, NodeEvaluationContext) + Send + Sync>;

#[async_trait]
pub(crate) trait Publisher: Send + Sync + DowncastSync {
    fn get_name(&self) -> &str;

    fn is_subscribable(&self) -> bool;

    fn get_latest_message(self: Arc<Self>) -> ValueHolder<'static, Option<Arc<dyn MessageDyn>>>;

    async fn activate(self: Arc<Self>) -> Result<(), PublisherActivateError>;
    async fn deactivate(&self) -> Result<(), PublisherDeactivateError>;

    async fn subscribe(&self, data_trigger_name: &str, callback: CallbackFn);
    async fn unsubscribe(&self, data_trigger_name: &str);
}

#[cfg(test)]
impl_downcast!(sync Publisher);

#[derive(Error, Debug)]
pub(crate) enum PublisherActivateError {
    #[error("Service adapter failed to create message provider: {error:#}")]
    ServiceAdapterGetMessageProviderError { error: anyhow::Error },
    #[error("Service adapter failed to process subscription request: {error:#}")]
    ServiceAdapterSubscribeError { error: anyhow::Error },
}

#[derive(Error, Debug)]
pub(crate) enum PublisherDeactivateError {
    #[error("Service adapter failed to process unsubscription request: {error:#}")]
    ServiceAdapterUnsubscribeError {
        #[from]
        error: anyhow::Error,
    },
}
