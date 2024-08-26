// Copyright 2023 Google LLC

use async_trait::async_trait;
use log::error;
use protobuf::MessageDyn;
use sdv_telemetry_utils::operation_result::OperationResult;
use std::fmt::Debug;
use std::marker::{Send, Sync};
use std::sync::Arc;
use thiserror::Error;

use crate::expr::node::NodeEvaluationContext;

mod base;

pub(crate) mod aggregation;
pub(crate) mod service;

#[cfg(test)]
pub(crate) mod mock;

// This cannot be CallbackFn<&Arc<NodeEvaluationContext>> due to a known issue in async_trait:
// https://github.com/dtolnay/async-trait/issues/8.
type CallbackFn = Box<dyn FnMut(Arc<NodeEvaluationContext>) + Send + Sync>;

#[async_trait]
#[cfg_attr(test, sdv_telemetry_utils_macros::with_super_trait(downcast_rs::DowncastSync))]
pub(crate) trait Publisher: Send + Sync {
    fn get_name(&self) -> &str;

    fn is_subscribable(&self) -> bool;

    fn get_latest_message(
        self: Arc<Self>,
    ) -> OperationResult<'static, Option<Arc<dyn MessageDyn>>, anyhow::Error>;

    async fn activate(self: Arc<Self>) -> Result<(), PublisherActivateError>;
    async fn deactivate(&self) -> Result<(), PublisherDeactivateError>;

    async fn subscribe(&self, data_trigger_name: &str, callback: CallbackFn);
    async fn unsubscribe(&self, data_trigger_name: &str);
}

#[cfg(test)]
downcast_rs::impl_downcast!(sync Publisher);

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
