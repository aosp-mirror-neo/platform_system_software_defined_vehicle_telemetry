// Copyright 2023 Google LLC

use async_trait::async_trait;
use protobuf::MessageDyn;
use sdv_telemetry_expr::node_evaluation_context::NodeEvaluationContext;
use sdv_telemetry_service_publisher_accessor::ServicePublisherAccessorError;
use sdv_telemetry_utils::operation_result::OperationResult;
use std::marker::{Send, Sync};
use std::sync::Arc;

mod base;

pub(crate) mod aggregation;
pub(crate) mod service;

#[cfg(test)]
pub(crate) mod mock;

// This cannot be CallbackFn<&Arc<NodeEvaluationContext>> due to a known issue in async_trait:
// https://github.com/dtolnay/async-trait/issues/8.
type CallbackFn = Box<dyn FnMut(Arc<NodeEvaluationContext>) + Send + Sync>;

#[async_trait]
#[cfg_attr(test, sdv_telemetry_utils::with_super_trait(downcast_rs::DowncastSync))]
pub(crate) trait Publisher: Send + Sync {
    fn get_name(&self) -> &str;

    fn get_metrics_config_uuid(&self) -> &str;

    fn is_subscribable(&self) -> bool;

    fn get_latest_message(
        self: Arc<Self>,
    ) -> OperationResult<'static, Option<Arc<dyn MessageDyn>>, ServicePublisherAccessorError>;

    async fn activate(self: Arc<Self>) -> Result<(), ServicePublisherAccessorError>;
    async fn deactivate(&self) -> anyhow::Result<()>;

    async fn subscribe(&self, data_trigger_name: &str, callback: CallbackFn);
    async fn unsubscribe(&self, data_trigger_name: &str);
}

#[cfg(test)]
downcast_rs::impl_downcast!(sync Publisher);
