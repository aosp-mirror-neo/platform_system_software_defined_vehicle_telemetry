// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;
use sdv_dump::DumpStateReporter;
use sdv_telemetry_expr::dynamic_message::DynamicMessage;
use sdv_telemetry_expr::node_evaluation_context::NodeEvaluationContext;
use sdv_telemetry_service_publisher_accessor::ServicePublisherAccessorError;
use sdv_telemetry_utils::operation_result::OperationResult;
use std::marker::{Send, Sync};
use std::sync::Arc;

use crate::metrics_config_uuid::MetricsConfigUuid;

mod base;

pub(crate) mod aggregation;
pub(crate) mod service;

#[cfg(test)]
pub(crate) mod mock;

// This cannot be CallbackFn<&Arc<NodeEvaluationContext>> due to a known issue in async_trait:
// https://github.com/dtolnay/async-trait/issues/8.
type CallbackFn = Box<dyn FnMut(Arc<NodeEvaluationContext>) + Send + Sync>;

#[async_trait]
pub(crate) trait Publisher: DumpStateReporter + Send + Sync {
    fn get_name(&self) -> &str;

    fn get_metrics_config_uuid(&self) -> &MetricsConfigUuid;

    fn is_subscribable(&self) -> bool;

    fn get_latest_message(
        self: Arc<Self>,
    ) -> OperationResult<'static, Option<Arc<dyn DynamicMessage>>, ServicePublisherAccessorError>;

    async fn activate(self: Arc<Self>);
    async fn deactivate(&self);

    async fn subscribe(&self, data_trigger_name: &str, callback: CallbackFn);
    async fn unsubscribe(&self, data_trigger_name: &str);
}
