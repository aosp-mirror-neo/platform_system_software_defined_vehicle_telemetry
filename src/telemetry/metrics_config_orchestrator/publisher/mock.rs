/*
 * Copyright (C) 2026 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use async_trait::async_trait;
use parking_lot::Mutex;
use sdv_dump::DumpStateReporter;
use sdv_telemetry_expr::dynamic_message::DynamicMessage;
use sdv_telemetry_expr::node_evaluation_context::NodeEvaluationContext;
use sdv_telemetry_service_publisher_accessor::ServicePublisherAccessorError;
use sdv_telemetry_utils::operation_result::OperationResult;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::Mutex as AsyncMutex;

use super::Publisher;
use crate::metrics_config_orchestrator::publisher::CallbackFn;
use crate::metrics_config_uuid::MetricsConfigUuid;

pub(crate) struct MockPublisher {
    pub expected_trigger_names: HashSet<String>,
    pub latest_message: Option<Arc<dyn DynamicMessage>>,

    name: String,
    metrics_config_uuid: MetricsConfigUuid,
    activate_callback: Option<Box<dyn Fn(&str) + Send + Sync>>,
    deactivate_callback: Option<Box<dyn Fn(&str) + Send + Sync>>,
    latest_message_error: Mutex<Option<ServicePublisherAccessorError>>,
    new_message_callback: AsyncMutex<Option<CallbackFn>>,
    unsubscribe_callback: Option<Box<dyn Fn() + Send + Sync>>,
}

#[async_trait]
impl Publisher for MockPublisher {
    fn get_name(&self) -> &str {
        &self.name
    }

    fn get_metrics_config_uuid(&self) -> &MetricsConfigUuid {
        &self.metrics_config_uuid
    }

    fn is_subscribable(&self) -> bool {
        true
    }

    fn get_latest_message(
        self: Arc<Self>,
    ) -> OperationResult<'static, Option<Arc<dyn DynamicMessage>>, ServicePublisherAccessorError>
    {
        if let Some(err) = self.latest_message_error.lock().take() {
            return Err(err).into();
        }

        Ok(self.latest_message.clone()).into()
    }

    async fn activate(self: Arc<Self>) {
        if let Some(callback) = &self.activate_callback {
            callback(&self.name)
        }
    }

    async fn deactivate(&self) {
        if let Some(callback) = &self.deactivate_callback {
            callback(&self.name);
        }
    }

    async fn subscribe(&self, data_trigger_name: &str, new_message_callback: CallbackFn) {
        assert!(self.expected_trigger_names.contains(data_trigger_name));
        self.new_message_callback.lock().await.replace(new_message_callback);
    }

    async fn unsubscribe(&self, data_trigger_name: &str) {
        assert!(self.expected_trigger_names.contains(data_trigger_name));

        if let Some(callback) = &self.unsubscribe_callback {
            callback();
        }
    }
}

impl DumpStateReporter for MockPublisher {
    fn dump(
        &self,
        writer: &mut dyn std::io::Write,
        _args: &[&std::ffi::CStr],
    ) -> Result<(), std::io::Error> {
        writeln!(writer, "MockPublisher")?;
        Ok(())
    }
}

impl MockPublisher {
    pub(crate) fn new(name: String, metrics_config_uuid: MetricsConfigUuid) -> Self {
        Self {
            expected_trigger_names: HashSet::new(),
            latest_message: None,
            latest_message_error: None.into(),
            name,
            metrics_config_uuid,
            activate_callback: None,
            deactivate_callback: None,
            new_message_callback: None.into(),
            unsubscribe_callback: None,
        }
    }

    pub(crate) async fn fire_new_message(&self, context: Arc<NodeEvaluationContext>) {
        self.new_message_callback.lock().await.as_mut().unwrap()(context);
    }

    pub(crate) fn set_activate_callback(
        &mut self,
        callback: impl Fn(&str) + Send + Sync + 'static,
    ) {
        self.activate_callback.replace(Box::new(callback));
    }

    pub(crate) fn set_deactivate_callback(
        &mut self,
        callback: impl Fn(&str) + Send + Sync + 'static,
    ) {
        self.deactivate_callback.replace(Box::new(callback));
    }

    pub(crate) fn set_latest_message_error(&self, error: ServicePublisherAccessorError) {
        self.latest_message_error.lock().replace(error);
    }

    pub(crate) fn set_unsubscribe_callback(&mut self, callback: impl Fn() + Send + Sync + 'static) {
        self.unsubscribe_callback.replace(Box::new(callback));
    }
}
