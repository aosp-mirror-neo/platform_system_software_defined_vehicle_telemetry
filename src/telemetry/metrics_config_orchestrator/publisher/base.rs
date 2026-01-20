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

use parking_lot::Mutex;
use sdv_telemetry_expr::{
    dynamic_message::DynamicMessage, node_evaluation_context::NodeEvaluationContext,
};
use std::{collections::HashMap, sync::Arc};

use super::CallbackFn;
use crate::metrics_config_uuid::MetricsConfigUuid;

pub(super) struct BasePublisher {
    name: String,
    metrics_config_uuid: MetricsConfigUuid,
    latest_message: Mutex<Option<Arc<dyn DynamicMessage>>>,
}

impl BasePublisher {
    pub(super) fn new(name: String, metrics_config_uuid: MetricsConfigUuid) -> Self {
        Self { name, metrics_config_uuid, latest_message: None.into() }
    }

    pub(super) fn get_name(&self) -> &str {
        &self.name
    }

    pub(super) fn get_metrics_config_uuid(&self) -> &MetricsConfigUuid {
        &self.metrics_config_uuid
    }

    pub(super) fn get_latest_message(&self) -> Option<Arc<dyn DynamicMessage>> {
        self.latest_message.lock().as_ref().map(Arc::clone)
    }

    pub(super) fn set_new_message(
        &self,
        message: Arc<dyn DynamicMessage>,
        context: &Arc<NodeEvaluationContext>,
        callback_map: &mut HashMap<String, CallbackFn>,
    ) {
        // Replace latest message.
        self.latest_message.lock().replace(message);

        // Notify subscribed data triggers.
        // TODO(b/294129114): Replace callback_map with just a single callback.
        for callback in callback_map.values_mut() {
            callback(Arc::clone(context));
        }
    }

    pub(super) fn reset_latest_message(&self) {
        *self.latest_message.lock() = None;
    }
}
