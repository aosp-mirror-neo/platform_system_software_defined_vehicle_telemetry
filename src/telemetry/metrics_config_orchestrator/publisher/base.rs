// Copyright 2024 Google LLC

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
