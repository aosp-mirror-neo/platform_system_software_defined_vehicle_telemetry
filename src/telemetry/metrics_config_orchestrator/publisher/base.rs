// Copyright 2024 Google LLC

use parking_lot::Mutex;
use protobuf::MessageDyn;
use sdv_telemetry_expr::node_evaluation_context::NodeEvaluationContext;
use std::{collections::HashMap, sync::Arc};

use super::CallbackFn;

pub(super) struct BasePublisher {
    name: String,
    metrics_config_uuid: String,
    latest_message: Mutex<Option<Arc<dyn MessageDyn>>>,
}

impl BasePublisher {
    pub(super) fn new(name: String, metrics_config_uuid: String) -> Self {
        Self { name, metrics_config_uuid, latest_message: None.into() }
    }

    pub(super) fn get_name(&self) -> &str {
        &self.name
    }

    pub(super) fn get_metrics_config_uuid(&self) -> &str {
        &self.metrics_config_uuid
    }

    pub(super) fn get_latest_message(&self) -> Option<Arc<dyn MessageDyn>> {
        self.latest_message.lock().as_ref().map(Arc::clone)
    }

    pub(super) fn set_new_message(
        &self,
        message: Arc<dyn MessageDyn>,
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
