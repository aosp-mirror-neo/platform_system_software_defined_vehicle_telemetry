// Copyright 2023 Google LLC

use async_trait::async_trait;
use protobuf::MessageDyn;
use sdv_telemetry_utils::operation_result::OperationResult;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::Mutex as AsyncMutex;

use super::{Publisher, PublisherActivateError, PublisherDeactivateError};
use crate::metrics_config_orchestrator::NodeEvaluationContext;

pub(crate) struct MockPublisher {
    pub expected_trigger_names: HashSet<String>,
    pub latest_message: Option<Arc<dyn MessageDyn>>,

    name: String,
    activate_callback: Option<Box<dyn Fn(&str) -> Option<PublisherActivateError> + Send + Sync>>,
    deactivate_callback:
        Option<Box<dyn Fn(&str) -> Option<PublisherDeactivateError> + Send + Sync>>,
    new_message_callback: AsyncMutex<
        Option<
            Box<dyn for<'a> FnMut(&'a dyn MessageDyn, Arc<NodeEvaluationContext>) + Send + Sync>,
        >,
    >,
    unsubscribe_callback: Option<Box<dyn Fn() + Send + Sync>>,
}

#[async_trait]
impl Publisher for MockPublisher {
    fn get_name(&self) -> &str {
        &self.name
    }

    fn is_subscribable(&self) -> bool {
        true
    }

    fn get_latest_message(
        self: Arc<Self>,
    ) -> OperationResult<'static, Option<Arc<dyn MessageDyn>>, anyhow::Error> {
        Ok(self.latest_message.clone()).into()
    }

    async fn activate(self: Arc<Self>) -> Result<(), PublisherActivateError> {
        if let Some(callback) = &self.activate_callback {
            if let Some(error) = callback(&self.name) {
                return Err(error);
            }
        }

        Ok(())
    }

    async fn deactivate(&self) -> Result<(), PublisherDeactivateError> {
        if let Some(callback) = &self.deactivate_callback {
            if let Some(error) = callback(&self.name) {
                return Err(error);
            }
        }

        Ok(())
    }

    async fn subscribe(
        &self,
        data_trigger_name: &str,
        new_message_callback: Box<
            dyn for<'a> FnMut(&'a dyn MessageDyn, Arc<NodeEvaluationContext>) + Send + Sync,
        >,
    ) {
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

impl MockPublisher {
    pub(crate) fn new(name: String) -> Self {
        Self {
            expected_trigger_names: HashSet::new(),
            latest_message: None,
            name,
            activate_callback: None,
            deactivate_callback: None,
            new_message_callback: None.into(),
            unsubscribe_callback: None,
        }
    }

    pub(crate) fn get_name(&self) -> &str {
        &self.name
    }

    pub(crate) async fn fire_new_message(
        &self,
        message: &dyn MessageDyn,
        context: Arc<NodeEvaluationContext>,
    ) {
        self.new_message_callback.lock().await.as_mut().unwrap()(message, context);
    }

    pub(crate) fn set_activate_callback(
        &mut self,
        callback: impl Fn(&str) -> Option<PublisherActivateError> + Send + Sync + 'static,
    ) {
        self.activate_callback.replace(Box::new(callback));
    }

    pub(crate) fn set_deactivate_callback(
        &mut self,
        callback: impl Fn(&str) -> Option<PublisherDeactivateError> + Send + Sync + 'static,
    ) {
        self.deactivate_callback.replace(Box::new(callback));
    }

    pub(crate) fn set_unsubscribe_callback(&mut self, callback: impl Fn() + Send + Sync + 'static) {
        self.unsubscribe_callback.replace(Box::new(callback));
    }
}
