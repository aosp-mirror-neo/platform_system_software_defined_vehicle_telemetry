// Copyright 2025 Google LLC

use protobuf::MessageDyn;
use std::sync::Arc;

use super::node_evaluation_context::NodeEvaluationContext;

pub(crate) trait NodeEvaluationContextFactory: Send + Sync {
    fn create(&self) -> NodeEvaluationContext;

    fn create_with_publisher_message(
        &self,
        publisher_name: String,
        publisher_message: Arc<dyn MessageDyn>,
    ) -> NodeEvaluationContext;
}

pub(crate) struct NodeEvaluationContextFactoryImpl {}

impl NodeEvaluationContextFactory for NodeEvaluationContextFactoryImpl {
    fn create(&self) -> NodeEvaluationContext {
        NodeEvaluationContext::new()
    }

    fn create_with_publisher_message(
        &self,
        publisher_name: String,
        publisher_message: Arc<dyn MessageDyn>,
    ) -> NodeEvaluationContext {
        NodeEvaluationContext::with_publisher_message(publisher_name, publisher_message)
    }
}

impl Default for NodeEvaluationContextFactoryImpl {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeEvaluationContextFactoryImpl {
    pub fn new() -> Self {
        Self {}
    }
}
