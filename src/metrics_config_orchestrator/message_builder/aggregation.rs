use sdvtelemetry_proto_rust::metrics_configuration::proto_message_builder::field_assignment::AggregationType;
use std::sync::Arc;

use crate::expr::{ast::Node, node_errors::NodeError, node_value::NodeValue};

#[allow(dead_code)]
pub struct AggregatedFieldValue {
    aggregation_type: AggregationType,
    // The node eval output is the latest value.
    node: Arc<Node>,
    // Current state for aggregation. With every new latest value, the state
    // will be updated. State has different meanings for different aggregation
    // types. It means the historical minimum for AggregationType::MIN,
    // running average for AggregationType::AVERGAE etc.
    state: Option<NodeValue>,
    count: usize,
}

impl AggregatedFieldValue {
    pub fn new(aggregation_type: AggregationType, node: &Arc<Node>) -> Self {
        AggregatedFieldValue { aggregation_type, node: node.clone(), state: None, count: 0 }
    }

    pub fn compute(&mut self) -> Result<NodeValue, NodeError> {
        self.node.evaluate()
    }
}
