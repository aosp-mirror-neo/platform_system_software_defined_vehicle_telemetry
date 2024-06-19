// Copyright 2024 Google LLC

use sdv_telemetry_utils::operation_result::OperationResult;
use std::sync::Arc;

use crate::expr::ast::NodeEvaluationContext;
use crate::expr::{ast::Node, node_value::NodeValue};
use crate::metrics_config_orchestrator::message_builder::{AggregationError, ValueAggregator};

/// Field value type where no aggregation is performed.
pub struct PassThroughAggregator {
    node: Arc<Node>,
}

impl PassThroughAggregator {
    pub fn new(node: &Arc<Node>) -> Self {
        Self { node: Arc::clone(node) }
    }
}

impl ValueAggregator for PassThroughAggregator {
    fn compute<'a>(
        &'a mut self,
        context: &'a NodeEvaluationContext,
    ) -> OperationResult<NodeValue, AggregationError> {
        self.node.evaluate(context).err_into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::mock::OP_ADD;

    #[test]
    fn pass_through_aggregator_compute_ok() {
        let int64_val = NodeValue::Int64(20);
        let mut int64_field_value =
            PassThroughAggregator::new(&Arc::new(Node::ConstantLeaf { value: int64_val.clone() }));

        let context = NodeEvaluationContext::new();
        assert_eq!(int64_field_value.compute(&context).unwrap_sync().unwrap(), int64_val);
    }

    #[test]
    fn pass_through_aggregator_compute_node_evaluation_error_fails() {
        let true_val = Arc::new(Node::ConstantLeaf { value: NodeValue::Bool(true) });
        let bad_node = Arc::new(Node::Combination {
            operator: OP_ADD.clone(),
            left: Some(Arc::clone(&true_val)),
            right: Some(true_val),
        });
        let mut bad_value = PassThroughAggregator::new(&bad_node);

        let context = NodeEvaluationContext::new();
        let err = bad_value.compute(&context).unwrap_sync().unwrap_err();

        assert!(matches!(err, AggregationError::ExpressionEvaluationError { .. }));
    }
}
