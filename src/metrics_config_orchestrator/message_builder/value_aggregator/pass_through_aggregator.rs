use std::sync::Arc;

use crate::expr::{ast::Node, node_value::NodeValue};
use crate::metrics_config_orchestrator::message_builder::{AggregationError, ValueAggregator};

/// Field value type where no aggregation is performed.
pub struct PassThroughAggregator {
    node: Arc<Node>,
}

impl PassThroughAggregator {
    pub fn new(node: &Arc<Node>) -> Self {
        Self { node: node.clone() }
    }
}

impl ValueAggregator for PassThroughAggregator {
    fn compute(&mut self) -> Result<NodeValue, AggregationError> {
        self.node.evaluate().map_err(|e| AggregationError::ExpressionEvaluationError { e })
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

        assert_eq!(int64_field_value.compute().unwrap(), int64_val);
    }

    #[test]
    fn pass_through_aggregator_compute_node_evaluation_error_fails() {
        let true_val = Arc::new(Node::ConstantLeaf { value: NodeValue::Bool(true) });
        let bad_node = Arc::new(Node::Combination {
            operator: OP_ADD.clone(),
            left: Some(true_val.clone()),
            right: Some(true_val),
        });
        let mut bad_value = PassThroughAggregator::new(&bad_node);

        let res = bad_value.compute();

        assert!(matches!(res.unwrap_err(), AggregationError::ExpressionEvaluationError { .. }));
    }
}
