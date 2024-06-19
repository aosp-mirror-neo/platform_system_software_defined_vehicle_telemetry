// Copyright 2024 Google LLC

use sdv_telemetry_utils::operation_result::OperationResult;

use crate::expr::ast::NodeEvaluationContext;
use crate::expr::node_value::NodeValue;
use crate::metrics_config_orchestrator::message_builder::{
    value_aggregator::AggregationError, ValueAggregator,
};

/// Implementation of ValueAggregator that has preset output for `compute()`.
pub(crate) struct MockValueAggregator {
    output: NodeValue,
    fail_compute: bool,
}

impl ValueAggregator for MockValueAggregator {
    fn compute(
        &mut self,
        _context: &NodeEvaluationContext,
    ) -> OperationResult<NodeValue, AggregationError> {
        if self.fail_compute {
            Err(AggregationError::VectorInitializationError {
                value: "Simulated failure".to_owned(),
            })
        } else {
            Ok(self.output.clone())
        }
        .into()
    }
}

impl MockValueAggregator {
    pub fn new(output: NodeValue) -> Self {
        Self { output, fail_compute: false }
    }

    pub fn fail_compute(&mut self) {
        self.fail_compute = true;
    }
}
