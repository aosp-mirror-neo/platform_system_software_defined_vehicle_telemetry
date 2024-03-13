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
    fn compute(&mut self) -> Result<NodeValue, AggregationError> {
        if self.fail_compute {
            return Err(AggregationError::VectorInitializationError {
                value: "Simulated failure".to_string(),
            });
        }
        Ok(self.output.clone())
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
