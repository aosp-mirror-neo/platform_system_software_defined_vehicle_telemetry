// Copyright 2024 Google LLC

use crate::expr::node_value::NodeValue;
use crate::metrics_config_orchestrator::message_builder::{
    value_aggregator::AggregationError, ValueAggregator,
};

/// Implementation of ValueAggregator that has preset output for `compute()`.
#[cfg_attr(test, derive(std::fmt::Debug))]
pub(crate) struct MockValueAggregator {
    fail_compute: bool,
}

impl ValueAggregator for MockValueAggregator {
    fn compute(&mut self, node_value: Option<NodeValue>) -> Result<NodeValue, AggregationError> {
        if self.fail_compute {
            Err(AggregationError::VectorInitializationError {
                value: "Simulated failure".to_owned(),
            })
        } else {
            Ok(node_value.unwrap())
        }
    }
}

impl MockValueAggregator {
    pub(crate) fn new() -> Self {
        Self { fail_compute: false }
    }

    pub(crate) fn fail_compute(mut self) -> Self {
        self.fail_compute = true;
        self
    }
}
