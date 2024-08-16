// Copyright 2024 Google LLC

use crate::expr::node_value::NodeValue;
use crate::metrics_config_orchestrator::message_builder::{AggregationError, ValueAggregator};

/// Field value type where count aggregation is performed.
pub(crate) struct CountAggregator {
    count: i32,
}

impl CountAggregator {
    pub(crate) fn new() -> Self {
        Self { count: 0 }
    }
}

impl ValueAggregator for CountAggregator {
    fn compute(&mut self, _node_value: Option<NodeValue>) -> Result<NodeValue, AggregationError> {
        self.count += 1;
        Ok(NodeValue::Int32(self.count))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_aggregator_compute_ok() {
        let mut aggregator = CountAggregator::new();

        // compute 5 times to see the count is incrementing
        for i in 0..5 {
            // action
            let res = aggregator.compute(None).unwrap();

            // assert
            assert_eq!(res, NodeValue::Int32(i + 1));
        }
    }
}
