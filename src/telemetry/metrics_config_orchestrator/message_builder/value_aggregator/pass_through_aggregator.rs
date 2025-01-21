// Copyright 2024 Google LLC

use sdv_telemetry_expr::node_value::NodeValue;

use crate::metrics_config_orchestrator::message_builder::{AggregationError, ValueAggregator};

/// Field value type where no aggregation is performed.
#[cfg_attr(test, derive(std::fmt::Debug))]
pub(crate) struct PassThroughAggregator {}

impl ValueAggregator for PassThroughAggregator {
    fn compute(&mut self, node_value: Option<NodeValue>) -> Result<NodeValue, AggregationError> {
        Ok(node_value.expect("Aggregator should be provided with a value"))
    }

    fn reset(&mut self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_ok() {
        let int64_val = NodeValue::Int64(20);
        let mut aggregator = PassThroughAggregator {};

        let res = aggregator.compute(Some(int64_val.clone())).unwrap();

        assert_eq!(res, int64_val);
    }

    #[test]
    #[should_panic]
    fn compute_without_node_value_panics() {
        let mut aggregator = PassThroughAggregator {};
        aggregator.compute(None).unwrap();
    }

    #[test]
    fn reset_ok() {
        let mut aggregator = PassThroughAggregator {};
        aggregator.reset();
    }
}
