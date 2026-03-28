// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

    fn default_value(&self) -> NodeValue {
        NodeValue::None
    }
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

    #[test]
    fn default_value_none() {
        let default_value = PassThroughAggregator {}.default_value();
        assert_eq!(default_value, NodeValue::None);
    }
}
