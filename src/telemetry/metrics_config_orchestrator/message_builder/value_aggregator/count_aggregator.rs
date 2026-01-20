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

/// Field value type where count aggregation is performed.
#[cfg_attr(test, derive(std::fmt::Debug))]
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

    fn reset(&mut self) {
        self.count = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_ok() {
        let mut aggregator = CountAggregator::new();

        // compute 5 times to see the count is incrementing
        for i in 0..5 {
            // action
            let res = aggregator.compute(None).unwrap();

            // assert
            assert_eq!(res, NodeValue::Int32(i + 1));
        }
    }

    #[test]
    fn reset_ok() {
        let mut aggregator = CountAggregator::new();
        aggregator.compute(None).unwrap();

        aggregator.reset();

        assert_eq!(aggregator.count, 0);
    }
}
