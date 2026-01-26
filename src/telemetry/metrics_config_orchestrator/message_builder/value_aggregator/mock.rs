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
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;

use crate::metrics_config_orchestrator::message_builder::{
    value_aggregator::AggregationError, ValueAggregator,
};

/// Implementation of ValueAggregator that has preset output for `compute()`.
#[derive(Clone)]
#[cfg_attr(test, derive(std::fmt::Debug))]
pub(crate) struct MockValueAggregator {
    fail_compute: Arc<AtomicBool>,
    num_reset_calls: Arc<AtomicU32>,
}

impl ValueAggregator for MockValueAggregator {
    fn compute(&mut self, node_value: Option<NodeValue>) -> Result<NodeValue, AggregationError> {
        if self.fail_compute.load(Ordering::Acquire) {
            Err(AggregationError::VectorInitializationError {
                value: "Simulated failure".to_owned(),
            })
        } else {
            Ok(node_value.unwrap())
        }
    }

    fn reset(&mut self) {
        self.num_reset_calls.fetch_add(1, Ordering::AcqRel);
    }
}

impl MockValueAggregator {
    pub(crate) fn new() -> Self {
        Self { fail_compute: Arc::new(false.into()), num_reset_calls: Arc::new(0.into()) }
    }

    pub(crate) fn get_num_reset_calls(&self) -> u32 {
        self.num_reset_calls.load(Ordering::Acquire)
    }

    pub(crate) fn fail_compute(self) -> Self {
        self.fail_compute.store(true, Ordering::Release);
        self
    }
}
