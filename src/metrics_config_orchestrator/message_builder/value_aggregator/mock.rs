// Copyright 2024 Google LLC

use crate::expr::node_value::NodeValue;
use crate::metrics_config_orchestrator::message_builder::{
    value_aggregator::AggregationError, ValueAggregator,
};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;

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
