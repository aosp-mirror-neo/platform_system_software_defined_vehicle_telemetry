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

pub(super) mod avg_aggregator;
pub(super) mod count_aggregator;
pub(super) mod delta_aggregator;
pub(super) mod max_aggregator;
pub(super) mod min_aggregator;
pub(super) mod node_value_extension;
pub(super) mod pass_through_aggregator;
pub(super) mod std_dev_aggregator;
pub(super) mod sum_aggregator;
pub(super) mod vector_aggregator;

use sdv_telemetry_expr::{node_error::NodeEvaluationError, node_value::NodeValue};

#[derive(thiserror::Error, Debug)]
pub(crate) enum AggregationError {
    #[error("Evaluation of expression node failed: {e}")]
    ExpressionEvaluationError {
        #[from]
        e: NodeEvaluationError,
    },
    #[error("Numeric aggregation requires expressions to output numeric values, but the value is {value}")]
    NumericAggregationTypeMismatch { value: String },
    #[error("Cannot create a vector containing elements of type: {value}.")]
    VectorInitializationError { value: String },
    #[error("Append has incompatible types. List is {list} and value to append is {value}")]
    VectorAppendTypeMismatch { list: String, value: String },
}

/// Aggregation types should implement this trait and perform the corresponding
/// aggregation at `compute()`.
#[cfg_attr(test, sdv_telemetry_utils::with_super_trait(std::fmt::Debug))]
pub(crate) trait ValueAggregator: Send + Sync {
    fn compute(&mut self, node_value: Option<NodeValue>) -> Result<NodeValue, AggregationError>;
    fn reset(&mut self);
}

#[cfg(test)]
pub(crate) mod mock;
