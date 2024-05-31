// Copyright 2024 Google LLC

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

use thiserror::Error;

use crate::expr::{ast::NodeEvaluationContext, node_errors::NodeError, node_value::NodeValue};

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum AggregationError {
    #[error("Evaluation of expression node failed: {e}")]
    ExpressionEvaluationError {
        #[from]
        e: NodeError,
    },
    #[error("Cannot create a vector containing elements of type: {value}.")]
    VectorInitializationError { value: String },
    #[error("Append has incompatible types. List is {list} and value to append is {value}")]
    VectorAppendTypeMismatch { list: String, value: String },
    #[error("Numeric aggregation requires expressions to output numeric values, but the value is {value}")]
    NumericAggregationTypeMismatch { value: String },
}

/// Aggregation types should implement this trait and perform the corresponding
/// aggregation at `compute()`.
pub trait ValueAggregator: Send + Sync {
    fn compute(&mut self, context: &NodeEvaluationContext) -> Result<NodeValue, AggregationError>;
}

#[cfg(test)]
pub(crate) mod mock;
