mod avg_aggregator;
mod count_aggregator;
mod max_aggregator;
mod min_aggregator;
mod node_value_extension;
mod pass_through_aggregator;
mod vector_aggregator;

use sdvtelemetry_proto_rust::metrics_configuration::proto_message_builder::field_assignment::AggregationType;
use std::sync::Arc;
use thiserror::Error;

use crate::expr::{ast::Node, node_errors::NodeError, node_value::NodeValue};
use avg_aggregator::AvgAggregator;
use count_aggregator::CountAggregator;
use max_aggregator::MaxAggregator;
use min_aggregator::MinAggregator;
use pass_through_aggregator::PassThroughAggregator;
use vector_aggregator::VectorAggregator;

#[cfg(test)]
pub mod mock;

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
    fn compute(&mut self) -> Result<NodeValue, AggregationError>;
}

pub struct AggregationFactory {}

impl AggregationFactory {
    pub fn create(aggregation_type: AggregationType, node: &Arc<Node>) -> Box<dyn ValueAggregator> {
        match aggregation_type {
            AggregationType::AGGREGATION_TYPE_UNSPECIFIED => {
                panic!("AggregationType is not set but it passed validation")
            }
            AggregationType::NO_AGGREGATION => Box::new(PassThroughAggregator::new(node)),
            AggregationType::VECTOR => Box::new(VectorAggregator::new(node)),
            AggregationType::MAX => Box::new(MaxAggregator::new(node)),
            AggregationType::MIN => Box::new(MinAggregator::new(node)),
            AggregationType::AVG => Box::new(AvgAggregator::new(node)),
            AggregationType::COUNT => Box::new(CountAggregator::new()),
        }
    }
}
