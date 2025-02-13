// Copyright 2025 Google LLC

use crate::expr::node_error::{MessageProviderError, NodeEvaluationError};
use crate::metrics_config_orchestrator::message_builder::{
    AggregationError, MessageBuilderError, MessageBuilderErrorType,
};
use sdv_telemetry_service_publisher_accessor::ServicePublisherAccessorError;

use google_sdv_telemetry::aidl::google::sdv::telemetry::status::TelemetryServiceStatusCodes::TelemetryServiceStatusCodes as StatusCodes;

// This trait specifies status codes of errors which are sent back to the client via Misc Status Listener
pub(crate) trait ErrorStatusCodes {
    fn get_status_code(&self) -> StatusCodes;
}

impl ErrorStatusCodes for MessageBuilderError {
    fn get_status_code(&self) -> StatusCodes {
        match &self.error {
            MessageBuilderErrorType::BadAggregation { e } => e.get_status_code(),
            MessageBuilderErrorType::ExpressionEvaluationError { e } => e.get_status_code(),
            MessageBuilderErrorType::FailedToAssignValueToField { e } => e.get_status_code(),
        }
    }
}

impl ErrorStatusCodes for AggregationError {
    fn get_status_code(&self) -> StatusCodes {
        match self {
            AggregationError::ExpressionEvaluationError { e } => e.get_status_code(),
            _ => StatusCodes::METRICS_CONFIG_RUNTIME_AGGREGATION_ERROR,
        }
    }
}

impl ErrorStatusCodes for NodeEvaluationError {
    fn get_status_code(&self) -> StatusCodes {
        match self {
            NodeEvaluationError::MessageProviderError { e } => e.get_status_code(),
            _ => StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR,
        }
    }
}

impl ErrorStatusCodes for MessageProviderError {
    fn get_status_code(&self) -> StatusCodes {
        match self {
            MessageProviderError::MessageRetrievalError { e } => e.get_status_code(),
            MessageProviderError::InvalidPublisherName => {
                StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR
            }
        }
    }
}

impl ErrorStatusCodes for ServicePublisherAccessorError {
    fn get_status_code(&self) -> StatusCodes {
        match self {
            ServicePublisherAccessorError::MessageParsingFailed(_) => {
                StatusCodes::METRICS_CONFIG_RUNTIME_SERVICE_MESSAGE_PARSING_ERROR
            }
            ServicePublisherAccessorError::MetadataParsingFailed(_) => {
                StatusCodes::METRICS_CONFIG_RUNTIME_SERVICE_METADATA_PARSING_ERROR
            }
            ServicePublisherAccessorError::RpcFailedToConnect(_) => {
                StatusCodes::METRICS_CONFIG_RUNTIME_SERVICE_CONNECTION_ERROR
            }
            ServicePublisherAccessorError::FailedToSubscribe(_) => {
                StatusCodes::METRICS_CONFIG_RUNTIME_SERVICE_CONNECTION_ERROR
            }
            ServicePublisherAccessorError::MessageRetrievalFailed(_) => {
                StatusCodes::METRICS_CONFIG_RUNTIME_SERVICE_MESSAGE_RETRIEVAL_ERROR
            }
            ServicePublisherAccessorError::ServiceNotSupported => {
                StatusCodes::METRICS_CONFIG_RUNTIME_SERVICE_CONNECTION_ERROR
            }
        }
    }
}
