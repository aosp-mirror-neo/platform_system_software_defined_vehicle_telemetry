// Copyright 2025 Google LLC

use crate::expr::node_error::{
    DescriptorProviderError, FunctionLeafNodeArgumentError,
    FunctionLeafNodeGetCurrentTimestampArgumentError, MessageProviderError, NodeEvaluationError,
};
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
            AggregationError::NumericAggregationTypeMismatch { .. }
            | AggregationError::VectorInitializationError { .. }
            | AggregationError::VectorAppendTypeMismatch { .. } => {
                StatusCodes::METRICS_CONFIG_RUNTIME_AGGREGATION_ERROR
            }
        }
    }
}

impl ErrorStatusCodes for NodeEvaluationError {
    fn get_status_code(&self) -> StatusCodes {
        match self {
            NodeEvaluationError::MessageProviderError { e } => e.get_status_code(),
            NodeEvaluationError::DescriptorProviderError { e } => e.get_status_code(),
            NodeEvaluationError::FunctionLeafNodeArgumentError { error } => error.get_status_code(),
            NodeEvaluationError::IndexOutOfBound { .. }
            | NodeEvaluationError::CyclicDependency { .. }
            | NodeEvaluationError::NodeTypeNotSet
            | NodeEvaluationError::NodeTypeNotSupported
            | NodeEvaluationError::TypeMismatch { .. }
            | NodeEvaluationError::InternalError { .. }
            | NodeEvaluationError::ConstantValueNotSet
            | NodeEvaluationError::ConstantValueNotSupported { .. }
            | NodeEvaluationError::FieldNotFound { .. }
            | NodeEvaluationError::WrongNestedFieldType { .. }
            | NodeEvaluationError::UnsupportedSingularFieldType
            | NodeEvaluationError::UnsupportedRepeatedFieldType
            | NodeEvaluationError::FunctionLeafNodeNotSet
            | NodeEvaluationError::FunctionLeafNodeNotSupported
            | NodeEvaluationError::OperatorNotSet
            | NodeEvaluationError::DeprecatedOperator { .. }
            | NodeEvaluationError::UnknownOperator { .. }
            | NodeEvaluationError::LeftChildNotSet
            | NodeEvaluationError::UnaryOperatorIncorrectChildren
            | NodeEvaluationError::BinaryOperatorIncorrectChildren
            | NodeEvaluationError::LogicalOperatorIncompatibleOperands
            | NodeEvaluationError::ComparisonOperatorIncomptaibleOperands
            | NodeEvaluationError::ComparisonOperatorListIncompatibleOperands
            | NodeEvaluationError::ArithmeticOperatorIncompatibleOperands
            | NodeEvaluationError::RoundingOperatorIncompatibleOperands
            | NodeEvaluationError::ModuloOperatorIncompatibleOperands
            | NodeEvaluationError::DivideByZero
            | NodeEvaluationError::ContainsOperatorIncompatibleOperands
            | NodeEvaluationError::RepeatedFieldWrongAssignment { .. }
            | NodeEvaluationError::ValueToProtoConversionError { .. } => {
                StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR
            }
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

impl ErrorStatusCodes for DescriptorProviderError {
    fn get_status_code(&self) -> StatusCodes {
        match self {
            DescriptorProviderError::InvalidPublisherName => {
                StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR
            }
        }
    }
}

impl ErrorStatusCodes for FunctionLeafNodeArgumentError {
    fn get_status_code(&self) -> StatusCodes {
        match self {
            FunctionLeafNodeArgumentError::GetCurrentTimestampError(e) => e.get_status_code(),
        }
    }
}

impl ErrorStatusCodes for FunctionLeafNodeGetCurrentTimestampArgumentError {
    fn get_status_code(&self) -> StatusCodes {
        match self {
            FunctionLeafNodeGetCurrentTimestampArgumentError::TimeSourceNotSet
            | FunctionLeafNodeGetCurrentTimestampArgumentError::TimeSourceNotSupported => {
                StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR
            }
        }
    }
}

impl ErrorStatusCodes for ServicePublisherAccessorError {
    fn get_status_code(&self) -> StatusCodes {
        match self {
            ServicePublisherAccessorError::MetadataParsingFailed(_) => {
                StatusCodes::METRICS_CONFIG_RUNTIME_SERVICE_METADATA_PARSING_ERROR
            }
            ServicePublisherAccessorError::MessageParsingFailed(_) => {
                StatusCodes::METRICS_CONFIG_RUNTIME_SERVICE_MESSAGE_PARSING_ERROR
            }
            ServicePublisherAccessorError::MessageRetrievalFailed(_) => {
                StatusCodes::METRICS_CONFIG_RUNTIME_SERVICE_MESSAGE_RETRIEVAL_ERROR
            }
            ServicePublisherAccessorError::RpcFailedToConnect(_)
            | ServicePublisherAccessorError::FailedToSubscribe(_)
            | ServicePublisherAccessorError::ServiceNotSupported => {
                StatusCodes::METRICS_CONFIG_RUNTIME_SERVICE_CONNECTION_ERROR
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use anyhow::anyhow;

    use sdv_status_rs::SdvStatus;
    use sdv_telemetry_proto_rust::expression::combination_node::{
        Operator as OperatorProto, RelationalOperator,
    };
    use sdv_telemetry_proto_rust::expression::constant_leaf_node::Node_value as NodeValueProto;
    use sdv_telemetry_test_utils::parameterized_test;

    parameterized_test!(
        #[test]
        aggregation_error,
        |error: AggregationError, expected_status_code| {
            assert_eq!(error.get_status_code(), expected_status_code);
        },
        aggregation_type_mismatch: (AggregationError::NumericAggregationTypeMismatch { value: String::default() }, StatusCodes::METRICS_CONFIG_RUNTIME_AGGREGATION_ERROR),
        aggregation_vector_initialization: (AggregationError::VectorInitializationError { value: String::default() }, StatusCodes::METRICS_CONFIG_RUNTIME_AGGREGATION_ERROR),
        aggregation_vector_append_type_mismatch: (AggregationError::VectorAppendTypeMismatch { list: String::default(), value: String::default() }, StatusCodes::METRICS_CONFIG_RUNTIME_AGGREGATION_ERROR),
    );

    parameterized_test!(
        #[test]
        node_evaluation_error,
        |error: NodeEvaluationError, expected_status_code| {
            assert_eq!(error.get_status_code(), expected_status_code);
        },
        node_evaluation_index_out_of_bound: (NodeEvaluationError::IndexOutOfBound { n: usize::default() }, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_cyclic_dependency: (NodeEvaluationError::CyclicDependency { cycle: Vec::default() }, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_node_type_not_set: (NodeEvaluationError::NodeTypeNotSet, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_node_type_not_supported: (NodeEvaluationError::NodeTypeNotSupported, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_type_mismatch: (NodeEvaluationError::TypeMismatch { msg: String::default() }, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_internal_error: (NodeEvaluationError::InternalError { msg: String::default() }, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_constant_value_not_set: (NodeEvaluationError::ConstantValueNotSet, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_constant_value_not_supported: (NodeEvaluationError::ConstantValueNotSupported { node_value: NodeValueProto::BoolValue(false) }, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_field_not_found: (NodeEvaluationError::FieldNotFound { field_name: String::default(), message_name: String::default() }, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_wrong_nested_field_type: (NodeEvaluationError::WrongNestedFieldType {  field_name: String::default(), e: String::default() }, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_unsupported_singular_field_type: (NodeEvaluationError::UnsupportedSingularFieldType, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_unsupported_repeated_field_type: (NodeEvaluationError::UnsupportedRepeatedFieldType, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_function_leaf_node_not_set: (NodeEvaluationError::FunctionLeafNodeNotSet, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_function_leaf_node_not_supported: (NodeEvaluationError::FunctionLeafNodeNotSupported, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_operator_not_set: (NodeEvaluationError::OperatorNotSet, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_deprecated_operator: (NodeEvaluationError::DeprecatedOperator { op: OperatorProto::RelationalOperator(RelationalOperator::EQ.into()) }, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_unknown_operator: (NodeEvaluationError::UnknownOperator { op: String::default()  }, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_left_child_not_set: (NodeEvaluationError::LeftChildNotSet, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_unary_operator_incorrect_children: (NodeEvaluationError::UnaryOperatorIncorrectChildren, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_binary_operator_incorrect_children: (NodeEvaluationError::BinaryOperatorIncorrectChildren, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_logical_operator_incompatible_operands: (NodeEvaluationError::LogicalOperatorIncompatibleOperands, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_comparison_operator_incompatible_operands: (NodeEvaluationError::ComparisonOperatorIncomptaibleOperands, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_comparison_operator_list_incompatible_operands: (NodeEvaluationError::ComparisonOperatorListIncompatibleOperands, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_arithmetic_operator_incompatible_operands: (NodeEvaluationError::ArithmeticOperatorIncompatibleOperands, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_rounding_operator_incompatible_operands: (NodeEvaluationError::RoundingOperatorIncompatibleOperands, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_modulo_operator_incompatible_operands: (NodeEvaluationError::ModuloOperatorIncompatibleOperands, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_divide_by_zero: (NodeEvaluationError::DivideByZero, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_contains_operator_incompatible_operands: (NodeEvaluationError::ContainsOperatorIncompatibleOperands, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_repeated_field_wrong_assignment: (NodeEvaluationError::RepeatedFieldWrongAssignment { value: String::default() }, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        node_evaluation_value_to_proto_conversion: (NodeEvaluationError::ValueToProtoConversionError { value: String::default(), refl_type: String::default() }, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
    );

    parameterized_test!(
        #[test]
        message_provider_error,
        |error: MessageProviderError, expected_status_code| {
            assert_eq!(error.get_status_code(), expected_status_code);
        },
        message_provider_invalid_publisher_name: (MessageProviderError::InvalidPublisherName, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
    );

    parameterized_test!(
        #[test]
        descriptor_provider_error,
        |error: DescriptorProviderError, expected_status_code| {
            assert_eq!(error.get_status_code(), expected_status_code);
        },
        descriptor_provider_invalid_publisher_name: (DescriptorProviderError::InvalidPublisherName, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
    );

    parameterized_test!(
        #[test]
        function_leaf_node_get_current_timestamp_argument_error,
        |error: FunctionLeafNodeGetCurrentTimestampArgumentError, expected_status_code| {
            assert_eq!(error.get_status_code(), expected_status_code);
        },
        function_leaf_time_source_not_set: (FunctionLeafNodeGetCurrentTimestampArgumentError::TimeSourceNotSet, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
        function_leaf_time_source_not_supported: (FunctionLeafNodeGetCurrentTimestampArgumentError::TimeSourceNotSupported, StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR),
    );

    parameterized_test!(
        #[test]
        service_publsher_accessor_error,
        |error: ServicePublisherAccessorError, expected_status_code| {
            assert_eq!(error.get_status_code(), expected_status_code);
        },
        service_publisher_message_parsing: (ServicePublisherAccessorError::MessageParsingFailed(anyhow!("")), StatusCodes::METRICS_CONFIG_RUNTIME_SERVICE_MESSAGE_PARSING_ERROR),
        service_publisher_metadata_parsing: (ServicePublisherAccessorError::MetadataParsingFailed(anyhow!("")), StatusCodes::METRICS_CONFIG_RUNTIME_SERVICE_METADATA_PARSING_ERROR),
        service_publisher_rpc_connect: (ServicePublisherAccessorError::RpcFailedToConnect(SdvStatus::from_raw_code(0)), StatusCodes::METRICS_CONFIG_RUNTIME_SERVICE_CONNECTION_ERROR),
        service_publisher_subscribe: (ServicePublisherAccessorError::FailedToSubscribe(anyhow!("")), StatusCodes::METRICS_CONFIG_RUNTIME_SERVICE_CONNECTION_ERROR),
        service_publisher_message_retrieval: (ServicePublisherAccessorError::MessageRetrievalFailed(anyhow!("")), StatusCodes::METRICS_CONFIG_RUNTIME_SERVICE_MESSAGE_RETRIEVAL_ERROR),
        service_publisher_service_not_supported: (ServicePublisherAccessorError::ServiceNotSupported, StatusCodes::METRICS_CONFIG_RUNTIME_SERVICE_CONNECTION_ERROR),
    );
}
