// Copyright 2024 Google LLC

//! This module represents runtime errors of a metrics config.

use either::Either;

use crate::{
    expr::node_error::NodeEvaluationError,
    metrics_config_orchestrator::message_builder::MessageBuilderError,
    service_publisher_accessor::CreateSubscriptionError,
    service_publisher_accessor::GetMessageProviderError,
};

/// Runtime errors of a metrics config.
#[derive(thiserror::Error, Debug)]
pub(crate) enum MetricsConfigRuntimeError {
    /// Runtime errors related to a service.
    ///
    /// TODO: Ideally, this should also contain the UUID(s) of the metrics config(s) that caused
    /// this error.
    #[error("Service runtime error for service '{service_name}': {error}")]
    Service {
        /// Name of the service.
        service_name: String,
        /// Service error.
        error: ServiceRuntimeError,
    },

    /// Runtime errors related to aggregation publishers.
    #[error("Aggregation publisher error for metrics config {metrics_config_uuid}: {error}")]
    AggregationPublisher {
        /// UUID of the metrics config.
        metrics_config_uuid: String,
        /// Aggregation publisher error.
        error: AggregationPublisherRuntimeError,
    },

    /// Runtime errors related to service publishers.
    #[error("Service publisher error for metrics config {metrics_config_uuid}: {error}")]
    ServicePublisher {
        /// UUID of the metrics config.
        metrics_config_uuid: String,
        /// Service publisher error.
        error: ServicePublisherRuntimeError,
    },

    /// Runtime errors related to conditional triggers.
    #[error("Conditional trigger error for metrics config {metrics_config_uuid}: {error}")]
    ConditionalTrigger {
        /// UUID of the metrics config.
        metrics_config_uuid: String,
        /// Conditional trigger error.
        error: ConditionalTriggerRuntimeError,
    },

    /// Runtime errors related to metrics config generation.
    #[error("Failed to generate metrics report: {0}")]
    MetricsReportGenerator(MetricsReportGeneratorError),
}

#[derive(thiserror::Error, Debug)]
#[error("Metrics config {metrics_config_uuid} metrics report generation for '{metrics_report_name}' failed due to message builder error: {message_builder_error}")]
pub(crate) struct MetricsReportGeneratorError {
    /// UUID of the metrics config.
    pub(crate) metrics_config_uuid: String,
    /// Name of the metrics report.
    pub(crate) metrics_report_name: String,
    /// Message builder error.
    pub(crate) message_builder_error: MessageBuilderError,
}

/// Runtime errors related to a service.
#[derive(thiserror::Error, Debug)]
pub enum ServiceRuntimeError {
    /// Failure to retrieve a message.
    #[error("Message retrieval error: {0:#}")]
    MessageRetrievalError(anyhow::Error),
    /// Failure to parse a retrieved message.
    #[error("Message parsing error: {0:#}")]
    MessageParsingError(anyhow::Error),
}

#[derive(thiserror::Error, Debug)]
#[error(
    "Aggregation publisher '{publisher_name}' failed to build message: {message_builder_error}"
)]
pub(crate) struct AggregationPublisherRuntimeError {
    /// Name of the aggregation publisher.
    pub(crate) publisher_name: String,

    /// Message builder error.
    pub(crate) message_builder_error: MessageBuilderError,
}

#[derive(thiserror::Error, Debug)]
#[error("Service publisher '{publisher_name}' failed to build message: {error}")]
pub(crate) struct ServicePublisherRuntimeError {
    /// Name of the service publisher.
    pub(crate) publisher_name: String,

    /// Service publisher error.
    pub(crate) error: ServicePublisherRuntimeErrorKind,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum ServicePublisherRuntimeErrorKind {
    /// Retrying activation of the publisher failed.
    #[error("Retrying activation failed: {0}")]
    RetryActivationFailed(Either<GetMessageProviderError, CreateSubscriptionError>),
}

#[derive(thiserror::Error, Debug)]
#[error("Conditional trigger '{trigger_name}' failed to evaluate condition: {node_error}")]
pub(crate) struct ConditionalTriggerRuntimeError {
    /// Name of the conditional trigger.
    pub(crate) trigger_name: String,

    /// Condition evaluation error.
    pub(crate) node_error: NodeEvaluationError,
}
