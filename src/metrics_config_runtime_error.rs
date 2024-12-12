// Copyright 2024 Google LLC

//! This module represents runtime errors of a metrics config.

use crate::metrics_config_orchestrator::message_builder::MessageBuilderError;

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
