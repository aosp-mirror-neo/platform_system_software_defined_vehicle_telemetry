// Copyright 2024 Google LLC

//! This module represents runtime errors of a metrics config.

/// Runtime errors of a metrics config.
#[derive(thiserror::Error, Debug)]
pub enum MetricsConfigRuntimeError {
    /// Runtime errors related to a service.
    #[error("Service runtime error for service '{service_name}': {error}")]
    Service {
        /// Name of the service.
        service_name: String,
        /// Service error.
        error: ServiceRuntimeError,
    },
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
