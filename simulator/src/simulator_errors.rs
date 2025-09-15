// Copyright 2024 Google LLC

use anyhow::Error;
use sdv_telemetry_rust_wrapper::TelemetryError;
use std::collections::HashSet;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum SimulatorError {
    #[error("Descriptor proto message {message_name} found more than once in metrics config \"{file_name}\"")]
    DuplicateDescriptorProtoMessage { message_name: String, file_name: String },

    #[error("Metrics config \"{file_name}\" could not be added because is has the same UUID as already added metrics config \"{duplicate_file_name}\"")]
    DuplicateMetricsConfigUuid { file_name: String, duplicate_file_name: String },

    #[error(
        "Telemetry Service produced an error{}:\n\"{error}\"",
        .file_name.as_ref().map(|f| format!(" (metrics config filename: {f})")).unwrap_or_default()
    )]
    TelemetryServiceError { file_name: Option<String>, error: TelemetryError },

    #[error("Received a report for report config \"{report_config_name}\" but failed to decode its data: \"{error}\"")]
    FailedToDecodeReport { report_config_name: String, error: protobuf::Error },

    #[error("Failed to parse file \"{file_name}\":\n{error_message}")]
    FailedToParseFile { file_name: String, error_message: String },

    #[error(
        "Failed to read file at \"{}\":\n\"{error}\"",
        .file_path.display()
    )]
    FailedToReadFile { file_path: PathBuf, error: Error },

    #[error("\"{file_name}\" contains header(s) {incorrect_headers:?} that do not correspond to a message field.")]
    IncorrectCSVHeaders { file_name: String, incorrect_headers: HashSet<String> },

    #[error("{error_message}")]
    Error { error_message: String },

    #[error(
        "\"{}\" is not a valid file path",
        .file_path.display()
    )]
    InvalidFilePath { file_path: PathBuf },

    #[error("I/O error: {error_message}")]
    IOError { error_message: String },

    #[error("Error in the application (this should never happen and is a bug): {error_message}")]
    ProgrammingError { error_message: String },

    #[error("{feature} is not supported (yet).")]
    NotImplemented { feature: String },

    #[error("{file_name}\" is missing the descriptor proto message \"{message_name}\"")]
    MissingDescriptorProto { file_name: String, message_name: String },

    #[error("Metrics config UUID not found: {config_uuid}")]
    MetricsConfigNotFound { config_uuid: String },

    #[error(
        "Received a report for an unknown metrics configuration, config UUID: {config_uuid}, \
        report UUID: {report_uuid}, report name: {report_name}, message: {message}"
    )]
    OrphanReport { config_uuid: String, report_uuid: String, report_name: String, message: String },
}
