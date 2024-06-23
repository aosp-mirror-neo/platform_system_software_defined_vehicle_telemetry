// Copyright 2024 Google LLC

use anyhow::Error;
use sdv_telemetry_rust_wrapper::TelemetryError;
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
        "Metrics config \"{file_name}\" could not be activated by telemetry service:\n\"{error}\""
    )]
    FailedToActivateMetricsConfig { file_name: String, error: TelemetryError },

    #[error(
        "Metrics config \"{file_name}\" could not be parsed by telemetry service:\n\"{error}\""
    )]
    FailedToParseMetricsConfig { file_name: String, error: TelemetryError },

    #[error("Received a report for report config \"{report_config_name}\" but failed to decode its data: \"{error}\"")]
    FailedToDecodeReport { report_config_name: String, error: protobuf::Error },

    #[error("Failed to parse file \"{file_name}\":\n{error_message}")]
    FileParsingError { file_name: String, error_message: String },

    #[error(
        "Failed to read file at \"{}\":\n\"{error}\"",
        .file_path.display()
    )]
    FailedToReadFile { file_path: PathBuf, error: Error },

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

    #[error("{file_name}\" is missing the descriptor proto message \"{message_name}\"")]
    MissingDescriptorProto { file_name: String, message_name: String },

    #[error("Received a report for an unknown metrics configuration with UUID \"{uuid}\"")]
    OrphanReport { uuid: String },
}
