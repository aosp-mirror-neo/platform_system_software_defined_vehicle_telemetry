// Copyright 2024 Google LLC

use anyhow::Error;
use sdv_telemetry_rust_wrapper::TelemetryError;
use std::collections::HashSet;
use std::ffi::OsString;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum SimulatorError {
    #[error(
        "Descriptor proto message {message_name} found more than once in metrics config \"{}\"",
        .file_name.display()
    )]
    DuplicateDescriptorProtoMessage { message_name: String, file_name: OsString },

    #[error(
        "Metrics config \"{}\" could not be added because is has the same UUID as already \
        added metrics config \"{}\"",
        file_name.display(), duplicate_file_name.display()
    )]
    DuplicateMetricsConfigUuid { file_name: OsString, duplicate_file_name: OsString },

    #[error(
        "Telemetry Service produced an error{}:\n\"{error}\"",
        .file_name.as_ref().map(|f| format!(" (metrics config filename: {})", f.display())).unwrap_or_default()
    )]
    TelemetryServiceError { file_name: Option<OsString>, error: TelemetryError },

    #[error("Received a report for report config \"{report_config_name}\" but failed to decode its data: \"{error}\"")]
    FailedToDecodeReport { report_config_name: String, error: protobuf::Error },

    #[error("Failed to parse file \"{}\":\n{error_message}", file_name.display())]
    FailedToParseFile { file_name: OsString, error_message: String },

    #[error(
        "Failed to read file at \"{}\":\n\"{error}\"",
        .file_path.display()
    )]
    FailedToReadFile { file_path: PathBuf, error: Error },

    #[error(
        "\"{}\" contains header(s) {incorrect_headers:?} that do not correspond to a message field",
        file_name.display(),
    )]
    IncorrectCSVHeaders { file_name: OsString, incorrect_headers: HashSet<String> },

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

    #[error(
        "{}\" is missing the descriptor proto message \"{message_name}\"",
        file_name.display(),
    )]
    MissingDescriptorProto { file_name: OsString, message_name: String },

    #[error("Metrics config UUID not found: {config_uuid}")]
    MetricsConfigNotFound { config_uuid: String },

    #[error(
        "Received a report for an unknown metrics configuration, config UUID: {config_uuid}, \
        report UUID: {report_uuid}, report name: {report_name}, message: {message}"
    )]
    OrphanReport { config_uuid: String, report_uuid: String, report_name: String, message: String },
}
