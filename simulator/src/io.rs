// Copyright 2024 Google LLC

use protobuf::Message;
use sdv_telemetry_proto_rust::metrics_configuration::MetricsReport;
use sdv_telemetry_sample_utils::metrics_config_reader_utils;
use std::{fs, fs::File, fs::OpenOptions, io::Write, path::PathBuf};

use crate::{output_strings::report_log_header, SimulatorError};

pub const OUTPUT_DIR: &str = "data/local/tmp/telemetry_simulator_out/";
pub const REPORTS_DIR: &str = "data/local/tmp/telemetry_simulator_out/reports/";

/// Reads the metrics config at the provided file path into a byte array.
pub fn read_metrics_config_from_file(
    file_path: PathBuf,
) -> Result<(String, Vec<u8>), SimulatorError> {
    let Some(file_name_os) = file_path.file_name() else {
        return Err(SimulatorError::InvalidFilePath { file_path });
    };
    let file_name = file_name_os.to_string_lossy().into_owned().clone();
    Ok((
        file_name,
        metrics_config_reader_utils::read(&file_path)
            .map_err(|error| SimulatorError::FailedToReadFile { file_path, error })?,
    ))
}

/// Creates a new file to collect readable metrics reports and writes a header
/// including the metrics config file name, UUID and report config name.
pub fn create_report_log(
    file_path: &PathBuf,
    metrics_config_file_name: &str,
    uuid: &str,
    report_config_name: &str,
) -> Result<(), SimulatorError> {
    let mut file = File::create(file_path).map_err(|error| SimulatorError::IOError {
        error_message: format!("Failed to create file \"{}\": {error}", file_path.display()),
    })?;

    writeln!(file, "{}", report_log_header(metrics_config_file_name, uuid, report_config_name))
        .map_err(|error| SimulatorError::IOError {
            error_message: format!("Failed to write to file \"{}\": {error}", file_path.display()),
        })?;
    Ok(())
}

pub fn open_report_log(file_path: &PathBuf) -> Result<File, SimulatorError> {
    OpenOptions::new().append(true).open(file_path).map_err(|error| SimulatorError::IOError {
        error_message: format!("Failed to open file \"{}\": {error}", file_path.display()),
    })
}

pub fn append_report_log(report_log: &mut File, report: &str) -> Result<(), SimulatorError> {
    writeln!(report_log, "{report}").map_err(|error| SimulatorError::IOError {
        error_message: format!("Could not write to report log file: {error}"),
    })?;
    Ok(())
}

/// Creates a file at the provided file path and writes the metrics report to it
/// in the binary protobuf format.
pub fn write_report_to_file(
    file_path: &PathBuf,
    report: &MetricsReport,
) -> Result<(), SimulatorError> {
    let mut file = File::create(file_path).map_err(|error| SimulatorError::IOError {
        error_message: format!("Failed to create file \"{}\": {error}", file_path.display()),
    })?;
    report.write_to_writer(&mut file).map_err(|error| SimulatorError::IOError {
        error_message: format!("Failed to write to file \"{}\": {error}", file_path.display()),
    })?;
    Ok(())
}

/// Creates the output directory at "/data/local/tmp/telemetry_simulator_out"
/// and a reports directory at "/data/local/tmp/telemetry_simulator_out/reports"
/// if create_reports_dir is true.
pub fn create_output_directory(create_reports_dir: bool) -> Result<(), SimulatorError> {
    fs::create_dir_all(OUTPUT_DIR).map_err(|error| SimulatorError::IOError {
        error_message: format!("Couldn't create output directory: \"{error}\""),
    })?;

    if create_reports_dir {
        fs::create_dir_all(REPORTS_DIR).map_err(|error| SimulatorError::IOError {
            error_message: format!("Couldn't create reports directory: \"{error}\""),
        })?;
    }

    Ok(())
}
