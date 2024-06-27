// Copyright 2024 Google LLC

use csv::{Reader, Trim};
use protobuf::{text_format::parse_from_str, Message};
use sdv_telemetry_proto_rust::metrics_configuration::MetricsReport;
use sdv_telemetry_proto_simulation_publisher::simulation_publisher::SimulationPublisher as PublisherConfigProto;
use sdv_telemetry_sample_utils::metrics_config_reader_utils;
use std::{
    collections::HashSet,
    fs,
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use crate::{output_strings::report_log_header, SimulatorError};

pub const OUTPUT_DIR: &str = "data/local/tmp/telemetry_simulator_out/";
pub const REPORTS_DIR: &str = "data/local/tmp/telemetry_simulator_out/reports/";

/// Reads the metrics config at the provided file path into a byte array.
pub fn read_metrics_config_from_file(
    file_path: &PathBuf,
) -> Result<(String, Vec<u8>), SimulatorError> {
    let Some(file_name_os) = file_path.file_name() else {
        return Err(SimulatorError::InvalidFilePath { file_path: file_path.clone() });
    };
    let file_name = file_name_os.to_string_lossy().into_owned().clone();
    Ok((
        file_name,
        metrics_config_reader_utils::read(file_path).map_err(|error| {
            SimulatorError::FailedToReadFile { file_path: file_path.clone(), error }
        })?,
    ))
}

/// Reads the publisher config at the provided file path into a
/// PublisherConfigProto message.
pub fn read_publisher_config_from_file(
    file_path: &PathBuf,
) -> Result<(String, PublisherConfigProto), SimulatorError> {
    let Some(file_name_os) = file_path.file_name() else {
        return Err(SimulatorError::InvalidFilePath { file_path: file_path.clone() });
    };
    let file_name = file_name_os.to_string_lossy().into_owned().clone();

    if Path::new(&file_path).extension().and_then(|o| o.to_str()) != Some("textproto") {
        return Err(SimulatorError::FailedToParseFile {
            file_name,
            error_message: "Publisher config files must have file extension .textproto".to_owned(),
        });
    }

    let textproto = fs::read_to_string(file_path).map_err(|error| SimulatorError::IOError {
        error_message: format!("Failed to read file \"{}\": {error}", file_path.display()),
    })?;
    Ok((
        file_name.clone(),
        parse_from_str::<PublisherConfigProto>(&textproto).map_err(|error| {
            SimulatorError::FailedToParseFile { file_name, error_message: error.to_string() }
        })?,
    ))
}

pub fn parse_csv_file(
    file_path: &str,
    delimiter: u8,
    file_name: &str,
    expected_headers_set: HashSet<String>,
) -> Result<(Vec<String>, Vec<Vec<String>>), SimulatorError> {
    let mut csv_reader: Reader<_> = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .trim(Trim::All)
        .comment(Some(b'#'))
        .from_path(file_path)
        .map_err(|error| SimulatorError::IOError {
            error_message: format!("Failed to open file \"{}\": {error}", file_path.to_owned()),
        })?;
    let headers = collect_headers(&mut csv_reader, file_name)?;
    check_headers(headers.clone(), expected_headers_set, file_name)?;
    let records = collect_records(&mut csv_reader, file_name)?;

    Ok((headers, records))
}

fn collect_headers(
    csv_reader: &mut Reader<File>,
    file_name: &str,
) -> Result<Vec<String>, SimulatorError> {
    Ok(csv_reader
        .headers()
        .map_err(|error| SimulatorError::FailedToParseFile {
            file_name: file_name.to_owned(),
            error_message: format!("Could not parse CSV headers: {error}"),
        })?
        .iter()
        .map(|entry| entry.to_owned())
        .collect())
}

fn check_headers(
    headers: Vec<String>,
    expected_headers_set: HashSet<String>,
    file_name: &str,
) -> Result<(), SimulatorError> {
    let headers_set = HashSet::from_iter(headers);
    if headers_set == expected_headers_set {
        Ok(())
    } else {
        let missing: HashSet<String> =
            expected_headers_set.difference(&headers_set).cloned().collect();
        let redundant: HashSet<String> =
            headers_set.difference(&expected_headers_set).cloned().collect();
        Err(SimulatorError::IncorrectCSVHeaders {
            file_name: file_name.to_owned(),
            missing,
            redundant,
        })
    }
}

fn collect_records(
    csv_reader: &mut Reader<File>,
    file_name: &str,
) -> Result<Vec<Vec<String>>, SimulatorError> {
    csv_reader
        .records()
        .map(|record| match record {
            Ok(record) => Ok(record.iter().map(|entry| entry.to_owned()).collect()),
            Err(error) => Err(SimulatorError::FailedToParseFile {
                file_name: file_name.to_owned(),
                error_message: format!("Could not parse CSV records: {error}"),
            }),
        })
        .collect()
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
