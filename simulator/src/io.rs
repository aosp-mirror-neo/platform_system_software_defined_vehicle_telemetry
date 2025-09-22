// Copyright 2024 Google LLC

use csv::{Reader, Trim};
use protobuf::{text_format::parse_from_str, Message};
use sdv_telemetry_proto_rust::metrics_configuration::{MetricsConfig, MetricsReport};
use sdv_telemetry_sample_utils::proto_reader;
use sdv_telemetry_simulator_proto::simulation_publisher::SimulationPublisher as PublisherConfigProto;
use std::{
    collections::HashSet,
    ffi::OsStr,
    fs,
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
};

use crate::{output_strings::report_log_header, SimulatorError};

pub const REPORTS_DIR_NAME: &str = "reports";

/// Reads the metrics config at the provided file path into a byte array.
pub fn read_metrics_config_from_file(file_path: &Path) -> Result<Vec<u8>, SimulatorError> {
    proto_reader::read_as_bytes::<MetricsConfig>(file_path).map_err(|error| {
        SimulatorError::FailedToReadFile { file_path: file_path.to_owned(), error }
    })
}

/// Reads the publisher config at the provided file path into a
/// PublisherConfigProto message.
pub fn read_publisher_config_from_file(
    file_path: &Path,
) -> Result<PublisherConfigProto, SimulatorError> {
    let file_name = file_path
        .file_name()
        .ok_or(SimulatorError::InvalidFilePath { file_path: file_path.to_owned() })?;

    let extension = file_path.extension().and_then(|o| o.to_str());
    if extension != Some("textproto") && extension != Some("txtpb") {
        return Err(SimulatorError::FailedToParseFile {
            file_name: file_name.to_owned(),
            error_message: "Publisher config files must have file extension .textproto".to_owned(),
        });
    }

    let textproto = fs::read_to_string(file_path).map_err(|error| {
        SimulatorError::FailedToReadFile { file_path: file_path.to_owned(), error: error.into() }
    })?;
    let textproto = parse_from_str::<PublisherConfigProto>(&textproto).map_err(|error| {
        SimulatorError::FailedToParseFile {
            file_name: file_name.to_owned(),
            error_message: error.to_string(),
        }
    })?;
    Ok(textproto)
}

pub fn parse_csv_file(
    file_path: &Path,
    delimiter: u8,
    file_name: &OsStr,
    expected_headers_set: HashSet<String>,
) -> Result<(Vec<String>, Vec<Vec<String>>), SimulatorError> {
    let mut csv_reader: Reader<_> = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .trim(Trim::All)
        .comment(Some(b'#'))
        .from_path(file_path)
        .map_err(|error| SimulatorError::IOError {
            error_message: format!("Failed to open file \"{}\": {error}", file_path.display()),
        })?;
    let headers = collect_headers(&mut csv_reader, file_name)?;
    check_headers(headers.clone(), expected_headers_set, file_name)?;
    let records = collect_records(&mut csv_reader, file_name)?;

    Ok((headers, records))
}

fn collect_headers(
    csv_reader: &mut Reader<File>,
    file_name: &OsStr,
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
    file_name: &OsStr,
) -> Result<(), SimulatorError> {
    let headers_set = HashSet::from_iter(headers);
    let incorrect_headers =
        headers_set.difference(&expected_headers_set).cloned().collect::<HashSet<_>>();
    if headers_set == expected_headers_set || incorrect_headers.is_empty() {
        Ok(())
    } else {
        Err(SimulatorError::IncorrectCSVHeaders {
            file_name: file_name.to_owned(),
            incorrect_headers,
        })
    }
}

fn collect_records(
    csv_reader: &mut Reader<File>,
    file_name: &OsStr,
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
    file_path: &Path,
    metrics_config_file_name: &OsStr,
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

pub fn open_report_log(file_path: &Path) -> Result<File, SimulatorError> {
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
    file_path: &Path,
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
pub fn create_output_directory(
    output_dir: &Path,
    reports_dir: Option<&Path>,
) -> Result<(), SimulatorError> {
    fs::create_dir_all(output_dir).map_err(|error| SimulatorError::IOError {
        error_message: format!("Couldn't create output directory: \"{error}\""),
    })?;

    if let Some(reports_dir) = reports_dir {
        fs::create_dir_all(reports_dir).map_err(|error| SimulatorError::IOError {
            error_message: format!("Couldn't create reports directory: \"{error}\""),
        })?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sdv_telemetry_test_utils::parameterized_test;
    use tempfile::tempdir;

    #[test]
    fn test_read_metrics_config_from_file_valid() {
        let file_path = Path::new("test_data/valid_metrics_config.textproto");
        let serialized_config = read_metrics_config_from_file(file_path).unwrap();
        assert!(!serialized_config.is_empty());
    }

    #[test]
    fn test_read_metrics_config_from_file_invalid_path() {
        let file_path = Path::new("test_data/invalid_path/metrics_config.textproto");
        assert!(
            read_metrics_config_from_file(file_path).is_err(),
            "Should have failed to return content for non-existent file"
        );
    }

    #[test]
    fn test_read_publisher_config_from_file_valid() {
        let file_path = Path::new("test_data/valid_publisher_config.textproto");
        let publisher_config_proto = read_publisher_config_from_file(file_path).unwrap();
        assert_eq!(publisher_config_proto.service_name, "TestPublisher");
    }

    parameterized_test!(
        #[test]
        test_read_publisher_config_from_file,

        |file_path, error_string| {
            let file_path = Path::new(file_path);
            assert!(read_publisher_config_from_file(file_path).is_err(), "{}", error_string);
        },

        invalid_path: ("test_data/invalid_path/.", "Should have failed to return content for non-existent file",),
        invalid_extension: ("test_data/valid_publisher_config.txt","Should have failed to parse file with incorrect extension",),
        invalid_textproto: ("test_data/invalid_publisher_config.textproto", "Should have failed to parse file with invalid textproto",),
    );

    #[test]
    fn test_parse_csv_file_valid() {
        let file_path = Path::new("test_data/valid_publisher_data.csv");
        let file_name = OsStr::new("valid_publisher_data.csv");
        let expected_headers_set = vec!["_delay_before_publishing", "speed", "above_speed_limit"]
            .into_iter()
            .map(ToString::to_string)
            .collect::<HashSet<_>>();

        let (headers, records) =
            parse_csv_file(file_path, b',', file_name, expected_headers_set).unwrap();

        assert_eq!(headers.len(), 3);
        assert_eq!(records.len(), 7);
    }

    #[test]
    fn test_parse_csv_file_custom_delimiter() {
        let file_path = Path::new("test_data/valid_publisher_data_custom_delimiter.csv");
        let file_name = OsStr::new("valid_publisher_data_custom_delimiter.csv");
        let expected_headers_set = vec!["_delay_before_publishing", "speed", "above_speed_limit"]
            .into_iter()
            .map(ToString::to_string)
            .collect::<HashSet<_>>();

        parse_csv_file(file_path, b'/', file_name, expected_headers_set).unwrap();
    }

    parameterized_test!(
        #[test]
        test_parse_csv_file,

        |file_path, file_name, error_string| {
            let file_path = Path::new(file_path);
            let file_name = OsStr::new(file_name);
            let expected_headers_set =
                vec!["_delay_before_publishing", "speed", "above_speed_limit"]
                    .into_iter()
                    .map(ToString::to_string)
                    .collect::<HashSet<_>>();

            assert!(
                parse_csv_file(file_path, b',', file_name, expected_headers_set).is_err(), "{}",
                error_string
            );
        },

        invalid_file_format: ("test_data/invalid_publisher_data_not_csv.txt.csv", "invalid_publisher_data_not_csv.txt.csv", "Parsed non-csv file",),
        missing_value: ("test_data/invalid_publisher_data_missing_value.csv", "invalid_publisher_data_missing_value.csv", "Parsed csv file with missing value",),
        redundant_value: ("test_data/invalid_publisher_data_redundant_value.csv", "invalid_publisher_data_redundant_value.csv", "Parsed csv file with redundant value",),
    );

    #[test]
    fn test_check_headers_valid() {
        let file_name = OsStr::new("file_name.csv");
        let expected_headers_set = vec!["_delay_before_publishing", "speed", "above_speed_limit"]
            .into_iter()
            .map(ToString::to_string)
            .collect::<HashSet<_>>();
        let headers_vector = vec!["_delay_before_publishing", "speed", "above_speed_limit"]
            .into_iter()
            .map(ToString::to_string)
            .collect();

        check_headers(headers_vector, expected_headers_set, file_name).unwrap();
    }

    #[test]
    fn test_check_headers_redundant_header() {
        let file_name = OsStr::new("file_name.csv");
        let expected_headers_set = vec!["_delay_before_publishing", "speed", "above_speed_limit"]
            .into_iter()
            .map(ToString::to_string)
            .collect::<HashSet<_>>();
        let headers_vector =
            vec!["_delay_before_publishing", "speed", "above_speed_limit", "redundant_header"]
                .into_iter()
                .map(ToString::to_string)
                .collect();

        assert!(
            check_headers(headers_vector, expected_headers_set, file_name).is_err(),
            "Missed redundant header"
        );
    }

    #[test]
    fn test_create_report_log() {
        let tmp_dir = tempdir().unwrap();

        let file_path = tmp_dir.path().join("report_log.txt");
        let metrics_config_file_name = OsStr::new("test_metrics_config");
        let uuid = "test uuid";
        let report_config_name = "test_report_config";

        create_report_log(&file_path, metrics_config_file_name, uuid, report_config_name).unwrap();

        let file_content = fs::read_to_string(file_path).unwrap();
        let expected_content = r#"Metrics config "test_metrics_config", UUID: "test uuid"
Report configuration "test_report_config"

Created reports below

"#;

        assert_eq!(file_content, expected_content);
    }

    #[test]
    fn test_open_report_log_valid() {
        let tmp_dir = tempdir().unwrap();

        let file_path = tmp_dir.path().join("report_log.txt");
        File::create(file_path.clone()).unwrap();

        let mut writable_file = open_report_log(&file_path).unwrap();
        write!(writable_file, "test").unwrap();

        drop(writable_file);
    }

    #[test]
    fn test_open_report_log_cannot_open_file() {
        let open_report_result = open_report_log(Path::new("non_existent_file"));
        assert!(open_report_result.is_err(), "Returned a file handle for a non-existent file");
    }

    #[test]
    fn test_append_report_log_valid() {
        let tmp_dir = tempdir().unwrap();

        let file_path = tmp_dir.path().join("report_log.txt");
        File::create(file_path.clone()).unwrap();
        let mut writable_file = OpenOptions::new().append(true).open(file_path).unwrap();

        append_report_log(&mut writable_file, "Test content").unwrap();

        drop(writable_file);
    }

    #[test]
    fn test_append_report_log_cannot_write_to_file() {
        let tmp_dir = tempdir().unwrap();
        let file_path = tmp_dir.path().join("report_log");
        File::create(file_path.clone()).unwrap();
        let mut unwritable_file = File::open(file_path).unwrap();

        assert!(
            append_report_log(&mut unwritable_file, "Test content").is_err(),
            "Wrote to file that should not be writable"
        );

        drop(unwritable_file);
    }

    #[test]
    fn test_write_report_to_file() {
        let tmp_dir = tempdir().unwrap();

        let file_path = tmp_dir.path().join("report.pb");
        let metrics_report = MetricsReport::new();

        write_report_to_file(&file_path, &metrics_report).unwrap();
        assert!(file_path.exists());
    }

    #[test]
    fn test_create_output_directory_with_reports_dir() {
        let tmp_dir = tempdir().unwrap();
        let output_dir = tmp_dir.path().join("output");
        let reports_dir = output_dir.join("reports");

        create_output_directory(&output_dir, Some(&reports_dir)).unwrap();

        assert!(Path::new(&output_dir).exists());
        assert!(Path::new(&reports_dir).exists());
    }

    #[test]
    fn test_create_output_directory_without_reports_dir() {
        let tmp_dir = tempdir().unwrap();
        let output_dir = tmp_dir.path().join("output");

        create_output_directory(&output_dir, None).unwrap();

        assert!(Path::new(&output_dir).exists());
        // Assert output directory is empty i.e. no reports directory was created.
        assert!(Path::new(&output_dir).read_dir().unwrap().next().is_none());
    }
}
