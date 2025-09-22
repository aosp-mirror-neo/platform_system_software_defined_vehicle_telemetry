// Copyright 2024 Google LLC

use protobuf::{text_format::print_to_string_pretty, MessageDyn};
use sdv_telemetry_proto_rust::metrics_configuration::publisher::Publisher_type as PublisherTypeProto;
use sdv_telemetry_proto_rust::metrics_configuration::{MetricsConfig, MetricsReport};
use sdv_telemetry_rust_wrapper::TelemetryServiceHolder;
use sdv_telemetry_sample_utils::proto_reader;
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    path::PathBuf,
};

use crate::{
    io,
    io::{create_report_log, write_report_to_file},
    metrics_config_info::{MetricsConfigInfo, MetricsConfigStatus, ReportConfigStatus},
    output_strings::report_log_entry,
    protobuf_parsing::{decode_report, parse_message_descriptors},
    SimulatorError,
};

pub struct MetricsConfigsTracker {
    metrics_configs: HashMap<String, MetricsConfigInfo>,
    write_report_files: bool,
    output_directory: PathBuf,
}

impl MetricsConfigsTracker {
    pub fn new(write_report_files_to_disk: bool, output_directory: PathBuf) -> Self {
        MetricsConfigsTracker {
            metrics_configs: HashMap::new(),
            write_report_files: write_report_files_to_disk,
            output_directory,
        }
    }

    /// Reads a metrics config from the provided file path, deserializes the
    /// content into a MetricsConfig object. Returns an error if the tracker
    /// already contains a metrics config with the same UUID.
    pub fn parse_metrics_config(&mut self, file_path: &Path) -> Result<(), SimulatorError> {
        let file_name = file_path
            .file_name()
            .ok_or(SimulatorError::InvalidFilePath { file_path: file_path.to_owned() })?;

        let serialized_config =
            proto_reader::read_as_bytes::<MetricsConfig>(file_path).map_err(|err| {
                SimulatorError::FailedToParseFile {
                    file_name: file_name.to_owned(),
                    error_message: err.to_string(),
                }
            })?;
        let config: MetricsConfig = protobuf::Message::parse_from_bytes(&serialized_config)
            .map_err(|error| SimulatorError::FailedToParseFile {
                file_name: file_name.to_owned(),
                error_message: error.to_string(),
            })?;

        if let Some(entry) = self.metrics_configs.get(&config.uuid) {
            let duplicate_file_name = entry.file_name.clone();
            return Err(SimulatorError::DuplicateMetricsConfigUuid {
                file_name: file_name.to_owned(),
                duplicate_file_name,
            });
        };

        let message_descriptors = parse_message_descriptors(&config, file_name)?;

        let mut config_info =
            MetricsConfigInfo::new(file_name, serialized_config, message_descriptors);

        for report_config in &config.metrics_report_configs {
            let report_log_path = self.output_directory.join(format!(
                "{}_all_reports_for_{}.txt",
                config.uuid,
                report_config.name.to_ascii_lowercase()
            ));
            create_report_log(&report_log_path, file_name, &config.uuid, &report_config.name)?;
            config_info.report_configs.insert(
                report_config.name.clone(),
                ReportConfigStatus::new(
                    report_config.message_builder.message_type.clone(),
                    &report_log_path,
                    self.write_report_files,
                )?,
            );
        }

        for publisher in &config.publishers {
            if let Some(PublisherTypeProto::ServicePublisher(service_publisher_proto)) =
                &publisher.publisher_type
            {
                config_info
                    .service_publisher_names
                    .insert(service_publisher_proto.service_name.clone());
            }
        }

        self.metrics_configs.insert(config.uuid, config_info);
        Ok(())
    }

    pub fn get_metrics_config_uuids(&self) -> HashSet<String> {
        self.metrics_configs.keys().cloned().collect()
    }

    pub async fn add_metrics_config(
        &mut self,
        telemetry_service: &TelemetryServiceHolder,
        uuid: &str,
    ) -> Result<(), SimulatorError> {
        let config_info = self.get_metrics_config_info(uuid)?;

        let returned_uuid = telemetry_service
            .add_metrics_config(&config_info.serialized_config)
            .await
            .map_err(|error| SimulatorError::TelemetryServiceError {
                file_name: Some(config_info.file_name.clone()),
                error,
            })?;

        if uuid != returned_uuid {
            return Err(SimulatorError::Error {
                error_message: format!(
                    "Telemetry service returned different UUID from serialized metrics config than\
                    expected. Expected: \"{uuid}\", Received: \"{returned_uuid}\""
                ),
            });
        }

        config_info.status = MetricsConfigStatus::Added;
        Ok(())
    }

    pub async fn activate_metrics_config(
        &mut self,
        telemetry_service: &TelemetryServiceHolder,
        uuid: &str,
    ) -> Result<(), SimulatorError> {
        let config_info = self.get_metrics_config_info(uuid)?;

        telemetry_service.activate_metrics_config(uuid).await.map_err(|error| {
            SimulatorError::TelemetryServiceError {
                file_name: Some(config_info.file_name.clone()),
                error,
            }
        })?;

        config_info.status = MetricsConfigStatus::Activated;
        Ok(())
    }

    pub async fn deactivate_metrics_config(
        &mut self,
        telemetry_service: &TelemetryServiceHolder,
        uuid: &str,
    ) -> Result<(), SimulatorError> {
        let config_info = self.get_metrics_config_info(uuid)?;

        telemetry_service.deactivate_metrics_config(uuid).await.map_err(|error| {
            SimulatorError::TelemetryServiceError {
                file_name: Some(config_info.file_name.clone()),
                error,
            }
        })?;

        config_info.status = MetricsConfigStatus::Deactivated;
        Ok(())
    }

    pub async fn remove_metrics_config(
        &mut self,
        telemetry_service: &TelemetryServiceHolder,
        uuid: &str,
    ) -> Result<(), SimulatorError> {
        let config_info = self.get_metrics_config_info(uuid)?;

        telemetry_service.remove_metrics_config(uuid).await.map_err(|error| {
            SimulatorError::TelemetryServiceError {
                file_name: Some(config_info.file_name.clone()),
                error,
            }
        })?;

        config_info.status = MetricsConfigStatus::Removed;
        Ok(())
    }

    pub async fn remove_all_metrics_configs(
        &mut self,
        telemetry_service: &TelemetryServiceHolder,
    ) -> Result<(), SimulatorError> {
        telemetry_service
            .remove_all_metrics_configs()
            .await
            .map_err(|error| SimulatorError::TelemetryServiceError { file_name: None, error })?;

        for config_info in self.metrics_configs.values_mut() {
            config_info.status = MetricsConfigStatus::Removed;
        }
        Ok(())
    }

    /// Gets the matching MetricsConfigInfo object for the specified metrics
    /// config uuid, returns an error if the UUID is not found.
    pub fn get_metrics_config_info(
        &mut self,
        uuid: &str,
    ) -> Result<&mut MetricsConfigInfo, SimulatorError> {
        self.metrics_configs
            .get_mut(uuid)
            .ok_or_else(|| SimulatorError::MetricsConfigNotFound { config_uuid: uuid.to_owned() })
    }

    /// Appends the report number, timestamp and content to the report log
    /// in a human readable format. If set in application args, also writes
    /// binary report to a file.
    pub fn process_new_report(&mut self, report: MetricsReport) -> Result<(), SimulatorError> {
        let reports_directory = self.output_directory.join(io::REPORTS_DIR_NAME);

        let metrics_config =
            self.get_metrics_config_info(&report.metrics_config_uuid).map_err(|err| {
                SimulatorError::OrphanReport {
                    config_uuid: report.metrics_config_uuid.clone(),
                    report_uuid: report.report_uuid.clone(),
                    report_name: report.report_config_name.clone(),
                    message: err.to_string(),
                }
            })?;
        let (report_config, message_descriptor) = metrics_config.get_report_config_info(&report)?;

        if let Some(report_file_names) = &mut report_config.report_file_names {
            let output_file_name = format!(
                "{}_{}_{}.pb",
                report.metrics_config_uuid, report.report_config_name, report.report_number
            );
            let output_file_path = reports_directory.join(&output_file_name);
            write_report_to_file(&output_file_path, &report)?;
            report_file_names.push(output_file_name);
        }

        let report_content = decode_report(&report, message_descriptor)?;
        let readable_report = create_readable_report(&report, report_content.clone_box())?;
        report_config.append_report_log(readable_report)?;
        report_config.num_reports += 1;
        Ok(())
    }
}

/// Formats the report data into a human-readable string with a UTC
/// timestamp and a call out if the report is incomplete.
fn create_readable_report(
    report: &MetricsReport,
    report_content: Box<dyn MessageDyn>,
) -> Result<String, SimulatorError> {
    let reason = report.report_reason.enum_value_or_default();
    let report_content = print_to_string_pretty(report_content.as_ref());
    let Some(date_time) = chrono::DateTime::from_timestamp(
        report.report_timestamp.seconds,
        report.report_timestamp.nanos.unsigned_abs(),
    ) else {
        return Err(SimulatorError::Error {
            error_message: "Failed to convert report timestamp.".to_owned(),
        });
    };

    Ok(report_log_entry(report.report_number, &date_time, reason, &report_content))
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::assert_that;
    use googletest::matchers::container_eq;
    use sdv_telemetry_test_utils::parameterized_test;
    use tempfile::tempdir;

    parameterized_test!(
        #[test]
        test_read_metrics_config_from,

        |path, matcher| {
            let tmp_dir = tempdir().unwrap();
            let mut tracker = MetricsConfigsTracker::new(false, tmp_dir.path().to_owned());

            // Ignore if error is returned
            let _  = tracker.parse_metrics_config(Path::new(path));

            let result = tracker.get_metrics_config_uuids().into_iter().collect::<Vec<_>>();
            assert_that!(result, matcher);
        },

        textproto: (
            "test_data/valid_metrics_config.textproto",
            container_eq(vec!["d3456cf0-8fde-436a-9f29-c12f16d10c10"])
        ),
        binary: (
            "test_data/valid_metrics_config.pb",
            container_eq(vec!["d3456cf0-8fde-436a-9f29-c12f16d10c10"])
        ),
        invalid_path: (
            "test_data/invalid_path/metrics_config.textproto",
            container_eq(Vec::<&str>::new())
        ),
    );
}
