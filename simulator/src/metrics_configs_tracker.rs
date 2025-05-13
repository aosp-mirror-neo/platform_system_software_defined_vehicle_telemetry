// Copyright 2024 Google LLC

use protobuf::{text_format::print_to_string_pretty, MessageDyn};
use sdv_telemetry_proto_rust::metrics_configuration::publisher::Publisher_type as PublisherTypeProto;
use sdv_telemetry_proto_rust::metrics_configuration::{MetricsConfig, MetricsReport};
use sdv_telemetry_rust_wrapper::TelemetryServiceHolder;
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    path::PathBuf,
};

use crate::{
    io,
    io::{create_report_log, read_metrics_config_from_file, write_report_to_file},
    metrics_config_info::{MetricsConfigInfo, MetricsConfigStatus, ReportConfigStatus},
    output_strings::report_log_entry,
    protobuf_parsing::{decode_report, parse_message_descriptors},
    telemetry_service_handler::{activate_metrics_config, add_metrics_config},
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
        let (file_name, serialized_config) = read_metrics_config_from_file(file_path)?;
        let config: MetricsConfig = protobuf::Message::parse_from_bytes(&serialized_config)
            .map_err(|error| SimulatorError::FailedToParseFile {
                file_name: file_name.clone(),
                error_message: error.to_string(),
            })?;

        if let Some(entry) = self.metrics_configs.get(&config.uuid) {
            let duplicate_file_name = entry.file_name.clone();
            return Err(SimulatorError::DuplicateMetricsConfigUuid {
                file_name,
                duplicate_file_name,
            });
        };

        let message_descriptors = parse_message_descriptors(&config, file_name.clone())?;

        let mut config_info =
            MetricsConfigInfo::new(&file_name, serialized_config, message_descriptors);

        for report_config in &config.metrics_report_configs {
            let report_log_path = self.output_directory.join(format!(
                "{}_all_reports_for_{}.txt",
                config.uuid,
                report_config.name.to_ascii_lowercase()
            ));
            create_report_log(&report_log_path, &file_name, &config.uuid, &report_config.name)?;
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

    /// Uses telemetry service to add and activate the specified metric config.
    /// Returns an error if any step fails.
    pub async fn activate_metrics_config(
        &mut self,
        telemetry_service: &TelemetryServiceHolder,
        uuid: &str,
    ) -> Result<(), SimulatorError> {
        let config_info =
            self.metrics_configs.get_mut(uuid).ok_or_else(|| SimulatorError::ProgrammingError {
                error_message: "Tried to get config info for non-existent metrics config"
                    .to_owned(),
            })?;
        add_metrics_config(
            telemetry_service,
            uuid,
            &config_info.serialized_config,
            config_info.file_name.clone(),
        )
        .await?;
        config_info.status = MetricsConfigStatus::Added;
        activate_metrics_config(telemetry_service, uuid, &config_info.file_name).await?;
        config_info.status = MetricsConfigStatus::Activated;
        Ok(())
    }

    /// Gets the matching MetricsConfigInfo object for the specified metrics
    /// config uuid, returns an error if the UUID is not found.
    pub fn get_metrics_config_info(
        &mut self,
        report: &MetricsReport,
    ) -> Result<&mut MetricsConfigInfo, SimulatorError> {
        self.metrics_configs.get_mut(&report.metrics_config_uuid).ok_or_else(|| {
            SimulatorError::OrphanReport {
                config_uuid: report.metrics_config_uuid.clone(),
                report_uuid: report.report_uuid.clone(),
                report_name: report.report_config_name.clone(),
                message: "Failed to retrieve simulator's metrics_configs".to_owned(),
            }
        })
    }

    /// Appends the report number, timestamp and content to the report log
    /// in a human readable format. If set in application args, also writes
    /// binary report to a file.
    pub fn process_new_report(&mut self, report: MetricsReport) -> Result<(), SimulatorError> {
        let reports_directory = self.output_directory.join(io::REPORTS_DIR_NAME);

        let metrics_config = self.get_metrics_config_info(&report)?;
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
