// Copyright 2024 Google LLC

use protobuf::reflect::MessageDescriptor;
use sdv_telemetry_proto_rust::metrics_configuration::MetricsReport;
use std::{
    collections::{HashMap, HashSet},
    ffi::{OsStr, OsString},
    fmt,
    fs::File,
    path::Path,
};

use crate::{io, simulator_errors::SimulatorError};

#[derive(strum_macros::Display)]
#[strum(serialize_all = "title_case")]
pub enum MetricsConfigStatus {
    New,
    Added,
    Activated,
    Deactivated,
    Removed,
}

pub struct ReportConfigStatus {
    pub descriptor_proto_name: String,
    pub report_log: File,
    pub num_reports: usize,
    pub report_file_names: Option<Vec<String>>,
}

impl ReportConfigStatus {
    pub fn new(
        descriptor_proto_name: String,
        report_log_path: &Path,
        write_report_files: bool,
    ) -> Result<Self, SimulatorError> {
        let report_log = io::open_report_log(report_log_path)?;
        let report_file_names = if write_report_files { Some(Vec::new()) } else { None };

        Ok(ReportConfigStatus {
            descriptor_proto_name,
            report_log,
            num_reports: 0,
            report_file_names,
        })
    }

    pub fn append_report_log(&mut self, report: String) -> Result<(), SimulatorError> {
        io::append_report_log(&mut self.report_log, &report)
    }
}

impl fmt::Display for ReportConfigStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.num_reports == 0 {
            write!(f, "No reports")?
        }
        if let Some(report_file_names) = &self.report_file_names {
            write!(f, "{} report(s): {}", self.num_reports, report_file_names.join(", "))?
        } else {
            write!(f, "{} report(s)", self.num_reports)?;
        };
        Ok(())
    }
}

pub struct MetricsConfigInfo {
    pub file_name: OsString,
    pub serialized_config: Vec<u8>,
    pub status: MetricsConfigStatus,
    pub descriptor_protos: HashMap<String, MessageDescriptor>,
    pub report_configs: HashMap<String, ReportConfigStatus>,
    pub service_publisher_names: HashSet<String>,
}

impl MetricsConfigInfo {
    pub fn new(
        file_name: &OsStr,
        serialized_config: Vec<u8>,
        descriptor_protos: HashMap<String, MessageDescriptor>,
    ) -> Self {
        MetricsConfigInfo {
            file_name: file_name.to_owned(),
            status: MetricsConfigStatus::New,
            serialized_config,
            descriptor_protos,
            report_configs: HashMap::new(),
            service_publisher_names: HashSet::new(),
        }
    }

    pub fn get_report_config_info(
        &mut self,
        report: &MetricsReport,
    ) -> Result<(&mut ReportConfigStatus, &MessageDescriptor), SimulatorError> {
        let report_config_status = self
            .report_configs
            .get_mut(&report.report_config_name)
            .ok_or_else(|| SimulatorError::OrphanReport {
                config_uuid: report.metrics_config_uuid.clone(),
                report_uuid: report.report_uuid.clone(),
                report_name: report.report_config_name.clone(),
                message: "Failed to retrieve simulator's report_configs".to_owned(),
            })?;

        let message_descriptor =
            self.descriptor_protos
                .get(&report_config_status.descriptor_proto_name[1..])
                .ok_or_else(|| SimulatorError::ProgrammingError {
                    error_message: format!(
                    "descriptor_protos for metrics config \"{}\" missing descriptor proto \"{}\"",
                    self.file_name.display(), report_config_status.descriptor_proto_name
                ),
                })?;

        Ok((report_config_status, message_descriptor))
    }
}

impl fmt::Display for MetricsConfigInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            r#"

METRICS CONFIG INFO
"{}":
Status: {}
Results for report configs:"#,
            self.file_name.display(),
            self.status
        )?;
        for (report_config_name, report_config_status) in &self.report_configs {
            writeln!(f, "\"{report_config_name}\": {report_config_status}")?;
        }
        writeln!(f)?;
        Ok(())
    }
}
