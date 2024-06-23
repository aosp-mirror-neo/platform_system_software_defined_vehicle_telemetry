// Copyright 2024 Google LLC

use protobuf::reflect::MessageDescriptor;
use std::{collections::HashMap, fmt, fs::File, path::PathBuf};

use crate::{io, simulator_errors::SimulatorError, ReportInfo};

pub enum MetricsConfigStatus {
    New,
    Added,
    Activated,
}

impl fmt::Display for MetricsConfigStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MetricsConfigStatus::New => write!(f, "New")?,
            MetricsConfigStatus::Added => write!(f, "Added")?,
            MetricsConfigStatus::Activated => write!(f, "Activated")?,
        };
        Ok(())
    }
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
        report_log_path: PathBuf,
        write_report_files: bool,
    ) -> Result<Self, SimulatorError> {
        let report_log = io::open_report_log(&report_log_path)?;
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
    pub file_name: String,
    pub serialized_config: Vec<u8>,
    pub status: MetricsConfigStatus,
    pub descriptor_protos: HashMap<String, MessageDescriptor>,
    pub report_configs: HashMap<String, ReportConfigStatus>,
}

impl MetricsConfigInfo {
    pub fn new(
        file_name: &str,
        serialized_config: Vec<u8>,
        descriptor_protos: HashMap<String, MessageDescriptor>,
    ) -> Self {
        MetricsConfigInfo {
            file_name: file_name.to_owned(),
            status: MetricsConfigStatus::New,
            serialized_config,
            descriptor_protos,
            report_configs: HashMap::new(),
        }
    }

    pub fn get_report_config_info(
        &mut self,
        report_info: &ReportInfo,
    ) -> Result<(&mut ReportConfigStatus, &MessageDescriptor), SimulatorError> {
        let report_config_status =
            self.report_configs.get_mut(&report_info.report_config_name).ok_or_else(|| {
                SimulatorError::OrphanReport { uuid: report_info.metrics_config_uuid.clone() }
            })?;

        let message_descriptor = self
            .descriptor_protos
            .get(&report_config_status.descriptor_proto_name[1..])
            .ok_or_else(|| SimulatorError::ProgrammingError {
                error_message: format!(
                    "descriptor_protos for metrics config \"{}\" missing descriptor proto \"{}\"",
                    self.file_name, report_config_status.descriptor_proto_name
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
            self.file_name, self.status
        )?;
        for (report_config_name, report_config_status) in &self.report_configs {
            writeln!(f, "\"{report_config_name}\": {report_config_status}")?;
        }
        writeln!(f)?;
        Ok(())
    }
}
