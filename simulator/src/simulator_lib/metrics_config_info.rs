// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use protobuf::reflect::MessageDescriptor;
use sdv_telemetry_proto_rust::metrics_configuration::MetricsReport;
use sdv_telemetry_utils::descriptor_utils::FileDescriptorsAndWkts;
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

impl Drop for ReportConfigStatus {
    fn drop(&mut self) {
        // While dropping `self.report_log` closes its file descriptor, it does not guarantee that
        // the file's content has actually been written to disk. Calling `File::sync_all` before
        // dropping (= closing) the file ensures that the content is written to disk.
        //
        // I suspect that this was the cause of some of our test flakiness, because I noticed that
        // some test failures are caused by a seemingly truncated report log, which cannot have been
        // caused by missing data, since the truncation happens in weird places (like the last line
        // missing `\n` even though we always write a `\n` at the end of the line).
        self.report_log.sync_all().expect("Failed to sync report log file.");
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
    pub file_descriptors: FileDescriptorsAndWkts,
    pub report_configs: HashMap<String, ReportConfigStatus>,
    pub service_publisher_names: HashSet<String>,
}

impl MetricsConfigInfo {
    pub fn new(
        file_name: &OsStr,
        serialized_config: Vec<u8>,
        file_descriptors: FileDescriptorsAndWkts,
    ) -> Self {
        MetricsConfigInfo {
            file_name: file_name.to_owned(),
            status: MetricsConfigStatus::New,
            serialized_config,
            file_descriptors,
            report_configs: HashMap::new(),
            service_publisher_names: HashSet::new(),
        }
    }

    pub fn get_report_config_info(
        &mut self,
        report: &MetricsReport,
    ) -> Result<(&mut ReportConfigStatus, MessageDescriptor), SimulatorError> {
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
            self.file_descriptors
                .find_message_by_full_name(&report_config_status.descriptor_proto_name)
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
