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

use chrono::{DateTime, Utc};
use sdv_telemetry_proto_rust::metrics_configuration::metrics_report::ReportReason;
use std::ffi::OsStr;

pub fn report_log_header(
    metrics_config_file_name: &OsStr,
    uuid: &str,
    report_config_name: &str,
) -> String {
    format!(
        r#"Metrics config "{}", UUID: "{uuid}"
Report configuration "{report_config_name}"

Created reports below
"#,
        metrics_config_file_name.display(),
    )
}

pub fn report_log_entry(
    report_number: i32,
    date_time: &DateTime<Utc>,
    reason: ReportReason,
    report_content: &str,
) -> String {
    if report_incomplete(reason) {
        format!(
            r#"INCOMPLETE
Report #{report_number} created {} UTC
Created because {}
{report_content}
"#,
            date_time.format("%v %T%.3f"),
            format_report_reason(reason)
        )
    } else {
        format!(
            r#"Report #{report_number} created on {} UTC
{report_content}
"#,
            date_time.format("%v %T%.3f")
        )
    }
}

fn report_incomplete(report_reason: ReportReason) -> bool {
    match report_reason {
        ReportReason::REPORT_TYPE_UNSPECIFIED | ReportReason::REGULAR | ReportReason::INITIAL => {
            false
        }
        ReportReason::INCOMPLETE_SHUTDOWN
        | ReportReason::INCOMPLETE_END
        | ReportReason::INCOMPLETE_FINISHED => true,
    }
}

fn format_report_reason(report_reason: ReportReason) -> String {
    match report_reason {
        ReportReason::INCOMPLETE_SHUTDOWN => "telemetry service shut down".to_owned(),
        ReportReason::INCOMPLETE_END => "end trigger was triggered".to_owned(),
        ReportReason::INCOMPLETE_FINISHED => {
            "either finish trigger was triggered or the config was deactvated/removed \
            by the simulation actions"
                .to_owned()
        }
        ReportReason::REPORT_TYPE_UNSPECIFIED => "REPORT_TYPE_UNSPECIFIED".to_owned(),
        ReportReason::REGULAR => "report was triggered".to_owned(),
        ReportReason::INITIAL => "initial report was triggered".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::*;

    fn date_time_from_y_m_d_h_m_s_milli(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
        milli: u32,
    ) -> DateTime<Utc> {
        NaiveDate::from_ymd_opt(year, month, day)
            .unwrap()
            .and_hms_milli_opt(hour, minute, second, milli)
            .unwrap()
            .and_local_timezone(Utc)
            .unwrap()
    }

    #[test]
    fn test_report_log_header() {
        let metrics_config_file_name = OsStr::new("metrics_config_1.textproto");
        let uuid = "uuid_1";
        let report_config_name = "report_config_1";
        let actual_output = report_log_header(metrics_config_file_name, uuid, report_config_name);

        let expected_output = r#"Metrics config "metrics_config_1.textproto", UUID: "uuid_1"
Report configuration "report_config_1"

Created reports below
"#
        .to_string();

        assert_eq!(actual_output, expected_output);
    }

    #[test]
    fn test_report_log_entry_complete() {
        let report_number = 1;
        let date_time = date_time_from_y_m_d_h_m_s_milli(2024, 7, 22, 9, 59, 8, 578);
        let reason = ReportReason::REGULAR;
        let report_content = "Report content";
        let actual_output = report_log_entry(report_number, &date_time, reason, report_content);

        let expected_output = "\
Report #1 created on 22-Jul-2024 09:59:08.578 UTC
Report content
";

        assert_eq!(actual_output, expected_output);
    }

    #[test]
    fn test_report_log_entry_incomplete_shutdown() {
        let report_number = 2;
        let date_time = date_time_from_y_m_d_h_m_s_milli(2024, 7, 22, 9, 59, 8, 578);
        let reason = ReportReason::INCOMPLETE_SHUTDOWN;
        let report_content = "Report content";
        let actual_output = report_log_entry(report_number, &date_time, reason, report_content);

        let expected_output = "\
INCOMPLETE
Report #2 created 22-Jul-2024 09:59:08.578 UTC
Created because telemetry service shut down
Report content
";

        assert_eq!(actual_output, expected_output);
    }

    #[test]
    fn test_report_log_entry_incomplete_end() {
        let report_number = 3;
        let date_time = date_time_from_y_m_d_h_m_s_milli(2024, 7, 22, 9, 59, 8, 578);
        let reason = ReportReason::INCOMPLETE_END;
        let report_content = "Report content";
        let actual_output = report_log_entry(report_number, &date_time, reason, report_content);

        let expected_output = "\
INCOMPLETE
Report #3 created 22-Jul-2024 09:59:08.578 UTC
Created because end trigger was triggered
Report content
";

        assert_eq!(actual_output, expected_output);
    }

    #[test]
    fn test_report_log_entry_incomplete_finished() {
        let report_number = 4;
        let date_time = date_time_from_y_m_d_h_m_s_milli(2024, 7, 22, 9, 59, 8, 578);
        let reason = ReportReason::INCOMPLETE_FINISHED;
        let report_content = "Report content";
        let actual_output = report_log_entry(report_number, &date_time, reason, report_content);

        let expected_output = "\
INCOMPLETE
Report #4 created 22-Jul-2024 09:59:08.578 UTC
Created because either finish trigger was triggered or the config was deactvated/removed by the simulation actions
Report content
";

        assert_eq!(actual_output, expected_output);
    }
}
