// Copyright 2024 Google LLC

use chrono::{DateTime, Utc};
use sdv_telemetry_proto_rust::metrics_configuration::metrics_report::ReportReason;

pub fn report_log_header(
    metrics_config_file_name: &str,
    uuid: &str,
    report_config_name: &str,
) -> String {
    format!(
        r#"Metrics config "{metrics_config_file_name}", UUID: "{uuid}"
Report configuration "{report_config_name}"

Created reports below
"#
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
Report #{report_number} created {} UTC\nCreated because {}
{report_content}
"#,
            format_report_reason(reason),
            date_time.format("%v %T%.3f")
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
        ReportReason::REPORT_TYPE_UNSPECIFIED | ReportReason::REGULAR => false,
        ReportReason::INCOMPLETE_SHUTDOWN
        | ReportReason::INCOMPLETE_END
        | ReportReason::INCOMPLETE_FINISHED => true,
    }
}

fn format_report_reason(report_reason: ReportReason) -> String {
    match report_reason {
        ReportReason::INCOMPLETE_SHUTDOWN => "telemetry service shut down".to_owned(),
        ReportReason::INCOMPLETE_END => "end trigger was triggered".to_owned(),
        ReportReason::INCOMPLETE_FINISHED => "finish trigger was triggered".to_owned(),
        ReportReason::REPORT_TYPE_UNSPECIFIED => "REPORT_TYPE_UNSPECIFIED".to_owned(),
        ReportReason::REGULAR => "report was triggered".to_owned(),
    }
}
