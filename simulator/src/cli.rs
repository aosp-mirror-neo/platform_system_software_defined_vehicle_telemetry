// Copyright 2024 Google LLC

use chrono::Duration as ChronoDuration;
use clap::{error::ErrorKind, ArgAction::Append, Error, Parser};
use std::{fmt, path::PathBuf, time::Duration};

use crate::util::printable_duration;

#[derive(Parser, Debug)]
#[command(long_about = None)]
pub struct Args {
    /// Metrics config files to execute
    #[arg(long = "metrics-configs", required = true, num_args = 1.., value_hint = clap::ValueHint::FilePath, action = Append)]
    pub metrics_config_paths: Vec<PathBuf>,

    /// Publisher config files to execute. If not provided, will run as simple telemetry client.
    #[arg(long = "publisher-configs", num_args = 0.., value_hint = clap::ValueHint::FilePath, action = Append)]
    pub publisher_config_paths: Vec<PathBuf>,

    /// Disable writing metrics reports to disk and only write human readable
    /// reports to report log(s)
    #[arg(long = "no-binary-reports")]
    pub no_binary_reports: bool,

    /// After what duration to finish the simulator
    /// Format: [time unit]:[value]
    /// time unit has to be one of: "seconds" | "minutes" | "hours"
    /// Example: "seconds:10"
    #[arg(long = "max-simulation-time", value_parser = parse_duration)]
    pub max_simulation_time: Duration,

    /// After how many returned reports to finish the simulator
    #[arg(long = "max-report-count")]
    pub max_report_count: u64,
}

impl fmt::Display for Args {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Metrics configs: {:?}", self.metrics_config_paths)?;
        if self.publisher_config_paths.is_empty() {
            writeln!(f, "Running as simple telemetry client without simulation publishers.")?;
        } else {
            writeln!(f, "Publisher configs: {:?}", self.publisher_config_paths)?;
        }
        if self.no_binary_reports {
            writeln!(f, "Not writing reports to disk.")?;
        }
        writeln!(
            f,
            "Finish simulator after {} or {} returned reports, whichever comes first.",
            printable_duration(self.max_simulation_time)?,
            self.max_report_count
        )
    }
}

fn parse_duration(s: &str) -> Result<Duration, Error> {
    let Some((text, value)) = s.split_once(':') else {
        return Err(clap::Error::new(ErrorKind::InvalidValue));
    };

    let Ok(value) = value.parse::<u64>() else {
        return Err(clap::Error::new(ErrorKind::InvalidValue));
    };

    match text {
        "second" | "seconds" => convert_duration(value, ChronoDuration::try_seconds),
        "minute" | "minutes" => convert_duration(value, ChronoDuration::try_minutes),
        "hour" | "hours" => convert_duration(value, ChronoDuration::try_hours),
        _ => Err(clap::Error::new(ErrorKind::InvalidValue)),
    }
}

/// Tries to convert a positive number of seconds/minutes/hours/... to a
/// std::time::Duration. If the number of seconds is too large, returns an error
/// but does not panic.
fn convert_duration(
    value: u64,
    chrono_fn: impl Fn(i64) -> Option<chrono::Duration>,
) -> Result<Duration, Error> {
    Ok(chrono_fn(value.try_into().map_err(|_| clap::Error::new(ErrorKind::InvalidValue))?)
        .ok_or(clap::Error::new(ErrorKind::InvalidValue))?
        .to_std()
        .expect("Could not convert chrono Duration to std Duration."))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_args_valid() {
        let args = Args::try_parse_from(vec![
            "sdv_telemetry_simulator",
            "--metrics-configs",
            "metrics_config_1.textproto",
            "metrics_config_2.textproto",
            "--publisher-configs",
            "publisher_config_1.textproto",
            "publisher_config_2.textproto",
            "--max-simulation-time",
            "seconds:10",
            "--max-report-count",
            "5",
        ]);
        let args = args.unwrap();
        assert_eq!(
            args.metrics_config_paths,
            vec![
                PathBuf::from("metrics_config_1.textproto"),
                PathBuf::from("metrics_config_2.textproto")
            ]
        );
        assert_eq!(
            args.publisher_config_paths,
            vec![
                PathBuf::from("publisher_config_1.textproto"),
                PathBuf::from("publisher_config_2.textproto")
            ]
        );
        assert_eq!(args.max_simulation_time, Duration::from_secs(10));
        assert_eq!(args.max_report_count, 5);
    }

    #[test]
    fn test_parse_args_no_publishers() {
        let args = Args::try_parse_from(vec![
            "telemetry_simulator",
            "--metrics-configs",
            "metrics_config_1.textproto",
            "metrics_config_2.textproto",
            "--max-simulation-time",
            "seconds:10",
            "--max-report-count",
            "5",
        ]);
        assert!(args.unwrap().publisher_config_paths.is_empty());
    }

    #[test]
    fn test_parse_args_no_binary_reports() {
        let args = Args::try_parse_from(vec![
            "telemetry_simulator",
            "--metrics-configs",
            "metrics_config_1.textproto",
            "metrics_config_2.textproto",
            "--publisher-configs",
            "publisher_config_1.textproto",
            "publisher_config_2.textproto",
            "--max-simulation-time",
            "seconds:10",
            "--max-report-count",
            "5",
            "--no-binary-reports",
        ]);
        assert!(args.unwrap().no_binary_reports);
    }

    #[test]
    fn test_parse_args_missing_metrics_configs() {
        let args = Args::try_parse_from(vec![
            "telemetry_simulator",
            "--publisher-configs",
            "publisher_config_1.textproto",
            "publisher_config_2.textproto",
            "--max-simulation-time",
            "seconds:10",
            "--max-report-count",
            "5",
        ]);
        assert_eq!(args.unwrap_err().kind(), ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn test_fmt_args() {
        let args = Args {
            metrics_config_paths: vec![PathBuf::from("metrics_config_1.textproto")],
            publisher_config_paths: vec![
                PathBuf::from("publisher_config_1.textproto"),
                PathBuf::from("publisher_config_2.textproto"),
            ],
            no_binary_reports: true,
            max_simulation_time: Duration::from_secs(10),
            max_report_count: 5,
        };

        let expected_output = r#"Metrics configs: ["metrics_config_1.textproto"]
Publisher configs: ["publisher_config_1.textproto", "publisher_config_2.textproto"]
Not writing reports to disk.
Finish simulator after 00:00:10 or 5 returned reports, whichever comes first.
"#;

        let actual_output = format!("{args}");

        assert_eq!(actual_output, expected_output);
    }

    #[test]
    fn test_fmt_args_no_publishers() {
        let args = Args {
            metrics_config_paths: vec![PathBuf::from("metrics_config_1.textproto")],
            publisher_config_paths: vec![],
            no_binary_reports: false,
            max_simulation_time: Duration::from_secs(30),
            max_report_count: 10,
        };

        let expected_output = r#"Metrics configs: ["metrics_config_1.textproto"]
Running as simple telemetry client without simulation publishers.
Finish simulator after 00:00:30 or 10 returned reports, whichever comes first.
"#;

        let actual_output = format!("{args}");

        assert_eq!(actual_output, expected_output);
    }

    #[test]
    fn test_convert_duration_seconds() {
        let duration = convert_duration(10, ChronoDuration::try_seconds).unwrap();
        assert_eq!(duration, Duration::from_secs(10));
    }

    #[test]
    fn test_convert_duration_minutes() {
        let duration = convert_duration(5, ChronoDuration::try_minutes).unwrap();
        assert_eq!(duration, Duration::from_secs(300));
    }

    #[test]
    fn test_convert_duration_hours() {
        let duration = convert_duration(2, ChronoDuration::try_hours).unwrap();
        assert_eq!(duration, Duration::from_secs(7200));
    }

    #[test]
    fn test_convert_duration_too_large() {
        let result = convert_duration(u64::MAX, ChronoDuration::try_seconds);
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidValue);
    }

    #[test]
    fn test_parse_duration_second() {
        let duration = parse_duration("second:1").unwrap();
        assert_eq!(duration, Duration::from_secs(1));
    }

    #[test]
    fn test_parse_duration_seconds() {
        let duration = parse_duration("seconds:10").unwrap();
        assert_eq!(duration, Duration::from_secs(10));
    }

    #[test]
    fn test_parse_duration_minute() {
        let duration = parse_duration("minutes:2").unwrap();
        assert_eq!(duration, Duration::from_secs(120));
    }

    #[test]
    fn test_parse_duration_minutes() {
        let duration = parse_duration("minute:1").unwrap();
        assert_eq!(duration, Duration::from_secs(60));
    }

    #[test]
    fn test_parse_duration_hour() {
        let duration = parse_duration("hour:1").unwrap();
        assert_eq!(duration, Duration::from_secs(3600));
    }

    #[test]
    fn test_parse_duration_hours() {
        let duration = parse_duration("hours:2").unwrap();
        assert_eq!(duration, Duration::from_secs(7200));
    }

    #[test]
    fn test_parse_duration_invalid_unit() {
        let result = parse_duration("days:1");
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidValue);
    }

    #[test]
    fn test_parse_duration_invalid_value() {
        let result = parse_duration("seconds:abc");
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidValue);
    }

    #[test]
    fn test_parse_duration_missing_colon() {
        let result = parse_duration("seconds10");
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidValue);
    }
}
