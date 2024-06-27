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
