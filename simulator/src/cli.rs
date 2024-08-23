// Copyright 2024 Google LLC

use chrono::Duration as ChronoDuration;
use clap::{error::ErrorKind, ArgAction::Append, Args, Error, Parser, Subcommand};
use std::{fmt, path::PathBuf, time::Duration};

use crate::util::printable_duration;

#[derive(Parser, Debug)]
#[command(long_about = None)]
pub struct CliArgs {
    #[command(subcommand)]
    pub command: Commands,

    /// After what duration to finish the simulator
    /// Format: [time unit]:[value]
    /// time unit has to be one of: "seconds" | "minutes" | "hours"
    /// Example: "seconds:10"
    #[arg(long = "max-simulation-time", value_parser = parse_duration, required = true)]
    pub max_simulation_time: Duration,
}

#[derive(Subcommand, Debug, Clone)]
pub enum Commands {
    /// Run only telemetry client
    #[command(arg_required_else_help = true)]
    TelemetryClient {
        #[command(flatten)]
        telemetry_client_args: TelemetryClientArgs,
    },

    /// Run only simulation publishers
    #[command(arg_required_else_help = true)]
    SimulationPublishers {
        #[command(flatten)]
        simulation_publisher_args: SimulationPublisherArgs,
    },

    /// Run telemetry client and simulation publishers
    #[command(arg_required_else_help = true)]
    FullSimulation {
        #[command(flatten)]
        telemetry_client_args: TelemetryClientArgs,
        #[command(flatten)]
        simulation_publisher_args: SimulationPublisherArgs,
    },
}

#[derive(Args, Debug, Clone)]
#[group(multiple = true)]
pub struct TelemetryClientArgs {
    /// Metrics config files to execute
    #[arg(long = "metrics-configs", required = true, num_args = 1.., value_hint = clap::ValueHint::FilePath, action = Append)]
    pub metrics_config_paths: Vec<PathBuf>,

    /// Disable writing metrics reports to disk and only write human readable
    /// reports to report log(s)
    #[arg(long = "no-binary-reports")]
    pub no_binary_reports: bool,

    /// After how many returned reports to finish the simulator
    #[arg(long = "max-report-count", required = true)]
    pub max_report_count: u64,
}

#[derive(Args, Debug, Clone)]
#[group(multiple = true)]
pub struct SimulationPublisherArgs {
    /// Publisher config files to execute
    #[arg(long = "publisher-configs", required = true, num_args = 1.., value_hint = clap::ValueHint::FilePath, action = Append)]
    pub publisher_config_paths: Vec<PathBuf>,
}

impl fmt::Display for CliArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.command {
            Commands::TelemetryClient { telemetry_client_args } => {
                writeln!(f, "Running telemetry client only")?;
                writeln!(f, "Metrics configs: {:?}", telemetry_client_args.metrics_config_paths)?;
                if telemetry_client_args.no_binary_reports {
                    writeln!(f, "Not writing reports to disk.")?;
                }
                writeln!(
                    f,
                    "Application will finish after {} or {} returned reports, whichever comes first.",
                    printable_duration(self.max_simulation_time)?,
                    telemetry_client_args.max_report_count
                )
            }
            Commands::SimulationPublishers { simulation_publisher_args } => {
                writeln!(f, "Running simulation publishers only")?;
                writeln!(
                    f,
                    "Publisher configs: {:?}",
                    simulation_publisher_args.publisher_config_paths
                )?;

                writeln!(
                    f,
                    "Application will finish after {}.",
                    printable_duration(self.max_simulation_time)?,
                )
            }
            Commands::FullSimulation { telemetry_client_args, simulation_publisher_args } => {
                writeln!(f, "Running telemetry client and simulation publishers")?;
                writeln!(f, "Metrics configs: {:?}", telemetry_client_args.metrics_config_paths)?;
                writeln!(
                    f,
                    "Publisher configs: {:?}",
                    simulation_publisher_args.publisher_config_paths
                )?;
                if telemetry_client_args.no_binary_reports {
                    writeln!(f, "Not writing reports to disk.")?;
                }
                writeln!(
                    f,
                    "Application will finish after {} or {} returned reports, whichever comes first.",
                    printable_duration(self.max_simulation_time)?,
                    telemetry_client_args.max_report_count
                )
            }
        }
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
    fn test_parse_cli_args_telemetry_client_subcommand() {
        let args = CliArgs::try_parse_from(vec![
            "sdv_telemetry_simulator",
            "--max-simulation-time",
            "seconds:10",
            "telemetry-client",
            "--metrics-configs",
            "metrics_config_1.textproto",
            "--max-report-count",
            "5",
            "--no-binary-reports",
        ]);
        let args = args.unwrap();
        if let Commands::TelemetryClient { telemetry_client_args } = args.command {
            assert!(telemetry_client_args.no_binary_reports);
        } else {
            panic!("Wrong subcommand: {:?}", args.command);
        };
    }

    #[test]
    fn test_parse_cli_args_simulation_publishers_subcommand() {
        let args = CliArgs::try_parse_from(vec![
            "sdv_telemetry_simulator",
            "--max-simulation-time",
            "seconds:10",
            "simulation-publishers",
            "--publisher-configs",
            "publisher_config_1.textproto",
        ]);
        let args = args.unwrap();

        assert!(matches!(args.command, Commands::SimulationPublishers { .. }), "Wrong subcommand");
    }

    #[test]
    fn test_parse_cli_args_full_simulation_subcommand() {
        let args = CliArgs::try_parse_from(vec![
            "sdv_telemetry_simulator",
            "--max-simulation-time",
            "seconds:10",
            "full-simulation",
            "--metrics-configs",
            "metrics_config_1.textproto",
            "metrics_config_2.textproto",
            "--publisher-configs",
            "publisher_config_1.textproto",
            "publisher_config_2.textproto",
            "--max-report-count",
            "5",
        ]);
        let args = args.unwrap();
        assert_eq!(args.max_simulation_time, Duration::from_secs(10));

        if let Commands::FullSimulation { telemetry_client_args, simulation_publisher_args } =
            args.command
        {
            assert_eq!(
                telemetry_client_args.metrics_config_paths,
                vec![
                    PathBuf::from("metrics_config_1.textproto"),
                    PathBuf::from("metrics_config_2.textproto")
                ]
            );
            assert_eq!(telemetry_client_args.max_report_count, 5);

            assert_eq!(
                simulation_publisher_args.publisher_config_paths,
                vec![
                    PathBuf::from("publisher_config_1.textproto"),
                    PathBuf::from("publisher_config_2.textproto")
                ]
            );
        } else {
            panic!("Wrong subcommand: {:?}", args.command);
        };
    }

    #[test]
    fn test_parse_cli_args_missing_parameter() {
        let args = CliArgs::try_parse_from(vec![
            "sdv_telemetry_simulator",
            "--max-simulation-time",
            "seconds:10",
            "simulation-publishers",
        ]);
        assert_eq!(args.unwrap_err().kind(), ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand);
    }

    #[test]
    fn test_parse_cli_args_redundant_parameter() {
        let args = CliArgs::try_parse_from(vec![
            "sdv_telemetry_simulator",
            "--max-simulation-time",
            "seconds:10",
            "simulation-publishers",
            "--metrics-configs",
            "metrics_config_1.textproto",
            "metrics_config_2.textproto",
            "--publisher-configs",
            "publisher_config_1.textproto",
            "publisher_config_2.textproto",
            "--max-report-count",
            "5",
        ]);
        assert_eq!(args.unwrap_err().kind(), ErrorKind::UnknownArgument);
    }

    #[test]
    fn test_fmt_args_telemetry_client() {
        let args = CliArgs {
            command: Commands::TelemetryClient {
                telemetry_client_args: TelemetryClientArgs {
                    max_report_count: 5,
                    metrics_config_paths: vec![PathBuf::from("metrics_config_1.textproto")],
                    no_binary_reports: false,
                },
            },
            max_simulation_time: Duration::from_secs(10),
        };

        let expected_output = r#"Running telemetry client only
Metrics configs: ["metrics_config_1.textproto"]
Application will finish after 00:00:10 or 5 returned reports, whichever comes first.
"#;
        let actual_output = format!("{args}");
        assert_eq!(actual_output, expected_output);
    }

    #[test]
    fn test_fmt_args_simulation_publisher() {
        let args = CliArgs {
            command: Commands::SimulationPublishers {
                simulation_publisher_args: SimulationPublisherArgs {
                    publisher_config_paths: vec![
                        PathBuf::from("publisher_config_1.textproto"),
                        PathBuf::from("publisher_config_2.textproto"),
                    ],
                },
            },
            max_simulation_time: Duration::from_secs(10),
        };

        let expected_output = r#"Running simulation publishers only
Publisher configs: ["publisher_config_1.textproto", "publisher_config_2.textproto"]
Application will finish after 00:00:10.
"#;
        let actual_output = format!("{args}");
        assert_eq!(actual_output, expected_output);
    }

    #[test]
    fn test_fmt_args_full_simulation() {
        let args = CliArgs {
            command: Commands::FullSimulation {
                telemetry_client_args: TelemetryClientArgs {
                    max_report_count: 5,
                    metrics_config_paths: vec![PathBuf::from("metrics_config_1.textproto")],
                    no_binary_reports: true,
                },
                simulation_publisher_args: SimulationPublisherArgs {
                    publisher_config_paths: vec![
                        PathBuf::from("publisher_config_1.textproto"),
                        PathBuf::from("publisher_config_2.textproto"),
                    ],
                },
            },
            max_simulation_time: Duration::from_secs(10),
        };

        let expected_output = r#"Running telemetry client and simulation publishers
Metrics configs: ["metrics_config_1.textproto"]
Publisher configs: ["publisher_config_1.textproto", "publisher_config_2.textproto"]
Not writing reports to disk.
Application will finish after 00:00:10 or 5 returned reports, whichever comes first.
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
