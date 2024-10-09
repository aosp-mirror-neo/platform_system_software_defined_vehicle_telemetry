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
    #[arg(long = "max-report-count", required = false)]
    pub max_report_count: Option<u64>,

    /// The output directory to write results to.
    #[arg(long = "output-directory", default_value = "/data/local/tmp/telemetry_simulator_out", value_hint = clap::ValueHint::DirPath)]
    pub output_directory: PathBuf,
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
        fn write_finish_condition(
            f: &mut fmt::Formatter<'_>,
            max_simulation_time: &Duration,
            max_report_count: &Option<u64>,
        ) -> fmt::Result {
            if let Some(max_report_count) = max_report_count {
                writeln!(
                    f,
                    "Application will finish after {} or {} returned reports, whichever comes first.",
                    printable_duration(max_simulation_time)?,
                    max_report_count
                )
            } else {
                writeln!(
                    f,
                    "Application will finish after {}.",
                    printable_duration(max_simulation_time)?,
                )
            }
        }

        match &self.command {
            Commands::TelemetryClient { telemetry_client_args } => {
                writeln!(f, "Running telemetry client only")?;
                writeln!(f, "Metrics configs: {:?}", telemetry_client_args.metrics_config_paths)?;
                if telemetry_client_args.no_binary_reports {
                    writeln!(f, "Not writing reports to disk.")?;
                }
                write_finish_condition(
                    f,
                    &self.max_simulation_time,
                    &telemetry_client_args.max_report_count,
                )
            }
            Commands::SimulationPublishers { simulation_publisher_args } => {
                writeln!(f, "Running simulation publishers only")?;
                writeln!(
                    f,
                    "Publisher configs: {:?}",
                    simulation_publisher_args.publisher_config_paths
                )?;
                write_finish_condition(f, &self.max_simulation_time, &None)
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
                write_finish_condition(
                    f,
                    &self.max_simulation_time,
                    &telemetry_client_args.max_report_count,
                )
            }
        }
    }
}

#[allow(clippy::as_conversions, reason = "No alternatives for casting from float to int exist")]
fn parse_duration(s: &str) -> Result<Duration, Error> {
    let Some((text, value)) = s.split_once(':') else {
        return Err(clap::Error::new(ErrorKind::InvalidValue));
    };

    let Ok(value) = value.parse::<f64>() else {
        return Err(clap::Error::new(ErrorKind::InvalidValue));
    };

    let seconds = match text {
        "second" | "seconds" => value,
        "minute" | "minutes" => value * 60.0,
        "hour" | "hours" => value * 3600.0,
        _ => return Err(clap::Error::new(ErrorKind::InvalidValue)),
    };

    if seconds < 0.0 {
        return Err(clap::Error::new(ErrorKind::InvalidValue));
    }

    let whole_seconds = seconds.trunc() as i64;
    let sub_seconds = seconds.fract();
    let sub_nanos = (sub_seconds * 1_000_000_000.0).floor() as u32;

    Ok(ChronoDuration::new(whole_seconds, sub_nanos)
        .ok_or(clap::Error::new(ErrorKind::InvalidValue))?
        .to_std()
        .expect("Could not convert chrono Duration to std Duration."))
}

#[cfg(test)]
mod tests {
    use super::*;

    use sdv_telemetry_test_utils::parameterized_test;

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
            assert_eq!(telemetry_client_args.max_report_count, Some(5));
            assert_eq!(
                telemetry_client_args.output_directory,
                PathBuf::from("/data/local/tmp/telemetry_simulator_out"),
            );
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
            "--output-directory",
            "/foo/bar",
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
            assert_eq!(telemetry_client_args.max_report_count, Some(5));
            assert_eq!(telemetry_client_args.output_directory, PathBuf::from("/foo/bar"));

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
    fn test_parse_cli_args_telemetry_client_subcommand_no_report_count() {
        let args = CliArgs::try_parse_from(vec![
            "sdv_telemetry_simulator",
            "--max-simulation-time",
            "seconds:10",
            "telemetry-client",
            "--metrics-configs",
            "metrics_config_1.textproto",
            "--no-binary-reports",
        ]);
        let args = args.unwrap();
        assert!(
            matches!(
                args.command,
                Commands::TelemetryClient {
                    telemetry_client_args: TelemetryClientArgs { max_report_count: None, .. },
                },
            ),
            "Unexpected result: {:?}",
            args.command
        )
    }

    #[test]
    fn test_parse_cli_args_full_simulation_subcommand_no_report_count() {
        let args = CliArgs::try_parse_from(vec![
            "sdv_telemetry_simulator",
            "--max-simulation-time",
            "seconds:10",
            "full-simulation",
            "--metrics-configs",
            "metrics_config_1.textproto",
            "--publisher-configs",
            "publisher_config_1.textproto",
        ]);
        let args = args.unwrap();
        assert!(
            matches!(
                args.command,
                Commands::FullSimulation {
                    telemetry_client_args: TelemetryClientArgs { max_report_count: None, .. },
                    ..
                },
            ),
            "Unexpected result: {:?}",
            args.command
        )
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
                    max_report_count: Some(5),
                    metrics_config_paths: vec![PathBuf::from("metrics_config_1.textproto")],
                    no_binary_reports: false,
                    output_directory: PathBuf::from("/foo/bar"),
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
                    max_report_count: Some(5),
                    metrics_config_paths: vec![PathBuf::from("metrics_config_1.textproto")],
                    no_binary_reports: true,
                    output_directory: PathBuf::from("/foo/bar"),
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

    parameterized_test!(
        test_parse_duration,

        |expected, input| {
            let result = parse_duration(input);
            if let Some(expected) = expected {
                assert_eq!(result.unwrap(), expected);
            } else {
                assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidValue);
            }
        },

        second: (Some(Duration::from_secs(1)), "second:1",),
        seconds: (Some(Duration::from_secs(10)), "seconds:10",),
        minute: (Some(Duration::from_secs(60)), "minute:1",),
        minutes: (Some(Duration::from_secs(120)), "minutes:2",),
        hour: (Some(Duration::from_secs(3600)), "hour:1",),
        hours: (Some(Duration::from_secs(7200)), "hours:2",),

        float_seconds: (Some(Duration::from_millis(6500)), "seconds:6.5",),
        float_minutes: (Some(Duration::from_secs(150)), "minutes:2.5",),
        float_hours: (Some(Duration::from_secs(30 * 60)), "hours:0.5",),

        invalid_unit: (None, "days:1",),
        invalid_value: (None, "seconds:abc",),
        negative_value: (None, "seconds:-1",),
        missing_colon: (None, "seconds10",),
    );
}
