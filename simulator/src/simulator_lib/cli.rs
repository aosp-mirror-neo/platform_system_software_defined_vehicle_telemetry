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

use chrono::Duration as ChronoDuration;
use clap::{error::ErrorKind, ArgAction::Append, Args, Error, Parser, Subcommand};
use std::{fmt, path::PathBuf, time::Duration};

use crate::util::printable_duration;

#[derive(Parser, Debug)]
#[command(long_about = None)]
/// CLI Args
pub struct CliArgs {
    #[command(subcommand)]
    /// Subcommand to run
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
    #[arg(
        long = "metrics-configs",
        required = true,
        num_args = 1..,
        value_hint = clap::ValueHint::FilePath,
        action = Append,
    )]
    pub metrics_config_paths: Vec<PathBuf>,

    /// Simulation actions config to execute
    #[arg(
        long = "simulation-actions",
        value_hint = clap::ValueHint::FilePath,
    )]
    pub sim_actions_path: Option<PathBuf>,

    /// Disable writing metrics reports to disk and only write human readable
    /// reports to report log(s)
    #[arg(long = "no-binary-reports")]
    pub no_binary_reports: bool,

    /// After how many returned reports to finish the simulator
    #[arg(long = "max-report-count", required = false)]
    pub max_report_count: Option<u64>,

    /// The output directory to write results to.
    #[arg(
        long = "output-directory",
        default_value = "/data/local/tmp/telemetry_simulator_out",
        value_hint = clap::ValueHint::DirPath,
    )]
    pub output_directory: PathBuf,
}

#[derive(Args, Debug, Clone)]
#[group(multiple = true)]
pub struct SimulationPublisherArgs {
    /// Publisher config files to execute
    #[arg(
        long = "publisher-configs",
        required = true,
        num_args = 1..,
        value_hint = clap::ValueHint::FilePath,
        action = Append
    )]
    pub publisher_config_paths: Vec<PathBuf>,
}

impl fmt::Display for CliArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (title, telemetry_args, publisher_args) = match &self.command {
            Commands::TelemetryClient { telemetry_client_args } => {
                ("Running telemetry client only", Some(telemetry_client_args), None)
            }
            Commands::SimulationPublishers { simulation_publisher_args } => {
                ("Running simulation publishers only", None, Some(simulation_publisher_args))
            }
            Commands::FullSimulation { telemetry_client_args, simulation_publisher_args } => (
                "Running telemetry client and simulation publishers",
                Some(telemetry_client_args),
                Some(simulation_publisher_args),
            ),
        };

        writeln!(f, "{title}")?;
        if let Some(args) = telemetry_args {
            write!(f, "{args}")?;
        }
        if let Some(args) = publisher_args {
            write!(f, "{args}")?;
        }

        let max_report_count = telemetry_args.and_then(|a| a.max_report_count);
        if let Some(max_report_count) = max_report_count {
            writeln!(
                f,
                "Application will finish after {} or {} returned reports, whichever comes first.",
                printable_duration(&self.max_simulation_time)?,
                max_report_count
            )
        } else {
            writeln!(
                f,
                "Application will finish after {}.",
                printable_duration(&self.max_simulation_time)?
            )
        }
    }
}

impl fmt::Display for TelemetryClientArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Metrics configs: {:?}", self.metrics_config_paths)?;
        writeln!(f, "Output directory: {}", self.output_directory.display())?;
        if let Some(path) = &self.sim_actions_path {
            writeln!(f, "Simulation actions config: {}", path.display())?;
        }
        if self.no_binary_reports {
            write!(f, "Writing only human-readable report summary, ")?;
            writeln!(f, "not writing binary reports.")
        } else {
            writeln!(f, "Writing both binary reports and human-readable report summary.")
        }
    }
}

impl fmt::Display for SimulationPublisherArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Publisher configs: {:?}", self.publisher_config_paths)
    }
}

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

    #[expect(clippy::cast_possible_truncation, reason = "We want to truncate the value.")]
    let whole_seconds = seconds.trunc() as i64;
    #[expect(clippy::cast_possible_truncation, reason = "We want to truncate the value.")]
    #[expect(
        clippy::cast_sign_loss,
        reason = "`seconds` is guaranteed to be positive due to the guard clause above."
    )]
    let sub_nanos = (seconds.fract() * 1_000_000_000.0).floor() as u32;

    Ok(ChronoDuration::new(whole_seconds, sub_nanos)
        .ok_or(clap::Error::new(ErrorKind::InvalidValue))?
        .to_std()
        .expect("Could not convert chrono Duration to std Duration."))
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::assert_that;
    use googletest::matchers::{container_eq, eq, matches_pattern, none, some};
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

        assert_that!(
            args.command,
            matches_pattern!(Commands::TelemetryClient {
                telemetry_client_args: matches_pattern!(TelemetryClientArgs {
                    no_binary_reports: eq(&true),
                    max_report_count: some(eq(&5u64)),
                    sim_actions_path: none(),
                    output_directory: eq(&PathBuf::from("/data/local/tmp/telemetry_simulator_out")),
                })
            })
        );
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

        assert_that!(args.command, matches_pattern!(Commands::SimulationPublishers { .. }));
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
            "--simulation-actions",
            "sim_actions.textproto",
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

        assert_that!(
            args.command,
            matches_pattern!(Commands::FullSimulation {
                telemetry_client_args: matches_pattern!(TelemetryClientArgs {
                    max_report_count: some(eq(&5u64)),
                    metrics_config_paths: container_eq([
                        PathBuf::from("metrics_config_1.textproto"),
                        PathBuf::from("metrics_config_2.textproto")
                    ]),
                    sim_actions_path: some(eq(&PathBuf::from("sim_actions.textproto"))),
                    output_directory: eq(&PathBuf::from("/foo/bar")),
                }),
                simulation_publisher_args: matches_pattern!(SimulationPublisherArgs {
                    publisher_config_paths: container_eq([
                        PathBuf::from("publisher_config_1.textproto"),
                        PathBuf::from("publisher_config_2.textproto")
                    ])
                })
            })
        );
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

        assert_that!(
            args.command,
            matches_pattern!(Commands::TelemetryClient {
                telemetry_client_args: matches_pattern!(TelemetryClientArgs {
                    max_report_count: none(),
                })
            })
        );
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

        assert_that!(
            args.command,
            matches_pattern!(Commands::FullSimulation {
                telemetry_client_args: matches_pattern!(TelemetryClientArgs {
                    max_report_count: none(),
                })
            })
        );
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
                    sim_actions_path: None,
                    no_binary_reports: false,
                    output_directory: PathBuf::from("/foo/bar"),
                },
            },
            max_simulation_time: Duration::from_secs(10),
        };

        let expected_output = r#"Running telemetry client only
Metrics configs: ["metrics_config_1.textproto"]
Output directory: /foo/bar
Writing both binary reports and human-readable report summary.
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
                    sim_actions_path: PathBuf::from("sim_actions.textproto").into(),
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
Output directory: /foo/bar
Simulation actions config: sim_actions.textproto
Writing only human-readable report summary, not writing binary reports.
Publisher configs: ["publisher_config_1.textproto", "publisher_config_2.textproto"]
Application will finish after 00:00:10 or 5 returned reports, whichever comes first.
"#;
        let actual_output = format!("{args}");
        assert_eq!(actual_output, expected_output);
    }

    parameterized_test!(
        #[test]
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
