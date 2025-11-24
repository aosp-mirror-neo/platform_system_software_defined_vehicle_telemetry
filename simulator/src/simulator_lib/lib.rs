// Copyright 2024 Google LLC

//! Command line tool to simulate the life cycle of a metrics configuration by
//! simulating service publishers with provided replay-data.

mod cli;
mod io;
mod metrics_config_info;
mod metrics_configs_tracker;
mod output_strings;
mod protobuf_parsing;
mod publisher_config;
mod publisher_config_parser;
mod simulation_publisher;
mod simulator_errors;
mod telemetry_service_handler;
mod util;

use anyhow::{Context, Error};
use futures::{future, stream, StreamExt, TryStreamExt};
use log::{error, info};
use protobuf::well_known_types::duration::Duration as DurationProto;
use sdv_telemetry_rust_wrapper::{ReportInfo, TelemetryServiceHolder, TelemetryServiceStatus};
use sdv_telemetry_sample_utils::proto_reader;
use sdv_telemetry_simulator_proto::simulation_actions::{
    metrics_config_action::ActionType as MetricsConfigActionTypeProto,
    simulation_action::Action as SimulationActionTypeProto,
    MetricsConfigAction as MetricsConfigActionProto, SimulationAction as SimulationActionProto,
    SimulationActions as SimulationActionsProto,
};
use sdv_telemetry_simulator_publishing_strategy::PublishingStrategyFactory;
use sdv_telemetry_utils::{
    collection_extension::HashMapExtension, duration_utils::try_into_duration,
};
use std::{
    collections::HashMap, collections::HashSet, ops::DerefMut, path::Path, path::PathBuf, pin::pin,
    sync::Arc, time::Duration,
};
use tokio::time::{error::Elapsed, interval, sleep_until, timeout, Instant, MissedTickBehavior};
use tokio::{sync::mpsc::UnboundedReceiver, sync::Mutex as AsyncMutex, task};
use tokio_stream::wrappers::IntervalStream;
use tokio_util::sync::CancellationToken;

use crate::{
    cli::Commands,
    io::create_output_directory,
    io::REPORTS_DIR_NAME,
    metrics_configs_tracker::MetricsConfigsTracker,
    publisher_config_parser::parse_publisher_config,
    simulation_publisher::SimulationPublisher,
    simulator_errors::SimulatorError,
    telemetry_service_handler::{
        get_report, get_telemetry_service_instance, init_telemetry_service,
    },
};

pub use cli::CliArgs;

/// run is the main entrypoint of the simulator.
pub async fn run(
    cli_args: CliArgs,
    publishing_strategy_factories: &mut [Box<dyn PublishingStrategyFactory>],
) -> anyhow::Result<()> {
    info!("Running simulator with args:\n{cli_args}");

    binder::ProcessState::start_thread_pool();

    match cli_args.command {
        Commands::TelemetryClient { telemetry_client_args } => {
            run_telemetry_client(
                None,
                &telemetry_client_args.metrics_config_paths,
                telemetry_client_args.sim_actions_path,
                telemetry_client_args.no_binary_reports,
                telemetry_client_args.max_report_count,
                cli_args.max_simulation_time,
                &telemetry_client_args.output_directory,
                publishing_strategy_factories,
            )
            .await?;
        }
        Commands::SimulationPublishers { simulation_publisher_args } => {
            // Publishers will unregister when dropped.
            let mut publishers = parse_and_register_simulation_publishers(
                &simulation_publisher_args.publisher_config_paths,
                publishing_strategy_factories,
            )
            .await?;

            // Ignore a timeout error.
            let _: Result<(), Elapsed> = timeout(
                cli_args.max_simulation_time,
                run_simulation_publishers(publishers.iter_mut().map(DerefMut::deref_mut)),
            )
            .await;
        }
        Commands::FullSimulation { telemetry_client_args, simulation_publisher_args } => {
            run_telemetry_client(
                Some(simulation_publisher_args.publisher_config_paths),
                &telemetry_client_args.metrics_config_paths,
                telemetry_client_args.sim_actions_path,
                telemetry_client_args.no_binary_reports,
                telemetry_client_args.max_report_count,
                cli_args.max_simulation_time,
                &telemetry_client_args.output_directory,
                publishing_strategy_factories,
            )
            .await?;
        }
    }

    Ok(())
}

#[expect(clippy::too_many_arguments, reason = "This just needs a lot of arguments.")]
async fn run_telemetry_client(
    publisher_config_paths: Option<Vec<PathBuf>>,
    metrics_config_paths: &Vec<PathBuf>,
    sim_actions_path: Option<PathBuf>,
    no_binary_reports: bool,
    max_report_count: Option<u64>,
    max_simulation_time: Duration,
    output_directory: &Path,
    publishing_strategy_factories: &mut [Box<dyn PublishingStrategyFactory>],
) -> anyhow::Result<()> {
    let reports_directory = output_directory.join(REPORTS_DIR_NAME);
    create_output_directory(
        output_directory,
        (!no_binary_reports).then_some(reports_directory.as_path()),
    )?;

    let mut metrics_configs_tracker =
        MetricsConfigsTracker::new(!no_binary_reports, output_directory.to_owned());
    for path in metrics_config_paths {
        metrics_configs_tracker.parse_metrics_config(path)?;
    }

    let telemetry_service = get_telemetry_service_instance()?;
    let (mut report_ready_rx, misc_status_rx) = init_telemetry_service(&telemetry_service).await?;
    handle_status_updates(misc_status_rx);

    let telemetry_service = Arc::new(telemetry_service);
    let metrics_configs_tracker = Arc::new(AsyncMutex::new(metrics_configs_tracker));
    let simulation_actions =
        get_simulation_actions(sim_actions_path, Arc::clone(&metrics_configs_tracker)).await?;
    let actions_task_handle = perform_simulation_actions(
        Arc::clone(&telemetry_service),
        Arc::clone(&metrics_configs_tracker),
        simulation_actions,
    )
    .await;

    // Publishers will unregister when dropped.
    let mut publishers = if let Some(paths) = publisher_config_paths {
        parse_and_register_simulation_publishers(&paths, publishing_strategy_factories).await?
    } else {
        Vec::new()
    };

    let cancellation_token = CancellationToken::new();
    let run_publishers_cancellation_drop_guard = cancellation_token.clone().drop_guard();
    tokio::spawn(async move {
        // Run publishers until the cancellation token is cancelled OR until they have no more data
        // to publish (this can only happen if none of them have `loop_data: true`).
        let _: Option<()> = cancellation_token
            .run_until_cancelled(run_simulation_publishers(
                publishers.iter_mut().map(DerefMut::deref_mut),
            ))
            .await;

        // If publishers finish executing before the cancellation token is cancelled, wait for the
        // cancellation token (i.e. the end of the simulation) before unregistering the publishers.
        cancellation_token.cancelled().await;

        // Unregister (by dropping) the publishers.
        drop(publishers);
    });

    // There are three conditions that stop the simulation, if any one of them is true:
    //
    // 1. The configured maximum number of reports is reached.
    // 2. The configured deadline is reached.
    // 3. All simulator actions have been performed and all associated metrics configs have
    //    finished execution.

    let mut num_reports = 0;
    let deadline = Instant::now() + max_simulation_time;

    // Since there is no API to be notified of when a metrics config finishes execution, we fall
    // back to regularly polling the Telemetry Service for which metrics configs are still active.
    // We poll every 3 seconds, which feels like a good balance between bombarding the Telemetry
    // Service with too many API calls and not waiting unnecessarily long after all configs have
    // finished in the worst case.
    //
    // We use `MissedTickBehavior::Delay`, so that even when a tick is missed, the interval ticks
    // _at most_ every 3 seconds.
    let mut active_config_uuids_changed = pin!({
        let mut i = interval(Duration::from_secs(3));
        i.set_missed_tick_behavior(MissedTickBehavior::Delay);
        IntervalStream::new(i)
            .skip(1) // Discard the first tick that occurs immediately.
            .then(|_| telemetry_service.get_active_metrics_config_uuids())
            .map_ok(HashSet::from_iter)
    });

    let metrics_config_uuids = metrics_configs_tracker.lock().await.get_metrics_config_uuids();
    loop {
        // This check is at the start of the loop, as opposed to after receiving a report, to catch
        // the edge case where `max_report_count` is `Some(0)`.
        if max_report_count.is_some_and(|max_report_count| num_reports >= max_report_count) {
            info!("Simulation max report count reached.");
            break;
        }

        let report_info = tokio::select! {
            biased;
            _ = sleep_until(deadline) => {
                info!("Simulation time limit reached.");
                break;
            },
            Some(active_config_uuids) = active_config_uuids_changed.next() => {
                if actions_task_handle.is_finished()
                    && active_config_uuids?.is_disjoint(&metrics_config_uuids)
                {
                    info!("Simulation stopped because all started metrics configs finished.");
                    break;
                }
                continue;
            },
            report_info = await_next_report(&mut report_ready_rx) => report_info?,
        };

        let report = get_report(&telemetry_service, &report_info.report_uuid).await?;
        metrics_configs_tracker.lock().await.process_new_report(report)?;
        num_reports += 1;
    }

    telemetry_service.remove_all_metrics_configs().await.with_context(|| {
        "Failed to remove all Metrics Configurations after simulation finished."
    })?;

    // Stop and unregister publishers.
    drop(run_publishers_cancellation_drop_guard);

    Ok(())
}

async fn perform_simulation_actions(
    service: Arc<TelemetryServiceHolder>,
    tracker: Arc<AsyncMutex<MetricsConfigsTracker>>,
    actions: SimulationActionsProto,
) -> task::JoinHandle<()> {
    tokio::spawn(async move {
        for simulator_action in actions.actions {
            let Ok(delay) = try_into_duration(&simulator_action.delay) else {
                error!("Invalid delay in simulator action {simulator_action}");
                continue;
            };

            tokio::time::sleep(delay).await;

            let action_result = match simulator_action.action {
                Some(SimulationActionTypeProto::MetricsConfigAction(ref config_action_proto)) => {
                    let uuid = &config_action_proto.config_uuid;

                    match config_action_proto.action_type.unwrap() {
                        MetricsConfigActionTypeProto::ADD => {
                            tracker.lock().await.add_metrics_config(&service, uuid).await
                        }
                        MetricsConfigActionTypeProto::ACTIVATE => {
                            tracker.lock().await.activate_metrics_config(&service, uuid).await
                        }
                        MetricsConfigActionTypeProto::DEACTIVATE => {
                            tracker.lock().await.deactivate_metrics_config(&service, uuid).await
                        }
                        MetricsConfigActionTypeProto::REMOVE => {
                            tracker.lock().await.remove_metrics_config(&service, uuid).await
                        }
                        _ => Err(SimulatorError::Error {
                            error_message: "Metrics config action type is unspecified".to_owned(),
                        }),
                    }
                }
                Some(SimulationActionTypeProto::RemoveAllMetricsConfigsAction(_)) => {
                    tracker.lock().await.remove_all_metrics_configs(&service).await
                }
                _ => Err(SimulatorError::Error {
                    error_message: "Unknown simulator action".to_owned(),
                }),
            };

            if let Err(err) = action_result {
                error!("Failed to process simulator action {simulator_action}: {err:?}");
            }
        }
    })
}

async fn get_simulation_actions(
    config_path: Option<PathBuf>,
    tracker: Arc<AsyncMutex<MetricsConfigsTracker>>,
) -> Result<SimulationActionsProto, SimulatorError> {
    if let Some(path) = config_path {
        let file_name = path
            .file_name()
            .ok_or(SimulatorError::InvalidFilePath { file_path: path.to_owned() })?;
        return proto_reader::read_as_message::<SimulationActionsProto>(&path).map_err(|err| {
            SimulatorError::FailedToParseFile {
                file_name: file_name.to_owned(),
                error_message: err.to_string(),
            }
        });
    }
    // Default simulator actions: add and activate all metrics configs on simulator start
    let actions = tracker
        .lock()
        .await
        .get_metrics_config_uuids()
        .into_iter()
        .flat_map(|config_uuid| {
            [
                SimulationActionProto {
                    delay: Some(DurationProto { seconds: 0, ..Default::default() }).into(),
                    action: Some(SimulationActionTypeProto::MetricsConfigAction(
                        MetricsConfigActionProto {
                            config_uuid: config_uuid.clone(),
                            action_type: MetricsConfigActionTypeProto::ADD.into(),
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                },
                SimulationActionProto {
                    delay: Some(DurationProto { seconds: 0, ..Default::default() }).into(),
                    action: Some(SimulationActionTypeProto::MetricsConfigAction(
                        MetricsConfigActionProto {
                            config_uuid: config_uuid.clone(),
                            action_type: MetricsConfigActionTypeProto::ACTIVATE.into(),
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                },
            ]
        })
        .collect::<Vec<_>>();

    Ok(SimulationActionsProto { actions, ..Default::default() })
}

/// Parses and registers the simulation publishers as configured in the provided config paths.
/// Returns a vector of `SimulationPublisher`s wrapped in `scopeguard::ScopeGuard`, which ensures
/// that the returned publishers will automatically be unregistered when dropped.
async fn parse_and_register_simulation_publishers(
    publisher_config_paths: &Vec<PathBuf>,
    publishing_strategy_factories: &mut [Box<dyn PublishingStrategyFactory>],
) -> anyhow::Result<
    Vec<scopeguard::ScopeGuard<SimulationPublisher, impl FnOnce(SimulationPublisher)>>,
> {
    let publishers =
        parse_simulation_publishers(publisher_config_paths, publishing_strategy_factories).await?;
    register_simulation_publishers(publishers.into_values()).await
}

async fn parse_simulation_publishers(
    publisher_config_paths: &Vec<PathBuf>,
    publishing_strategy_factories: &mut [Box<dyn PublishingStrategyFactory>],
) -> Result<HashMap<String, SimulationPublisher>, Error> {
    let mut publishers = HashMap::new();
    for path in publisher_config_paths {
        let publisher_config = parse_publisher_config(path)?;
        let service_name = publisher_config.service_name.clone();

        let publishing_strategy = {
            let mut found_publishing_strategy = None;
            for publishing_strategy_factory in &mut *publishing_strategy_factories {
                if let Some(publishing_strategy) =
                    publishing_strategy_factory.create(&publisher_config.publishing_strategy).await
                {
                    found_publishing_strategy = Some(publishing_strategy);
                    break;
                }
            }
            found_publishing_strategy
        }
        .ok_or_else(|| SimulatorError::Error {
            error_message: format!(
                "Publishing strategy {:?} not supported.",
                publisher_config.publishing_strategy
            ),
        })??;

        let simulation_publisher = SimulationPublisher::new(publisher_config, publishing_strategy);

        publishers.try_insert_value(service_name.clone(), simulation_publisher).map_err(|_| {
            SimulatorError::Error {
                error_message: format!(
                    "Found more than one publisher config for the service name {service_name}"
                ),
            }
        })?;
    }
    Ok(publishers)
}

/// Registers the provided simulation publishers with the configurable publisher registry. Returns a
/// vector of `SimulationPublisher`s wrapped in `scopeguard::ScopeGuard`, which ensures that the
/// returned publishers will automatically be unregistered when dropped.
async fn register_simulation_publishers(
    publishers: impl IntoIterator<Item = SimulationPublisher>,
) -> Result<Vec<scopeguard::ScopeGuard<SimulationPublisher, impl FnOnce(SimulationPublisher)>>, Error>
{
    stream::iter(publishers.into_iter()).then(|publisher| publisher.register()).try_collect().await
}

async fn run_simulation_publishers<'a>(
    publishers: impl IntoIterator<Item = &'a mut SimulationPublisher>,
) {
    future::join_all(publishers.into_iter().map(|publisher| publisher.publish_data())).await;
}

/// Logs miscellaneous telemetry service status updates with info level.
fn handle_status_updates(mut misc_status_rx: UnboundedReceiver<TelemetryServiceStatus>) {
    task::spawn(async move {
        while let Some(status) = misc_status_rx.recv().await {
            info!("Telemetry service sent status update: {}", status.message);
        }
    });
}

/// Returns when the receiving channel receives a new report.
async fn await_next_report(
    report_ready_tx: &mut UnboundedReceiver<ReportInfo>,
) -> Result<ReportInfo, SimulatorError> {
    report_ready_tx.recv().await.ok_or_else(|| SimulatorError::Error {
        error_message: "Report listener dropped.".to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::assert_that;
    use googletest::matchers::{eq, err, len, ok, pat};
    use sdv_telemetry_test_utils::parameterized_test;
    use tempfile::tempdir;

    parameterized_test!(
        #[tokio::test]
        test_get_simulator_actions,

        |path: Option<PathBuf>, matcher| async move {
            let tempdir = tempdir().unwrap().path().to_owned();
            let tracker = Arc::new(AsyncMutex::new(MetricsConfigsTracker::new(false, tempdir)));

            let result = get_simulation_actions(path, tracker).await;
            assert_that!(result, matcher);
        },

        none: (None, ok(pat!(SimulationActionsProto { actions: len(eq(0)) }))),
        txtpb: (Some("valid_simulation_actions.txtpb".into()), ok(pat!(SimulationActionsProto { actions: len(eq(3)) }))),
        pb: (Some("valid_simulation_actions.pb".into()), ok(pat!(SimulationActionsProto { actions: len(eq(3)) }))),
        invalid: (Some("invalid_simulation_actions.txtpb".into()), err(pat!(SimulatorError::FailedToParseFile { .. }))),
        does_not_exist: (Some("does_not_exist.txtpb".into()), err(pat!(SimulatorError::FailedToParseFile { .. }))),
    );
}
