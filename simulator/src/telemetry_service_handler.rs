// Copyright 2024 Google LLC

use sdv_telemetry_proto_rust::metrics_configuration::MetricsReport;
use sdv_telemetry_rust_wrapper::{TelemetryServiceHolder, TelemetryServiceStatus};
use std::{future::Future, time::Duration};
use tokio::{
    sync::{mpsc, mpsc::UnboundedReceiver},
    time::timeout,
};

use crate::SimulatorError;

pub const DEFAULT_MAX_TIMEOUT_DURATION: Duration = Duration::from_secs(10);

/// Returns an error if the provided function does not complete within the
/// specified duration. Only works for non-blocking functions.
async fn run_with_timeout<R>(
    f: impl Future<Output = R>,
    duration: Duration,
) -> Result<R, SimulatorError> {
    timeout(duration, f).await.map_err(|_| SimulatorError::Error {
        error_message: "Telemetry service timed out".to_owned(),
    })
}

pub fn get_telemetry_service_instance() -> Result<TelemetryServiceHolder, SimulatorError> {
    TelemetryServiceHolder::get().map_err(|error| SimulatorError::Error {
        error_message: format!("Failed to connect to telemetry service: {error}"),
    })
}

/// Initializes the telemetry service and returns the listener channels for
/// "report ready" and "misc status" updates. Times out after 10 seconds if the
/// telemetry service does not respond.
pub async fn get_telemetry_service_listeners(
    telemetry_service: &TelemetryServiceHolder,
) -> Result<(UnboundedReceiver<String>, UnboundedReceiver<TelemetryServiceStatus>), SimulatorError>
{
    let (report_ready_tx, report_ready_rx) = mpsc::unbounded_channel::<String>();
    let (misc_status_tx, misc_status_rx) = mpsc::unbounded_channel::<TelemetryServiceStatus>();

    let misc_status_callback = move |status| {
        let _ = misc_status_tx.send(status);
    };

    let report_ready_callback = move |report_uuid: &str| {
        let _ = report_ready_tx.send(report_uuid.to_owned());
    };

    run_with_timeout(
        telemetry_service.init(misc_status_callback, report_ready_callback),
        DEFAULT_MAX_TIMEOUT_DURATION,
    )
    .await?
    .map_err(|error| SimulatorError::Error {
        error_message: format!("Failed to initiate telemetry service: \"{}\"", error),
    })?;

    Ok((report_ready_rx, misc_status_rx))
}

/// Retrieves a report with the gived uuid from the telemetry service.
pub async fn get_report(
    telemetry_service: &TelemetryServiceHolder,
    report_uuid: &str,
) -> Result<MetricsReport, SimulatorError> {
    telemetry_service.get_metrics_report(report_uuid).await.map_err(|error| SimulatorError::Error {
        error_message: format!(
            "Failed to read report {report_uuid} from telemetry service: \"{error}\""
        ),
    })
}

/// Calls the telemetry service API to add a serialized metrics config. Checks
/// the returned UUID against the expected uuid.
pub async fn add_metrics_config(
    telemetry_service: &TelemetryServiceHolder,
    expected_uuid: &str,
    serialized_config: &[u8],
    file_name: String,
) -> Result<(), SimulatorError> {
    let uuid = telemetry_service
        .add_metrics_config(serialized_config)
        .await
        .map_err(|error| SimulatorError::FailedToParseMetricsConfig { file_name, error })?;

    if uuid != *expected_uuid {
        return Err(SimulatorError::Error {
            error_message: format!("Telemetry service returned different UUID from serialized metrics config than expected. Expected: \"{expected_uuid}\", Received: \"{uuid}\""),
        });
    }
    Ok(())
}

/// Calls the telemetry service API to activate a metrics config with the given
/// uuid.
pub async fn activate_metrics_config(
    telemetry_service: &TelemetryServiceHolder,
    uuid: &str,
    file_name: &str,
) -> Result<(), SimulatorError> {
    telemetry_service.activate_metrics_config(uuid).await.map_err(|error| {
        SimulatorError::FailedToActivateMetricsConfig { file_name: file_name.to_owned(), error }
    })?;
    Ok(())
}
