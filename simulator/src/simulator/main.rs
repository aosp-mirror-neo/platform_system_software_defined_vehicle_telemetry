// Copyright 2025 Google LLC

//! Command line tool to simulate the life cycle of a metrics configuration by simulating service
//! publishers with provided replay-data.

use log::error;
use sdv_telemetry_simulator_lib::run;
use sdv_telemetry_utils::logging::validated_log_tag;

const LOG_TAG: &str = validated_log_tag("SdvTelemetrySimulator");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    sdv_log::init_logger(LOG_TAG)?;
    sdv_tracing::init_tracing();

    run().await.inspect_err(|error| {
        error!("Simulator failed with error: {error:?}");
    })
}
