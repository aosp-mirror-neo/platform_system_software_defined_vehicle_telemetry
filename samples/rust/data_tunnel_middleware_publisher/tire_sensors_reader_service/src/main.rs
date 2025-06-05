// Copyright 2023 Google LLC

//! A sample service that publishes information about a car's tire pressure.

use sdv_mw_rs_com_sdv_telemetry_sample_tire_sensors_reader::{publisher, TireSensorsReader};
use sdv_telemetry_comms_utils::wait_for_sdv_agents_and_create_context;
use sdv_telemetry_sample_middleware_proto::tire::{TirePressure, TireTemperature, VehicleSpeed};

use anyhow::Context;
use log::{error, trace};
use rand::Rng;
use sdv_comms::id::ServiceFqin;
use sdv_mw_rs_sdv_comms::SdvComms;
use sdv_telemetry_utils::logging::{init_logger_for_samples_or_tests, validated_log_tag};
use std::str::FromStr;
use std::sync::Arc;
use std::time;
use tokio::time::sleep;

const LOG_TAG: &str = validated_log_tag("TelemetryMddlWrDataPub");

const SERVICE_FQIN: &str =
    "vm:com.google.sdv.telemetry.samples.DataTunnelPublisherMiddleware/instance";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // `wrapped_main` is used solely to make sure that any errors are logged via `error!` so that
    // they show up in logs.
    wrapped_main().await.inspect_err(|err| {
        error!("{err:?}");
    })
}

async fn wrapped_main() -> anyhow::Result<()> {
    init_logger_for_samples_or_tests(LOG_TAG);

    let fqin = ServiceFqin::from_str(SERVICE_FQIN)
        .with_context(|| format!("Failed to parse FQIN '{SERVICE_FQIN}'."))?;
    let ctx = wait_for_sdv_agents_and_create_context(fqin).await;

    let mut service = TireSensorsReader::new(Arc::new(SdvComms { context: ctx }))
        .await
        .with_context(|| "Failed to start service")?;

    let tire_pressure_publishers = publisher::Variant::<TirePressure>::ALL_VARIANTS
        .iter()
        .map(|variant| (variant, service.take_publisher(variant).unwrap()))
        .collect::<Vec<_>>();
    let tire_temperature_publishers = publisher::Variant::<TireTemperature>::ALL_VARIANTS
        .iter()
        .map(|variant| (variant, service.take_publisher(variant).unwrap()))
        .collect::<Vec<_>>();
    let vehicle_speed_publisher =
        service.take_publisher(publisher::Variant::<VehicleSpeed>::UNIQUE).unwrap();

    let mut rng = rand::thread_rng();
    loop {
        for (variant, publisher) in &tire_pressure_publishers {
            let message = TirePressure { pressure: rng.random_range(0..100), ..Default::default() };
            if let Err(err) = publisher.publish(&message) {
                error!("Failed to publish TirePressure for variant '{variant:?}': {err:?}");
            } else {
                trace!(
                    "Published TirePressure for variant '{variant:?}': pressure: {}",
                    message.pressure
                );
            }
        }

        for (variant, publisher) in &tire_temperature_publishers {
            let message =
                TireTemperature { temperature: rng.random_range(0..100), ..Default::default() };
            if let Err(err) = publisher.publish(&message) {
                error!("Failed to publish TireTemperature for variant '{variant:?}' {err:?}");
            } else {
                trace!(
                    "Published TireTemperature for variant '{variant:?}': temperature: {}",
                    message.temperature
                );
            }
        }

        let message = VehicleSpeed { speed: rng.random_range(0.0..150.0), ..Default::default() };
        if let Err(err) = vehicle_speed_publisher.publish(&message) {
            error!("Failed to publish VehicleSpeed: {err:?}");
        } else {
            trace!("Published VehicleSpeed: {}", message.speed);
        }

        sleep(time::Duration::from_secs(1)).await;
    }
}
