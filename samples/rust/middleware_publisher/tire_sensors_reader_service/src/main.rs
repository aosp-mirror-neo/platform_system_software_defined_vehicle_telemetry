// Copyright 2023 Google LLC

//! A sample service that publishes information about a car's tire pressure.

mod rpc;

use anyhow::Context;
use log::{error, trace};
use rand::Rng;
use sdv_comms::id::ServiceFqin;
use sdv_mw_rs_com_sdv_telemetry_sample_tire_sensors_reader::{publisher, TireSensorsReader};
use sdv_mw_rs_sdv_comms::SdvComms;
use sdv_telemetry_comms_utils::wait_for_sdv_agents_and_create_context;
use sdv_telemetry_sample_middleware_proto::tire::{
    Location, TirePressure, TireTemperature, VehicleSpeed,
};
use sdv_telemetry_utils::logging::{init_logger_for_samples_or_tests, validated_log_tag};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time;
use tokio::time::sleep;

const LOG_TAG: &str = validated_log_tag("TelemetryMddlWrPub");

const SERVICE_FQIN: &str = "vm:com.google.sdv.telemetry.samples.MiddlewarePublisher/instance";

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

    let rpc_server = Arc::new(rpc::TireStatusServer::new());

    let mut service = TireSensorsReader::new(
        Arc::new(SdvComms { context: ctx }),
        (Arc::clone(&rpc_server) as Arc<dyn com_sdv_telemetry_sample_tire_status_rpc::Interface>,),
    )
    .await
    .with_context(|| "Failed to start service")?;

    let tire_pressure_publishers = [
        (Location::FRONT_LEFT, publisher::Variant::<TirePressure>::FRONT_LEFT),
        (Location::FRONT_RIGHT, publisher::Variant::<TirePressure>::FRONT_RIGHT),
        (Location::REAR_LEFT, publisher::Variant::<TirePressure>::REAR_LEFT),
        (Location::REAR_RIGHT, publisher::Variant::<TirePressure>::REAR_RIGHT),
    ]
    .into_iter()
    .map(|(location, variant)| (location, service.take_publisher(variant).unwrap()))
    .collect::<HashMap<_, _>>();
    let tire_temperature_publishers = publisher::Variant::<TireTemperature>::ALL_VARIANTS
        .iter()
        .map(|variant| (variant, service.take_publisher(variant).unwrap()))
        .collect::<Vec<_>>();
    let vehicle_speed_publisher =
        service.take_publisher(publisher::Variant::<VehicleSpeed>::UNIQUE).unwrap();

    let mut rng = rand::thread_rng();
    loop {
        for (location, publisher) in &tire_pressure_publishers {
            let pressure = rng.random_range(0..100);

            let message = TirePressure { pressure, ..Default::default() };
            if let Err(err) = publisher.publish(&message) {
                error!("Failed to publish TirePressure for tire '{location:?}': {err:?}",);
            } else {
                trace!(
                    "Published TirePressure for tire '{location:?}': pressure: {}",
                    message.pressure
                );
            }

            rpc_server.set_pressure(location, pressure);
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
