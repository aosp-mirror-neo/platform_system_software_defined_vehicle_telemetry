// Copyright 2024 Google LLC

//! A sample data tunnel publisher that publishes information about a car's tire pressure.

mod data_generator;

use anyhow::Context;
use data_generator::TirePressureDataGenerator;
use log::{error, info, trace};
use protobuf::{Message, MessageFull};
use sdv_comms::id::ServiceFqin;
use sdv_comms::sd::UnitType;
use sdv_comms::ContextRef;
use sdv_proto_reflection_rs::message_metadata::MessageMetadata;
use sdv_telemetry_comms_utils::wait_for_sdv_agents_and_create_context;
use sdv_telemetry_dt_wrapper::data_tunnel_publisher::DataTunnelPublisher;
use sdv_telemetry_utils::logging::{init_logger_for_samples_or_tests, validated_log_tag};
use std::str::FromStr;
use std::time::Duration;
use tire_pressure_publisher_proto::tire_pressure_publisher;
use tire_pressure_publisher_proto::tire_pressure_publisher::TirePressureData;
use tokio::time::sleep;

const LOG_TAG: &str = validated_log_tag("TelemetryTrPrsDataPub");

const SERVICE_FQIN: &str =
    "vm:com.google.sdv.telemetry.samples.DataTunnelPublisherTirePressure/instance";
const UNIT_TYPE: &str =
    "com.google.sdv.telemetry.samples.DataTunnelPublisherTirePressure#TirePressure";
const UNIT_NAME: &str = "default";

const MESSAGE_COUNT: u32 = 2000;
// This must be big enough to fit the biggest possible protobuf message that we can produce, plus a
// varint prefix for the message size. 64 bytes are plenty for this publisher's message.
const MESSAGE_SIZE: u32 = 64;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // `wrapped_main` is used solely to make sure that any errors are logged via `error!` so that
    // they show up in logs.
    wrapped_main().await.inspect_err(|err| {
        error!("{err:?}");
    })
}

async fn create_dt_publisher(
    ctx: ContextRef,
    unit_type: &UnitType,
    unit_name: &str,
) -> anyhow::Result<DataTunnelPublisher> {
    let publisher_metadata = MessageMetadata {
        file_descriptors: vec![tire_pressure_publisher::file_descriptor().proto().clone()],
        message_full_name: format!(".{}", TirePressureData::descriptor().full_name()),
        ..Default::default()
    };

    DataTunnelPublisher::register_topic(
        ctx,
        unit_type,
        unit_name,
        MESSAGE_COUNT,
        MESSAGE_SIZE,
        publisher_metadata.write_to_bytes()?,
    )
    .await
    .with_context(|| format!("Failed to register data tunnel publisher with unit type '{unit_type}' and unit name '{unit_name}'."))
}

async fn wrapped_main() -> anyhow::Result<()> {
    init_logger_for_samples_or_tests(LOG_TAG);

    let fqin = ServiceFqin::from_str(SERVICE_FQIN)
        .with_context(|| format!("Failed to parse FQIN '{SERVICE_FQIN}'."))?;
    let ctx = wait_for_sdv_agents_and_create_context(fqin).await;

    info!("Creating publisher");
    let unit_type = UnitType::from_str(UNIT_TYPE)
        .with_context(|| format!("Failed to parse unit type '{UNIT_TYPE}'"))?;
    let dt_publisher = create_dt_publisher(ctx, &unit_type, UNIT_NAME).await?;

    let mut data_generator = TirePressureDataGenerator::new();
    loop {
        let message = data_generator.generate();
        match dt_publisher.publish_length_delimited_proto(&message) {
            Ok(()) => trace!("Message written to DT: {message}"),
            Err(err) => error!("Failed to write message to DT: {err:?}"),
        }

        sleep(Duration::from_millis(200)).await;
    }
}
