// Copyright 2023 Google LLC

//! Test the rust wrapper for telemetry and DT.
use sample_publisher_proto::sample_publisher;
use sample_publisher_proto::sample_publisher::SampleMessage;
use sdv_proto_reflection_rs::message_metadata::MessageMetadata;
use sdv_telemetry_comms_utils::wait_for_sdv_agents_and_create_context;
use sdv_telemetry_dt_wrapper::data_tunnel_publisher::DataTunnelPublisher;
use sdv_telemetry_utils::logging::{init_logger_for_samples_or_tests, validated_log_tag};

use anyhow::Context;
use log::{error, info};
use protobuf::{Message, MessageFull};
use rand::Rng;
use sdv_comms::id::ServiceFqin;
use sdv_comms::sd::UnitType;
use sdv_comms::ContextRef;
use std::str::FromStr;
use tokio::time::sleep;

const LOG_TAG: &str = validated_log_tag("TelemetrySampleDataPub");

const SERVICE_FQIN: &str = "vm:com.google.sdv.telemetry.samples.DataTunnelPublisherSample/instance";
const UNIT_TYPE: &str = "com.google.sdv.telemetry.samples.DataTunnelPublisherSample#Sample";
const UNIT_NAME: &str = "default";

const MESSAGE_COUNT: u32 = 1024;
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
        file_descriptors: vec![sample_publisher::file_descriptor().proto().clone()],
        message_full_name: format!(".{}", SampleMessage::descriptor().full_name()),
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

    loop {
        // Generate random data and send to subscribers.
        let mut rng = rand::thread_rng();
        let mut message = SampleMessage::new();
        message.sample_data = rng.gen();

        match dt_publisher.publish_length_delimited_proto(&message) {
            Ok(()) => info!("Message written to DT: {}", message),
            Err(err) => error!("Failed to write message to DT: {:?}", err),
        }

        sleep(std::time::Duration::from_secs(3)).await;
    }
}
