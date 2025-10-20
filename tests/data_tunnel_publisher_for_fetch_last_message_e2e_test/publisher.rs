// Copyright 2025 Google LLC

//! A Data Tunnel publisher used in the `SdvE2ETelemetryDtFetchLastMessageTest`.
//! TODO: b/420890689 - Use the Simulator instead, once it supports simulation of DT publishers.

use anyhow::{bail, Context};
use log::error;
use protobuf::{Message, MessageFull};
use sdv_comms::id::ServiceFqin;
use sdv_comms::sd::UnitType;
use sdv_comms::ContextRef;
use sdv_proto_reflection_rs::message_metadata::MessageMetadata;
use sdv_telemetry_comms_utils::wait_for_sdv_agents_and_create_context;
use sdv_telemetry_dt_publisher_for_fetch_last_message_e2e_test_proto::publisher::Data;
use sdv_telemetry_dt_wrapper::data_tunnel_publisher::DataTunnelPublisher;
use sdv_telemetry_utils::logging::{init_logger_for_samples_or_tests, validated_log_tag};
use std::env;
use std::process::ExitCode;
use std::str::FromStr;
use std::thread::sleep;
use std::time::Duration;

const LOG_TAG: &str = validated_log_tag("TelemetryTestDtFlmPub");

const SERVICE_FQIN: &str =
    "vm:com.google.sdv.telemetry.tests.DtPublisherForFetchLastMessageE2ETest/instance";
const UNIT_TYPE: &str =
    "com.google.sdv.telemetry.tests.DtPublisherForFetchLastMessageE2ETest#Value";
const UNIT_NAME: &str = "default";

const MESSAGE_COUNT: u32 = 10;
const MESSAGE_SIZE: u32 = 32;

#[tokio::main]
async fn main() -> ExitCode {
    init_logger_for_samples_or_tests(LOG_TAG);

    wrapped_main()
        .await
        .inspect_err(|err| {
            eprintln!("{err:#?}");
            error!("{err:#?}");
        })
        .map_or_else(|_| ExitCode::FAILURE, |_| ExitCode::SUCCESS)
}

async fn create_dt_publisher(
    ctx: ContextRef,
    unit_type: &UnitType,
    unit_name: &str,
) -> anyhow::Result<DataTunnelPublisher> {
    let publisher_metadata = MessageMetadata {
        file_descriptors: vec![Data::descriptor().file_descriptor_proto().clone()],
        message_full_name: format!(".{}", Data::descriptor().full_name()),
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
    let args = env::args()
        // Skip the first entry, which is the program name.
        .skip(1)
        .collect::<Vec<_>>();

    let [ref value] = args[..] else {
        bail!(
            "Expected exactly one argument, which is the u32 value of the message to be published, but got: {}",
            args.join(" ")
        );
    };
    let value = value.parse::<u32>().with_context(|| "Failed to parse value as u32.")?;

    let fqin = ServiceFqin::from_str(SERVICE_FQIN)
        .with_context(|| format!("Failed to parse FQIN '{SERVICE_FQIN}'."))?;
    let ctx = wait_for_sdv_agents_and_create_context(fqin).await;

    let unit_type = UnitType::from_str(UNIT_TYPE)
        .with_context(|| format!("Failed to parse unit type '{UNIT_TYPE}'"))?;
    let dt_publisher = create_dt_publisher(ctx, &unit_type, UNIT_NAME).await?;

    let message = Data { value, ..Default::default() };
    dt_publisher
        .publish_length_delimited_proto(&message)
        .with_context(|| format!("Failed to write message: {message}"))?;

    // Sleep forever and never return. This is necessary, so that the DT topic remains available.
    sleep(Duration::MAX);
    Ok(())
}
