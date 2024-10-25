// Copyright 2024 Google LLC

//! A sample data tunnel publisher that publishes error reports.

mod data_generator;

use anyhow::Context;
use data_generator::ErrorReportDataGenerator;
use error_report_publisher_proto::error_report_publisher;
use error_report_publisher_proto::error_report_publisher::ErrorReportData;
use log::{error, info, trace};
use protobuf::{Message, MessageFull};
use rand::Rng;
use sdv_comms::id::ServiceFqin;
use sdv_comms::sd::UnitType;
use sdv_comms::ContextRef;
use sdv_telemetry_comms_utils::wait_for_sdv_agents_and_create_context;
use sdv_telemetry_dt_wrapper::data_tunnel_publisher::DataTunnelPublisher;
use sdv_telemetry_metadata_proto_rust::publisher_metadata::TelemetryPublisherMetadata;
use sdv_telemetry_utils::logging::{init_logger_for_samples_or_tests, validated_log_tag};
use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;

const LOG_TAG: &str = validated_log_tag("TelemetryErrRptDataPub");

const SERVICE_FQIN: &str =
    "vm:com.google.sdv.telemetry.samples.DataTunnelPublisherErrorReport/instance";
const UNIT_TYPE: &str =
    "com.google.sdv.telemetry.samples.DataTunnelPublisherErrorReport#ErrorReport";
const UNIT_NAME: &str = "default";

const MESSAGE_COUNT: u32 = 2;
// This must be big enough to fit the biggest possible protobuf message that we can produce, plus a
// varint prefix for the message size.
const MESSAGE_SIZE: u32 = 3_500_000;

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
    let mut publisher_metadata = TelemetryPublisherMetadata::new();
    publisher_metadata.descriptor_protos =
        vec![error_report_publisher::file_descriptor().proto().clone()];
    publisher_metadata.message_type_name =
        format!(".{}", ErrorReportData::descriptor().full_name());

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

    let mut data_generator = ErrorReportDataGenerator::new();
    loop {
        let message = data_generator.generate();
        match dt_publisher.publish_length_delimited_proto(&message) {
            Ok(()) => trace!("Error report with title {} written to DT", message.error_info.title),
            Err(err) => error!("Failed to write message to DT: {:?}", err),
        }

        let mut rng = rand::thread_rng();
        sleep(rng.gen_range(Duration::from_secs(1)..=Duration::from_secs(3))).await;
    }
}
