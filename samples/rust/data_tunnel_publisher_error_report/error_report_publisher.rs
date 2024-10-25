// Copyright 2024 Google LLC

//! A sample data tunnel publisher that publishes error reports.

mod data_generator;

use anyhow::anyhow;
use data_generator::ErrorReportDataGenerator;
use error_report_publisher_proto::error_report_publisher;
use error_report_publisher_proto::error_report_publisher::ErrorReportData;
use log::{error, info, trace};
use protobuf::{Message, MessageFull};
use rand::Rng;
use sdv_telemetry_dt_wrapper::data_tunnel_publisher::DeprecatedDataTunnelPublisher;
use sdv_telemetry_metadata_proto_rust::publisher_metadata::TelemetryPublisherMetadata;
use sdv_telemetry_utils::logging::{init_logger_for_samples_or_tests, validated_log_tag};
use std::{thread, time::Duration};

const LOG_TAG: &str = validated_log_tag("TelemetryErrRptDataPub");

const DT_TOPIC_NAME: &str = "DataTunnelErrorReportSamplePublisher";
const DT_QUEUE_SIZE: usize = 2;
// This must be big enough to fit the biggest possible protobuf message that we can produce, plus a
// varint prefix for the message size.
const DT_MESSAGE_SIZE: usize = 3_500_000;

fn main() -> anyhow::Result<()> {
    // `wrapped_main` is used solely to make sure that any errors are logged via `error!` so that
    // they show up in logs.
    wrapped_main().inspect_err(|err| {
        error!("{err:?}");
    })
}

fn create_dt_publisher() -> anyhow::Result<DeprecatedDataTunnelPublisher> {
    let mut publisher_metadata = TelemetryPublisherMetadata::new();
    publisher_metadata.descriptor_protos =
        vec![error_report_publisher::file_descriptor().proto().clone()];
    publisher_metadata.message_type_name =
        format!(".{}", ErrorReportData::descriptor().full_name());

    DeprecatedDataTunnelPublisher::register_topic(
        DT_TOPIC_NAME,
        DT_QUEUE_SIZE,
        DT_MESSAGE_SIZE,
        publisher_metadata.write_to_bytes()?,
    )
    .map_err(|err| anyhow!("Failed to register data tunnel topic {DT_TOPIC_NAME}: {err:?}"))
}

fn wrapped_main() -> anyhow::Result<()> {
    init_logger_for_samples_or_tests(LOG_TAG);

    info!("Creating publisher");
    let dt_publisher = create_dt_publisher()?;

    let mut data_generator = ErrorReportDataGenerator::new();
    loop {
        let message = data_generator.generate();
        match dt_publisher.publish_length_delimited_proto(&message) {
            Ok(()) => trace!("Error report with title {} written to DT", message.error_info.title),
            Err(err) => error!("Failed to write message to DT: {:?}", err),
        }

        let mut rng = rand::thread_rng();
        thread::sleep(rng.gen_range(Duration::from_secs(1)..=Duration::from_secs(3)));
    }
}
