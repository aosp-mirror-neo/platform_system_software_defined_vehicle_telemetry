// Copyright 2024 Google LLC. All Rights Reserved.

//! A sample data tunnel publisher that publishes information about a car's speed.

mod data_generator;

use anyhow::anyhow;
use data_generator::SpeedDataGenerator;
use log::{error, info, trace};
use protobuf::{Message, MessageFull};
use sdv_telemetry_dt_wrapper::data_tunnel_publisher::DataTunnelPublisher;
use sdv_telemetry_metadata_proto_rust::publisher_metadata::TelemetryPublisherMetadata;
use sdv_telemetry_utils::logging::{init_logger_for_samples_or_tests, validated_log_tag};
use speed_publisher_proto::speed_publisher;
use speed_publisher_proto::speed_publisher::SpeedData;
use std::{thread, time::Duration};

const LOG_TAG: &str = validated_log_tag("TelemetrySpeedDataPub");

const DT_TOPIC_NAME: &str = "DataTunnelSpeedSamplePublisher";
const DT_QUEUE_SIZE: usize = 2000;
// This must be big enough to fit the biggest possible protobuf message that we can produce, plus a
// varint prefix for the message size. 64 bytes are plenty for this publisher's message.
const DT_MESSAGE_SIZE: usize = 64;

fn main() -> anyhow::Result<()> {
    // `wrapped_main` is used solely to make sure that any errors are logged via `error!` so that
    // they show up in logs.
    wrapped_main().inspect_err(|err| {
        error!("{err:?}");
    })
}

fn create_dt_publisher() -> anyhow::Result<DataTunnelPublisher> {
    let mut publisher_metadata = TelemetryPublisherMetadata::new();
    publisher_metadata.descriptor_protos = vec![speed_publisher::file_descriptor().proto().clone()];
    publisher_metadata.message_type_name = format!(".{}", SpeedData::descriptor().full_name());

    DataTunnelPublisher::register_topic(
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

    let mut data_generator = SpeedDataGenerator::new();
    loop {
        let message = data_generator.generate();
        match dt_publisher.publish_length_delimited_proto(&message) {
            Ok(()) => trace!("Message written to DT: {}", message),
            Err(err) => error!("Failed to write message to DT: {:?}", err),
        }

        thread::sleep(Duration::from_millis(5));
    }
}
