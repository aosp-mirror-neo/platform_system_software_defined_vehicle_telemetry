// Copyright 2023 Google LLC

//! Test the rust wrapper for telemetry and DT.
use sample_publisher_proto::sample_publisher;
use sample_publisher_proto::sample_publisher::SampleMessage;
use sdv_telemetry_dt_wrapper::data_tunnel_publisher::DeprecatedDataTunnelPublisher;
use sdv_telemetry_metadata_proto_rust::publisher_metadata::TelemetryPublisherMetadata;
use sdv_telemetry_utils::logging::{init_logger_for_samples_or_tests, validated_log_tag};

use anyhow::Context;
use log::{error, info};
use protobuf::{Message, MessageFull};
use rand::Rng;
use tokio::time::sleep;

const LOG_TAG: &str = validated_log_tag("TelemetrySampleDataPub");

// Hardcoded topic name and msg size for testing
const TOPIC_NAME: &str = "DataTunnelSampleService";
const MESSAGE_COUNT: usize = 1024;
const MESSAGE_SIZE: usize = 64;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // `wrapped_main` is used solely to make sure that any errors are logged via `error!` so that
    // they show up in logs.
    wrapped_main().await.inspect_err(|err| {
        error!("{err:?}");
    })
}

async fn create_dt_publisher() -> anyhow::Result<DeprecatedDataTunnelPublisher> {
    let mut publisher_metadata = TelemetryPublisherMetadata::new();
    publisher_metadata.descriptor_protos =
        vec![sample_publisher::file_descriptor().proto().clone()];
    publisher_metadata.message_type_name = format!(".{}", SampleMessage::descriptor().full_name());

    DeprecatedDataTunnelPublisher::register_topic(
        TOPIC_NAME,
        MESSAGE_COUNT,
        MESSAGE_SIZE,
        publisher_metadata.write_to_bytes().unwrap(),
    )
    .with_context(|| {
        format!("Failed to register data tunnel publisher with topic name '{TOPIC_NAME}'.")
    })
}

async fn wrapped_main() -> anyhow::Result<()> {
    init_logger_for_samples_or_tests(LOG_TAG);

    info!("Creating publisher");
    let dt_publisher = create_dt_publisher().await?;

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
