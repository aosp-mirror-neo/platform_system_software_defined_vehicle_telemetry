// Copyright 2023 Google LLC. All Rights Reserved.

//! Test the rust wrapper for telemetry and DT.
use sample_publisher_proto::sample_publisher;
use sample_publisher_proto::sample_publisher::SampleMessage;
use sdv_telemetry_dt_wrapper::data_tunnel_publisher::DataTunnelPublisher;
use sdvtelemetry_metadata_proto_rust::publisher_metadata::TelemetryPublisherMetadata;
use std::thread;

use log::{error, info, LevelFilter};
use protobuf::{Message, MessageFull};
use rand::Rng;

const LOG_TAG: &str = "telemetry_dt_publisher";
const _: () = assert!(
    LOG_TAG.len() <= 23,
    "LOG_TAG should be less than or equal to 23 characters, as it will otherwise be truncated by adb."
);

// Hardcoded topic name and msg size for testing
const TOPIC_NAME: &str = "DataTunnelSampleService";
const TOPIC_MSG_SIZE: usize = 64;

fn main() {
    logger::init(
        logger::Config::default().with_tag_on_device(LOG_TAG).with_max_level(LevelFilter::Trace),
    );
    info!("Creating publisher");

    let mut publisher_metadata = TelemetryPublisherMetadata::new();
    publisher_metadata.descriptor_protos =
        vec![sample_publisher::file_descriptor().proto().clone()];
    publisher_metadata.message_type_name = format!(".{}", SampleMessage::descriptor().full_name());

    let maybe_dt_publisher = DataTunnelPublisher::register_topic(
        TOPIC_NAME,
        1024,
        TOPIC_MSG_SIZE,
        publisher_metadata.write_to_bytes().unwrap(),
    );

    let dt_publisher = match maybe_dt_publisher {
        Ok(publisher) => publisher,
        Err(err) => {
            error!("Failed to register topic: {:?}", err);
            return;
        }
    };

    loop {
        // Generate random data and send to subscribers.
        let mut rng = rand::thread_rng();
        let mut message = SampleMessage::new();
        message.sample_data = rng.gen();

        match dt_publisher.publish_length_delimited_proto(&message) {
            Ok(()) => info!("Message written to DT: {}", message),
            Err(err) => error!("Failed to write message to DT: {:?}", err),
        }

        thread::sleep(std::time::Duration::from_secs(3));
    }
}
