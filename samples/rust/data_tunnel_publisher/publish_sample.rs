// Copyright 2023 Google LLC. All Rights Reserved.

//! Test the rust wrapper for telemetry and DT.
use sample_publisher_proto::sample_publisher::SampleMessage;
use sdv_telemetry_dt_wrapper::register_topic;
use std::thread;

use log::info;
use protobuf::Message;
use rand::Rng;

// Hardcoded topic name and msg size for testing
const TOPIC_NAME: &str = "DataTunnelTopic";
const TOPIC_MSG_SIZE: usize = 64;

fn main() {
    info!("Creating publisher");
    let result = register_topic(TOPIC_NAME, 1024, TOPIC_MSG_SIZE);

    let publisher = match result {
        Ok(publisher) => publisher,
        Err(result) => {
            println!("Could not register topic, result={:?}", result);
            return;
        }
    };

    loop {
        // Generate random data and send to subscribers.
        let mut rng = rand::thread_rng();
        let mut message = SampleMessage::new();
        message.sample_data = rng.gen(); // Generates a random i32;

        let mut raw_message: Vec<u8> = Vec::with_capacity(TOPIC_MSG_SIZE);
        // Prepend message length to be read
        let _ = message.write_length_delimited_to_vec(&mut raw_message);

        let result = publisher.publish(&raw_message);
        match result {
            Ok(()) => println!("Message written to DT {} of size: {}", message, raw_message.len()),
            _ => println!("Failed to write message {:?}", result),
        }

        thread::sleep(std::time::Duration::from_millis(3000));
    }
}
