// Copyright 2024 Google LLC

//! Binary for testing getter configurable publishers.

mod random_integer_publisher;

use log::info;
use protobuf::descriptor::FileDescriptorSet;
use protobuf::{Message, MessageFull};
use sample_publisher_proto::sample_publisher::SampleMessage;
use sdv_configurable_publisher_rust_wrapper::configurable_publisher_registry::{
    ConfigurablePublisherRegistry, ConfigurablePublisherRegistryError,
};
use sdv_configurable_publisher_rust_wrapper::publisher_info::PublisherInfo;
use sdv_telemetry_utils::logging;
use std::sync::Arc;

use random_integer_publisher::RandomIntegerPublisher;

const LOG_TAG: &str = logging::validated_log_tag("TelemetrySamplGetterPub");

#[tokio::main]
async fn main() -> Result<(), ConfigurablePublisherRegistryError> {
    logging::init_logger_for_samples_or_tests(LOG_TAG);

    info!("Starting thread pool");
    binder::ProcessState::start_thread_pool();

    let mut registry = ConfigurablePublisherRegistry::new()?;

    info!("Creating publisher");
    let publisher = RandomIntegerPublisher::new();

    info!("Registering publisher");
    let message_descriptor = SampleMessage::descriptor();
    let file_descriptor_set = FileDescriptorSet {
        file: vec![message_descriptor.file_descriptor_proto().to_owned()],
        ..Default::default()
    };

    let publisher_info = PublisherInfo {
        service_name: "SampleService".to_owned(),
        file_descriptor_set: Some(file_descriptor_set.write_to_bytes().unwrap()),
        message_type_name: format!(".{}", message_descriptor.full_name()),
        supports_get: true,
    };

    registry.register_publisher(
        publisher_info,
        Arc::<random_integer_publisher::RandomIntegerPublisher>::clone(&publisher),
    )?;

    info!("Activating publisher");
    publisher.activate().await;

    Ok(())
}
