// Copyright 2024 Google LLC

//! Binary for testing getter configurable publishers.

mod random_integer_publisher;
mod random_integer_publisher_service;

use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisherRegistry::{IConfigurablePublisherRegistry, BpConfigurablePublisherRegistry};
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisher::BnConfigurablePublisher;
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::PublisherInfo::PublisherInfo;
use log::info;
use protobuf::{Message, MessageFull};
use sample_publisher_proto::sample_publisher::{self, SampleMessage};
use sdv_telemetry_utils::logging;
use std::sync::Arc;

use random_integer_publisher::RandomIntegerPublisher;
use random_integer_publisher_service::RandomIntegerPublisherService;

const LOG_TAG: &str = logging::validated_log_tag("TelemetrySamplGetterPub");

/// Connects to the Telemetry Configurable Publisher Registry.
pub fn connect() -> Result<binder::Strong<dyn IConfigurablePublisherRegistry>, binder::StatusCode> {
    let configurable_publisher_registry_descriptor = format!(
        "{}/default",
        <BpConfigurablePublisherRegistry as IConfigurablePublisherRegistry>::get_descriptor()
    );
    binder::get_interface(&configurable_publisher_registry_descriptor)
}

#[tokio::main]
async fn main() -> Result<(), binder::Status> {
    logging::init_logger_for_samples_or_tests(LOG_TAG);

    info!("Starting thread pool");
    binder::ProcessState::start_thread_pool();

    let service = connect().expect("Failed to connect to IConfigurablePublisherRegistry");

    info!("Creating publisher");
    let publisher = Arc::new(RandomIntegerPublisher::new());
    let publisher_wrapper = RandomIntegerPublisherService::new(Arc::clone(&publisher));
    let binder_publisher =
        BnConfigurablePublisher::new_binder(publisher_wrapper, binder::BinderFeatures::default());

    info!("Registering publisher");
    let file_descriptor = sample_publisher::file_descriptor();
    let file_descriptor_proto = file_descriptor.proto();
    let message_descriptor = SampleMessage::descriptor();
    let publisher_info = PublisherInfo {
        serviceName: "SampleService".to_owned(),
        fileDescriptorProto: file_descriptor_proto.write_to_bytes().unwrap(),
        messageTypeName: format!(".{}", message_descriptor.full_name()),
        supportsGet: true,
    };

    service.registerPublisher(&publisher_info, &binder_publisher)?;

    info!("Activating publisher");
    publisher.activate().await;

    Ok(())
}
