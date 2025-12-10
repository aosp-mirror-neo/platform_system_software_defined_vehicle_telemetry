// Copyright 2023 Google LLC

//! Binary for testing configurable publishers

mod random_integer_publisher;
mod random_integer_publisher_service;

use binder::BinderFeatures;
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{
    IConfigurablePublisher::BnConfigurablePublisher, IConfigurablePublisherRegistry::IConfigurablePublisherRegistry,
    PublisherInfo::PublisherInfo, IConfigurablePublisherRegistry::BpConfigurablePublisherRegistry
};
use log::info;
use protobuf::descriptor::FileDescriptorSet;
use protobuf::{Message, MessageFull};
use sdv_telemetry_utils::logging::{init_logger_for_samples_or_tests, validated_log_tag};
use sample_publisher_proto::sample_publisher::SampleMessage;
use std::sync::Arc;

use random_integer_publisher::RandomIntegerPublisher;
use random_integer_publisher_service::RandomIntegerPublisherService;

const LOG_TAG: &str = validated_log_tag("TelemetrySamplConfigPub");

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
    init_logger_for_samples_or_tests(LOG_TAG);

    info!("Starting thread pool");
    binder::ProcessState::start_thread_pool();
    // fetch the binder service
    let service = connect().expect("Failed to connect to IConfigurablePublisherRegistry");

    info!("Creating publisher");
    let publisher = Arc::new(RandomIntegerPublisher {});
    let publisher_wrapper = RandomIntegerPublisherService { publisher: Arc::clone(&publisher) };
    let binder_publisher =
        BnConfigurablePublisher::new_binder(publisher_wrapper, BinderFeatures::default());

    info!("Registering publisher");
    let message_descriptor = SampleMessage::descriptor();
    let file_descriptor_set = FileDescriptorSet {
        file: vec![message_descriptor.file_descriptor_proto().to_owned()],
        ..Default::default()
    };

    let publisher_info = PublisherInfo {
        serviceName: "SampleService".to_owned(),
        fileDescriptorSet: Some(file_descriptor_set.write_to_bytes().unwrap()),
        messageTypeName: format!(".{}", message_descriptor.full_name()),
        supportsGet: false,
        // Deprecated
        fileDescriptorProto: None,
    };

    service.registerPublisher(&publisher_info, &binder_publisher)?;

    info!("Waiting for subscription");
    tokio::task::block_in_place(binder::ProcessState::join_thread_pool);

    Ok(())
}
