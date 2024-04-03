// Copyright 2023 Google LLC. All Rights Reserved.

//! Binary for testing configurable publishers

#![allow(missing_docs)]

mod random_integer_publisher;
mod random_integer_publisher_service;

use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{
    IConfigurablePublisher::BnConfigurablePublisher, IConfigurablePublisherRegistry::IConfigurablePublisherRegistry,
    PublisherInfo::PublisherInfo, IConfigurablePublisherRegistry::BpConfigurablePublisherRegistry
};
use google_sdv_telemetry_configurable_publisher::binder::BinderFeatures;
use sample_publisher_proto::sample_publisher;
use sample_publisher_proto::sample_publisher::SampleMessage;
use crate::random_integer_publisher::RandomIntegerPublisher;
use crate::random_integer_publisher_service::RandomIntegerPublisherService;

use log::info;
use protobuf::{Message, MessageFull};
use std::sync::Arc;
use tokio::sync::Notify;

pub fn connect() -> Result<binder::Strong<dyn IConfigurablePublisherRegistry>, binder::StatusCode> {
    let configurable_publisher_registry_descriptor = format!(
        "{}/default",
        <BpConfigurablePublisherRegistry as IConfigurablePublisherRegistry>::get_descriptor()
    );
    binder::get_interface(&configurable_publisher_registry_descriptor)
}

#[tokio::main]
async fn main() -> Result<(), binder::Status> {
    info!("Starting thread pool");
    binder::ProcessState::start_thread_pool();
    // fetch the binder service
    let service = connect().expect("Failed to connect to IConfigurablePublisherRegistry");

    let file_descriptor = sample_publisher::file_descriptor();
    let file_descriptor_proto = file_descriptor.proto();
    let descriptor = SampleMessage::descriptor();

    info!("Creating publisher");
    let notify_subscribed = Arc::new(Notify::new());
    let publisher = Arc::new(RandomIntegerPublisher::new(notify_subscribed.clone()));

    let publisher_wrapper = RandomIntegerPublisherService { publisher: publisher.clone() };

    let publisher_info = PublisherInfo {
        serviceName: "SampleService".to_string(),
        topicName: "SampleData".to_string(),
        fileDescriptorProto: file_descriptor_proto.write_to_bytes().unwrap(),
        messageTypeName: format!(".{}", descriptor.full_name()),
    };

    let binder_publisher =
        BnConfigurablePublisher::new_binder(publisher_wrapper, BinderFeatures::default());

    info!("Registering publisher");
    let _ = service.registerPublisher(&publisher_info, &binder_publisher);

    info!("Waiting for subscription");
    notify_subscribed.notified().await;

    info!("Publishing data");
    publisher.publish_data_randomly(1000, 1000).await;

    Ok(())
}
