// Copyright 2023 Google LLC. All Rights Reserved.

//! Binary for testing configurable publishers

#![allow(missing_docs)]

mod random_integer_publisher;
mod random_integer_publisher_service;
mod random_integer_publisher_subscription;

use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::{
    IConfigurablePublisher::BnConfigurablePublisher, IConfigurablePublisherRegistry::IConfigurablePublisherRegistry,
    PublisherInfo::PublisherInfo, IConfigurablePublisherRegistry::BpConfigurablePublisherRegistry
};
use google_sdv_telemetry_configurable_publisher::binder::BinderFeatures;
use sample_publisher_proto::sample_publisher;
use sample_publisher_proto::sample_publisher::SampleMessage;
use crate::random_integer_publisher::RandomIntegerPublisher;
use crate::random_integer_publisher_service::RandomIntegerPublisherService;

use log::{info, LevelFilter};
use protobuf::{Message, MessageFull};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

const LOG_TAG: &str = "telemetry_sample_pblshr";
const _: () = assert!(
    LOG_TAG.len() <= 23,
    "LOG_TAG should be less than or equal to 23 characters, as it will otherwise be truncated by adb."
);

pub fn connect() -> Result<binder::Strong<dyn IConfigurablePublisherRegistry>, binder::StatusCode> {
    let configurable_publisher_registry_descriptor = format!(
        "{}/default",
        <BpConfigurablePublisherRegistry as IConfigurablePublisherRegistry>::get_descriptor()
    );
    binder::get_interface(&configurable_publisher_registry_descriptor)
}

#[tokio::main]
async fn main() -> Result<(), binder::Status> {
    logger::init(
        logger::Config::default().with_tag_on_device(LOG_TAG).with_max_level(LevelFilter::Trace),
    );

    info!("Starting thread pool");
    binder::ProcessState::start_thread_pool();
    // fetch the binder service
    let service = connect().expect("Failed to connect to IConfigurablePublisherRegistry");

    info!("Creating publisher");
    let notify_subscribed = Arc::new(Notify::new());
    let publisher = Arc::new(RandomIntegerPublisher::new(Arc::clone(&notify_subscribed)));
    let publisher_wrapper = RandomIntegerPublisherService { publisher: Arc::clone(&publisher) };
    let binder_publisher =
        BnConfigurablePublisher::new_binder(publisher_wrapper, BinderFeatures::default());

    info!("Registering publisher");
    let file_descriptor = sample_publisher::file_descriptor();
    let file_descriptor_proto = file_descriptor.proto();
    let message_descriptor = SampleMessage::descriptor();
    let publisher_info = PublisherInfo {
        serviceName: "SampleService".to_owned(),
        fileDescriptorProto: file_descriptor_proto.write_to_bytes().unwrap(),
        messageTypeName: format!(".{}", message_descriptor.full_name()),
    };

    service.registerPublisher(&publisher_info, &binder_publisher)?;

    loop {
        info!("Waiting for subscription");
        notify_subscribed.notified().await;

        info!("Publishing data");
        publisher
            .publish_data_randomly(Duration::from_secs(1), Duration::from_secs(1))
            .await
            .expect("Failed to publish data");

        info!("Stopped publishing data");
    }
}
