// Copyright 2023 Google LLC. All Rights Reserved.

//! Binary for testing Telemetry service.
use android_sdv_telemetry::aidl::android::sdv::telemetry::ITelemetryService::{
    BpTelemetryService, ITelemetryService,
};

extern crate protobuf;

use binder::Strong;
use log::{info, Level};
use protobuf::Message;

use sdvtelemetry_proto_rust::metrics_configuration::MetricsConfig;

/// Fetch the ITelemetryService interface
pub fn get_binder_service() -> Result<Strong<dyn ITelemetryService>, binder::StatusCode> {
    let interface_name = <BpTelemetryService as ITelemetryService>::get_descriptor();
    println!("Getting service {interface_name}");
    binder::get_interface(interface_name)
}

/// Communicate with the telemetry service.
fn main() -> Result<(), binder::Status> {
    binder::ProcessState::start_thread_pool();
    let _init_success = logger::init(
        logger::Config::default()
            .with_tag_on_device("telemetry_rust_client")
            .with_min_level(Level::Trace),
    );
    info!("Connecting to service");
    let service = get_binder_service().expect("Unable to connect to telemetry service");

    let mut config = MetricsConfig::new();
    config.uuid = Some("01bf0754-df95-46bb-bb35-522b6af66600".to_string());
    config.version = Some(2);
    config.script = Some("Sample Script".to_string());

    info!("Serializing metric config");
    let serialized_config: &[u8] = &config.write_to_bytes().unwrap();

    let _ = service.addMetricsConfig("TestConfig", serialized_config);
    let _ = service.removeAllMetricsConfigs();
    Ok(())
}
