// Copyright 2023 Google LLC. All Rights Reserved.

//! Binary for testing Telemetry service.

mod binding_c;

extern crate protobuf;

use crate::binding_c as rust_to_c;
use log::{info, Level};
use protobuf::Message;

use sdvtelemetry_proto_rust::metrics_configuration::MetricsConfig;

/// Communicate with the telemetry service.
fn main() -> Result<(), binder::Status> {
    binder::ProcessState::start_thread_pool();
    let _init_success = logger::init(
        logger::Config::default()
            .with_tag_on_device("telemetry_rust_c_client")
            .with_min_level(Level::Trace),
    );

    let service;

    // Unsafe block needed to be able to call C API functions
    // More info at: https://doc.rust-lang.org/book/ch19-01-unsafe-rust.html
    unsafe {
        info!("Connecting to service");
        service = rust_to_c::start_telemetry_service();
    }

    let mut config = MetricsConfig::new();
    config.uuid = "01bf0754-df95-46bb-bb35-522b6af66600".to_string();
    config.version = 2;
    config.script = "Sample Script".to_string();

    info!("Serializing metric config");
    let serialized_config: &[u8] = &config.write_to_bytes().unwrap();

    unsafe {
        rust_to_c::addMetricsConfig(
            service,
            serialized_config.as_ptr() as *mut u8,
            serialized_config.len(),
        );
        rust_to_c::removeMetricsConfig(service, config.uuid.as_ptr() as *const std::ffi::c_char);
        rust_to_c::stop_telemetry_service(service);
    }

    Ok(())
}
