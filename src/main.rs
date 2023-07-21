// Copyright 2023 Google LLC. All Rights Reserved.

//! Binary for registering Telemetry service.
use android_sdv_telemetry::aidl::android::sdv::telemetry::ITelemetryService::{
    BnTelemetryService, BpTelemetryService, ITelemetryService,
};
use binder_tokio::TokioRuntime;
use log::info;
use tokio::runtime::Builder;

use rustutils::system_properties::PropertyWatcher;
use sdv_telemetry::SdvTelemetryService;

const SIZE_MB: usize = 1024 * 1024;

static METRIC_CONFIG_STORE_SIZE_PROPERTY: &str = "ro.telemetry.metric_config_store_in_mb";
static LOG_TAG: &str = "SdvTelemetryService";
static ROOT_DIR: &str = "/data/local/tmp";

fn main() {
    sdv_log::init_logger(LOG_TAG).unwrap_or_else(|error| match &error {
        sdv_log::LoggerError::AlreadyInitializedError(_) => {
            // Only inform error, not panic
            info!("{}", error)
        }
        _ => panic!("{}", error),
    });

    let mut watcher = PropertyWatcher::new(METRIC_CONFIG_STORE_SIZE_PROPERTY).unwrap();
    let value = watcher.read(|_, value| Ok(value.trim().to_string())).unwrap();

    let configured_size: usize = value.parse().unwrap_or(1); // Default size 1 MB

    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    info!("Creating telemetry service with {}MBs metric store size", configured_size);

    let service = runtime.block_on(SdvTelemetryService::new(ROOT_DIR, configured_size * SIZE_MB));
    let descriptor = <BpTelemetryService as ITelemetryService>::get_descriptor();

    println!("Adding {descriptor} service to binder");
    let binder = BnTelemetryService::new_async_binder(
        service,
        TokioRuntime(runtime),
        binder::BinderFeatures::default(),
    );
    binder::add_service(descriptor, binder.as_binder()).expect("Failed to register service");

    binder::ProcessState::start_thread_pool();
    // Does not return - spawn or perform any work you mean to do before this call.
    binder::ProcessState::join_thread_pool()
}
