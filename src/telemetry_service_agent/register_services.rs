// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use binder_tokio::TokioRuntime;
use google_sdv_telemetry::aidl::google::sdv::telemetry::ITelemetryService::{
    BnTelemetryService, BpTelemetryService, ITelemetryService,
};
use log::info;
use rustutils::android::system_properties::PropertyWatcher;
use sdv_telemetry::service_publisher_manager::ServicePublisherManager;
use sdv_telemetry::SdvTelemetryService;
use sdv_telemetry_platform_abstraction_layer::PlatformAbstractionLayer;
use std::sync::Arc;

const DEFAULT_METRIC_CONFIG_STORE_SIZE_MB: usize = 1; // 1 MB
const METRIC_CONFIG_STORE_SIZE_MB_PROPERTY: &str = "ro.sdv.telemetry.metric_config_store_in_mb";
const ROOT_DIR: &str = "/data/";

pub async fn register_services(
    service_publisher_manager_impl: Arc<ServicePublisherManager>,
    pal: Arc<dyn PlatformAbstractionLayer>,
    dump_manager: Arc<sdv_dump::DumpManager>,
) -> Result<(), Box<dyn std::error::Error>> {
    let config_store_size_mb = PropertyWatcher::new(METRIC_CONFIG_STORE_SIZE_MB_PROPERTY)
        .unwrap_or_else(|err| panic!("Failed to create property watcher for property '{METRIC_CONFIG_STORE_SIZE_MB_PROPERTY}': {err:?}"))
        .read(|_property_name, value| value.trim().parse::<usize>().ok())
        .ok()
        .flatten()
        .unwrap_or(DEFAULT_METRIC_CONFIG_STORE_SIZE_MB);

    info!("Creating telemetry service with {config_store_size_mb}MB metrics config store size.");

    let Some(telemetry_service) = SdvTelemetryService::new(
        ROOT_DIR,
        config_store_size_mb * 1024 * 1024,
        service_publisher_manager_impl,
        pal,
        dump_manager,
    )
    .await
    else {
        return Err("Error in initialization telemetry service. Aborting.".into());
    };

    let telemetry_descriptor =
        format!("{}/default", <BpTelemetryService as ITelemetryService>::get_descriptor());

    info!("Adding {telemetry_descriptor} service to binder");
    let telemetry_binder = BnTelemetryService::new_async_binder(
        telemetry_service,
        TokioRuntime(tokio::runtime::Handle::current()),
        binder::BinderFeatures::default(),
    );

    binder::add_service(&telemetry_descriptor, telemetry_binder.as_binder())
        .expect("Failed to register service");

    info!("Service {telemetry_descriptor} added to binder");

    Ok(())
}
