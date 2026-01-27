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

//! Binary for registering Telemetry service.

mod register_services;

use binder_tokio::TokioRuntime;
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisherRegistry::BnConfigurablePublisherRegistry;
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisherRegistry::BpConfigurablePublisherRegistry;
use google_sdv_telemetry_configurable_publisher::aidl::google::sdv::telemetry::configurable_publisher::IConfigurablePublisherRegistry::IConfigurablePublisherRegistry;
use log::error;
use log::info;
use register_services::register_services;
use sdv_telemetry::service_publisher_manager::ServicePublisherManager;
use sdv_telemetry_configurable_publisher::ConfigurablePublisherRegistry;
use sdv_telemetry_configurable_publisher::ConfigurablePublisherRegistryService;
use sdv_telemetry_service_publisher_accessor::ServicePublisherAccessor;
use sdv_telemetry_utils::logging::validated_log_tag;
use std::sync::Arc;

const LOG_TAG: &str = validated_log_tag("SdvTelemetryService");
const STARTED_SUCCESSFULLY: &str = "sdv_telemetry_service_agent has started successfully";
const DUMP_REPORT_TITLE: &str = "SDV Agent dump - Telemetry";

#[cfg(feature = "comms_stack")]
fn create_comms_stack_accessor(
    dump_manager: &sdv_dump::DumpManager,
) -> Arc<dyn ServicePublisherAccessor> {
    use futures::FutureExt;
    use sdv_comms::id::ServiceFqin;
    use sdv_telemetry_comms_stack_wrapper::comms_stack_accessor::CommsStackAccessor;
    use sdv_telemetry_comms_utils::wait_for_sdv_agents_and_create_context;
    use std::str::FromStr;

    // WARNING: The VM name is automatically updated in the created `ContextRef` to match the actual
    // VM name. Do not use this string anywhere else, always retrieve the actual FQIN using
    // `ContextRef::get_self_fqin`!
    const SERVICE_FQIN: &str = "vm:com.google.sdv.telemetry.TelemetryService/default";
    let fqin = ServiceFqin::from_str(SERVICE_FQIN).unwrap();
    dump_manager.set_self_fqin(&fqin);

    // Make a shared future of ContextRef to avoid blocking Telemetry Service startup.
    let ctx_future = wait_for_sdv_agents_and_create_context(fqin).boxed().shared();

    Arc::new(CommsStackAccessor::new(ctx_future))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    sdv_log::init_logger(LOG_TAG)?;
    sdv_tracing::init_tracing();
    binder::ProcessState::start_thread_pool();

    let dump_manager = Arc::new(sdv_dump::DumpManager::new(DUMP_REPORT_TITLE));
    dump_manager.set_agent_state("Initializing...");

    // Create Configurable Publishers
    let configurable_publisher_registry = Arc::new(ConfigurablePublisherRegistry::new());

    // Add all service publishers to the manager in order of priority
    let service_publishers = vec![
        Arc::clone(&configurable_publisher_registry) as Arc<dyn ServicePublisherAccessor>,
        #[cfg(feature = "comms_stack")]
        create_comms_stack_accessor(&dump_manager),
    ];

    // Create Service Publisher Manager
    let service_publisher_manager_impl = Arc::new(ServicePublisherManager::new(service_publishers));

    #[cfg(all(
        feature = "platform_abstraction_layer_core",
        feature = "platform_abstraction_layer_ivi"
    ))]
    compile_error!("feature \"platform_abstraction_layer_core\" and feature \"platform_abstraction_layer_ivi\" cannot be enabled at the same time");

    #[cfg(feature = "platform_abstraction_layer_core")]
    let pal = Arc::new(sdv_telemetry_platform_abstraction_layer_core::CorePal::new());
    #[cfg(feature = "platform_abstraction_layer_ivi")]
    let pal = Arc::new(sdv_telemetry_platform_abstraction_layer_ivi::IviPal::new());

    let res = register_services(service_publisher_manager_impl, pal, dump_manager).await;

    if res.is_err() {
        error!("Error {res:?} in initialization telemetry service");
    }

    let configurable_publisher_registry_descriptor = format!(
        "{}/default",
        <BpConfigurablePublisherRegistry as IConfigurablePublisherRegistry>::get_descriptor()
    );

    info!("Adding {configurable_publisher_registry_descriptor} service to binder");
    let configurable_publisher_service =
        ConfigurablePublisherRegistryService::new(configurable_publisher_registry);
    let configurable_publisher_binder = BnConfigurablePublisherRegistry::new_async_binder(
        configurable_publisher_service,
        TokioRuntime(tokio::runtime::Handle::current()),
        binder::BinderFeatures::default(),
    );
    binder::add_service(
        &configurable_publisher_registry_descriptor,
        configurable_publisher_binder.as_binder(),
    )
    .expect("Failed to register service");

    log::debug!("{STARTED_SUCCESSFULLY}");
    tracing::event!(tracing::Level::DEBUG, STARTED_SUCCESSFULLY);

    // Does not return - spawn or perform any work you mean to do before this call.
    tokio::task::block_in_place(binder::ProcessState::join_thread_pool);

    Ok(())
}
