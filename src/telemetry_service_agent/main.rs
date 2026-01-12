// Copyright 2023 Google LLC

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
    use sdv_telemetry_comms_utils::wait_for_sdv_agents_and_create_context;
    use sdv_telemetry_dt_wrapper::comms_stack_accessor::CommsStackAccessor;
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
