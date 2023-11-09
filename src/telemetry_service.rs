// Copyright 2023 Google LLC. All Rights Reserved.

//! This module implements the ITelemetryService AIDL interface

mod collection_extension;
mod expr;
mod file_io_utils;
mod metrics_config_orchestrator;
mod metrics_report_listener;

use android_sdv_telemetry::aidl::android::sdv::telemetry::{
    ITelemetryReportListener::ITelemetryReportListener,
    ITelemetryReportReadyListener::ITelemetryReportReadyListener,
    ITelemetryService::ITelemetryServiceAsyncServer,
    ITelemetryStatusListener::ITelemetryStatusListener,
    TelemetryServiceStatusCodes::TelemetryServiceStatusCodes as StatusCodes,
};
use android_sdv_telemetry::binder::{Interface, Result as BinderResult, Strong};
use async_trait::async_trait;
use log::{error, info};
use protobuf::Message;
use sdv_telemetry_configurable_publisher::configurable_publisher_registry::ConfigurablePublisherRegistry;
use sdvtelemetry_proto_rust::metrics_configuration::MetricsConfig;
use std::collections::HashMap;
use std::fs::{self};
use std::marker::{Send, Sync};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::metrics_report_listener::MetricsReportListener;

pub use metrics_config_orchestrator::MetricsConfigOrchestrator;
use metrics_config_orchestrator::{
    MetricsConfigOrchestratorError as ConfigError, MetricsConfigOrchestratorImpl,
};

static ACTIVE_CONFIG_DATA_DIR: &str = "telemetry/active_configs";
static INACTIVE_CONFIG_DATA_DIR: &str = "telemetry/inactive_configs";

const GLOBAL_STATUS_LISTENER_API_CALL_ID: i32 = 0;

fn get_error_code_and_message(err: ConfigError) -> (i32, String) {
    let err_code = match err {
        ConfigError::DuplicatePublisherDefinition { .. } => {
            StatusCodes::ADD_METRICS_CONFIG_FAILED_DUPLICATE_PUBLISHER_DEFINITION
        }
        ConfigError::DuplicateTriggerDefinition { .. } => {
            StatusCodes::ADD_METRICS_CONFIG_FAILED_DUPLICATE_TRIGGER_DEFINITION
        }
        ConfigError::CycledPublishersTriggers => {
            StatusCodes::ADD_METRICS_CONFIG_FAILED_CYCLED_PUBLISHERS_TRIGGERS
        }
        ConfigError::InvalidDependency { .. } => {
            StatusCodes::ADD_METRICS_CONFIG_FAILED_INVALID_DEPENDENCY
        }
        ConfigError::DuplicateMetricsConfigUuid { .. } => {
            StatusCodes::ADD_METRICS_CONFIG_FAILED_DUPLICATE_METRICS_CONFIG_UUID
        }
        ConfigError::InvalidMetricsReportConfig { .. } => {
            StatusCodes::ADD_METRICS_CONFIG_FAILED_INVALID_METRICS_REPORT_CONFIG
        }
        ConfigError::InvalidPublisherDefinition { .. } => {
            StatusCodes::ADD_METRICS_CONFIG_FAILED_INVALID_PUBLISHER_DEFINITION
        }
        ConfigError::InvalidTriggerDefinition { .. } => {
            StatusCodes::ADD_METRICS_CONFIG_FAILED_INVALID_TRIGGER_DEFINITION
        }
        ConfigError::InvalidDescriptorProtos { .. } => {
            StatusCodes::ADD_METRICS_CONFIG_FAILED_INVALID_DESCRIPTOR_PROTOS
        }
        ConfigError::InvalidConfigUuid { .. } => {
            StatusCodes::ADD_METRICS_CONFIG_FAILED_INVALID_CONFIG_UUID
        }
        ConfigError::InvalidExpressionNodes { .. } => {
            StatusCodes::ADD_METRICS_CONFIG_FAILED_INVALID_EXPRESSION_NODES
        }
        ConfigError::UnableToActivateStartTrigger { .. } => {
            StatusCodes::ACTIVATE_METRICS_CONFIG_FAILED_UNABLE_TO_ACTIVATE_START_TRIGGER
        }
        ConfigError::InvalidPublisherAndDescriptorCombination { .. } => {
            StatusCodes::ADD_METRICS_CONFIG_FAILED_INVALID_PUBLISHER_DEFINITION
        }
    };

    (err_code.0, err.to_string())
}

/// This struct is defined to implement ITelemetryService AIDL interface.
pub struct SdvTelemetryService {
    active_data_dir: String,
    inactive_data_dir: String,
    cur_persistent_config_store_size: AtomicUsize,
    max_persistent_config_store_size: usize,
    /// Instance of Metrics Configuration Orchestrator.
    metrics_config_orchestrator: Box<dyn MetricsConfigOrchestrator + Send + Sync>,
    /// Hashmap to store the metricsConfigs's size passed by the client.
    metrics_configs_store: Arc<Mutex<HashMap<String, usize>>>,
    /// Report Ready listener to inform the client when a report is ready
    metrics_report_listener: Arc<MetricsReportListener>,
    /// Status Listener to send back status to client
    status_listener: Mutex<Option<Strong<dyn ITelemetryStatusListener>>>,
}

impl Interface for SdvTelemetryService {}

impl SdvTelemetryService {
    /// Instantiates an instance of SdvTelemetryService
    pub async fn new(
        root_path: &str,
        config_store_size: usize,
        configurable_publisher_registry: Arc<dyn ConfigurablePublisherRegistry>,
    ) -> Option<Self> {
        let (active_data_dir, inactive_data_dir) = if !root_path.is_empty() {
            (
                format!("{}/{}", root_path, ACTIVE_CONFIG_DATA_DIR),
                format!("{}/{}", root_path, INACTIVE_CONFIG_DATA_DIR),
            )
        } else {
            (ACTIVE_CONFIG_DATA_DIR.to_owned(), INACTIVE_CONFIG_DATA_DIR.to_owned())
        };

        let active_dir_path = Path::new(&active_data_dir);
        let inactive_dir_path = Path::new(&inactive_data_dir);

        let metrics_report_listener = Arc::new(MetricsReportListener::new());
        let listener = metrics_report_listener.clone(); // Clone the Arc to share ownership

        let configs_store = Arc::new(Mutex::new(HashMap::new()));
        let config_orchestrator = Box::new(MetricsConfigOrchestratorImpl::new(
            configurable_publisher_registry,
            move |report| listener.on_report_ready(report),
        ));

        let mut num_active_configs = 0;
        let mut num_inactive_configs = 0;
        // Needed to get the size of all files
        let mut total_size = 0;

        if active_dir_path.is_dir() {
            // Read the contents of each file in the directory into a byte array
            for entry in fs::read_dir(active_dir_path).unwrap() {
                let entry = entry.unwrap();
                let path = entry.path();
                if path.is_file() {
                    let bytes = match file_io_utils::read_from_file(&path).await {
                        Ok(data) => data,
                        Err(e) => {
                            info!("An error occurred while trying to read file: {}", e);
                            continue;
                        }
                    };

                    // bytes now contains the contents of the file
                    let config: MetricsConfig = match protobuf::Message::parse_from_bytes(&bytes) {
                        Ok(cfg) => cfg,
                        Err(_) => {
                            error!("Error in reading {} config from memory", path.display());
                            continue;
                        }
                    };
                    let uuid = &config.uuid;

                    // Validates and initializes metrics config
                    match config_orchestrator.initialize_config(&config).await {
                        Ok(()) => {
                            let mut metric_config_map = configs_store.lock().unwrap();
                            metric_config_map.insert(uuid.clone(), bytes.len());
                            num_active_configs += 1;
                            total_size += bytes.len();
                        }
                        Err(e) => {
                            error!("Error {} in loading {} Config", e, uuid);
                            file_io_utils::remove_file(&path).await.unwrap_or_else(|e| {
                                error!("An error occurred in removing corrupt config: {}", e)
                            });
                        }
                    }
                }
            }
        } else {
            match file_io_utils::create_dir(&active_dir_path.to_path_buf()).await {
                Ok(_) => {
                    // Success, directory was created or already exists
                    info!("active config telemetry directory created");
                }
                Err(e) => {
                    error!("An error occurred in creating directory: {}", e);
                    return None;
                }
            };
        }

        if inactive_dir_path.is_dir() {
            // Needed to get the size of all files
            for entry in fs::read_dir(inactive_dir_path).unwrap() {
                let entry = entry.unwrap();
                let metadata = entry.metadata().unwrap();

                // Check if it's a file
                if metadata.is_file() {
                    let file_name = entry.file_name();
                    let mut metric_config_map = configs_store.lock().unwrap();
                    metric_config_map
                        .insert(file_name.into_string().unwrap(), metadata.len() as usize);
                    total_size += metadata.len() as usize;
                    num_inactive_configs += 1;
                } else {
                    // remove corrupt inactive config
                    file_io_utils::remove_file(&entry.path()).await.unwrap_or_else(|e| {
                        error!("An error occurred in removing corrupt config: {}", e)
                    });
                }
            }
        } else {
            match file_io_utils::create_dir(&inactive_dir_path.to_path_buf()).await {
                Ok(_) => {
                    // Success, directory was created or already exists
                    info!("inactive config telemetry directory created");
                }
                Err(e) => {
                    error!("An error occurred in creating directory: {}", e);
                    return None;
                }
            };
        }

        info!(
            "Telemetry service initialized with {:} active configs and {:} inactive configs occupying {} memory",
            num_active_configs,
            num_inactive_configs,
            total_size
        );
        Some(SdvTelemetryService {
            active_data_dir,
            inactive_data_dir,
            cur_persistent_config_store_size: AtomicUsize::new(total_size),
            max_persistent_config_store_size: config_store_size,
            metrics_config_orchestrator: config_orchestrator,
            metrics_configs_store: configs_store,
            metrics_report_listener,
            status_listener: Mutex::new(None),
        })
    }

    /// Uses the listener object set by the client to send back status codes
    /// and error messages for API calls
    pub fn send_status(
        &self,
        api_call_id: i32,
        status_code: i32,
        status_message: &str,
    ) -> BinderResult<()> {
        if let Ok(listener) = self.status_listener.lock() {
            if let Some(listener_ref) = listener.as_ref() {
                let _ = listener_ref.deref().sendStatus(api_call_id, status_code, status_message);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ITelemetryServiceAsyncServer for SdvTelemetryService {
    async fn addMetricsConfig(&self, api_call_id: i32, metrics_config: &[u8]) -> BinderResult<()> {
        let config: MetricsConfig = match protobuf::Message::parse_from_bytes(metrics_config) {
            Ok(cfg) => cfg,
            Err(_) => {
                error!("Error in parsing config");
                return self.send_status(
                    api_call_id,
                    StatusCodes::ADD_METRICS_CONFIG_FAILED_CONFIG_PARSE_ERROR.0,
                    "Unable to parse metric config",
                );
            }
        };
        info!("addMetricsConfig: {:?} of size: {}\n", config, metrics_config.len());

        if metrics_config.len() + self.cur_persistent_config_store_size.load(Ordering::SeqCst)
            > self.max_persistent_config_store_size
        {
            error!("Memory limit exceeded");
            return self.send_status(
                api_call_id,
                StatusCodes::ADD_METRICS_CONFIG_FAILED_MEMORY_LIMIT_EXCEEDED.0,
                "Memory Limited exceeded",
            );
        }

        // Validate the metrics config and report any errors to client
        if let Err(e) = self.metrics_config_orchestrator.validate_config(&config) {
            info!("Invalid Metric Config");
            let (err_code, err_msg) = get_error_code_and_message(e);
            return self.send_status(api_call_id, err_code, &err_msg);
        }

        let bytes = metrics_config.to_vec();
        let path = PathBuf::from(self.inactive_data_dir.clone() + "/" + &config.uuid);

        if let Err(e) = file_io_utils::write_to_file(&path, &bytes).await {
            error!("An error occurred while writing to file: {}", e);
            if path.is_file() {
                file_io_utils::remove_file(&path).await.unwrap_or_else(|e| {
                    error!("An error occurred in removing corrupt config: {}", e)
                });
            }
            return self.send_status(
                api_call_id,
                StatusCodes::ADD_METRICS_CONFIG_FAILED_UNABLE_TO_PERSIST_CONFIG.0,
                "Unable to write metric config to memory",
            );
        }

        // Storing the size of the metrics configs in metric_config_map to allow easy access
        // during remove metrics API call. self.cur_persistent_config_store_size needs to be
        // updated during each API call that modifies active metrics configs.
        // Alternate to metric_config_map would be read the file size from memory which constitutes
        // an IO operation and is inefficient.
        self.metrics_configs_store.lock().unwrap().insert(config.uuid.clone(), bytes.len());
        self.cur_persistent_config_store_size.fetch_add(bytes.len(), Ordering::SeqCst);

        return self.send_status(
            api_call_id,
            StatusCodes::ADD_METRICS_CONFIG_SUCCEEDED.0,
            &config.uuid,
        );
    }

    async fn activateMetricsConfig(
        &self,
        api_call_id: i32,
        metrics_config_uuid: &str,
    ) -> BinderResult<()> {
        info!("activateMetricsConfig: {:?} \n", metrics_config_uuid);
        let path = PathBuf::from(self.inactive_data_dir.clone() + "/" + metrics_config_uuid);
        if path.is_file() {
            let bytes = match file_io_utils::read_from_file(&path).await {
                Ok(data) => data,
                Err(e) => {
                    let err_msg = format!("{:?}", e);

                    return self.send_status(
                        api_call_id,
                        StatusCodes::ACTIVATE_METRICS_CONFIG_FAILED_CONFIG_CORRUPTED.0,
                        &err_msg,
                    );
                }
            };

            // bytes now contains the contents of the file
            let config: MetricsConfig = match protobuf::Message::parse_from_bytes(&bytes) {
                Ok(cfg) => cfg,
                Err(e) => {
                    let err_msg = format!("{:?}", e);
                    error!("{}", err_msg);
                    return self.send_status(
                        api_call_id,
                        StatusCodes::ACTIVATE_METRICS_CONFIG_FAILED_CONFIG_CORRUPTED.0,
                        &err_msg,
                    );
                }
            };
            let uuid = &config.uuid;

            // Initializes metrics config
            match self.metrics_config_orchestrator.initialize_config(&config).await {
                Ok(()) => {
                    let new_path = PathBuf::from(self.active_data_dir.clone() + "/" + uuid);
                    file_io_utils::rename_file(&path, &new_path)
                        .await
                        .unwrap_or_else(|e| error!("An error occurred in renaming config: {}", e));
                }
                Err(e) => {
                    error!("Error {} in initializing {} Config", e, uuid);
                    file_io_utils::remove_file(&path).await.unwrap_or_else(|e| {
                        error!("An error occurred in removing corrupt config: {}", e)
                    });
                    let (err_code, err_msg) = get_error_code_and_message(e);
                    return self.send_status(api_call_id, err_code, &err_msg);
                }
            }
        } else {
            let err_msg = format!("Config {} not found", metrics_config_uuid);
            return self.send_status(
                api_call_id,
                StatusCodes::METRICS_CONFIG_DOES_NOT_EXIST.0,
                &err_msg,
            );
        }

        self.send_status(
            api_call_id,
            StatusCodes::ACTIVATE_METRICS_CONFIG_SUCCEEDED.0,
            metrics_config_uuid,
        )
    }

    async fn deactivateMetricsConfig(
        &self,
        api_call_id: i32,
        metrics_config_uuid: &str,
    ) -> BinderResult<()> {
        info!("deactivateMetricsConfig: {}", metrics_config_uuid);

        let res = self.metrics_config_orchestrator.finish_config(metrics_config_uuid);
        if res.is_err() {
            info!("Invalid Metric Config UUID");
            let err_msg = format!("Config {} not found", metrics_config_uuid);
            return self.send_status(
                api_call_id,
                StatusCodes::METRICS_CONFIG_DOES_NOT_EXIST.0,
                &err_msg,
            );
        }

        let old_path = PathBuf::from(self.active_data_dir.clone() + "/" + metrics_config_uuid);
        let new_path = PathBuf::from(self.inactive_data_dir.clone() + "/" + metrics_config_uuid);

        // TODO: b/299329266: Handle file io errors gracefully
        file_io_utils::rename_file(&old_path, &new_path)
            .await
            .unwrap_or_else(|e| error!("An error occurred in renaming config: {}", e));

        self.send_status(
            api_call_id,
            StatusCodes::DEACTIVATE_METRICS_CONFIG_SUCCEEDED.0,
            metrics_config_uuid,
        )
    }

    async fn removeMetricsConfig(
        &self,
        api_call_id: i32,
        metrics_config_uuid: &str,
    ) -> BinderResult<()> {
        info!("removeMetricsConfig: {}", metrics_config_uuid);
        let uuid = String::from(metrics_config_uuid);
        let config_size = {
            let mut metric_config_map = self.metrics_configs_store.lock().unwrap();
            metric_config_map.remove(&uuid).unwrap_or_default()
        };

        if config_size == 0 {
            let err_msg = format!("Config {} not found", metrics_config_uuid);
            error!("{}", err_msg);
            return self.send_status(
                api_call_id,
                StatusCodes::METRICS_CONFIG_DOES_NOT_EXIST.0,
                &err_msg,
            );
        }

        let active_config = {
            let res = self.metrics_config_orchestrator.finish_config(metrics_config_uuid);
            if let Err(err) = res {
                if let ConfigError::InvalidConfigUuid { .. } = err {
                    info!("UUID {} doesn't exist or is inactive", uuid);
                    false
                } else {
                    error!("Unknown Error in finishing Metric Config");
                    return self.send_status(
                        api_call_id,
                        StatusCodes::TELEMETRY_SERVICE_UNKNOWN_ERROR.0,
                        "Unknown Error",
                    );
                }
            } else {
                true
            }
        };

        let path = {
            if active_config {
                PathBuf::from(self.active_data_dir.clone() + "/" + &uuid)
            } else {
                PathBuf::from(self.inactive_data_dir.clone() + "/" + &uuid)
            }
        };

        if let Err(e) = file_io_utils::remove_file(&path).await {
            error!("An error occurred in removing file: {}", e);
            return self.send_status(
                api_call_id,
                StatusCodes::REMOVE_METRICS_CONFIG_FAILED_UNABLE_TO_REMOVE_CONFIG_FROM_MEMEORY.0,
                &uuid,
            );
        }

        self.cur_persistent_config_store_size.fetch_sub(config_size, Ordering::SeqCst);

        self.send_status(api_call_id, StatusCodes::REMOVE_METRICS_CONFIG_SUCCEEDED.0, &uuid)
    }

    async fn removeAllMetricsConfigs(&self, api_call_id: i32) -> BinderResult<()> {
        info!("removeAllMetricsConfigs");

        // remove all configs from active directory
        for entry in fs::read_dir(self.active_data_dir.clone()).unwrap() {
            file_io_utils::remove_file(&entry.unwrap().path())
                .await
                .unwrap_or_else(|e| error!("An error occurred in removing active config: {}", e));
        }

        // remove all configs from inactive directory
        for entry in fs::read_dir(self.inactive_data_dir.clone()).unwrap() {
            file_io_utils::remove_file(&entry.unwrap().path())
                .await
                .unwrap_or_else(|e| error!("An error occurred in removing inactive config: {}", e));
        }

        {
            let mut metric_config_map = self.metrics_configs_store.lock().unwrap();
            for uuid in metric_config_map.keys() {
                let res = self.metrics_config_orchestrator.finish_config(uuid);
                if let Err(err) = res {
                    if let ConfigError::InvalidConfigUuid { .. } = err {
                        info!("UUID {} doesn't exist or is not active", uuid);
                    } else {
                        error!("Error in finishing Metric Config {:?}", err);
                    }
                }
            }

            metric_config_map.clear();
        }

        self.cur_persistent_config_store_size.store(0, Ordering::SeqCst);
        self.send_status(
            api_call_id,
            StatusCodes::REMOVE_ALL_METRICS_CONFIGS_SUCCEEDED.0,
            "Successfully removed all configs",
        )
    }

    async fn setReportReadyListener(
        &self,
        listener: &Strong<dyn ITelemetryReportReadyListener>,
    ) -> BinderResult<()> {
        info!("setReportReadyListener");
        let (status_code, status_message) =
            self.metrics_report_listener.set_report_ready_listener(listener).map_or_else(
                |_| {
                    (
                        StatusCodes::SET_REPORT_READY_LISTENER_FAILED_LISTENER_EXISTS.0,
                        "Report Ready Listener already exists",
                    )
                },
                |_| {
                    (
                        StatusCodes::SET_REPORT_READY_LISTENER_SUCCEEDED.0,
                        "Report Ready Listener successfully added",
                    )
                },
            );
        self.send_status(GLOBAL_STATUS_LISTENER_API_CALL_ID, status_code, status_message)
    }

    async fn setStatusListener(
        &self,
        listener: &Strong<dyn ITelemetryStatusListener>,
    ) -> BinderResult<()> {
        let (status_code, status_message) = {
            let mut guard = self.status_listener.lock().unwrap();
            if guard.is_none() {
                *guard = Some(listener.clone());
                (StatusCodes::SET_STATUS_LISTENER_SUCCEEDED.0, "Status listener successfully added")
            } else {
                (
                    StatusCodes::SET_STATUS_LISTENER_FAILED_LISTENER_EXISTS.0,
                    "Status listener already exists",
                )
            }
        };
        self.send_status(GLOBAL_STATUS_LISTENER_API_CALL_ID, status_code, status_message)
    }

    async fn getMetricsReport(
        &self,
        report_number: i32,
        metrics_report_uuid: &str,
        report_listener: &Strong<dyn ITelemetryReportListener>,
    ) -> BinderResult<()> {
        info!("getMetricsReport: {}", metrics_report_uuid);
        if let Ok(metrics_report) =
            self.metrics_report_listener.get_metrics_report(report_number, metrics_report_uuid)
        {
            let serialized_report: &[u8] = &metrics_report.write_to_bytes().unwrap();
            let _ = report_listener.deref().onReport(
                StatusCodes::GET_FINISHED_REPORT_SUCCEEDED.0,
                metrics_report_uuid,
                serialized_report,
            );
        } else {
            let _ = report_listener.deref().onReport(
                StatusCodes::GET_FINISHED_REPORT_FAILED_LISTENER_REPORT_NOT_FOUND.0,
                metrics_report_uuid,
                &[],
            );
        }

        Ok(())
    }

    async fn clearReportReadyListener(&self) -> BinderResult<()> {
        info!("clearReportReadyListener");
        let (status_code, status_message) = {
            match self.metrics_report_listener.clear_report_ready_listener() {
                Ok(()) => (
                    StatusCodes::CLEAR_REPORT_READY_LISTENER_SUCCEEDED.0,
                    "Report listener successfully cleared",
                ),
                Err(_) => (
                    StatusCodes::CLEAR_REPORT_READY_LISTENER_FAILED_LISTENER_NOT_SET.0,
                    "Report listener not set",
                ),
            }
        };
        self.send_status(GLOBAL_STATUS_LISTENER_API_CALL_ID, status_code, status_message)
    }

    async fn clearStatusListener(&self) -> BinderResult<()> {
        info!("clearStatusListener");
        *self.status_listener.lock().unwrap() = None;
        Ok(())
    }
}

#[cfg(test)]
mod test_utils;

#[cfg(test)]
mod tests {
    use android_sdv_telemetry::aidl::android::sdv::telemetry::{
        ITelemetryReportListener::BnTelemetryReportListener,
        ITelemetryReportReadyListener::BnTelemetryReportReadyListener,
        ITelemetryStatusListener::BnTelemetryStatusListener,
    };
    use binder::BinderFeatures;
    use protobuf::reflect::MessageDescriptor;
    use protobuf::{Message, MessageDyn};
    use sdv_telemetry_configurable_publisher::configurable_publisher_registry::ConfigurablePublisherRegistrySubscribeError;
    use sdvtelemetry_proto_rust::metrics_configuration::{
        MetricsConfig as MetricsConfigProto, MetricsReport,
    };

    use super::*;

    const MAX_PERSISTENT_CONFIGURATION_SIZE_MB: usize = 10 * 1024 * 1024; // 10Mbs

    struct MockConfigurablePublisherRegistry {}

    #[async_trait]
    impl ConfigurablePublisherRegistry for MockConfigurablePublisherRegistry {
        fn get_message_descriptor(
            &self,
            _service_name: String,
            _topic_name: String,
        ) -> Option<MessageDescriptor> {
            None
        }

        async fn subscribe(
            &self,
            _service_name: String,
            _topic_name: String,
            _subscriber: Box<dyn Fn(Box<dyn MessageDyn>) + Send + Sync>,
            _configuration: Option<&[u8]>,
        ) -> Result<(), ConfigurablePublisherRegistrySubscribeError> {
            Ok(())
        }
    }

    struct MockMetricsConfigOrchestratorImpl {
        num_initialize_config_calls: Arc<Mutex<i32>>,
        num_finish_config_calls: Arc<Mutex<i32>>,
        num_validate_config_calls: Arc<Mutex<i32>>,
    }

    impl MockMetricsConfigOrchestratorImpl {
        fn new(
            num_initialize_config_calls: Arc<Mutex<i32>>,
            num_finish_config_calls: Arc<Mutex<i32>>,
            num_validate_config_calls: Arc<Mutex<i32>>,
        ) -> Self {
            MockMetricsConfigOrchestratorImpl {
                num_initialize_config_calls,
                num_finish_config_calls,
                num_validate_config_calls,
            }
        }
    }

    #[async_trait]
    impl MetricsConfigOrchestrator for MockMetricsConfigOrchestratorImpl {
        async fn initialize_config(&self, _: &MetricsConfigProto) -> Result<(), ConfigError> {
            let mut num_calls = self.num_initialize_config_calls.lock().unwrap();
            *num_calls += 1;
            Ok(())
        }

        fn finish_config(&self, _: &str) -> Result<(), ConfigError> {
            let mut num_calls = self.num_finish_config_calls.lock().unwrap();
            *num_calls += 1;
            Ok(())
        }

        fn validate_config(&self, _: &MetricsConfigProto) -> Result<(), ConfigError> {
            let mut num_calls = self.num_validate_config_calls.lock().unwrap();
            *num_calls += 1;
            Ok(())
        }
    }

    /// This struct is defined to implement ITelemetryReportListener AIDL interface for unit testing purposes
    #[derive(Clone)]
    struct MockTelemetryReportListener {
        status_code: Arc<Mutex<i32>>,
        status_message: Arc<Mutex<String>>,
        num_on_result_calls: Arc<Mutex<i32>>,
        uuid: Arc<Mutex<String>>,
        report_number: Arc<Mutex<u32>>,
        metrics_report: Arc<Mutex<Option<MetricsReport>>>,
    }

    impl Interface for MockTelemetryReportListener {}

    impl MockTelemetryReportListener {
        fn new() -> Self {
            MockTelemetryReportListener {
                status_code: Arc::new(Mutex::new(0)),
                status_message: Arc::new(Mutex::new(String::new())),
                num_on_result_calls: Arc::new(Mutex::new(0)),
                uuid: Arc::new(Mutex::new(String::new())),
                report_number: Arc::new(Mutex::new(0)),
                metrics_report: Arc::new(Mutex::new(None)),
            }
        }

        fn get_status_code(&self) -> i32 {
            *self.status_code.lock().unwrap()
        }

        fn get_status_message(&self) -> String {
            self.status_message.lock().unwrap().to_string()
        }

        fn get_num_on_result_calls(&self) -> i32 {
            *self.num_on_result_calls.lock().unwrap()
        }

        fn get_report_number(&self) -> u32 {
            *self.report_number.lock().unwrap()
        }

        fn get_uuid(&self) -> String {
            self.uuid.lock().unwrap().to_string()
        }
    }

    impl ITelemetryReportListener for MockTelemetryReportListener {
        fn onReport(
            &self,
            status_code: i32,
            status_message: &str,
            serialized_report: &[u8],
        ) -> BinderResult<()> {
            // bytes now contains the contents of the file
            let metrics_report: MetricsReport =
                match protobuf::Message::parse_from_bytes(serialized_report) {
                    Ok(report) => report,
                    Err(_) => return Err(binder::StatusCode::BAD_VALUE.into()),
                };

            let mut mut_status_code = self.status_code.lock().unwrap();
            *mut_status_code = status_code;
            let mut mut_status_message = self.status_message.lock().unwrap();
            *mut_status_message = status_message.to_string();
            let mut num_on_result_calls = self.num_on_result_calls.lock().unwrap();
            *num_on_result_calls += 1;
            let mut report_number = self.report_number.lock().unwrap();
            *report_number = metrics_report.report_number;
            let mut uuid = self.uuid.lock().unwrap();
            *uuid = metrics_report.clone().metrics_config_uuid;
            let mut report = self.metrics_report.lock().unwrap();
            *report = Some(metrics_report);

            Ok(())
        }
    }

    /// This struct is defined to implement ITelemetryReportReadyListener AIDL interface for unit testing purposes
    #[derive(Clone)]
    struct MockTelemetryReportReadyListener {
        num_on_ready_report_calls: Arc<Mutex<i32>>,
        report_number: Arc<Mutex<u32>>,
        uuid: Arc<Mutex<String>>,
    }

    impl Interface for MockTelemetryReportReadyListener {}

    impl MockTelemetryReportReadyListener {
        fn new() -> Self {
            MockTelemetryReportReadyListener {
                num_on_ready_report_calls: Arc::new(Mutex::new(0)),
                report_number: Arc::new(Mutex::new(0)),
                uuid: Arc::new(Mutex::new(String::new())),
            }
        }

        fn get_num_on_ready_report_calls(&self) -> i32 {
            *self.num_on_ready_report_calls.lock().unwrap()
        }

        fn get_report_number(&self) -> u32 {
            *self.report_number.lock().unwrap()
        }

        fn get_uuid(&self) -> String {
            self.uuid.lock().unwrap().to_string()
        }
    }

    impl ITelemetryReportReadyListener for MockTelemetryReportReadyListener {
        fn onReady(&self, metrics_report_number: i32, report_uuid: &str) -> BinderResult<()> {
            let mut num_calls = self.num_on_ready_report_calls.lock().unwrap();
            *num_calls += 1;
            let mut report_number = self.report_number.lock().unwrap();
            *report_number = metrics_report_number as u32;
            let mut uuid = self.uuid.lock().unwrap();
            *uuid = String::from(report_uuid);
            Ok(())
        }
    }

    /// This struct is defined to implement ITelemetryStatusListener AIDL interface for unit testing purposes
    #[derive(Clone)]
    struct MockTelemetryStatusListener {
        num_send_status_calls: Arc<Mutex<i32>>,
        status: Arc<Mutex<i32>>,
    }

    impl Interface for MockTelemetryStatusListener {}

    impl MockTelemetryStatusListener {
        fn new() -> Self {
            MockTelemetryStatusListener {
                num_send_status_calls: Arc::new(Mutex::new(0)),
                status: Arc::new(Mutex::new(0)),
            }
        }

        pub fn get_num_send_status_calls(&self) -> i32 {
            *self.num_send_status_calls.lock().unwrap()
        }

        pub fn get_status_code(&self) -> i32 {
            *self.status.lock().unwrap()
        }
    }

    impl ITelemetryStatusListener for MockTelemetryStatusListener {
        fn sendStatus(
            &self,
            _api_call_id: i32,
            status_code: i32,
            _status_msg: &str,
        ) -> BinderResult<()> {
            let mut num_calls = self.num_send_status_calls.lock().unwrap();
            *num_calls += 1;

            let mut mut_status = self.status.lock().unwrap();
            *mut_status = status_code;

            Ok(())
        }
    }

    struct TestEnvironment {
        dir_path: String,
        service: SdvTelemetryService,
        status_listener: Option<MockTelemetryStatusListener>,
        num_initialize_config_calls: Arc<Mutex<i32>>,
        num_finish_config_calls: Arc<Mutex<i32>>,
        num_validate_config_calls: Arc<Mutex<i32>>,
    }

    impl TestEnvironment {
        // Test setup
        async fn new(dir_path: String) -> TestEnvironment {
            let num_initialize_config_calls = Arc::new(Mutex::new(0));
            let num_finish_config_calls = Arc::new(Mutex::new(0));
            let num_validate_config_calls = Arc::new(Mutex::new(0));

            let mut service = SdvTelemetryService::new(
                dir_path.as_str(),
                MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
                Arc::new(MockConfigurablePublisherRegistry {}),
            )
            .await
            .unwrap();
            service.metrics_config_orchestrator = Box::new(MockMetricsConfigOrchestratorImpl::new(
                num_initialize_config_calls.clone(),
                num_finish_config_calls.clone(),
                num_validate_config_calls.clone(),
            ));

            TestEnvironment {
                dir_path,
                service,
                status_listener: None,
                num_initialize_config_calls: num_initialize_config_calls.clone(),
                num_finish_config_calls: num_finish_config_calls.clone(),
                num_validate_config_calls: num_validate_config_calls.clone(),
            }
        }

        pub async fn set_mock_status_listener(&mut self) {
            let status_listener = MockTelemetryStatusListener::new();

            let binder_status_listener = BnTelemetryStatusListener::new_binder(
                status_listener.clone(),
                BinderFeatures::default(),
            );

            let _ = self.service.setStatusListener(&binder_status_listener).await;
            self.status_listener = Some(status_listener);

            assert_eq!(StatusCodes::SET_STATUS_LISTENER_SUCCEEDED.0, self.get_status_code());
        }

        pub fn get_status_code(&self) -> i32 {
            self.status_listener.as_ref().unwrap().get_status_code()
        }

        pub fn get_num_initialize_config_calls(&self) -> i32 {
            *self.num_initialize_config_calls.lock().unwrap()
        }

        pub fn get_num_finish_config_calls(&self) -> i32 {
            *self.num_finish_config_calls.lock().unwrap()
        }

        pub fn get_num_validate_config_calls(&self) -> i32 {
            *self.num_validate_config_calls.lock().unwrap()
        }
    }

    // Drop is executed as soon as the object goes out of scope.
    // Drop will work even in case of a panic.
    impl Drop for TestEnvironment {
        fn drop(&mut self) {
            // Removing all files created during the test
            fs::remove_dir_all(PathBuf::from(self.dir_path.to_string())).unwrap();
        }
    }

    fn create_serialized_config(uuid: &str) -> Vec<u8> {
        let mut config = MetricsConfig::new();
        config.uuid = uuid.to_string();
        config.version = 2;
        config.script = "Sample Script".to_string();

        info!("Serializing metric config");
        config.write_to_bytes().unwrap()
    }

    #[tokio::test]
    async fn add_metrics_config_validate_config_calls_received() {
        const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";

        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "add_metrics_config";
        let file_path =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, INACTIVE_CONFIG_DATA_DIR, UUID));

        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_status_listener().await;

        info!("Serializing metric config");
        let serialized_config = create_serialized_config(UUID);

        let _ = test_environment.service.addMetricsConfig(-1, &serialized_config).await;
        assert_eq!(StatusCodes::ADD_METRICS_CONFIG_SUCCEEDED.0, test_environment.get_status_code());
        assert!(file_path.is_file());
        assert_eq!(1, test_environment.get_num_validate_config_calls());
    }

    #[tokio::test]
    async fn activate_metrics_config_initialize_configs_calls_received() {
        const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";

        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "activate_metrics_config";
        let old_file_path =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, INACTIVE_CONFIG_DATA_DIR, UUID));
        let new_file_path =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, ACTIVE_CONFIG_DATA_DIR, UUID));

        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_status_listener().await;

        let serialized_config = create_serialized_config(UUID);
        test_environment.service.addMetricsConfig(-1, &serialized_config).await.unwrap();
        assert!(old_file_path.is_file());
        assert_eq!(1, test_environment.get_num_validate_config_calls());

        let _ = test_environment.service.activateMetricsConfig(-1, UUID).await;
        assert_eq!(
            StatusCodes::ACTIVATE_METRICS_CONFIG_SUCCEEDED.0,
            test_environment.get_status_code()
        );
        assert!(new_file_path.is_file());
        assert_eq!(1, test_environment.get_num_initialize_config_calls());
    }

    #[tokio::test]
    async fn deactivate_metrics_config_finish_config_calls_received() {
        const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";

        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "deactivate_metrics_config";
        let old_file_path =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, INACTIVE_CONFIG_DATA_DIR, UUID));
        let new_file_path =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, ACTIVE_CONFIG_DATA_DIR, UUID));

        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_status_listener().await;

        let serialized_config = create_serialized_config(UUID);
        test_environment.service.addMetricsConfig(-1, &serialized_config).await.unwrap();
        assert!(old_file_path.is_file());
        assert_eq!(1, test_environment.get_num_validate_config_calls());

        let _ = test_environment.service.activateMetricsConfig(-1, UUID).await;
        assert!(new_file_path.is_file());
        assert_eq!(1, test_environment.get_num_initialize_config_calls());

        let _ = test_environment.service.deactivateMetricsConfig(-1, UUID).await;
        assert_eq!(
            StatusCodes::DEACTIVATE_METRICS_CONFIG_SUCCEEDED.0,
            test_environment.get_status_code()
        );
        assert!(old_file_path.is_file());
        assert_eq!(1, test_environment.get_num_finish_config_calls());
    }

    #[tokio::test]
    async fn remove_metrics_config() {
        const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";

        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "remove_metrics_config";
        let old_file_path =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, INACTIVE_CONFIG_DATA_DIR, UUID));
        let new_file_path =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, ACTIVE_CONFIG_DATA_DIR, UUID));
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_status_listener().await;

        let serialized_config = create_serialized_config(UUID);
        test_environment.service.addMetricsConfig(-1, &serialized_config).await.unwrap();
        assert!(old_file_path.is_file());
        assert_eq!(1, test_environment.get_num_validate_config_calls());

        let _ = test_environment.service.activateMetricsConfig(-1, UUID).await;
        assert!(new_file_path.is_file());
        assert_eq!(1, test_environment.get_num_initialize_config_calls());

        let _ = test_environment.service.removeMetricsConfig(-1, UUID).await;
        assert_eq!(1, test_environment.get_num_finish_config_calls());
        assert_eq!(
            StatusCodes::REMOVE_METRICS_CONFIG_SUCCEEDED.0,
            test_environment.get_status_code()
        );
        assert!(!old_file_path.is_file());
        assert!(!new_file_path.is_file());
    }

    #[tokio::test]
    async fn remove_all_metrics_configs() {
        const UUID1: &str = "01bf0754-df95-46bb-bb35-522b6af66600";
        const UUID2: &str = "f47ac10b-58cc-4372-a567-0e02b2c3d479";

        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "remove_all_metrics_configs";
        let inactive_dir_path =
            PathBuf::from(format!("{}/{}", ROOT_PATH, INACTIVE_CONFIG_DATA_DIR));
        let active_dir_path = PathBuf::from(format!("{}/{}", ROOT_PATH, ACTIVE_CONFIG_DATA_DIR));

        let old_file_path1 =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, INACTIVE_CONFIG_DATA_DIR, UUID1));
        let new_file_path1 =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, ACTIVE_CONFIG_DATA_DIR, UUID1));
        let old_file_path2 =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, INACTIVE_CONFIG_DATA_DIR, UUID2));
        let new_file_path2 =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, ACTIVE_CONFIG_DATA_DIR, UUID2));
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_status_listener().await;

        let serialized_config1 = create_serialized_config(UUID1);
        let serialized_config2 = create_serialized_config(UUID2);

        // Add configs
        test_environment.service.addMetricsConfig(-1, &serialized_config1).await.unwrap();
        test_environment.service.addMetricsConfig(-1, &serialized_config2).await.unwrap();
        assert!(old_file_path1.is_file());
        assert!(old_file_path2.is_file());
        assert_eq!(2, test_environment.get_num_validate_config_calls());

        // Activate configs
        test_environment.service.activateMetricsConfig(-1, UUID1).await.unwrap();
        test_environment.service.activateMetricsConfig(-1, UUID2).await.unwrap();
        assert!(new_file_path1.is_file());
        assert!(new_file_path2.is_file());
        assert_eq!(2, test_environment.get_num_initialize_config_calls());

        // Remove all configs
        let _ = test_environment.service.removeAllMetricsConfigs(-1).await;
        assert_eq!(
            StatusCodes::REMOVE_ALL_METRICS_CONFIGS_SUCCEEDED.0,
            test_environment.get_status_code()
        );
        assert_eq!(active_dir_path.read_dir().unwrap().count(), 0); // check if directory is empty
        assert_eq!(inactive_dir_path.read_dir().unwrap().count(), 0); // check if directory is empty
        assert_eq!(2, test_environment.get_num_finish_config_calls());
    }

    #[tokio::test]
    async fn clear_report_ready_listener() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "clear_report_ready_listener";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_status_listener().await;

        let report_ready_listener = MockTelemetryReportReadyListener::new();

        let binder_report_ready_listener = BnTelemetryReportReadyListener::new_binder(
            report_ready_listener.clone(),
            BinderFeatures::default(),
        );

        let _ =
            test_environment.service.setReportReadyListener(&binder_report_ready_listener).await;

        assert_eq!(
            StatusCodes::SET_REPORT_READY_LISTENER_SUCCEEDED.0,
            test_environment.get_status_code()
        );

        let _ = test_environment.service.clearReportReadyListener().await;

        assert_eq!(
            StatusCodes::CLEAR_REPORT_READY_LISTENER_SUCCEEDED.0,
            test_environment.get_status_code()
        );
    }

    #[tokio::test]
    async fn set_report_ready_listener() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "set_report_ready_listener";

        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_status_listener().await;

        let report_ready_listener = MockTelemetryReportReadyListener::new();

        let binder_report_ready_listener = BnTelemetryReportReadyListener::new_binder(
            report_ready_listener.clone(),
            BinderFeatures::default(),
        );

        let _ =
            test_environment.service.setReportReadyListener(&binder_report_ready_listener).await;

        assert_eq!(
            StatusCodes::SET_REPORT_READY_LISTENER_SUCCEEDED.0,
            test_environment.get_status_code()
        );
    }

    #[tokio::test]
    async fn set_report_ready_listener_and_check_on_ready_calls() {
        const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";
        const REPORT_NUMBER: u32 = 42;

        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "check_on_ready_calls";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_status_listener().await;

        let report_ready_listener = MockTelemetryReportReadyListener::new();

        let binder_report_ready_listener = BnTelemetryReportReadyListener::new_binder(
            report_ready_listener.clone(),
            BinderFeatures::default(),
        );

        let _ =
            test_environment.service.setReportReadyListener(&binder_report_ready_listener).await;

        assert_eq!(
            StatusCodes::SET_REPORT_READY_LISTENER_SUCCEEDED.0,
            test_environment.get_status_code()
        );

        // Call onReady on report ready listener
        let _ = report_ready_listener.onReady(REPORT_NUMBER.try_into().unwrap(), UUID);
        assert_eq!(1, report_ready_listener.get_num_on_ready_report_calls());
        assert_eq!(REPORT_NUMBER, report_ready_listener.get_report_number());
        assert_eq!(UUID, &report_ready_listener.get_uuid());
    }

    #[tokio::test]
    async fn set_multiple_report_ready_listener_fails() {
        const ROOT_PATH: &str = "set_multiple_report_ready_listener";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_status_listener().await;

        let report_listener = BnTelemetryReportReadyListener::new_binder(
            MockTelemetryReportReadyListener::new(),
            BinderFeatures::default(),
        );

        let _ = test_environment.service.setReportReadyListener(&report_listener).await;

        assert_eq!(
            StatusCodes::SET_REPORT_READY_LISTENER_SUCCEEDED.0,
            test_environment.get_status_code()
        );

        let _ = test_environment.service.setReportReadyListener(&report_listener).await;
        assert_eq!(
            StatusCodes::SET_REPORT_READY_LISTENER_FAILED_LISTENER_EXISTS.0,
            test_environment.get_status_code()
        );
    }

    #[tokio::test]
    async fn set_status_listener_and_check_status_code() {
        const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "set_status_listener";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        let status_listener = MockTelemetryStatusListener::new();

        let binder_listener = BnTelemetryStatusListener::new_binder(
            status_listener.clone(),
            BinderFeatures::default(),
        );

        let serialized_config = create_serialized_config(UUID);
        let _ = test_environment.service.setStatusListener(&binder_listener).await;
        assert_eq!(StatusCodes::SET_STATUS_LISTENER_SUCCEEDED.0, status_listener.get_status_code());

        let _ = test_environment.service.addMetricsConfig(-1, &serialized_config).await;

        assert_eq!(2, status_listener.get_num_send_status_calls());
    }

    #[tokio::test]
    async fn set_multiple_status_listener_fails() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "set_multiple_status_listener";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        let status_listener = MockTelemetryStatusListener::new();

        let binder_listener = BnTelemetryStatusListener::new_binder(
            status_listener.clone(),
            BinderFeatures::default(),
        );

        let _ = test_environment.service.setStatusListener(&binder_listener).await;
        assert_eq!(StatusCodes::SET_STATUS_LISTENER_SUCCEEDED.0, status_listener.get_status_code());

        let _ = test_environment.service.setStatusListener(&binder_listener).await;
        assert_eq!(
            StatusCodes::SET_STATUS_LISTENER_FAILED_LISTENER_EXISTS.0,
            status_listener.get_status_code()
        );
    }

    #[tokio::test]
    async fn clear_status_listener() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "clear_status_listener";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_status_listener().await;

        let result = test_environment.service.clearStatusListener().await;
        assert!(result.is_ok());
        assert!(test_environment.service.status_listener.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn get_metrics_report() {
        const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";
        const REPORT_NUMBER: u32 = 42;

        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "get_metrics_report";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_status_listener().await;

        // Setting Report Ready Listener

        let report_ready_listener = MockTelemetryReportReadyListener::new();

        let binder_report_ready_listener = BnTelemetryReportReadyListener::new_binder(
            report_ready_listener.clone(),
            BinderFeatures::default(),
        );

        let _ =
            test_environment.service.setReportReadyListener(&binder_report_ready_listener).await;

        assert_eq!(
            StatusCodes::SET_REPORT_READY_LISTENER_SUCCEEDED.0,
            test_environment.get_status_code()
        );

        // Setting Report Listener

        let report_listener = MockTelemetryReportListener::new();

        let binder_report_listener = BnTelemetryReportListener::new_binder(
            report_listener.clone(),
            BinderFeatures::default(),
        );

        // Send report to metric report listener
        let mut report = MetricsReport::new();
        report.metrics_config_uuid = UUID.to_string();
        report.report_number = REPORT_NUMBER;

        // Call on_report_ready callback on metrics report listener
        test_environment.service.metrics_report_listener.on_report_ready(report);
        let _ = test_environment
            .service
            .getMetricsReport(REPORT_NUMBER.try_into().unwrap(), UUID, &binder_report_listener)
            .await;

        assert_eq!(StatusCodes::GET_FINISHED_REPORT_SUCCEEDED.0, report_listener.get_status_code());
        assert_eq!(UUID, report_listener.get_status_message());
        assert_eq!(1, report_ready_listener.get_num_on_ready_report_calls());
        assert_eq!(1, report_listener.get_num_on_result_calls());
        assert_eq!(UUID, report_listener.get_uuid());
        assert_eq!(REPORT_NUMBER, report_listener.get_report_number());
    }
}
