// Copyright 2023 Google LLC. All Rights Reserved.

//! This module implements the ITelemetryService AIDL interface

mod collection_extension;
mod file_io_utils;
mod metrics_config_orchestrator;

use android_sdv_telemetry::aidl::android::sdv::telemetry::{
    ITelemetryReportListener::ITelemetryReportListener,
    ITelemetryReportReadyListener::ITelemetryReportReadyListener,
    ITelemetryService::ITelemetryServiceAsyncServer,
    ITelemetryStatusListener::ITelemetryStatusListener,
};

use android_sdv_telemetry::binder::{Interface, Result as BinderResult, Strong};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use log::{error, info};
use sdvtelemetry_proto_rust::metrics_configuration::MetricsConfig;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::{self};
use std::marker::{Send, Sync};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use metrics_config_orchestrator::{MetricsConfigOrchestratorError, MetricsConfigOrchestratorImpl};

pub use metrics_config_orchestrator::MetricsConfigOrchestrator;

static ACTIVE_CONFIG_DATA_DIR: &str = "telemetry/active_configs";
static INACTIVE_CONFIG_DATA_DIR: &str = "telemetry/inactive_configs";

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
    /// Status Callabck to send back status to client
    status_listener: Mutex<Option<Strong<dyn ITelemetryStatusListener>>>,
}

impl Interface for SdvTelemetryService {}

impl SdvTelemetryService {
    /// Instantiates an instance of SdvTelemetryService
    pub async fn new(root_path: &str, config_store_size: usize) -> Option<Self> {
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

        let configs_store = Arc::new(Mutex::new(HashMap::new()));
        let config_orchestrator = Box::new(MetricsConfigOrchestratorImpl::new());

        let mut num_active_configs = 0;
        let mut num_inactive_configs = 0;
        // Needed to get the size of all files
        let mut total_size = 0;

        if active_dir_path.is_dir() {
            // Read the contents of each file in the directory into a byte array
            for entry_result in fs::read_dir(active_dir_path).unwrap() {
                let entry = entry_result.unwrap();
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
                    let config: MetricsConfig =
                        protobuf::Message::parse_from_bytes(&bytes).unwrap();
                    let uuid = &config.uuid;

                    // Validates and initializes metrics config
                    match config_orchestrator.initialize_config(&config) {
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
            status_listener: Mutex::new(None),
        })
    }

    /// Uses the callback object set by the client to send back status codes
    /// and error messages for API calls
    pub fn send_status<E: Debug>(
        &self,
        status_code: i32,
        result: Result<&str, E>,
    ) -> BinderResult<()> {
        if let Ok(callback) = self.status_listener.lock() {
            if let Some(callback_ref) = callback.as_ref() {
                let status_message = match result {
                    Ok(message) => message.to_owned(),
                    Err(e) => format!("{:?}", e),
                };
                //TODO: b/296147474 Create status codes to be shared with OEM client
                let _ = callback_ref.deref().sendStatus(status_code, &status_message);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ITelemetryServiceAsyncServer for SdvTelemetryService {
    async fn addMetricsConfig(&self, metrics_config: &[u8]) -> BinderResult<()> {
        let config: MetricsConfig = protobuf::Message::parse_from_bytes(metrics_config).unwrap();
        info!("addMetricsConfig: {:?} of size: {}\n", config, metrics_config.len());

        if metrics_config.len() + self.cur_persistent_config_store_size.load(Ordering::SeqCst)
            > self.max_persistent_config_store_size
        {
            error!("Memory limit exceeded");
            return self.send_status(-1, Err("Memory limit exceeded"));
        }

        let res = self.metrics_config_orchestrator.validate_config(&config);

        // Validates and initializes metrics config
        if res.is_err() {
            info!("Invalid Metric Config");
            return self.send_status(-1, Err(res));
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
            return self
                .send_status(-1, Err("Metric config activated but not persisted in memory"));
        }

        let mut metric_config_map = self.metrics_configs_store.lock().unwrap();
        // Storing the size of the metrics configs in metric_config_map to allow easy access
        // during remove metrics API call. self.cur_persistent_config_store_size needs to be
        // updated during each API call that modifies active metrics configs.
        // Alternate to metric_config_map would be read the file size from memory which constitutes
        // an IO operation and is inefficient.
        metric_config_map.insert(config.uuid.clone(), bytes.len());
        self.cur_persistent_config_store_size.fetch_add(bytes.len(), Ordering::SeqCst);

        return self.send_status::<MetricsConfigOrchestratorError>(1, Ok(&config.uuid));
    }

    async fn activateMetricsConfig(&self, metrics_config_uuid: &str) -> BinderResult<()> {
        info!("activateMetricsConfig: {:?} \n", metrics_config_uuid);
        let path = PathBuf::from(self.inactive_data_dir.clone() + "/" + metrics_config_uuid);
        if path.is_file() {
            let bytes = match file_io_utils::read_from_file(&path).await {
                Ok(data) => data,
                Err(e) => {
                    error!("An error occurred while trying to read file: {}", e);
                    return self.send_status(-1, Err(e));
                }
            };

            // bytes now contains the contents of the file
            let config: MetricsConfig = protobuf::Message::parse_from_bytes(&bytes).unwrap();
            let uuid = &config.uuid;

            // Initializes metrics config
            match self.metrics_config_orchestrator.initialize_config(&config) {
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
                    return self.send_status(-1, Err(e));
                }
            }
        } else {
            return self.send_status(-1, Err("Config not found"));
        }

        self.send_status::<MetricsConfigOrchestratorError>(1, Ok(metrics_config_uuid))
    }

    async fn deactivateMetricsConfig(&self, metrics_config_uuid: &str) -> BinderResult<()> {
        info!("deactivateMetricsConfig: {}", metrics_config_uuid);

        let res = self.metrics_config_orchestrator.finish_config(metrics_config_uuid);
        if res.is_err() {
            info!("Invalid Metric Config UUID");
            return self.send_status(-1, Err(res));
        }

        let old_path = PathBuf::from(self.active_data_dir.clone() + "/" + metrics_config_uuid);
        let new_path = PathBuf::from(self.inactive_data_dir.clone() + "/" + metrics_config_uuid);

        file_io_utils::rename_file(&old_path, &new_path)
            .await
            .unwrap_or_else(|e| error!("An error occurred in renaming config: {}", e));

        self.send_status::<MetricsConfigOrchestratorError>(1, Ok(metrics_config_uuid))
    }

    async fn removeMetricsConfig(&self, metrics_config_uuid: &str) -> BinderResult<()> {
        info!("removeMetricsConfig: {}", metrics_config_uuid);
        let uuid = String::from(metrics_config_uuid);
        let config_size = {
            let mut metric_config_map = self.metrics_configs_store.lock().unwrap();
            match metric_config_map.remove(&uuid) {
                Some(size) => size,
                None => {
                    error!("Config not found {:}", uuid);
                    return self.send_status(-1, Err("Config not found"));
                }
            }
        };

        let active_config = {
            let res = self.metrics_config_orchestrator.finish_config(metrics_config_uuid);
            if let Err(err) = res {
                if let MetricsConfigOrchestratorError::InvalidConfigUuid { uuid } = err {
                    info!("UUID {} doesn't exist", uuid);
                    false
                } else {
                    error!("Error in finishing Metric Config");
                    return self.send_status(-1, Err(err));
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
            return self.send_status(-1, Err("Could not remove config from memory"));
        }

        self.cur_persistent_config_store_size.fetch_sub(config_size, Ordering::SeqCst);
        self.send_status::<MetricsConfigOrchestratorError>(1, Ok(metrics_config_uuid))
    }

    async fn removeAllMetricsConfigs(&self) -> BinderResult<()> {
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

        let mut metric_config_map = self.metrics_configs_store.lock().unwrap();
        for uuid in metric_config_map.keys() {
            let res = self.metrics_config_orchestrator.finish_config(uuid);
            if let Err(err) = res {
                if let MetricsConfigOrchestratorError::InvalidConfigUuid { uuid } = err {
                    info!("UUID {} doesn't exist or is not active", uuid);
                } else {
                    error!("Error in finishing Metric Config {:?}", err);
                }
            }
        }

        metric_config_map.clear();

        self.cur_persistent_config_store_size.store(0, Ordering::SeqCst);
        self.send_status::<MetricsConfigOrchestratorError>(1, Ok("Ok"))
    }

    async fn setReportReadyListener(
        &self,
        listener: &Strong<dyn ITelemetryReportReadyListener>,
    ) -> BinderResult<()> {
        info!("setReportReadyListener");
        let _ = listener.onReady("Received CB from Telemetry Binder service");
        self.send_status::<MetricsConfigOrchestratorError>(1, Ok("Ok"))
    }

    async fn setStatusListener(
        &self,
        callback: &Strong<dyn ITelemetryStatusListener>,
    ) -> BinderResult<()> {
        if let Ok(mut guard) = self.status_listener.lock() {
            if guard.is_none() {
                *guard = Some(callback.clone());
            } else {
                return self.send_status(-1, Err("Status Listener already added"));
            }
        }

        Ok(())
    }

    #[allow(unused_variables)]
    async fn getFinishedReport(
        &self,
        metrics_config_uuid: &str,
        listener: &Strong<(dyn ITelemetryReportListener)>,
    ) -> BinderResult<()> {
        info!("getFinishedReport: {}", metrics_config_uuid);
        self.send_status::<MetricsConfigOrchestratorError>(1, Ok(metrics_config_uuid))
    }

    async fn getAllFinishedReports(
        &self,
        _listener: &Strong<dyn ITelemetryReportListener>,
    ) -> BinderResult<()> {
        info!("getAllFinishedReports");
        self.send_status::<MetricsConfigOrchestratorError>(1, Ok("Ok"))
    }

    async fn clearReportReadyListener(&self) -> BinderResult<()> {
        info!("clearReportReadyListener");
        self.send_status::<MetricsConfigOrchestratorError>(1, Ok("Ok"))
    }

    async fn clearStatusListener(&self) -> BinderResult<()> {
        info!("clearReportReadyListener");
        if let Ok(mut guard) = self.status_listener.lock() {
            *guard = None;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    extern crate protobuf;
    use super::*;
    use binder::ParcelFileDescriptor;
    use protobuf::Message;
    use sdvtelemetry_proto_rust::metrics_configuration::MetricsConfig as MetricsConfigProto;

    const MAX_PERSISTENT_CONFIGURATION_SIZE_MB: usize = 10 * 1024 * 1024; // 10Mbs

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

    impl MetricsConfigOrchestrator for MockMetricsConfigOrchestratorImpl {
        fn initialize_config(
            &self,
            _: &MetricsConfigProto,
        ) -> Result<(), MetricsConfigOrchestratorError> {
            let mut num_calls = self.num_initialize_config_calls.lock().unwrap();
            *num_calls += 1;
            Ok(())
        }

        fn finish_config(&self, _: &str) -> Result<(), MetricsConfigOrchestratorError> {
            let mut num_calls = self.num_finish_config_calls.lock().unwrap();
            *num_calls += 1;
            Ok(())
        }

        fn validate_config(
            &self,
            _: &MetricsConfigProto,
        ) -> Result<(), MetricsConfigOrchestratorError> {
            let mut num_calls = self.num_validate_config_calls.lock().unwrap();
            *num_calls += 1;
            Ok(())
        }
    }

    struct TestEnvironment {
        dir_path: String,
        service: SdvTelemetryService,
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

            let mut service =
                SdvTelemetryService::new(dir_path.as_str(), MAX_PERSISTENT_CONFIGURATION_SIZE_MB)
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
                num_initialize_config_calls: num_initialize_config_calls.clone(),
                num_finish_config_calls: num_finish_config_calls.clone(),
                num_validate_config_calls: num_validate_config_calls.clone(),
            }
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
            // Run a future to completion on the current thread.
            futures::executor::block_on(file_io_utils::remove_dir(&PathBuf::from(
                self.dir_path.to_string(),
            )))
            .unwrap();
        }
    }

    /// This struct is defined to implement ITelemetryReportListener AIDL interface for unit testing purposes
    struct TelemetryReportListener;

    impl Interface for TelemetryReportListener {}

    impl ITelemetryReportListener for TelemetryReportListener {
        fn onResult(
            &self,
            _metrics_config_name: &str,
            _parcel: &ParcelFileDescriptor,
            _err: &[u8],
            _status: i32,
        ) -> BinderResult<()> {
            info!("onResult");
            Ok(())
        }
    }

    /// This struct is defined to implement ITelemetryReportReadyListener AIDL interface for unit testing purposes
    struct TelemetryReportReadyListener;

    impl Interface for TelemetryReportReadyListener {}

    impl ITelemetryReportReadyListener for TelemetryReportReadyListener {
        fn onReady(&self, _metrics_config_name: &str) -> BinderResult<()> {
            info!("onResult");
            Ok(())
        }
    }

    #[tokio::test]
    async fn add_metrics_config() {
        const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";

        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "add_metrics_config";
        let file_path =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, INACTIVE_CONFIG_DATA_DIR, UUID));

        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        let mut config = MetricsConfig::new();
        config.uuid = UUID.to_string();
        config.version = 2;
        config.script = "Sample Script".to_string();

        info!("Serializing metric config");
        let serialized_config: &[u8] = &config.write_to_bytes().unwrap();

        let result = test_environment.service.addMetricsConfig(serialized_config).await;
        assert!(result.is_ok());
        assert!(file_path.is_file());
        assert_eq!(1, test_environment.get_num_validate_config_calls());
    }

    #[tokio::test]
    async fn activate_metrics_config() {
        const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";

        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "activate_metrics_config";
        let old_file_path =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, INACTIVE_CONFIG_DATA_DIR, UUID));
        let new_file_path =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, ACTIVE_CONFIG_DATA_DIR, UUID));
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        let mut config = MetricsConfig::new();
        config.uuid = UUID.to_owned();
        test_environment.service.addMetricsConfig(&config.write_to_bytes().unwrap()).await.unwrap();
        assert!(old_file_path.is_file());
        assert_eq!(1, test_environment.get_num_validate_config_calls());

        let result = test_environment.service.activateMetricsConfig(UUID).await;
        assert!(result.is_ok());
        assert!(new_file_path.is_file());
        assert_eq!(1, test_environment.get_num_initialize_config_calls());
    }

    #[tokio::test]
    async fn deactivate_metrics_config() {
        const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";

        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "deactivate_metrics_config";
        let old_file_path =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, INACTIVE_CONFIG_DATA_DIR, UUID));
        let new_file_path =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, ACTIVE_CONFIG_DATA_DIR, UUID));
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        let mut config = MetricsConfig::new();
        config.uuid = UUID.to_owned();
        test_environment.service.addMetricsConfig(&config.write_to_bytes().unwrap()).await.unwrap();
        assert!(old_file_path.is_file());
        assert_eq!(1, test_environment.get_num_validate_config_calls());

        let result = test_environment.service.activateMetricsConfig(UUID).await;
        assert!(result.is_ok());
        assert!(new_file_path.is_file());
        assert_eq!(1, test_environment.get_num_initialize_config_calls());

        let result = test_environment.service.deactivateMetricsConfig(UUID).await;
        assert!(result.is_ok());
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
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        let mut config = MetricsConfig::new();
        config.uuid = UUID.to_owned();
        test_environment.service.addMetricsConfig(&config.write_to_bytes().unwrap()).await.unwrap();
        assert!(old_file_path.is_file());
        assert_eq!(1, test_environment.get_num_validate_config_calls());

        let result = test_environment.service.activateMetricsConfig(UUID).await;
        assert!(result.is_ok());
        assert!(new_file_path.is_file());
        assert_eq!(1, test_environment.get_num_initialize_config_calls());

        let result = test_environment.service.removeMetricsConfig(UUID).await;
        assert_eq!(1, test_environment.get_num_finish_config_calls());
        assert!(result.is_ok());
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
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        let mut config1 = MetricsConfig::new();
        let mut config2 = MetricsConfig::new();
        config1.uuid = UUID1.to_owned();
        config2.uuid = UUID2.to_owned();

        // Add configs
        test_environment
            .service
            .addMetricsConfig(&config1.write_to_bytes().unwrap())
            .await
            .unwrap();
        test_environment
            .service
            .addMetricsConfig(&config2.write_to_bytes().unwrap())
            .await
            .unwrap();
        assert!(old_file_path1.is_file());
        assert!(old_file_path2.is_file());
        assert_eq!(2, test_environment.get_num_validate_config_calls());

        // Activate configs
        test_environment.service.activateMetricsConfig(UUID1).await.unwrap();
        test_environment.service.activateMetricsConfig(UUID2).await.unwrap();
        assert!(new_file_path1.is_file());
        assert!(new_file_path2.is_file());
        assert_eq!(2, test_environment.get_num_initialize_config_calls());

        // Remove all configs
        let result = test_environment.service.removeAllMetricsConfigs().await;
        assert!(result.is_ok());
        assert_eq!(active_dir_path.read_dir().unwrap().count(), 0); // check if directory is empty
        assert_eq!(inactive_dir_path.read_dir().unwrap().count(), 0); // check if directory is empty
        assert_eq!(2, test_environment.get_num_finish_config_calls());
    }

    #[tokio::test]
    async fn clear_report_ready_listener() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "clear_repot_ready_listener";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        let result = test_environment.service.clearReportReadyListener().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn set_report_ready_listener() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "set_report_ready_listener";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        let listener: Strong<dyn ITelemetryReportReadyListener> =
            Strong::new(Box::new(TelemetryReportReadyListener));

        let result = test_environment.service.setReportReadyListener(&listener).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn get_finished_report() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "get_finished_report";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        let listener: Strong<dyn ITelemetryReportListener> =
            Strong::new(Box::new(TelemetryReportListener));

        let result = test_environment.service.getFinishedReport("TestConfig", &listener).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn get_all_finished_report() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "get_all_finished_report";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        let listener: Strong<dyn ITelemetryReportListener> =
            Strong::new(Box::new(TelemetryReportListener));

        let result = test_environment.service.getAllFinishedReports(&listener).await;
        assert!(result.is_ok());
    }
}
