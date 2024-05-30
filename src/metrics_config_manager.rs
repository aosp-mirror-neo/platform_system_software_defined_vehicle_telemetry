// Copyright 2023 Google LLC. All Rights Reserved.

// External dependencies.
use async_trait::async_trait;
use log::{error, info};

use google_sdv_telemetry::aidl::google::sdv::telemetry::TelemetryServiceStatusCodes::TelemetryServiceStatusCodes as StatusCodes;

use sdv_telemetry_utils::file_io_utils;
use sdvtelemetry_proto_rust::metrics_configuration::{MetricsConfig, MetricsReport};
use std::collections::{HashMap, HashSet};
use std::fs::{self};
use std::marker::{Send, Sync};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use thiserror::Error;
use tokio::io;

// Crate module dependencies.
use crate::metrics_config_orchestrator::{
    MetricsConfigOrchestrator, MetricsConfigOrchestratorError as ConfigOrchestratorError,
    MetricsConfigOrchestratorImpl,
};
use crate::service_publisher_accessor::ServicePublisherAccessor;

/// Interface of a service that manages Metrics Configurations.
#[async_trait]
pub trait MetricsConfigManager {
    /// Adds/Persists metrics config in memory
    async fn add_metrics_config(
        &self,
        metrics_config: &[u8],
    ) -> Result<String, MetricsConfigManagerError>;

    /// Changes the given inactive metrics config to active
    async fn activate_metrics_config(
        &self,
        metrics_config_uuid: &str,
    ) -> Result<(), MetricsConfigManagerError>;

    /// Changes the given active metrics config to inactive
    async fn deactivate_metrics_config(
        &self,
        metrics_config_uuid: &str,
    ) -> Result<(), MetricsConfigManagerError>;

    /// Removes metrics config with the given uuid from memory
    async fn remove_metrics_config(
        &self,
        metrics_config_uuid: &str,
    ) -> Result<(), MetricsConfigManagerError>;

    /// Removes all metrics config from memory
    async fn remove_all_metrics_configs(&self) -> Result<(), MetricsConfigManagerError>;

    /// Get all metrics configs in active state
    async fn get_active_metrics_config_uuids(&self) -> Vec<String>;

    /// Get all metrics configs in inactive state
    async fn get_inactive_metrics_config_uuids(&self) -> Vec<String>;
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum MetricsConfigManagerError {
    #[error("Config Orchestrator Error: {error}")]
    ConfigOrchestratorError {
        #[from]
        error: ConfigOrchestratorError,
    },
    #[error("File IO Error: {error}")]
    FileIOError {
        #[from]
        error: io::Error,
    },
    #[error("Metric Config with UUID {uuid} doesn't exist")]
    InvalidConfigUuid { uuid: String },
    #[error("Error in reading metrics config from memory: {error}")]
    ConfigReadError { error: io::Error },
    #[error("Error in removing metrics config from memory: {error}")]
    ConfigRemoveError { error: io::Error },
    #[error("Error in parsing metrics config")]
    ConfigParseError,
    #[error("Error in persisting metrics config to memory: {error}")]
    ConfigWriteError { error: io::Error },
    #[error("Preallocated Persistence Storage Exceeded")]
    PersistentStorageLimitExceeded,
}

pub(super) struct MetricsConfigManagerImpl {
    active_data_dir: String,
    inactive_data_dir: String,
    cur_persistent_config_store_size: AtomicUsize,
    max_persistent_config_store_size: usize,
    /// Instance of Metrics Configuration Orchestrator.
    metrics_config_orchestrator: Arc<dyn MetricsConfigOrchestrator + Send + Sync>,
    /// Hashmap to store the metricsConfigs's size passed by the client.
    metrics_configs_store: Mutex<HashMap<String, usize>>,
}

/// Subscriber struct impl that encapsulates the data required for reading messages.
impl MetricsConfigManagerImpl {
    /// Creates a new instance of the struct.
    pub async fn new(
        active_data_dir: String,
        inactive_data_dir: String,
        config_store_size: usize,
        service_publisher_manager: Arc<dyn ServicePublisherAccessor>,
        report_ready_callback: impl Fn(MetricsReport) + Send + Sync + 'static,
        misc_error_callback: impl Fn(StatusCodes, String) + Send + Sync + 'static,
    ) -> Result<Arc<Self>, MetricsConfigManagerError> {
        let handle_file_io_error = |misc_error_callback: &dyn Fn(StatusCodes, String),
                                    err_code: StatusCodes,
                                    err_msg: &str| {
            error!("{}", err_msg);
            misc_error_callback(err_code, err_msg.to_string());
        };

        let config_manager = Arc::new_cyclic(
            |manager_reference: &Weak<MetricsConfigManagerImpl>| {
                let manager_reference_clone = Weak::clone(manager_reference);
                let config_orchestrator = Arc::new(MetricsConfigOrchestratorImpl::new(
                    service_publisher_manager,
                    move |config_uuid| {
                        if let Some(manager) = manager_reference_clone.upgrade() {
                            tokio::spawn(async move {
                                manager
                                    .deactivate_metrics_config(&config_uuid)
                                    .await
                                    .unwrap_or_else(|e| {
                                        if let MetricsConfigManagerError::InvalidConfigUuid{..} = e  {
                                            // In theory it's possible that by the time the task is executed
                                            // the OEM service manually finishes the config
                                            info!("Unable to deactivate config. Invalid Config UUID: {}", e)
                                        } else {
                                            error!("Unable to deactivate config. Reported error: {}", e)
                                        }
                                    });
                            });
                        }
                    },
                    report_ready_callback,
                ));

                MetricsConfigManagerImpl {
                    active_data_dir: active_data_dir.clone(),
                    inactive_data_dir: inactive_data_dir.clone(),
                    cur_persistent_config_store_size: AtomicUsize::new(0),
                    max_persistent_config_store_size: config_store_size,
                    metrics_config_orchestrator: config_orchestrator,
                    metrics_configs_store: Mutex::new(HashMap::new()),
                }
            },
        );

        // Activate configs that are stored in the active directory.
        let active_dir_path = Path::new(&active_data_dir);
        let inactive_dir_path = Path::new(&inactive_data_dir);

        let configs_store = &config_manager.metrics_configs_store;
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
                            let err = format!("Error {} in reading file {:?} from disk", e, entry);
                            handle_file_io_error(
                                &misc_error_callback,
                                StatusCodes::FILE_IO_ERROR,
                                &err,
                            );
                            continue;
                        }
                    };

                    // bytes now contains the contents of the file
                    let config: MetricsConfig = match protobuf::Message::parse_from_bytes(&bytes) {
                        Ok(cfg) => cfg,
                        Err(e) => {
                            let err = format!(
                                "Error {} in parsing metric config {:?} read from disk",
                                e, entry
                            );
                            handle_file_io_error(
                                &misc_error_callback,
                                StatusCodes::METRICS_CONFIG_PARSE_ERROR,
                                &err,
                            );
                            continue;
                        }
                    };
                    let uuid = &config.uuid;

                    // Validates and initializes metrics config
                    match Arc::clone(&config_manager.metrics_config_orchestrator)
                        .initialize_config(&config)
                        .await
                    {
                        Ok(()) => {
                            let mut metric_config_map = configs_store.lock().unwrap();
                            metric_config_map.insert(uuid.clone(), bytes.len());
                            num_active_configs += 1;
                            total_size += bytes.len();
                        }
                        Err(e) => {
                            let err = format!(
                                "Error {} in initializing metric config {:?} read from disk",
                                e, entry
                            );
                            handle_file_io_error(
                                &misc_error_callback,
                                StatusCodes::ACTIVATE_METRICS_CONFIG_FAILED,
                                &err,
                            );

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
                    let err = format!("Error {} in creating active config directory", e);
                    handle_file_io_error(&misc_error_callback, StatusCodes::FILE_IO_ERROR, &err);
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
                    let err = format!("Metric config corrupted {:?}", entry);
                    handle_file_io_error(
                        &misc_error_callback,
                        StatusCodes::METRICS_CONFIG_CORRUPTED,
                        &err,
                    );

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
                    let err = format!("Error {} in creating active config directory", e);
                    handle_file_io_error(&misc_error_callback, StatusCodes::FILE_IO_ERROR, &err);
                }
            };
        }

        config_manager.cur_persistent_config_store_size.store(total_size, Ordering::SeqCst);

        info!(
            "Telemetry service initialized with {:} active configs and {:} inactive configs
            occupying {} memory",
            num_active_configs, num_inactive_configs, total_size
        );

        Ok(config_manager)
    }
}

#[async_trait]
impl MetricsConfigManager for MetricsConfigManagerImpl {
    async fn add_metrics_config(
        &self,
        metrics_config: &[u8],
    ) -> Result<String, MetricsConfigManagerError> {
        let config: MetricsConfig = protobuf::Message::parse_from_bytes(metrics_config)
            .map_err(|_| MetricsConfigManagerError::ConfigParseError)?;
        info!("adding config: {:?} of size: {}\n", config, metrics_config.len());

        if metrics_config.len() + self.cur_persistent_config_store_size.load(Ordering::SeqCst)
            > self.max_persistent_config_store_size
        {
            return Err(MetricsConfigManagerError::PersistentStorageLimitExceeded);
        }

        // Validate the metrics config and report any errors to client
        self.metrics_config_orchestrator.validate_config(&config)?;

        let path = PathBuf::from(self.inactive_data_dir.clone() + "/" + &config.uuid);

        if let Err(e) = file_io_utils::write_to_file(&path, metrics_config).await {
            error!("An error occurred while writing to file: {}", e);
            if path.is_file() {
                file_io_utils::remove_file(&path).await.unwrap_or_else(|e| {
                    error!("An error occurred in removing corrupt config: {}", e)
                });
            }
            return Err(MetricsConfigManagerError::ConfigWriteError { error: e });
        }

        // Storing the size of the metrics configs in metric_config_map to allow easy access
        // during remove metrics API call. self.cur_persistent_config_store_size needs to be
        // updated during each API call that modifies active metrics configs.
        // Alternate to metric_config_map would be read the file size from memory which constitutes
        // an IO operation and is inefficient.
        self.metrics_configs_store
            .lock()
            .unwrap()
            .insert(config.uuid.clone(), metrics_config.len());
        self.cur_persistent_config_store_size.fetch_add(metrics_config.len(), Ordering::SeqCst);

        Ok(config.uuid)
    }

    async fn activate_metrics_config(
        &self,
        metrics_config_uuid: &str,
    ) -> Result<(), MetricsConfigManagerError> {
        info!("activate_metrics_config: {:?} \n", metrics_config_uuid);
        let path = PathBuf::from(self.inactive_data_dir.clone() + "/" + metrics_config_uuid);
        if path.is_file() {
            let bytes = file_io_utils::read_from_file(&path)
                .await
                .map_err(|e| MetricsConfigManagerError::ConfigReadError { error: e })?;

            // bytes now contains the contents of the file
            let config: MetricsConfig = protobuf::Message::parse_from_bytes(&bytes)
                .map_err(|_| MetricsConfigManagerError::ConfigParseError)?;
            let uuid = &config.uuid;
            info!("initializing {} Config", uuid);
            // Initializes metrics config
            match Arc::clone(&self.metrics_config_orchestrator).initialize_config(&config).await {
                Ok(()) => {
                    info!("Metrics config orchestrator successfully initialized {} config", uuid);
                    let new_path = PathBuf::from(self.active_data_dir.clone() + "/" + uuid);
                    file_io_utils::rename_file(&path, &new_path).await.unwrap_or_else(|e| {
                        error!(
                            "An error occurred in renaming config to {}: {}",
                            new_path.display(),
                            e
                        )
                    });
                }
                Err(e) => {
                    error!("Error {} in initializing {} Config", e, uuid);
                    file_io_utils::remove_file(&path).await.unwrap_or_else(|e| {
                        error!("An error occurred in removing corrupt config: {}", e)
                    });
                    return Err(MetricsConfigManagerError::ConfigOrchestratorError { error: e });
                }
            }
        } else {
            return Err(MetricsConfigManagerError::InvalidConfigUuid {
                uuid: metrics_config_uuid.to_string(),
            });
        }

        Ok(())
    }

    async fn deactivate_metrics_config(
        &self,
        metrics_config_uuid: &str,
    ) -> Result<(), MetricsConfigManagerError> {
        info!("deactivate_metrics_config: {}", metrics_config_uuid);

        self.metrics_config_orchestrator.finish_config(metrics_config_uuid).await.map_err(
            |_| MetricsConfigManagerError::InvalidConfigUuid {
                uuid: metrics_config_uuid.to_string(),
            },
        )?;

        let old_path = PathBuf::from(self.active_data_dir.clone() + "/" + metrics_config_uuid);
        let new_path = PathBuf::from(self.inactive_data_dir.clone() + "/" + metrics_config_uuid);

        // TODO: b/299329266: Handle file io errors gracefully
        file_io_utils::rename_file(&old_path, &new_path)
            .await
            .unwrap_or_else(|e| error!("An error occurred in renaming config: {}", e));

        Ok(())
    }

    async fn remove_metrics_config(
        &self,
        metrics_config_uuid: &str,
    ) -> Result<(), MetricsConfigManagerError> {
        info!("remove_metrics_config: {}", metrics_config_uuid);
        let uuid = metrics_config_uuid.to_string();
        let config_size = {
            let mut metric_config_map = self.metrics_configs_store.lock().unwrap();
            metric_config_map.remove(&uuid).ok_or_else(|| {
                MetricsConfigManagerError::InvalidConfigUuid {
                    uuid: metrics_config_uuid.to_string(),
                }
            })?
        };

        let active_config = {
            let res = self.metrics_config_orchestrator.finish_config(metrics_config_uuid).await;
            if let Err(err) = res {
                if let ConfigOrchestratorError::InvalidConfigUuid { .. } = err {
                    info!("UUID {} doesn't exist or is inactive", uuid);
                    false
                } else {
                    return Err(MetricsConfigManagerError::ConfigOrchestratorError { error: err });
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

        file_io_utils::remove_file(&path)
            .await
            .map_err(|e| MetricsConfigManagerError::ConfigRemoveError { error: e })?;

        self.cur_persistent_config_store_size.fetch_sub(config_size, Ordering::SeqCst);

        Ok(())
    }

    async fn remove_all_metrics_configs(&self) -> Result<(), MetricsConfigManagerError> {
        info!("remove_all_metrics_configs");

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
            let metric_config_map = mem::take(&mut *self.metrics_configs_store.lock().unwrap());
            for uuid in metric_config_map.keys() {
                let res = self.metrics_config_orchestrator.finish_config(uuid).await;
                if let Err(err) = res {
                    if let ConfigOrchestratorError::InvalidConfigUuid { .. } = err {
                        info!("UUID {} doesn't exist or is not active", uuid);
                    } else {
                        error!("Error in finishing Metric Config {:?}", err);
                    }
                }
            }
        }

        self.cur_persistent_config_store_size.store(0, Ordering::SeqCst);
        Ok(())
    }

    async fn get_active_metrics_config_uuids(&self) -> Vec<String> {
        info!("get_active_metrics_config_uuids");

        self.metrics_config_orchestrator.get_active_metrics_config_uuids()
    }

    async fn get_inactive_metrics_config_uuids(&self) -> Vec<String> {
        info!("get_active_metrics_config_uuids");

        let active_configs = self.metrics_config_orchestrator.get_active_metrics_config_uuids();
        let metrics_config_store_lock = self.metrics_configs_store.lock().unwrap();
        let all_configs_set = metrics_config_store_lock.keys().collect::<HashSet<_>>();

        let inactive_configs = all_configs_set
            .difference(&active_configs.iter().collect())
            .map(|uuid| uuid.to_string())
            .collect::<Vec<_>>();

        inactive_configs
    }
}
#[cfg(test)]
pub(crate) mod mock;
#[cfg(test)]
mod tests {

    use crate::service_publisher_accessor::mock::MockServicePublisherAccessor;
    use protobuf::Message;
    use rand::prelude::SliceRandom;
    use sdvtelemetry_proto_rust::metrics_configuration::MetricsConfig as MetricsConfigProto;
    use tokio::sync::oneshot;

    use super::*;

    static ACTIVE_CONFIGS_DATA_DIR: &str = "telemetry/active_configs";
    static INACTIVE_CONFIGS_DATA_DIR: &str = "telemetry/inactive_configs";
    const MAX_PERSISTENT_CONFIGURATION_SIZE_MB: usize = 10 * 1024 * 1024; // 10Mbs
    const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";

    struct MockMetricsConfigOrchestratorImpl {
        num_initialize_config_calls: Arc<Mutex<i32>>,
        num_finish_config_calls: Arc<Mutex<i32>>,
        num_validate_config_calls: Arc<Mutex<i32>>,
        num_get_active_config_calls: Arc<Mutex<i32>>,
        configs: Mutex<Vec<String>>,
        /// When set, the error will be returned on the next api call
        pub next_error: Mutex<Option<ConfigOrchestratorError>>,
    }

    impl MockMetricsConfigOrchestratorImpl {
        fn new(
            num_initialize_config_calls: Arc<Mutex<i32>>,
            num_finish_config_calls: Arc<Mutex<i32>>,
            num_validate_config_calls: Arc<Mutex<i32>>,
            num_get_active_config_calls: Arc<Mutex<i32>>,
        ) -> Self {
            Self {
                num_initialize_config_calls,
                num_finish_config_calls,
                num_validate_config_calls,
                num_get_active_config_calls,
                configs: Vec::new().into(),
                next_error: None.into(),
            }
        }

        pub(crate) fn populate_active_configs(&self, config_uuids: Vec<String>) {
            *self.configs.lock().unwrap() = config_uuids;
        }
    }

    #[async_trait]
    impl MetricsConfigOrchestrator for MockMetricsConfigOrchestratorImpl {
        async fn initialize_config(
            &self,
            _: &MetricsConfigProto,
        ) -> Result<(), ConfigOrchestratorError> {
            let mut num_calls = self.num_initialize_config_calls.lock().unwrap();
            *num_calls += 1;

            if let Some(err) = self.next_error.lock().unwrap().take() {
                return Err(err);
            }

            Ok(())
        }

        async fn finish_config(&self, _: &str) -> Result<(), ConfigOrchestratorError> {
            let mut num_calls = self.num_finish_config_calls.lock().unwrap();
            *num_calls += 1;

            if let Some(err) = self.next_error.lock().unwrap().take() {
                return Err(err);
            }

            Ok(())
        }

        fn validate_config(&self, _: &MetricsConfigProto) -> Result<(), ConfigOrchestratorError> {
            let mut num_calls = self.num_validate_config_calls.lock().unwrap();
            *num_calls += 1;

            if let Some(err) = self.next_error.lock().unwrap().take() {
                return Err(err);
            }

            Ok(())
        }

        fn get_active_metrics_config_uuids(&self) -> Vec<String> {
            *self.num_get_active_config_calls.lock().unwrap() += 1;

            let active_configs = self.configs.lock().unwrap().clone().into_iter().collect();
            active_configs
        }
    }

    struct TestEnvironment {
        root_path: String,
        manager: Arc<dyn MetricsConfigManager>,
        metrics_config_orchestrator: Arc<MockMetricsConfigOrchestratorImpl>,
        num_initialize_config_calls: Arc<Mutex<i32>>,
        num_finish_config_calls: Arc<Mutex<i32>>,
        num_validate_config_calls: Arc<Mutex<i32>>,
        num_get_active_config_calls: Arc<Mutex<i32>>,
    }

    impl TestEnvironment {
        // Test setup
        async fn new(
            root_path: String,
            max_persistent_config_store_size: usize,
        ) -> TestEnvironment {
            let (active_data_dir, inactive_data_dir) = if !root_path.is_empty() {
                (
                    format!("{}/{}", root_path, ACTIVE_CONFIGS_DATA_DIR),
                    format!("{}/{}", root_path, INACTIVE_CONFIGS_DATA_DIR),
                )
            } else {
                (ACTIVE_CONFIGS_DATA_DIR.to_owned(), INACTIVE_CONFIGS_DATA_DIR.to_owned())
            };

            let num_initialize_config_calls = Arc::new(Mutex::new(0));
            let num_finish_config_calls = Arc::new(Mutex::new(0));
            let num_validate_config_calls = Arc::new(Mutex::new(0));
            let num_get_active_config_calls = Arc::new(Mutex::new(0));

            let metrics_config_orchestrator = Arc::new(MockMetricsConfigOrchestratorImpl::new(
                Arc::clone(&num_initialize_config_calls),
                Arc::clone(&num_finish_config_calls),
                Arc::clone(&num_validate_config_calls),
                Arc::clone(&num_get_active_config_calls),
            ));

            // Used to initialize/create root directories
            MetricsConfigManagerImpl::new(
                active_data_dir.clone(),
                inactive_data_dir.clone(),
                max_persistent_config_store_size,
                Arc::new(MockServicePublisherAccessor::new()),
                |_| {},
                |_, _| {},
            )
            .await
            .unwrap();

            // Used to assign MockMetricsConfigOrchestrator
            let manager = Arc::new(MetricsConfigManagerImpl {
                active_data_dir,
                inactive_data_dir,
                cur_persistent_config_store_size: AtomicUsize::new(0),
                max_persistent_config_store_size,
                metrics_config_orchestrator: Arc::<MockMetricsConfigOrchestratorImpl>::clone(
                    &metrics_config_orchestrator,
                ),
                metrics_configs_store: HashMap::new().into(),
            });

            TestEnvironment {
                root_path,
                manager,
                metrics_config_orchestrator,
                num_initialize_config_calls: Arc::clone(&num_initialize_config_calls),
                num_finish_config_calls: Arc::clone(&num_finish_config_calls),
                num_validate_config_calls: Arc::clone(&num_validate_config_calls),
                num_get_active_config_calls: Arc::clone(&num_get_active_config_calls),
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

        pub fn get_num_get_active_config_calls(&self) -> i32 {
            *self.num_get_active_config_calls.lock().unwrap()
        }
    }

    // Drop is executed as soon as the object goes out of scope.
    // Drop will work even in case of a panic.
    impl Drop for TestEnvironment {
        fn drop(&mut self) {
            // Removing all files created during the test
            fs::remove_dir_all(PathBuf::from(self.root_path.to_string())).unwrap();
        }
    }

    fn create_serialized_config(uuid: &str) -> Vec<u8> {
        let mut config = MetricsConfig::new();
        config.uuid = uuid.to_string();
        config.version = 2;

        config.write_to_bytes().unwrap()
    }

    fn create_corrupt_config(uuid: &str) -> Vec<u8> {
        let mut config = MetricsConfig::new();
        config.uuid = uuid.to_string();
        config.version = 2;

        let mut data = config.write_to_bytes().unwrap();
        let mut rng = rand::thread_rng();
        data.shuffle(&mut rng);
        data
    }

    #[tokio::test]
    async fn read_corrupt_config_misc_callback_received() {
        const ROOT_PATH: &str = "misc_error_callback";
        let (active_data_dir, inactive_data_dir) = (
            format!("{}/{}", ROOT_PATH, ACTIVE_CONFIGS_DATA_DIR),
            format!("{}/{}", ROOT_PATH, INACTIVE_CONFIGS_DATA_DIR),
        );

        let active_dir_path = Path::new(&active_data_dir);
        match file_io_utils::create_dir(&active_dir_path.to_path_buf()).await {
            Ok(_) => {
                info!("active config telemetry directory created");
            }
            Err(e) => {
                panic!("Error {} in creating active config directory", e);
            }
        };

        let file_path = PathBuf::from(active_data_dir.clone() + "/" + UUID);
        let corrupt_config = create_corrupt_config(UUID);

        file_io_utils::write_to_file(&file_path, &corrupt_config).await.unwrap_or_else(|e| {
            panic!("An error occurred in writing corrupt config: {:?} {}", file_path, e)
        });

        let (add_config_sender, add_config_receiver) = oneshot::channel();
        let sender_wrapper = Arc::new(Mutex::new(Some(add_config_sender)));
        let sender_clone = Arc::clone(&sender_wrapper);

        MetricsConfigManagerImpl::new(
            active_data_dir.clone(),
            inactive_data_dir.clone(),
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
            Arc::new(MockServicePublisherAccessor::new()),
            |_| {},
            move |status_code, status_msg| {
                let mut maybe_sender = sender_clone.lock().unwrap();
                maybe_sender.take().unwrap().send((status_code, status_msg)).unwrap();
            },
        )
        .await
        .unwrap();

        let (status, _) = add_config_receiver.await.unwrap();
        assert_eq!(status, StatusCodes::METRICS_CONFIG_PARSE_ERROR);
    }

    #[tokio::test]
    async fn add_metrics_config_validate_config_calls_received() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "add_metrics_config";
        let file_path =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, INACTIVE_CONFIGS_DATA_DIR, UUID));

        let test_environment =
            TestEnvironment::new(ROOT_PATH.to_owned(), MAX_PERSISTENT_CONFIGURATION_SIZE_MB).await;

        info!("Serializing metric config");
        let serialized_config = create_serialized_config(UUID);

        let res = test_environment.manager.add_metrics_config(&serialized_config).await;
        assert!(res.is_ok());
        assert!(file_path.is_file());
        assert_eq!(1, test_environment.get_num_validate_config_calls());
    }

    #[tokio::test]
    async fn add_metrics_config_validate_config_fail() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "add_metrics_config_fail";

        let test_environment =
            TestEnvironment::new(ROOT_PATH.to_owned(), MAX_PERSISTENT_CONFIGURATION_SIZE_MB).await;
        test_environment.metrics_config_orchestrator.next_error.lock().unwrap().replace(
            ConfigOrchestratorError::DuplicateMetricsConfigUuid { uuid: UUID.to_string() },
        );

        info!("Serializing metric config");
        let serialized_config = create_serialized_config(UUID);

        let err =
            test_environment.manager.add_metrics_config(&serialized_config).await.unwrap_err();
        assert!(
            matches!(&err, MetricsConfigManagerError::ConfigOrchestratorError { .. }),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(1, test_environment.get_num_validate_config_calls());
    }

    #[tokio::test]
    async fn add_metrics_config_memory_exceeded_fail() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "add_metrics_config_memory_exceeded";

        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned(), 0).await;

        info!("Serializing metric config");
        let serialized_config = create_serialized_config(UUID);

        let err =
            test_environment.manager.add_metrics_config(&serialized_config).await.unwrap_err();
        assert!(
            matches!(&err, MetricsConfigManagerError::PersistentStorageLimitExceeded),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(0, test_environment.get_num_validate_config_calls());
    }

    #[tokio::test]
    async fn activate_metrics_config_initialize_configs_calls_received() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "activate_metrics_config";
        let old_file_path =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, INACTIVE_CONFIGS_DATA_DIR, UUID));
        let new_file_path =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, ACTIVE_CONFIGS_DATA_DIR, UUID));

        let test_environment =
            TestEnvironment::new(ROOT_PATH.to_owned(), MAX_PERSISTENT_CONFIGURATION_SIZE_MB).await;

        let serialized_config = create_serialized_config(UUID);
        test_environment.manager.add_metrics_config(&serialized_config).await.unwrap();
        assert!(old_file_path.is_file());
        assert_eq!(1, test_environment.get_num_validate_config_calls());

        let res = test_environment.manager.activate_metrics_config(UUID).await;
        assert!(res.is_ok());
        assert!(new_file_path.is_file());
        assert_eq!(1, test_environment.get_num_initialize_config_calls());
    }

    #[tokio::test]
    async fn activate_metrics_config_initialize_config_fail() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "activate_metrics_config_fail";
        let old_file_path =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, INACTIVE_CONFIGS_DATA_DIR, UUID));

        let test_environment =
            TestEnvironment::new(ROOT_PATH.to_owned(), MAX_PERSISTENT_CONFIGURATION_SIZE_MB).await;

        let serialized_config = create_serialized_config(UUID);
        test_environment.manager.add_metrics_config(&serialized_config).await.unwrap();
        assert!(old_file_path.is_file());
        assert_eq!(1, test_environment.get_num_validate_config_calls());

        test_environment.metrics_config_orchestrator.next_error.lock().unwrap().replace(
            ConfigOrchestratorError::DuplicateMetricsConfigUuid { uuid: UUID.to_string() },
        );

        let err = test_environment.manager.activate_metrics_config(UUID).await.unwrap_err();
        assert!(
            matches!(&err, MetricsConfigManagerError::ConfigOrchestratorError { .. }),
            "Unexpected error: {:?}",
            err
        );
        assert_eq!(1, test_environment.get_num_initialize_config_calls());
    }

    #[tokio::test]
    async fn deactivate_metrics_config_finish_config_calls_received() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "deactivate_metrics_config";
        let old_file_path =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, INACTIVE_CONFIGS_DATA_DIR, UUID));
        let new_file_path =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, ACTIVE_CONFIGS_DATA_DIR, UUID));

        let test_environment =
            TestEnvironment::new(ROOT_PATH.to_owned(), MAX_PERSISTENT_CONFIGURATION_SIZE_MB).await;

        let serialized_config = create_serialized_config(UUID);
        test_environment.manager.add_metrics_config(&serialized_config).await.unwrap();
        assert!(old_file_path.is_file());
        assert_eq!(1, test_environment.get_num_validate_config_calls());

        test_environment.manager.activate_metrics_config(UUID).await.unwrap();
        assert!(new_file_path.is_file());
        assert_eq!(1, test_environment.get_num_initialize_config_calls());

        let res = test_environment.manager.deactivate_metrics_config(UUID).await;
        assert!(res.is_ok());
        assert!(old_file_path.is_file());
        assert_eq!(1, test_environment.get_num_finish_config_calls());
    }

    #[tokio::test]
    async fn deactivate_metrics_config_finish_config_fail() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "deactivate_metrics_config_fail";
        let old_file_path =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, INACTIVE_CONFIGS_DATA_DIR, UUID));
        let new_file_path =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, ACTIVE_CONFIGS_DATA_DIR, UUID));

        let test_environment =
            TestEnvironment::new(ROOT_PATH.to_owned(), MAX_PERSISTENT_CONFIGURATION_SIZE_MB).await;

        let serialized_config = create_serialized_config(UUID);
        test_environment.manager.add_metrics_config(&serialized_config).await.unwrap();
        assert!(old_file_path.is_file());
        assert_eq!(1, test_environment.get_num_validate_config_calls());

        test_environment.manager.activate_metrics_config(UUID).await.unwrap();
        assert!(new_file_path.is_file());
        assert_eq!(1, test_environment.get_num_initialize_config_calls());

        test_environment.metrics_config_orchestrator.next_error.lock().unwrap().replace(
            ConfigOrchestratorError::DuplicateMetricsConfigUuid { uuid: UUID.to_string() },
        );

        let err = test_environment.manager.deactivate_metrics_config(UUID).await.unwrap_err();
        assert!(
            matches!(&err, MetricsConfigManagerError::InvalidConfigUuid { .. }),
            "Unexpected error: {:?}",
            err
        );

        assert!(new_file_path.is_file());
        assert_eq!(1, test_environment.get_num_finish_config_calls());
    }

    #[tokio::test]
    async fn remove_metrics_config_success() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "remove_metrics_config";
        let old_file_path =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, INACTIVE_CONFIGS_DATA_DIR, UUID));
        let new_file_path =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, ACTIVE_CONFIGS_DATA_DIR, UUID));
        let test_environment =
            TestEnvironment::new(ROOT_PATH.to_owned(), MAX_PERSISTENT_CONFIGURATION_SIZE_MB).await;

        let serialized_config = create_serialized_config(UUID);
        test_environment.manager.add_metrics_config(&serialized_config).await.unwrap();
        assert!(old_file_path.is_file());
        assert_eq!(1, test_environment.get_num_validate_config_calls());

        test_environment.manager.activate_metrics_config(UUID).await.unwrap();
        assert!(new_file_path.is_file());
        assert_eq!(1, test_environment.get_num_initialize_config_calls());

        let res = test_environment.manager.remove_metrics_config(UUID).await;
        assert_eq!(1, test_environment.get_num_finish_config_calls());
        assert!(res.is_ok());
        assert!(!old_file_path.is_file());
        assert!(!new_file_path.is_file());
    }

    #[tokio::test]
    async fn remove_metrics_config_invalid_uuid_fail() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "remove_metrics_config_fail";

        let test_environment =
            TestEnvironment::new(ROOT_PATH.to_owned(), MAX_PERSISTENT_CONFIGURATION_SIZE_MB).await;
        let err = test_environment.manager.remove_metrics_config(UUID).await.unwrap_err();
        assert!(
            matches!(&err, MetricsConfigManagerError::InvalidConfigUuid { uuid } if uuid == UUID),
            "Unexpected error: {:?}",
            err
        );
    }

    #[tokio::test]
    async fn remove_all_metrics_configs() {
        const UUID1: &str = "01bf0754-df95-46bb-bb35-522b6af66600";
        const UUID2: &str = "f47ac10b-58cc-4372-a567-0e02b2c3d479";

        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "remove_all_metrics_configs";
        let inactive_dir_path =
            PathBuf::from(format!("{}/{}", ROOT_PATH, INACTIVE_CONFIGS_DATA_DIR));
        let active_dir_path = PathBuf::from(format!("{}/{}", ROOT_PATH, ACTIVE_CONFIGS_DATA_DIR));

        let old_file_path1 =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, INACTIVE_CONFIGS_DATA_DIR, UUID1));
        let new_file_path1 =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, ACTIVE_CONFIGS_DATA_DIR, UUID1));
        let old_file_path2 =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, INACTIVE_CONFIGS_DATA_DIR, UUID2));
        let new_file_path2 =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, ACTIVE_CONFIGS_DATA_DIR, UUID2));
        let test_environment =
            TestEnvironment::new(ROOT_PATH.to_owned(), MAX_PERSISTENT_CONFIGURATION_SIZE_MB).await;

        let serialized_config1 = create_serialized_config(UUID1);
        let serialized_config2 = create_serialized_config(UUID2);

        // Add configs
        test_environment.manager.add_metrics_config(&serialized_config1).await.unwrap();
        test_environment.manager.add_metrics_config(&serialized_config2).await.unwrap();
        assert!(old_file_path1.is_file());
        assert!(old_file_path2.is_file());
        assert_eq!(2, test_environment.get_num_validate_config_calls());

        // Activate configs
        test_environment.manager.activate_metrics_config(UUID1).await.unwrap();
        test_environment.manager.activate_metrics_config(UUID2).await.unwrap();
        assert!(new_file_path1.is_file());
        assert!(new_file_path2.is_file());
        assert_eq!(2, test_environment.get_num_initialize_config_calls());

        // Remove all configs
        let res = test_environment.manager.remove_all_metrics_configs().await;
        assert!(res.is_ok());
        assert_eq!(active_dir_path.read_dir().unwrap().count(), 0); // check if directory is empty
        assert_eq!(inactive_dir_path.read_dir().unwrap().count(), 0); // check if directory is empty
        assert_eq!(2, test_environment.get_num_finish_config_calls());
    }

    #[tokio::test]
    async fn get_active_metrics_configs_success() {
        const UUID2: &str = "f47ac10b-58cc-4372-a567-0e02b2c3d479";
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "get_active_metrics_configs_success";
        let file_path =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, ACTIVE_CONFIGS_DATA_DIR, UUID));

        let test_environment =
            TestEnvironment::new(ROOT_PATH.to_owned(), MAX_PERSISTENT_CONFIGURATION_SIZE_MB).await;

        info!("Serializing metric config");
        let serialized_config = create_serialized_config(UUID);
        let serialized_config2 = create_serialized_config(UUID2);

        test_environment.manager.add_metrics_config(&serialized_config).await.unwrap();
        test_environment.manager.activate_metrics_config(UUID).await.unwrap();
        test_environment.manager.add_metrics_config(&serialized_config2).await.unwrap();

        test_environment
            .metrics_config_orchestrator
            .populate_active_configs(vec![UUID.to_string()]);
        let active_configs = test_environment.manager.get_active_metrics_config_uuids().await;
        assert!(file_path.is_file());
        assert_eq!(1, test_environment.get_num_get_active_config_calls());
        assert_eq!(active_configs, vec![UUID.to_string()], "active configs: {:?}", active_configs);
    }

    #[tokio::test]
    async fn get_inactive_metrics_configs_success() {
        const UUID2: &str = "f47ac10b-58cc-4372-a567-0e02b2c3d479";
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "get_inactive_metrics_configs_success";
        let file_path =
            PathBuf::from(format!("{}/{}/{}", ROOT_PATH, INACTIVE_CONFIGS_DATA_DIR, UUID));

        let test_environment =
            TestEnvironment::new(ROOT_PATH.to_owned(), MAX_PERSISTENT_CONFIGURATION_SIZE_MB).await;

        info!("Serializing metric config");
        let serialized_config = create_serialized_config(UUID);
        let serialized_config2 = create_serialized_config(UUID2);

        test_environment.manager.add_metrics_config(&serialized_config).await.unwrap();
        test_environment.manager.add_metrics_config(&serialized_config2).await.unwrap();
        test_environment
            .metrics_config_orchestrator
            .populate_active_configs(vec![UUID2.to_string()]);

        let inactive_configs = test_environment.manager.get_inactive_metrics_config_uuids().await;

        assert!(file_path.is_file());
        assert_eq!(1, test_environment.get_num_get_active_config_calls());
        assert_eq!(
            inactive_configs,
            vec![UUID.to_string()],
            "inactive configs: {:?}",
            inactive_configs
        );
    }
}
