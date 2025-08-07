// Copyright 2023 Google LLC

use async_trait::async_trait;
use google_sdv_telemetry::aidl::google::sdv::telemetry::status::{
    StatusDetails::FileIOErrorDetails::FileIOErrorDetails, StatusDetails::StatusDetails,
    TelemetryServiceStatusCodes::TelemetryServiceStatusCodes as StatusCodes,
};
use log::{error, info};
use parking_lot::Mutex;
use sdv_telemetry_platform_abstraction_layer::PlatformAbstractionLayer;
use sdv_telemetry_proto_rust::metrics_configuration::{MetricsConfig, MetricsReport};
use sdv_telemetry_service_publisher_accessor::ServicePublisherAccessor;
use sdv_telemetry_utils::file_io_utils;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fs::{self};
use std::marker::{Send, Sync};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::SystemTime;
use tokio::io;
use uuid::Uuid;

// Crate module dependencies.
use crate::metrics_config_orchestrator::{
    MetricsConfigOrchestrator, MetricsConfigOrchestratorError as ConfigOrchestratorError,
};
use crate::metrics_config_orchestrator_factory::MetricsConfigOrchestratorFactory;
use crate::status_reporter::StatusReporter;

fn is_valid_uuid_format(uuid_str: &str) -> bool {
    Uuid::try_parse(uuid_str).is_ok()
}

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

#[derive(thiserror::Error, Debug)]
pub(crate) enum MetricsConfigManagerError {
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
    #[error("UUID '{uuid}' is either incorrectly formatted or no matching metrics configuration exists.")]
    InvalidConfigUuid { uuid: String },
    #[error("Error in reading metrics config from memory: {error}")]
    ConfigReadError { error: io::Error },
    #[error("Error in removing metrics config from memory: {error}")]
    ConfigRemoveError { error: io::Error },
    #[error("Error in parsing metrics config")]
    ConfigParseError,
    #[error("Error in persisting metrics config to memory: {error}")]
    ConfigWriteError { error: io::Error },
    #[error("Duplicate metrics config identifier: {uuid}")]
    DuplicateMetricsConfigUuid { uuid: String },
    #[error("Preallocated Persistence Storage Exceeded")]
    PersistentStorageLimitExceeded,
}

pub(super) struct MetricsConfigManagerImpl {
    active_dir_path: PathBuf,
    inactive_dir_path: PathBuf,
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
    #[expect(clippy::too_many_arguments)]
    pub async fn new(
        active_dir_path: PathBuf,
        inactive_dir_path: PathBuf,
        config_store_size: usize,
        service_publisher_manager: Arc<dyn ServicePublisherAccessor>,
        pal: Arc<dyn PlatformAbstractionLayer>,
        report_ready_callback: impl Fn(MetricsReport) + Send + Sync + 'static,
        status_reporter: Arc<dyn StatusReporter>,
        orchestrator_factory: Box<dyn MetricsConfigOrchestratorFactory>,
    ) -> Result<Arc<Self>, MetricsConfigManagerError> {
        let report_ready_callback = Box::new(report_ready_callback);

        let config_manager =
            Arc::new_cyclic(|manager_reference: &Weak<MetricsConfigManagerImpl>| {
                let config_orchestrator = orchestrator_factory.create(
                    Weak::clone(manager_reference) as Weak<dyn MetricsConfigManager + Send + Sync>,
                    Arc::clone(&service_publisher_manager),
                    pal,
                    report_ready_callback,
                    Arc::clone(&status_reporter),
                );

                MetricsConfigManagerImpl {
                    active_dir_path: active_dir_path.clone(),
                    inactive_dir_path: inactive_dir_path.clone(),
                    cur_persistent_config_store_size: AtomicUsize::new(0),
                    max_persistent_config_store_size: config_store_size,
                    metrics_config_orchestrator: config_orchestrator,
                    metrics_configs_store: Mutex::new(HashMap::new()),
                }
            });

        let error_handler = |code: StatusCodes, msg: &str, path: &Path| {
            error!("{msg}");

            let details = StatusDetails::FileIOErrorDetails(FileIOErrorDetails {
                path: path.to_string_lossy().to_string(),
            });
            status_reporter.report(code, msg.to_owned(), details);
        };

        let (active_count, active_size) =
            config_manager.process_active_directory(error_handler).await;

        let (inactive_count, inactive_size) =
            config_manager.process_inactive_directory(error_handler).await;

        let total_size = active_size + inactive_size;
        config_manager.cur_persistent_config_store_size.store(total_size, Ordering::Release);

        info!(
            "Telemetry service initialized with {active_count} active configs \
            and {inactive_count} inactive configs occupying {total_size} memory",
        );

        Ok(config_manager)
    }

    /// Activates config stored in active directory and returns their number and total size in bytes
    async fn process_active_directory(
        &self,
        error_handler: impl Fn(StatusCodes, &str, &Path),
    ) -> (usize, usize) {
        if !self.active_dir_path.is_dir() {
            match file_io_utils::create_dir(&self.active_dir_path).await {
                Ok(_) => {
                    // Success, directory was created or already exists
                    info!("Active config telemetry directory created");
                }
                Err(e) => {
                    let err = format!("Error {e} in creating active config directory");
                    error_handler(StatusCodes::FILE_IO_ERROR, &err, &self.active_dir_path);
                }
            };
            return (0, 0);
        }

        let mut configs_count = 0;
        let mut configs_size = 0;
        // Read the contents of each file in the directory into a byte array
        for entry in fs::read_dir(&self.active_dir_path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            let bytes = match file_io_utils::read_from_file(&path).await {
                Ok(data) => data,
                Err(e) => {
                    let err = format!("Error {e} in reading file {entry:?} from disk");
                    error_handler(StatusCodes::FILE_IO_ERROR, &err, &path);
                    continue;
                }
            };

            // bytes now contains the contents of the file
            let config: MetricsConfig = match protobuf::Message::parse_from_bytes(&bytes) {
                Ok(cfg) => cfg,
                Err(e) => {
                    error_handler(
                        StatusCodes::METRICS_CONFIG_PARSE_ERROR,
                        &format!("Error {e} in parsing metric config {entry:?} read from disk"),
                        &path,
                    );
                    continue;
                }
            };

            // Validates and initializes metrics config
            match self.metrics_config_orchestrator.initialize_config(&config).await {
                Ok(()) => {
                    let mut metric_config_map = self.metrics_configs_store.lock();
                    metric_config_map.insert(config.uuid.clone(), bytes.len());
                    configs_count += 1;
                    configs_size += bytes.len();
                }
                Err(e) => {
                    let err =
                        format!("Error {e} in initializing metric config {entry:?} read from disk");
                    error_handler(StatusCodes::ACTIVATE_METRICS_CONFIG_FAILED, &err, &path);

                    file_io_utils::remove_file(&path).await.unwrap_or_else(|e| {
                        error!("An error occurred in removing corrupt config: {e}")
                    });
                }
            }
        }

        (configs_count, configs_size)
    }

    /// Returns number of inactive configs stored and their total size in bytes
    async fn process_inactive_directory(
        &self,
        error_handler: impl Fn(StatusCodes, &str, &Path),
    ) -> (usize, usize) {
        if !self.inactive_dir_path.is_dir() {
            match file_io_utils::create_dir(&self.inactive_dir_path).await {
                Ok(_) => {
                    // Success, directory was created or already exists
                    info!("Inactive config telemetry directory created");
                }
                Err(e) => {
                    let err = format!("Error {e} in creating inactive config directory");
                    error_handler(StatusCodes::FILE_IO_ERROR, &err, &self.inactive_dir_path);
                }
            };
            return (0, 0);
        }

        let mut configs_count = 0;
        let mut configs_size = 0;
        for entry in fs::read_dir(&self.inactive_dir_path).unwrap() {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();

            if !metadata.is_file() {
                error_handler(
                    StatusCodes::METRICS_CONFIG_CORRUPTED,
                    &format!("Non-file entry found in inactive config directory: {entry:?}"),
                    &entry.path(),
                );

                // remove corrupt entry
                file_io_utils::remove_dir(&entry.path()).await.unwrap_or_else(|e| {
                    error!("An error occurred in removing directory entry config: {e}")
                });
                continue;
            }

            let file_name = entry.file_name().into_string().unwrap();
            let size = metadata.len() as usize;

            let mut metric_config_map = self.metrics_configs_store.lock();
            metric_config_map.insert(file_name, size);

            configs_size += size;
            configs_count += 1;
        }

        (configs_count, configs_size)
    }
}

#[async_trait]
impl MetricsConfigManager for MetricsConfigManagerImpl {
    #[tracing::instrument(skip(self, metrics_config), fields(
        await_start_timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_else(
            |e| e.duration()).as_nanos())
    )]
    async fn add_metrics_config(
        &self,
        metrics_config: &[u8],
    ) -> Result<String, MetricsConfigManagerError> {
        let config: MetricsConfig = protobuf::Message::parse_from_bytes(metrics_config)
            .map_err(|_| MetricsConfigManagerError::ConfigParseError)?;
        info!("adding config: {:?} of size: {}\n", config, metrics_config.len());

        if metrics_config.len() + self.cur_persistent_config_store_size.load(Ordering::Acquire)
            > self.max_persistent_config_store_size
        {
            return Err(MetricsConfigManagerError::PersistentStorageLimitExceeded);
        }

        if !is_valid_uuid_format(&config.uuid) {
            return Err(MetricsConfigManagerError::InvalidConfigUuid { uuid: config.uuid.clone() });
        }

        // Validate the metrics config and report any errors to client
        self.metrics_config_orchestrator.validate_config(&config).await?;

        let path = self.inactive_dir_path.join(&config.uuid);

        match self.metrics_configs_store.lock().entry(config.uuid.clone()) {
            Entry::Occupied(_) => {
                return Err(MetricsConfigManagerError::DuplicateMetricsConfigUuid {
                    uuid: config.uuid.clone(),
                });
            }
            Entry::Vacant(v) => {
                v.insert(metrics_config.len());
            }
        }

        if let Err(e) = file_io_utils::write_to_file(&path, metrics_config).await {
            error!("An error occurred while writing to file: {e}");
            if path.is_file() {
                file_io_utils::remove_file(&path).await.unwrap_or_else(|e| {
                    error!("An error occurred in removing corrupt config: {e}")
                });
            }
            self.metrics_configs_store.lock().remove(&config.uuid);
            return Err(MetricsConfigManagerError::ConfigWriteError { error: e });
        }

        // self.cur_persistent_config_store_size needs to be updated during each API call that
        // modifies active metrics configs. Alternate to metric_config_map would be read the
        // file size from memory which constitutes an IO operation and is inefficient.
        self.cur_persistent_config_store_size.fetch_add(metrics_config.len(), Ordering::AcqRel);

        Ok(config.uuid)
    }

    #[tracing::instrument(skip(self, metrics_config_uuid), fields(
        await_start_timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_else(
            |e| e.duration()).as_nanos())
    )]
    async fn activate_metrics_config(
        &self,
        metrics_config_uuid: &str,
    ) -> Result<(), MetricsConfigManagerError> {
        info!("activate_metrics_config: {:?} \n", metrics_config_uuid);
        let path = self.inactive_dir_path.join(metrics_config_uuid);
        if path.is_file() {
            let bytes = file_io_utils::read_from_file(&path)
                .await
                .map_err(|e| MetricsConfigManagerError::ConfigReadError { error: e })?;

            // bytes now contains the contents of the file
            let config: MetricsConfig = protobuf::Message::parse_from_bytes(&bytes)
                .map_err(|_| MetricsConfigManagerError::ConfigParseError)?;
            let uuid = &config.uuid;
            info!("initializing {uuid} Config");
            // Initializes metrics config
            match Arc::clone(&self.metrics_config_orchestrator).initialize_config(&config).await {
                Ok(()) => {
                    info!("Metrics config orchestrator successfully initialized {uuid} config");
                    let new_path = self.active_dir_path.join(uuid);
                    file_io_utils::rename_file(&path, &new_path).await.unwrap_or_else(|e| {
                        error!(
                            "An error occurred in renaming config to {}: {}",
                            new_path.display(),
                            e
                        )
                    });
                }
                Err(e) => {
                    error!("Error {e} in initializing {uuid} Config");
                    file_io_utils::remove_file(&path).await.unwrap_or_else(|e| {
                        error!("An error occurred in removing corrupt config: {e}")
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

    #[tracing::instrument(skip(self, metrics_config_uuid), fields(
        await_start_timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_else(
            |e| e.duration()).as_nanos())
    )]
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

        let old_path = self.active_dir_path.join(metrics_config_uuid);
        let new_path = self.inactive_dir_path.join(metrics_config_uuid);

        // TODO: b/299329266: Handle file io errors gracefully
        file_io_utils::rename_file(&old_path, &new_path)
            .await
            .unwrap_or_else(|e| error!("An error occurred in renaming config: {e}"));

        Ok(())
    }

    #[tracing::instrument(skip(self, metrics_config_uuid), fields(
        await_start_timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_else(
            |e| e.duration()).as_nanos())
    )]
    async fn remove_metrics_config(
        &self,
        metrics_config_uuid: &str,
    ) -> Result<(), MetricsConfigManagerError> {
        info!("remove_metrics_config: {}", metrics_config_uuid);
        let uuid = metrics_config_uuid.to_string();
        let config_size = {
            let mut metric_config_map = self.metrics_configs_store.lock();
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
                    info!("UUID {uuid} doesn't exist or is inactive");
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
                self.active_dir_path.join(&uuid)
            } else {
                self.inactive_dir_path.join(&uuid)
            }
        };

        file_io_utils::remove_file(&path)
            .await
            .map_err(|e| MetricsConfigManagerError::ConfigRemoveError { error: e })?;

        self.cur_persistent_config_store_size.fetch_sub(config_size, Ordering::AcqRel);

        Ok(())
    }

    #[tracing::instrument(skip(self), fields(
        await_start_timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_else(
            |e| e.duration()).as_nanos())
    )]
    async fn remove_all_metrics_configs(&self) -> Result<(), MetricsConfigManagerError> {
        info!("remove_all_metrics_configs");

        // remove all configs from active directory
        for entry in fs::read_dir(self.active_dir_path.clone()).unwrap() {
            file_io_utils::remove_file(&entry.unwrap().path())
                .await
                .unwrap_or_else(|e| error!("An error occurred in removing active config: {e}"));
        }

        // remove all configs from inactive directory
        for entry in fs::read_dir(self.inactive_dir_path.clone()).unwrap() {
            file_io_utils::remove_file(&entry.unwrap().path())
                .await
                .unwrap_or_else(|e| error!("An error occurred in removing inactive config: {e}"));
        }

        {
            let metric_config_map = mem::take(&mut *self.metrics_configs_store.lock());
            for uuid in metric_config_map.keys() {
                let res = self.metrics_config_orchestrator.finish_config(uuid).await;
                if let Err(err) = res {
                    if let ConfigOrchestratorError::InvalidConfigUuid { .. } = err {
                        info!("UUID {uuid} doesn't exist or is not active");
                    } else {
                        error!("Error in finishing Metric Config {err:?}");
                    }
                }
            }
        }

        self.cur_persistent_config_store_size.store(0, Ordering::Release);
        Ok(())
    }

    async fn get_active_metrics_config_uuids(&self) -> Vec<String> {
        info!("get_active_metrics_config_uuids");

        self.metrics_config_orchestrator.get_active_metrics_config_uuids()
    }

    async fn get_inactive_metrics_config_uuids(&self) -> Vec<String> {
        info!("get_active_metrics_config_uuids");

        let active_configs = self.metrics_config_orchestrator.get_active_metrics_config_uuids();
        let metrics_config_store_lock = self.metrics_configs_store.lock();
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
    use super::*;

    use futures::future::{self, BoxFuture, FutureExt};
    use google_sdv_telemetry::aidl::google::sdv::telemetry::status::TelemetryServiceStatus::TelemetryServiceStatus;
    use protobuf::Message;
    use rand::prelude::SliceRandom;
    use sdv_telemetry_platform_abstraction_layer::new_stub_pal;
    use sdv_telemetry_proto_rust::metrics_configuration::MetricsConfig as MetricsConfigProto;
    use sdv_telemetry_test_utils::oneshot_callback;
    use std::path::Path;
    use tempfile::Builder;

    use crate::mock_service_publisher_accessor::MockServicePublisherAccessor;
    use crate::StatusReporterImpl;

    static ACTIVE_CONFIGS_DATA_DIR: &str = "telemetry/active_configs";
    static INACTIVE_CONFIGS_DATA_DIR: &str = "telemetry/inactive_configs";
    const MAX_PERSISTENT_CONFIGURATION_SIZE_MB: usize = 10 * 1024 * 1024; // 10Mbs
    const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";
    const INVALID_UUID: &str = "../01bf0754-df95-46bb-bb35-522b6af66600";

    struct MockMetricsConfigOrchestratorFactory {
        orchestrator: Arc<MockMetricsConfigOrchestratorImpl>,
    }

    impl MetricsConfigOrchestratorFactory for MockMetricsConfigOrchestratorFactory {
        fn create(
            &self,
            _manager_reference: Weak<dyn MetricsConfigManager + Send + Sync>,
            _service_publisher_manager: Arc<dyn ServicePublisherAccessor>,
            _pal: Arc<dyn PlatformAbstractionLayer>,
            _report_ready_callback: Box<dyn Fn(MetricsReport) + Send + Sync + 'static>,
            _status_reporter: Arc<dyn StatusReporter>,
        ) -> Arc<dyn MetricsConfigOrchestrator + Send + Sync> {
            Arc::clone(&self.orchestrator) as Arc<dyn MetricsConfigOrchestrator + Send + Sync>
        }
    }

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
        fn new() -> Self {
            Self {
                num_initialize_config_calls: Arc::new(Mutex::new(0)),
                num_finish_config_calls: Arc::new(Mutex::new(0)),
                num_validate_config_calls: Arc::new(Mutex::new(0)),
                num_get_active_config_calls: Arc::new(Mutex::new(0)),
                configs: Vec::new().into(),
                next_error: None.into(),
            }
        }

        pub(crate) fn populate_active_configs(&self, config_uuids: Vec<String>) {
            *self.configs.lock() = config_uuids;
        }
    }

    #[async_trait]
    impl MetricsConfigOrchestrator for MockMetricsConfigOrchestratorImpl {
        async fn initialize_config(
            &self,
            _: &MetricsConfigProto,
        ) -> Result<(), ConfigOrchestratorError> {
            let mut num_calls = self.num_initialize_config_calls.lock();
            *num_calls += 1;

            if let Some(err) = self.next_error.lock().take() {
                return Err(err);
            }

            Ok(())
        }

        async fn finish_config(&self, _: &str) -> Result<(), ConfigOrchestratorError> {
            let mut num_calls = self.num_finish_config_calls.lock();
            *num_calls += 1;

            if let Some(err) = self.next_error.lock().take() {
                return Err(err);
            }

            Ok(())
        }

        async fn validate_config(
            &self,
            _: &MetricsConfigProto,
        ) -> Result<(), ConfigOrchestratorError> {
            let mut num_calls = self.num_validate_config_calls.lock();
            *num_calls += 1;

            if let Some(err) = self.next_error.lock().take() {
                return Err(err);
            }

            Ok(())
        }

        fn get_active_metrics_config_uuids(&self) -> Vec<String> {
            *self.num_get_active_config_calls.lock() += 1;

            let active_configs = self.configs.lock().clone().into_iter().collect();
            active_configs
        }
    }

    struct TestEnvironment {
        manager: Arc<dyn MetricsConfigManager>,
        error_receiver: future::Shared<BoxFuture<'static, Arc<TelemetryServiceStatus>>>,
        metrics_config_orchestrator: Arc<MockMetricsConfigOrchestratorImpl>,
    }

    impl TestEnvironment {
        // Test setup
        async fn new(
            active_dir_path: &Path,
            inactive_dir_path: &Path,
            max_persistent_config_store_size: usize,
        ) -> TestEnvironment {
            Self::new_with_orchestrator(
                active_dir_path,
                inactive_dir_path,
                max_persistent_config_store_size,
                Arc::new(MockMetricsConfigOrchestratorImpl::new()),
            )
            .await
        }

        async fn new_with_orchestrator(
            active_dir_path: &Path,
            inactive_dir_path: &Path,
            max_persistent_config_store_size: usize,
            metrics_config_orchestrator: Arc<MockMetricsConfigOrchestratorImpl>,
        ) -> TestEnvironment {
            let (tx, rx) = oneshot_callback();
            let manager = MetricsConfigManagerImpl::new(
                active_dir_path.to_path_buf(),
                inactive_dir_path.to_path_buf(),
                max_persistent_config_store_size,
                Arc::new(MockServicePublisherAccessor::new()),
                new_stub_pal(),
                |_| {},
                Arc::new(StatusReporterImpl::new(move |status| {
                    tx(status.into());
                })),
                Box::new(MockMetricsConfigOrchestratorFactory {
                    orchestrator: Arc::clone(&metrics_config_orchestrator),
                }),
            )
            .await
            .unwrap();

            TestEnvironment {
                manager,
                error_receiver: rx.boxed().shared(),
                metrics_config_orchestrator,
            }
        }

        pub fn get_num_initialize_config_calls(&self) -> i32 {
            *self.metrics_config_orchestrator.num_initialize_config_calls.lock()
        }

        pub fn get_num_finish_config_calls(&self) -> i32 {
            *self.metrics_config_orchestrator.num_finish_config_calls.lock()
        }

        pub fn get_num_validate_config_calls(&self) -> i32 {
            *self.metrics_config_orchestrator.num_validate_config_calls.lock()
        }

        pub fn get_num_get_active_config_calls(&self) -> i32 {
            *self.metrics_config_orchestrator.num_get_active_config_calls.lock()
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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn read_active_config_from_disk_success() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);
        file_io_utils::create_dir(&active_dir_path).await.unwrap_or_else(|e| {
            panic!(
                "An error occurred in creating active config directory: {active_dir_path:?} {e}"
            );
        });

        let file_path = active_dir_path.join(UUID);
        let serialized_config = create_serialized_config(UUID);
        file_io_utils::write_to_file(&file_path, &serialized_config).await.unwrap_or_else(|e| {
            panic!("An error occurred in writing serialized config: {file_path:?} {e}")
        });

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        assert_eq!(test_environment.get_num_initialize_config_calls(), 1);
        assert!(file_path.exists());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn read_active_config_from_disk_initialize_config_fails() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);
        file_io_utils::create_dir(&active_dir_path).await.unwrap_or_else(|e| {
            panic!(
                "An error occurred in creating active config directory: {active_dir_path:?} {e}"
            );
        });

        let file_path = active_dir_path.join(UUID);
        let serialized_config = create_serialized_config(UUID);
        file_io_utils::write_to_file(&file_path, &serialized_config).await.unwrap_or_else(|e| {
            panic!("An error occurred in writing serialized config: {file_path:?} {e}")
        });

        let orchestrator = Arc::new(MockMetricsConfigOrchestratorImpl::new());
        orchestrator.next_error.lock().replace(ConfigOrchestratorError::CycledPublishersTriggers);

        let test_environment = TestEnvironment::new_with_orchestrator(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
            orchestrator,
        )
        .await;

        assert_eq!(test_environment.get_num_initialize_config_calls(), 1);

        let status = test_environment.error_receiver.await;
        assert_eq!(status.code, StatusCodes::ACTIVATE_METRICS_CONFIG_FAILED);
        // Config which can't be activated is removed
        assert!(!file_path.exists());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn read_active_corrupt_config_from_disk_misc_callback_received() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);
        file_io_utils::create_dir(&active_dir_path).await.unwrap_or_else(|e| {
            panic!(
                "An error occurred in creating active config directory: {active_dir_path:?} {e}"
            );
        });

        let file_path = active_dir_path.join(UUID);
        let corrupt_config = create_corrupt_config(UUID);
        file_io_utils::write_to_file(&file_path, &corrupt_config).await.unwrap_or_else(|e| {
            panic!("An error occurred in writing corrupt config: {file_path:?} {e}")
        });

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        assert_eq!(test_environment.get_num_initialize_config_calls(), 0);

        let status = test_environment.error_receiver.await;
        assert_eq!(status.code, StatusCodes::METRICS_CONFIG_PARSE_ERROR);
        // Corrupted config is not removed.
        // TODO: b/436613605: Reconsider this behavior
        assert!(file_path.exists());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn read_active_configs_from_disk_corrupted_entry_success() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);
        file_io_utils::create_dir(&inactive_dir_path).await.unwrap_or_else(|e| {
            panic!(
                "An error occurred in creating inactive config directory: {inactive_dir_path:?} {e}"
            );
        });

        // Create a directory instead of a file to simulate a corrupted entry.
        let corrupt_entry_path = active_dir_path.join("corrupt_config");
        file_io_utils::create_dir(&corrupt_entry_path).await.unwrap_or_else(|e| {
            panic!("An error occurred in creating corrupt entry: {corrupt_entry_path:?} {e}");
        });

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        assert_eq!(test_environment.get_num_initialize_config_calls(), 0);

        // Corrupted entry doesn't gets removed and no error is being sent
        // TODO: b/436613605: Reconsider this behavior
        assert!(corrupt_entry_path.exists());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn read_inactive_config_from_disk_success() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);
        file_io_utils::create_dir(&inactive_dir_path).await.unwrap_or_else(|e| {
            panic!(
                "An error occurred in creating inactive config directory: {active_dir_path:?} {e}"
            );
        });

        let file_path = inactive_dir_path.join(UUID);
        let serialized_config = create_serialized_config(UUID);
        file_io_utils::write_to_file(&file_path, &serialized_config).await.unwrap_or_else(|e| {
            panic!("An error occurred in writing serialized config: {file_path:?} {e}")
        });

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        assert_eq!(test_environment.get_num_initialize_config_calls(), 0);
        assert!(file_path.exists());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn read_inactive_corrupt_config_from_disk_success() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);
        file_io_utils::create_dir(&inactive_dir_path).await.unwrap_or_else(|e| {
            panic!(
                "An error occurred in creating inactive config directory: {active_dir_path:?} {e}"
            );
        });

        let file_path = inactive_dir_path.join(UUID);
        let corrupt_config = create_corrupt_config(UUID);
        file_io_utils::write_to_file(&file_path, &corrupt_config).await.unwrap_or_else(|e| {
            panic!("An error occurred in writing corrupt config: {file_path:?} {e}")
        });

        let _test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        // Even though the config is corrupted, it is not removed
        // TODO: b/436613605: Reconsider this behavior
        assert!(file_path.exists());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn read_inactive_configs_from_disk_corrupted_entry_removed() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);
        file_io_utils::create_dir(&inactive_dir_path).await.unwrap_or_else(|e| {
            panic!(
                "An error occurred in creating inactive config directory: {inactive_dir_path:?} {e}"
            );
        });

        // Create a directory instead of a file to simulate a corrupted entry.
        let corrupt_entry_path = inactive_dir_path.join("corrupt_config");
        file_io_utils::create_dir(&corrupt_entry_path).await.unwrap_or_else(|e| {
            panic!("An error occurred in creating corrupt entry: {corrupt_entry_path:?} {e}");
        });

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        let status = test_environment.error_receiver.await;
        assert_eq!(status.code, StatusCodes::METRICS_CONFIG_CORRUPTED);
        // Corrupted entry gets removed
        assert!(!corrupt_entry_path.exists());
    }

    #[sdv_telemetry_test_utils::timeout(2_000)]
    #[tokio::test]
    async fn create_active_dir_on_file_path_fail() {
        let root_dir = Builder::new().tempdir().unwrap();
        let file = Builder::new().tempfile_in(root_dir.path()).unwrap();
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);

        // We provide a path to a file as a directory path
        let test_environment = TestEnvironment::new(
            file.path(),
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        let status = test_environment.error_receiver.await;
        assert_eq!(status.code, StatusCodes::FILE_IO_ERROR);
    }

    #[sdv_telemetry_test_utils::timeout(2_000)]
    #[tokio::test]
    async fn create_inactive_dir_on_file_path_fail() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let file = Builder::new().tempfile_in(root_dir.path()).unwrap();

        // We provide a path to a file as a directory path
        let test_environment = TestEnvironment::new(
            &active_dir_path,
            file.path(),
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        let status = test_environment.error_receiver.await;
        assert_eq!(status.code, StatusCodes::FILE_IO_ERROR);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn add_metrics_config_validate_config_calls_received() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);
        let file_path = inactive_dir_path.join(UUID);

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        info!("Serializing metric config");
        let serialized_config = create_serialized_config(UUID);

        let res = test_environment.manager.add_metrics_config(&serialized_config).await;
        assert!(res.is_ok());
        assert!(file_path.is_file());
        assert_eq!(1, test_environment.get_num_validate_config_calls());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn add_metrics_config_duplicate_config_fail() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);
        let file_path = inactive_dir_path.join(UUID);

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        info!("Serializing metric config");
        let serialized_config = create_serialized_config(UUID);

        let res = test_environment.manager.add_metrics_config(&serialized_config).await;
        let err =
            test_environment.manager.add_metrics_config(&serialized_config).await.unwrap_err();

        assert!(res.is_ok());
        assert!(file_path.is_file());
        assert!(
            matches!(&err, MetricsConfigManagerError::DuplicateMetricsConfigUuid { .. }),
            "Unexpected error: {err:?}"
        );
        assert_eq!(2, test_environment.get_num_validate_config_calls());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn add_metrics_config_invalid_config_uuid_fail() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);
        let file_path = inactive_dir_path.join(INVALID_UUID);

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        info!("Serializing metric config");
        let serialized_config = create_serialized_config(INVALID_UUID);

        let err =
            test_environment.manager.add_metrics_config(&serialized_config).await.unwrap_err();

        assert!(!file_path.is_file());
        assert!(
            matches!(&err, MetricsConfigManagerError::InvalidConfigUuid { .. }),
            "Unexpected error: {err:?}"
        );
        assert_eq!(0, test_environment.get_num_validate_config_calls());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn add_metrics_config_validate_config_fail() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;
        test_environment.metrics_config_orchestrator.next_error.lock().replace(
            ConfigOrchestratorError::DuplicateMetricsConfigUuid { uuid: UUID.to_string() },
        );

        info!("Serializing metric config");
        let serialized_config = create_serialized_config(UUID);

        let err =
            test_environment.manager.add_metrics_config(&serialized_config).await.unwrap_err();
        assert!(
            matches!(&err, MetricsConfigManagerError::ConfigOrchestratorError { .. }),
            "Unexpected error: {err:?}"
        );
        assert_eq!(1, test_environment.get_num_validate_config_calls());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn add_metrics_config_memory_exceeded_fail() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);

        let test_environment = TestEnvironment::new(&active_dir_path, &inactive_dir_path, 0).await;

        info!("Serializing metric config");
        let serialized_config = create_serialized_config(UUID);

        let err =
            test_environment.manager.add_metrics_config(&serialized_config).await.unwrap_err();
        assert!(
            matches!(&err, MetricsConfigManagerError::PersistentStorageLimitExceeded),
            "Unexpected error: {err:?}"
        );
        assert_eq!(0, test_environment.get_num_validate_config_calls());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_metrics_config_initialize_configs_calls_received() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);

        let old_file_path = inactive_dir_path.join(UUID);
        let new_file_path = active_dir_path.join(UUID);

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        let serialized_config = create_serialized_config(UUID);
        test_environment.manager.add_metrics_config(&serialized_config).await.unwrap();
        assert!(old_file_path.is_file());
        assert_eq!(1, test_environment.get_num_validate_config_calls());

        let res = test_environment.manager.activate_metrics_config(UUID).await;
        assert!(res.is_ok());
        assert!(new_file_path.is_file());
        assert_eq!(1, test_environment.get_num_initialize_config_calls());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_metrics_config_initialize_config_fail() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);
        let old_file_path = inactive_dir_path.join(UUID);

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        let serialized_config = create_serialized_config(UUID);
        test_environment.manager.add_metrics_config(&serialized_config).await.unwrap();
        assert!(old_file_path.is_file());
        assert_eq!(1, test_environment.get_num_validate_config_calls());

        test_environment.metrics_config_orchestrator.next_error.lock().replace(
            ConfigOrchestratorError::DuplicateMetricsConfigUuid { uuid: UUID.to_string() },
        );

        let err = test_environment.manager.activate_metrics_config(UUID).await.unwrap_err();
        assert!(
            matches!(&err, MetricsConfigManagerError::ConfigOrchestratorError { .. }),
            "Unexpected error: {err:?}"
        );
        assert_eq!(1, test_environment.get_num_initialize_config_calls());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn deactivate_metrics_config_finish_config_calls_received() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);

        let old_file_path = inactive_dir_path.join(UUID);
        let new_file_path = active_dir_path.join(UUID);

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn deactivate_metrics_config_finish_config_fail() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);
        let old_file_path = inactive_dir_path.join(UUID);
        let new_file_path = active_dir_path.join(UUID);

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        let serialized_config = create_serialized_config(UUID);
        test_environment.manager.add_metrics_config(&serialized_config).await.unwrap();
        assert!(old_file_path.is_file());
        assert_eq!(1, test_environment.get_num_validate_config_calls());

        test_environment.manager.activate_metrics_config(UUID).await.unwrap();
        assert!(new_file_path.is_file());
        assert_eq!(1, test_environment.get_num_initialize_config_calls());

        test_environment.metrics_config_orchestrator.next_error.lock().replace(
            ConfigOrchestratorError::DuplicateMetricsConfigUuid { uuid: UUID.to_string() },
        );

        let err = test_environment.manager.deactivate_metrics_config(UUID).await.unwrap_err();
        assert!(
            matches!(&err, MetricsConfigManagerError::InvalidConfigUuid { .. }),
            "Unexpected error: {err:?}"
        );

        assert!(new_file_path.is_file());
        assert_eq!(1, test_environment.get_num_finish_config_calls());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn remove_metrics_config_success() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);
        let old_file_path = inactive_dir_path.join(UUID);
        let new_file_path = active_dir_path.join(UUID);

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn remove_metrics_config_invalid_uuid_fail() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;
        let err = test_environment.manager.remove_metrics_config(UUID).await.unwrap_err();
        assert!(
            matches!(&err, MetricsConfigManagerError::InvalidConfigUuid { uuid } if uuid == UUID),
            "Unexpected error: {err:?}"
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn remove_all_metrics_configs() {
        const UUID1: &str = "01bf0754-df95-46bb-bb35-522b6af66600";
        const UUID2: &str = "f47ac10b-58cc-4372-a567-0e02b2c3d479";

        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);

        let old_file_path1 = inactive_dir_path.join(UUID1);
        let new_file_path1 = active_dir_path.join(UUID1);
        let old_file_path2 = inactive_dir_path.join(UUID2);
        let new_file_path2 = active_dir_path.join(UUID2);

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_active_metrics_configs_success() {
        const UUID2: &str = "f47ac10b-58cc-4372-a567-0e02b2c3d479";

        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);

        let file_path = active_dir_path.join(UUID);

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

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
        assert_eq!(active_configs, vec![UUID.to_string()], "active configs: {active_configs:?}");
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_inactive_metrics_configs_success() {
        const UUID2: &str = "f47ac10b-58cc-4372-a567-0e02b2c3d479";

        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);
        let file_path = inactive_dir_path.join(UUID);

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

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
            "inactive configs: {inactive_configs:?}"
        );
    }
}
