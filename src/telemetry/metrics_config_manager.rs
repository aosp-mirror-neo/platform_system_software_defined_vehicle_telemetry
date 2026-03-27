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

use async_trait::async_trait;
use google_sdv_telemetry::aidl::google::sdv::telemetry::status::{
    StatusDetails::FileIOErrorDetails::FileIOErrorDetails, StatusDetails::StatusDetails,
    TelemetryServiceStatusCodes::TelemetryServiceStatusCodes as StatusCodes,
};
use log::{error, info};
use parking_lot::Mutex;
use protobuf::Message;
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
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::SystemTime;
use tokio::io;

// Crate module dependencies.
use crate::metrics_config_orchestrator::{
    MetricsConfigOrchestrator, MetricsConfigOrchestratorError as ConfigOrchestratorError,
};
use crate::metrics_config_orchestrator_factory::MetricsConfigOrchestratorFactory;
use crate::metrics_config_uuid::MetricsConfigUuid;
use crate::status_reporter::StatusReporter;

/// Interface of a service that manages Metrics Configurations.
#[async_trait]
pub trait MetricsConfigManager {
    /// Adds/Persists metrics config in memory
    async fn add_metrics_config(
        &self,
        metrics_config: &[u8],
    ) -> Result<MetricsConfigUuid, MetricsConfigManagerError>;

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
    async fn get_active_metrics_config_uuids(&self) -> Vec<MetricsConfigUuid>;

    /// Get all metrics configs in inactive state
    async fn get_inactive_metrics_config_uuids(&self) -> Vec<MetricsConfigUuid>;
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
    DuplicateMetricsConfigUuid { uuid: MetricsConfigUuid },
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
    metrics_configs_store: Mutex<HashMap<MetricsConfigUuid, usize>>,
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
        dump_manager: Arc<sdv_dump::DumpManager>,
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
                    dump_manager,
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

    async fn load_metrics_config_from_path(
        &self,
        path: &Path,
        error_handler: &impl Fn(StatusCodes, &str, &Path),
    ) -> Option<(MetricsConfigUuid, MetricsConfig, usize)> {
        let bytes = file_io_utils::read_from_file(path)
            .await
            .inspect_err(|err| {
                error_handler(
                    StatusCodes::FILE_IO_ERROR,
                    &format!("Error {err} in reading file {} from disk", path.display()),
                    path,
                );
            })
            .ok()?;

        // bytes now contains the contents of the file
        let config = MetricsConfig::parse_from_bytes(&bytes)
            .inspect_err(|err| {
                error_handler(
                    StatusCodes::METRICS_CONFIG_PARSE_ERROR,
                    &format!(
                        "Error {err} in parsing metric config {} read from disk",
                        path.display()
                    ),
                    path,
                );
            })
            .ok()?;

        let uuid = MetricsConfigUuid::from_str(&config.uuid)
            .inspect_err(|err| {
                error_handler(
                    StatusCodes::METRICS_CONFIG_CORRUPTED,
                    &format!(
                        "Error {err} in parsing metric config {} read from disk",
                        path.display()
                    ),
                    path,
                );
            })
            .ok()?;

        if Some(uuid.to_os_string().as_os_str()) != path.file_name() {
            error_handler(
                StatusCodes::METRICS_CONFIG_CORRUPTED,
                &format!(
                    "Error 'Mismatched UUID' in parsing metric config {} read from disk",
                    path.display()
                ),
                path,
            );
            return None;
        }

        Some((uuid, config, bytes.len()))
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
                Err(err) => {
                    let err = format!(
                        "Failed to create active metrics config directory '{}': {err}",
                        self.active_dir_path.display()
                    );
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

            let Some((config_uuid, config, config_size)) =
                self.load_metrics_config_from_path(&path, &error_handler).await
            else {
                continue;
            };

            // Validates and initializes metrics config
            match self.metrics_config_orchestrator.initialize_config(&config).await {
                Ok(()) => {
                    let mut metric_config_map = self.metrics_configs_store.lock();
                    metric_config_map.insert(config_uuid, config_size);
                    configs_count += 1;
                    configs_size += config_size;
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
                Err(err) => {
                    let err = format!(
                        "Failed to create inactive metrics config directory '{}': {err}",
                        self.inactive_dir_path.display()
                    );
                    error_handler(StatusCodes::FILE_IO_ERROR, &err, &self.inactive_dir_path);
                }
            };
            return (0, 0);
        }

        let mut configs_count = 0;
        let mut configs_size = 0;
        for entry in fs::read_dir(&self.inactive_dir_path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if !path.is_file() {
                error_handler(
                    StatusCodes::METRICS_CONFIG_CORRUPTED,
                    &format!("Non-file entry found in inactive config directory: {entry:?}"),
                    &path,
                );

                // remove corrupt entry
                file_io_utils::remove_dir(&path).await.unwrap_or_else(|e| {
                    error!("An error occurred in removing directory entry config: {e}")
                });
                continue;
            }

            let Some((config_uuid, _, config_size)) =
                self.load_metrics_config_from_path(&path, &error_handler).await
            else {
                continue;
            };

            let mut metric_config_map = self.metrics_configs_store.lock();
            metric_config_map.insert(config_uuid, config_size);

            configs_size += config_size;
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
    ) -> Result<MetricsConfigUuid, MetricsConfigManagerError> {
        let config: MetricsConfig = protobuf::Message::parse_from_bytes(metrics_config)
            .map_err(|_| MetricsConfigManagerError::ConfigParseError)?;

        let Ok(uuid) = MetricsConfigUuid::from_str(&config.uuid) else {
            return Err(MetricsConfigManagerError::InvalidConfigUuid { uuid: config.uuid.clone() });
        };

        info!("adding config: {:?} of size: {}\n", config, metrics_config.len());

        if metrics_config.len() + self.cur_persistent_config_store_size.load(Ordering::Acquire)
            > self.max_persistent_config_store_size
        {
            return Err(MetricsConfigManagerError::PersistentStorageLimitExceeded);
        }

        // Validate the metrics config and report any errors to client
        self.metrics_config_orchestrator.validate_config(&config).await?;

        let path = self.inactive_dir_path.join(uuid.to_string());

        match self.metrics_configs_store.lock().entry(uuid) {
            Entry::Occupied(_) => {
                return Err(MetricsConfigManagerError::DuplicateMetricsConfigUuid { uuid });
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
            self.metrics_configs_store.lock().remove(&uuid);
            return Err(MetricsConfigManagerError::ConfigWriteError { error: e });
        }

        // self.cur_persistent_config_store_size needs to be updated during each API call that
        // modifies active metrics configs. Alternate to metric_config_map would be read the
        // file size from memory which constitutes an IO operation and is inefficient.
        self.cur_persistent_config_store_size.fetch_add(metrics_config.len(), Ordering::AcqRel);

        Ok(uuid)
    }

    #[tracing::instrument(skip(self, metrics_config_uuid), fields(
        await_start_timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_else(
            |e| e.duration()).as_nanos())
    )]
    async fn activate_metrics_config(
        &self,
        metrics_config_uuid: &str,
    ) -> Result<(), MetricsConfigManagerError> {
        let Ok(uuid) = MetricsConfigUuid::from_str(metrics_config_uuid) else {
            return Err(MetricsConfigManagerError::InvalidConfigUuid {
                uuid: metrics_config_uuid.to_owned(),
            });
        };

        info!("activate_metrics_config: {uuid}");
        let path = self.inactive_dir_path.join(uuid.to_string());
        if !path.is_file() {
            return Err(MetricsConfigManagerError::InvalidConfigUuid { uuid: uuid.to_string() });
        }

        let bytes = file_io_utils::read_from_file(&path)
            .await
            .map_err(|e| MetricsConfigManagerError::ConfigReadError { error: e })?;

        let config: MetricsConfig = protobuf::Message::parse_from_bytes(&bytes)
            .map_err(|_| MetricsConfigManagerError::ConfigParseError)?;

        match Arc::clone(&self.metrics_config_orchestrator).initialize_config(&config).await {
            Ok(()) => {
                info!("Successfully initialized config {uuid}");
                let new_path = self.active_dir_path.join(uuid.to_string());
                file_io_utils::rename_file(&path, &new_path).await.unwrap_or_else(|e| {
                    error!("An error occurred in renaming config to {}: {e}", new_path.display())
                });
            }
            Err(e) => {
                error!("Error {e} in initializing config {uuid}");
                file_io_utils::remove_file(&path).await.unwrap_or_else(|e| {
                    error!("An error occurred in removing corrupt config: {e}")
                });
                return Err(MetricsConfigManagerError::ConfigOrchestratorError { error: e });
            }
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
        let Ok(uuid) = metrics_config_uuid.parse() else {
            return Err(MetricsConfigManagerError::InvalidConfigUuid {
                uuid: metrics_config_uuid.to_owned(),
            });
        };

        info!("deactivate_metrics_config: {uuid}");

        self.metrics_config_orchestrator
            .finish_config(&uuid)
            .await
            .map_err(|_| MetricsConfigManagerError::InvalidConfigUuid { uuid: uuid.to_string() })?;

        let old_path = self.active_dir_path.join(uuid.to_string());
        let new_path = self.inactive_dir_path.join(uuid.to_string());

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
        let Ok(uuid) = metrics_config_uuid.parse() else {
            return Err(MetricsConfigManagerError::InvalidConfigUuid {
                uuid: metrics_config_uuid.to_owned(),
            });
        };

        info!("remove_metrics_config: {uuid}");
        let config_size = {
            let mut metric_config_map = self.metrics_configs_store.lock();
            metric_config_map.remove(&uuid).ok_or_else(|| {
                MetricsConfigManagerError::InvalidConfigUuid { uuid: uuid.to_string() }
            })?
        };

        let path = match self.metrics_config_orchestrator.finish_config(&uuid).await {
            Ok(()) => self.active_dir_path.join(uuid.to_string()),
            Err(ConfigOrchestratorError::MetricsConfigNotFound { .. }) => {
                info!("UUID {uuid} doesn't exist or is inactive");
                self.inactive_dir_path.join(uuid.to_string())
            }
            Err(err) => {
                return Err(MetricsConfigManagerError::ConfigOrchestratorError { error: err });
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
                    if let ConfigOrchestratorError::MetricsConfigNotFound { .. } = err {
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

    async fn get_active_metrics_config_uuids(&self) -> Vec<MetricsConfigUuid> {
        info!("get_active_metrics_config_uuids");

        self.metrics_config_orchestrator.get_active_metrics_config_uuids()
    }

    async fn get_inactive_metrics_config_uuids(&self) -> Vec<MetricsConfigUuid> {
        info!("get_active_metrics_config_uuids");

        let all_uuids = self.metrics_configs_store.lock().keys().cloned().collect::<HashSet<_>>();
        let active_uuids = self
            .metrics_config_orchestrator
            .get_active_metrics_config_uuids()
            .into_iter()
            .collect::<HashSet<_>>();

        all_uuids.difference(&active_uuids).cloned().collect::<Vec<_>>()
    }
}

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
mod tests {
    use super::*;

    use futures::future::{self, BoxFuture, FutureExt};
    use google_sdv_telemetry::aidl::google::sdv::telemetry::status::TelemetryServiceStatus::TelemetryServiceStatus;
    use googletest::assert_that;
    use googletest::matchers::{container_eq, empty, eq, err, pat};
    use lazy_static::lazy_static;
    use protobuf::Message;
    use rand::seq::SliceRandom;
    use sdv_telemetry_platform_abstraction_layer::new_stub_pal;
    use sdv_telemetry_proto_rust::metrics_configuration::MetricsConfig as MetricsConfigProto;
    use sdv_telemetry_test_utils::assert_matches;
    use sdv_telemetry_test_utils::{oneshot_callback, parameterized_test};
    use std::fs::{create_dir, create_dir_all, File};
    use std::io::Write;
    use std::path::Path;
    use tempfile::Builder;

    use crate::mock_service_publisher_accessor::MockServicePublisherAccessor;
    use crate::{MetricsConfigOrchestratorFactoryImpl, StatusReporterImpl};

    static ACTIVE_CONFIGS_DATA_DIR: &str = "telemetry/active_configs";
    static INACTIVE_CONFIGS_DATA_DIR: &str = "telemetry/inactive_configs";
    const MAX_PERSISTENT_CONFIGURATION_SIZE_MB: usize = 10 * 1024 * 1024; // 10Mbs

    lazy_static! {
        static ref UUID: MetricsConfigUuid =
            "01bf0754-df95-46bb-bb35-522b6af66600".parse().unwrap();
        static ref UUID2: MetricsConfigUuid =
            "f47ac10b-58cc-4372-a567-0e02b2c3d479".parse().unwrap();
    }
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
            _dump_manager: Arc<sdv_dump::DumpManager>,
        ) -> Arc<dyn MetricsConfigOrchestrator + Send + Sync> {
            Arc::clone(&self.orchestrator) as Arc<dyn MetricsConfigOrchestrator + Send + Sync>
        }
    }

    struct MockMetricsConfigOrchestratorImpl {
        num_initialize_config_calls: Arc<Mutex<i32>>,
        num_finish_config_calls: Arc<Mutex<i32>>,
        num_validate_config_calls: Arc<Mutex<i32>>,
        num_get_active_config_calls: Arc<Mutex<i32>>,
        active_configs: Mutex<HashSet<MetricsConfigUuid>>,
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
                active_configs: HashSet::new().into(),
                next_error: None.into(),
            }
        }
    }

    #[async_trait]
    impl MetricsConfigOrchestrator for MockMetricsConfigOrchestratorImpl {
        async fn initialize_config(
            &self,
            config: &MetricsConfigProto,
        ) -> Result<(), ConfigOrchestratorError> {
            *self.num_initialize_config_calls.lock() += 1;

            if let Some(err) = self.next_error.lock().take() {
                return Err(err);
            }

            self.active_configs.lock().insert(config.uuid.parse().unwrap());
            Ok(())
        }

        async fn finish_config(
            &self,
            uuid: &MetricsConfigUuid,
        ) -> Result<(), ConfigOrchestratorError> {
            *self.num_finish_config_calls.lock() += 1;

            if let Some(err) = self.next_error.lock().take() {
                return Err(err);
            }

            if self.active_configs.lock().remove(uuid) {
                Ok(())
            } else {
                Err(ConfigOrchestratorError::MetricsConfigNotFound { uuid: *uuid })
            }
        }

        async fn validate_config(
            &self,
            _: &MetricsConfigProto,
        ) -> Result<(), ConfigOrchestratorError> {
            *self.num_validate_config_calls.lock() += 1;

            if let Some(err) = self.next_error.lock().take() {
                return Err(err);
            }

            Ok(())
        }

        fn get_active_metrics_config_uuids(&self) -> Vec<MetricsConfigUuid> {
            *self.num_get_active_config_calls.lock() += 1;

            self.active_configs.lock().clone().into_iter().collect()
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
                Arc::new(sdv_dump::DumpManager::new("Test Dump Manager")),
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

        let file_path = active_dir_path.join(UUID.to_string());
        let serialized_config = create_serialized_config(&UUID.to_string());
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

        let file_path = active_dir_path.join(UUID.to_string());
        let serialized_config = create_serialized_config(&UUID.to_string());
        file_io_utils::write_to_file(&file_path, &serialized_config).await.unwrap_or_else(|e| {
            panic!("An error occurred in writing serialized config: {file_path:?} {e}")
        });

        let orchestrator = Arc::new(MockMetricsConfigOrchestratorImpl::new());
        orchestrator.next_error.lock().replace(ConfigOrchestratorError::CycledSourcesTriggers);

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

    parameterized_test!(
        #[sdv_telemetry_test_utils::timeout]
        #[tokio::test]
        read_invalid_active_config_from_disk,

        |create_config: Box<dyn FnOnce(PathBuf)>, expected_error_code: Option<StatusCodes>, should_remove: bool| async move {
            let root_dir = Builder::new().tempdir().unwrap();
            let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
            let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);
            create_dir_all(&active_dir_path).unwrap();

            let config_path = active_dir_path.join(UUID.to_os_string());
            create_config(config_path.clone());

            let test_environment = TestEnvironment::new(
                &active_dir_path,
                &inactive_dir_path,
                MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
            )
            .await;

            if let Some(error_code) = expected_error_code {
                let status = test_environment.error_receiver.clone().await;
                assert_that!(status.as_ref(), pat!(&TelemetryServiceStatus {
                    code: eq(error_code),
                    ..
                }));
            }
            assert_eq!(test_environment.get_num_initialize_config_calls(), 0);
            assert_eq!(should_remove, !config_path.exists());
        },

        directory: (
            Box::new(|path| {
                // Create a directory instead of a file to simulate a corrupted entry.
                create_dir(&path).unwrap();
            }),
            None,
            // Invalid entry doesn't get removed.
            // TODO: b/436613605: Reconsider this behavior
            false,
        ),
        invalid_protobuf: (
            Box::new(|path| {
                let config = MetricsConfig {
                    uuid: UUID.to_string(),
                    version: 2,
                    ..Default::default()
                };

                let mut config = config.write_to_bytes().unwrap();
                let mut rng = rand::thread_rng();
                config.shuffle(&mut rng);
                File::create(&path).unwrap().write_all(&config).unwrap();
            }),
            Some(StatusCodes::METRICS_CONFIG_PARSE_ERROR),
            // Invalid entry doesn't get removed.
            // TODO: b/436613605: Reconsider this behavior
            false,
        ),
        invalid_uuid: (
            Box::new(|path| {
                let config = MetricsConfig {
                    uuid: INVALID_UUID.to_string(),
                    ..Default::default()
                };
                let config = config.write_to_bytes().unwrap();
                File::create(&path).unwrap().write_all(&config).unwrap();
            }),
            Some(StatusCodes::METRICS_CONFIG_CORRUPTED),
            // Invalid entry doesn't get removed.
            // TODO: b/436613605: Reconsider this behavior
            false,
        ),
        incorrect_uuid: (
            Box::new(|path| {
                let config = MetricsConfig {
                    uuid: UUID2.to_string(),
                    ..Default::default()
                };
                let config = config.write_to_bytes().unwrap();
                File::create(&path).unwrap().write_all(&config).unwrap();
            }),
            Some(StatusCodes::METRICS_CONFIG_CORRUPTED),
            // Invalid entry doesn't get removed.
            // TODO: b/436613605: Reconsider this behavior
            false,
        ),
    );

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

        let file_path = inactive_dir_path.join(UUID.to_string());
        let serialized_config = create_serialized_config(&UUID.to_string());
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

    parameterized_test!(
        #[sdv_telemetry_test_utils::timeout]
        #[tokio::test]
        read_invalid_inactive_config_from_disk,

        |create_config: Box<dyn FnOnce(PathBuf)>, expected_error_code: Option<StatusCodes>, should_remove: bool| async move {
            let root_dir = Builder::new().tempdir().unwrap();
            let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
            let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);
            create_dir_all(&inactive_dir_path).unwrap();

            let config_path = inactive_dir_path.join(UUID.to_os_string());
            create_config(config_path.clone());

            let test_environment = TestEnvironment::new(
                &active_dir_path,
                &inactive_dir_path,
                MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
            )
            .await;

            if let Some(error_code) = expected_error_code {
                let status = test_environment.error_receiver.clone().await;
                assert_that!(status.as_ref(), pat!(&TelemetryServiceStatus {
                    code: eq(error_code),
                    ..
                }));
            }
            assert_eq!(test_environment.get_num_initialize_config_calls(), 0);
            assert_eq!(should_remove, !config_path.exists());
        },

        directory: (
            Box::new(|path| {
                // Create a directory instead of a file to simulate a corrupted entry.
                create_dir(&path).unwrap();
            }),
            None,
            true,
        ),
        invalid_protobuf: (
            Box::new(|path| {
                let config = MetricsConfig {
                    uuid: UUID.to_string(),
                    version: 2,
                    ..Default::default()
                };

                let mut config = config.write_to_bytes().unwrap();
                let mut rng = rand::thread_rng();
                config.shuffle(&mut rng);
                File::create(&path).unwrap().write_all(&config).unwrap();
            }),
            Some(StatusCodes::METRICS_CONFIG_PARSE_ERROR),
            // Invalid entry doesn't get removed.
            // TODO: b/436613605: Reconsider this behavior
            false,
        ),
        invalid_uuid: (
            Box::new(|path| {
                let config = MetricsConfig {
                    uuid: INVALID_UUID.to_string(),
                    ..Default::default()
                };
                let config = config.write_to_bytes().unwrap();
                File::create(&path).unwrap().write_all(&config).unwrap();
            }),
            Some(StatusCodes::METRICS_CONFIG_CORRUPTED),
            // Invalid entry doesn't get removed.
            // TODO: b/436613605: Reconsider this behavior
            false,
        ),
        incorrect_uuid: (
            Box::new(|path| {
                let config = MetricsConfig {
                    uuid: UUID2.to_string(),
                    ..Default::default()
                };
                let config = config.write_to_bytes().unwrap();
                File::create(&path).unwrap().write_all(&config).unwrap();
            }),
            Some(StatusCodes::METRICS_CONFIG_CORRUPTED),
            // Invalid entry doesn't get removed.
            // TODO: b/436613605: Reconsider this behavior
            false,
        ),
    );

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
        let file_path = inactive_dir_path.join(UUID.to_string());

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        info!("Serializing metric config");
        let serialized_config = create_serialized_config(&UUID.to_string());

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
        let file_path = inactive_dir_path.join(UUID.to_string());

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        info!("Serializing metric config");
        let serialized_config = create_serialized_config(&UUID.to_string());

        let res = test_environment.manager.add_metrics_config(&serialized_config).await;
        let err =
            test_environment.manager.add_metrics_config(&serialized_config).await.unwrap_err();

        assert!(res.is_ok());
        assert!(file_path.is_file());
        assert_matches!(&err, MetricsConfigManagerError::DuplicateMetricsConfigUuid { .. });
        assert_eq!(2, test_environment.get_num_validate_config_calls());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn add_metrics_config_invalid_config_uuid_fail() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);
        let file_path = inactive_dir_path.join(INVALID_UUID);

        let manager = MetricsConfigManagerImpl::new(
            active_dir_path.to_path_buf(),
            inactive_dir_path.to_path_buf(),
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
            Arc::new(MockServicePublisherAccessor::new()),
            new_stub_pal(),
            |_| {},
            Arc::new(StatusReporterImpl::new(move |_| {})),
            Box::new(MetricsConfigOrchestratorFactoryImpl {}),
            Arc::new(sdv_dump::DumpManager::new("Test Dump Manager")),
        )
        .await
        .unwrap();

        info!("Serializing metric config");
        let serialized_config = create_serialized_config(INVALID_UUID);

        let result = manager.add_metrics_config(&serialized_config).await;

        assert_that!(
            result,
            err(pat!(MetricsConfigManagerError::InvalidConfigUuid { uuid: eq(INVALID_UUID), .. })),
        );
        assert!(!file_path.is_file());
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
        test_environment
            .metrics_config_orchestrator
            .next_error
            .lock()
            .replace(ConfigOrchestratorError::DuplicateMetricsConfigUuid { uuid: *UUID });

        info!("Serializing metric config");
        let serialized_config = create_serialized_config(&UUID.to_string());

        let err =
            test_environment.manager.add_metrics_config(&serialized_config).await.unwrap_err();
        assert_matches!(&err, MetricsConfigManagerError::ConfigOrchestratorError { .. });
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
        let serialized_config = create_serialized_config(&UUID.to_string());

        let err =
            test_environment.manager.add_metrics_config(&serialized_config).await.unwrap_err();
        assert_matches!(&err, MetricsConfigManagerError::PersistentStorageLimitExceeded);
        assert_eq!(0, test_environment.get_num_validate_config_calls());
    }

    #[sdv_telemetry_test_utils::timeout(2_000)]
    #[tokio::test]
    async fn add_metrics_config_write_to_file_fails() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        let config_path = inactive_dir_path.join(UUID.to_string());
        // Create a directory on a path corresponding to the config file
        file_io_utils::create_dir(&config_path).await.unwrap_or_else(|e| {
            panic!("An error occurred in creating a directory over {config_path:?}: {e}");
        });

        let serialized_config = create_serialized_config(&UUID.to_string());
        let result = test_environment.manager.add_metrics_config(&serialized_config).await;

        assert_that!(result, err(pat!(MetricsConfigManagerError::ConfigWriteError { .. })));
        // TODO: b/436613605: Reconsider this behavior
        assert!(config_path.exists());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_metrics_config_initialize_configs_calls_received() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);

        let old_file_path = inactive_dir_path.join(UUID.to_string());
        let new_file_path = active_dir_path.join(UUID.to_string());

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        let serialized_config = create_serialized_config(&UUID.to_string());
        test_environment.manager.add_metrics_config(&serialized_config).await.unwrap();
        assert!(old_file_path.is_file());
        assert_eq!(1, test_environment.get_num_validate_config_calls());

        let res = test_environment.manager.activate_metrics_config(&UUID.to_string()).await;
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
        let old_file_path = inactive_dir_path.join(UUID.to_string());

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        let serialized_config = create_serialized_config(&UUID.to_string());
        test_environment.manager.add_metrics_config(&serialized_config).await.unwrap();
        assert!(old_file_path.is_file());
        assert_eq!(1, test_environment.get_num_validate_config_calls());

        test_environment
            .metrics_config_orchestrator
            .next_error
            .lock()
            .replace(ConfigOrchestratorError::DuplicateMetricsConfigUuid { uuid: *UUID });

        let err =
            test_environment.manager.activate_metrics_config(&UUID.to_string()).await.unwrap_err();
        assert_matches!(&err, MetricsConfigManagerError::ConfigOrchestratorError { .. });
        assert_eq!(1, test_environment.get_num_initialize_config_calls());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_metrics_config_no_file_fail() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        let result = test_environment.manager.activate_metrics_config(&UUID.to_string()).await;
        assert_that!(
            result,
            err(pat!(MetricsConfigManagerError::InvalidConfigUuid {
                uuid: eq(&UUID.to_string()),
                ..
            }))
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn deactivate_metrics_config_finish_config_calls_received() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);

        let old_file_path = inactive_dir_path.join(UUID.to_string());
        let new_file_path = active_dir_path.join(UUID.to_string());

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        let serialized_config = create_serialized_config(&UUID.to_string());
        test_environment.manager.add_metrics_config(&serialized_config).await.unwrap();
        assert!(old_file_path.is_file());
        assert_eq!(1, test_environment.get_num_validate_config_calls());

        test_environment.manager.activate_metrics_config(&UUID.to_string()).await.unwrap();
        assert!(new_file_path.is_file());
        assert_eq!(1, test_environment.get_num_initialize_config_calls());

        let res = test_environment.manager.deactivate_metrics_config(&UUID.to_string()).await;
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
        let old_file_path = inactive_dir_path.join(UUID.to_string());
        let new_file_path = active_dir_path.join(UUID.to_string());

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        let serialized_config = create_serialized_config(&UUID.to_string());
        test_environment.manager.add_metrics_config(&serialized_config).await.unwrap();
        assert!(old_file_path.is_file());
        assert_eq!(1, test_environment.get_num_validate_config_calls());

        test_environment.manager.activate_metrics_config(&UUID.to_string()).await.unwrap();
        assert!(new_file_path.is_file());
        assert_eq!(1, test_environment.get_num_initialize_config_calls());

        test_environment
            .metrics_config_orchestrator
            .next_error
            .lock()
            .replace(ConfigOrchestratorError::DuplicateMetricsConfigUuid { uuid: *UUID });

        let err = test_environment
            .manager
            .deactivate_metrics_config(&UUID.to_string())
            .await
            .unwrap_err();
        assert_matches!(&err, MetricsConfigManagerError::InvalidConfigUuid { .. });

        assert!(new_file_path.is_file());
        assert_eq!(1, test_environment.get_num_finish_config_calls());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn remove_active_metrics_config_success() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);
        let old_file_path = inactive_dir_path.join(UUID.to_string());
        let new_file_path = active_dir_path.join(UUID.to_string());

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        let serialized_config = create_serialized_config(&UUID.to_string());
        test_environment.manager.add_metrics_config(&serialized_config).await.unwrap();
        assert!(old_file_path.is_file());
        assert_eq!(1, test_environment.get_num_validate_config_calls());

        test_environment.manager.activate_metrics_config(&UUID.to_string()).await.unwrap();
        assert!(new_file_path.is_file());
        assert_eq!(1, test_environment.get_num_initialize_config_calls());

        let res = test_environment.manager.remove_metrics_config(&UUID.to_string()).await;
        assert_eq!(1, test_environment.get_num_finish_config_calls());
        assert!(res.is_ok());
        assert!(!old_file_path.is_file());
        assert!(!new_file_path.is_file());
    }

    #[sdv_telemetry_test_utils::timeout(2_000)]
    #[tokio::test]
    async fn remove_inactive_metrics_config_success() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);
        let file_path = inactive_dir_path.join(UUID.to_string());

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        let serialized_config = create_serialized_config(&UUID.to_string());
        test_environment.manager.add_metrics_config(&serialized_config).await.unwrap();
        assert!(file_path.is_file());
        assert_eq!(test_environment.get_num_validate_config_calls(), 1);

        test_environment.manager.remove_metrics_config(&UUID.to_string()).await.unwrap();
        assert!(!file_path.is_file());
        assert_eq!(test_environment.get_num_finish_config_calls(), 1);
    }

    #[sdv_telemetry_test_utils::timeout(2_000)]
    #[tokio::test]
    async fn remove_inactive_metrics_config_twice_fails() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        let serialized_config = create_serialized_config(&UUID.to_string());
        test_environment.manager.add_metrics_config(&serialized_config).await.unwrap();
        assert_eq!(test_environment.get_num_validate_config_calls(), 1);

        test_environment.manager.remove_metrics_config(&UUID.to_string()).await.unwrap();
        let result = test_environment.manager.remove_metrics_config(&UUID.to_string()).await;

        assert_that!(result, err(pat!(MetricsConfigManagerError::InvalidConfigUuid { .. })));
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
        let err =
            test_environment.manager.remove_metrics_config(&UUID.to_string()).await.unwrap_err();
        assert_matches!(
            &err,
            MetricsConfigManagerError::InvalidConfigUuid { uuid } if *uuid == UUID.to_string()
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn remove_all_metrics_configs() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);

        let old_file_path1 = inactive_dir_path.join(UUID.to_string());
        let new_file_path1 = active_dir_path.join(UUID.to_string());
        let old_file_path2 = inactive_dir_path.join(UUID2.to_string());
        let new_file_path2 = active_dir_path.join(UUID2.to_string());

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        let serialized_config1 = create_serialized_config(&UUID.to_string());
        let serialized_config2 = create_serialized_config(&UUID2.to_string());

        // Add configs
        test_environment.manager.add_metrics_config(&serialized_config1).await.unwrap();
        test_environment.manager.add_metrics_config(&serialized_config2).await.unwrap();
        assert!(old_file_path1.is_file());
        assert!(old_file_path2.is_file());
        assert_eq!(2, test_environment.get_num_validate_config_calls());

        // Activate one of the configs
        test_environment.manager.activate_metrics_config(&UUID.to_string()).await.unwrap();
        assert!(new_file_path1.is_file());
        assert!(!new_file_path2.is_file());
        assert_eq!(1, test_environment.get_num_initialize_config_calls());

        // Remove all configs
        let res = test_environment.manager.remove_all_metrics_configs().await;
        assert!(res.is_ok());
        assert_eq!(active_dir_path.read_dir().unwrap().count(), 0); // check if directory is empty
        assert_eq!(inactive_dir_path.read_dir().unwrap().count(), 0); // check if directory is empty
        assert_eq!(2, test_environment.get_num_finish_config_calls());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_active_inactive_metrics_configs_success() {
        let root_dir = Builder::new().tempdir().unwrap();
        let active_dir_path = root_dir.path().join(ACTIVE_CONFIGS_DATA_DIR);
        let inactive_dir_path = root_dir.path().join(INACTIVE_CONFIGS_DATA_DIR);

        let test_environment = TestEnvironment::new(
            &active_dir_path,
            &inactive_dir_path,
            MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
        )
        .await;

        let serialized_config = create_serialized_config(&UUID.to_string());

        test_environment.manager.add_metrics_config(&serialized_config).await.unwrap();
        let active_configs = test_environment.manager.get_active_metrics_config_uuids().await;
        let inactive_configs = test_environment.manager.get_inactive_metrics_config_uuids().await;

        assert_that!(inactive_configs, container_eq([*UUID]));
        assert_that!(active_configs, empty());
        assert_eq!(test_environment.get_num_get_active_config_calls(), 2);

        test_environment.manager.activate_metrics_config(&UUID.to_string()).await.unwrap();
        let active_configs = test_environment.manager.get_active_metrics_config_uuids().await;
        let inactive_configs = test_environment.manager.get_inactive_metrics_config_uuids().await;

        assert_that!(inactive_configs, empty());
        assert_that!(active_configs, container_eq([*UUID]));
        assert_eq!(test_environment.get_num_get_active_config_calls(), 4);
    }
}
