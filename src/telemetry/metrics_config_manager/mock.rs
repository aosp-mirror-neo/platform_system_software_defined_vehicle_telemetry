// Copyright 2023 Google LLC

use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::sync::Arc;

use crate::metrics_config_orchestrator::MetricsConfigOrchestratorError as ConfigOrchestratorError;
use crate::metrics_config_uuid::MetricsConfigUuid;
use crate::{
    metrics_config_manager::MetricsConfigManagerError as ConfigManagerError, MetricsConfigManager,
};
use sdv_telemetry_proto_rust::metrics_configuration::MetricsConfig;

pub(crate) struct MockMetricsConfigManager {
    num_add_metrics_config_calls: Arc<Mutex<i32>>,
    num_activate_metrics_config_calls: Arc<Mutex<i32>>,
    num_deactivate_metrics_config_calls: Arc<Mutex<i32>>,
    num_remove_config_calls: Arc<Mutex<i32>>,
    num_remove_all_config_calls: Arc<Mutex<i32>>,
    num_get_active_config_calls: Arc<Mutex<i32>>,
    num_get_inactive_config_calls: Arc<Mutex<i32>>,
    active_configs: Mutex<HashSet<MetricsConfigUuid>>,
    inactive_configs: Mutex<HashSet<MetricsConfigUuid>>,
}

impl MockMetricsConfigManager {
    pub(crate) fn new(
        num_add_metrics_config_calls: Arc<Mutex<i32>>,
        num_activate_metrics_config_calls: Arc<Mutex<i32>>,
        num_deactivate_metrics_config_calls: Arc<Mutex<i32>>,
        num_remove_config_calls: Arc<Mutex<i32>>,
        num_remove_all_config_calls: Arc<Mutex<i32>>,
        num_get_active_config_calls: Arc<Mutex<i32>>,
        num_get_inactive_config_calls: Arc<Mutex<i32>>,
    ) -> Self {
        Self {
            num_add_metrics_config_calls,
            num_activate_metrics_config_calls,
            num_deactivate_metrics_config_calls,
            num_remove_config_calls,
            num_remove_all_config_calls,
            num_get_active_config_calls,
            num_get_inactive_config_calls,
            active_configs: HashSet::new().into(),
            inactive_configs: HashSet::new().into(),
        }
    }

    pub(crate) fn populate_active_configs(&self, config_uuids: Vec<MetricsConfigUuid>) {
        let mut active_configs = self.active_configs.lock();
        for uuid in config_uuids {
            active_configs.insert(uuid);
        }
    }

    pub(crate) fn populate_inactive_configs(&self, config_uuids: Vec<MetricsConfigUuid>) {
        let mut inactive_configs = self.inactive_configs.lock();
        for uuid in config_uuids {
            inactive_configs.insert(uuid);
        }
    }
}

#[async_trait]
impl MetricsConfigManager for MockMetricsConfigManager {
    async fn add_metrics_config(
        &self,
        metrics_config: &[u8],
    ) -> Result<MetricsConfigUuid, ConfigManagerError> {
        let config: MetricsConfig = protobuf::Message::parse_from_bytes(metrics_config)
            .map_err(|_| ConfigManagerError::ConfigParseError)?;
        *self.num_add_metrics_config_calls.lock() += 1;
        let config_uuid = config.uuid.parse().map_err(|_err| {
            ConfigManagerError::InvalidConfigUuid { uuid: config.uuid.to_owned() }
        })?;
        if self.inactive_configs.lock().insert(config_uuid) {
            return Ok(config_uuid);
        }

        Err(ConfigManagerError::ConfigOrchestratorError {
            error: ConfigOrchestratorError::DuplicateMetricsConfigUuid { uuid: config_uuid },
        })
    }

    async fn activate_metrics_config(&self, config_uuid: &str) -> Result<(), ConfigManagerError> {
        *self.num_activate_metrics_config_calls.lock() += 1;
        let config_uuid = config_uuid.parse().map_err(|_err| {
            ConfigManagerError::InvalidConfigUuid { uuid: config_uuid.to_owned() }
        })?;
        let maybe_uuid = self.inactive_configs.lock().take(&config_uuid);
        if let Some(uuid) = maybe_uuid {
            self.active_configs.lock().insert(uuid);
            return Ok(());
        }

        Err(ConfigManagerError::InvalidConfigUuid { uuid: config_uuid.to_string() })
    }

    async fn deactivate_metrics_config(&self, config_uuid: &str) -> Result<(), ConfigManagerError> {
        *self.num_deactivate_metrics_config_calls.lock() += 1;
        let config_uuid = config_uuid.parse().map_err(|_err| {
            ConfigManagerError::InvalidConfigUuid { uuid: config_uuid.to_owned() }
        })?;
        let maybe_uuid = self.active_configs.lock().take(&config_uuid);
        if let Some(uuid) = maybe_uuid {
            self.inactive_configs.lock().insert(uuid);
            return Ok(());
        }

        Err(ConfigManagerError::InvalidConfigUuid { uuid: config_uuid.to_string() })
    }

    async fn remove_metrics_config(&self, config_uuid: &str) -> Result<(), ConfigManagerError> {
        *self.num_remove_config_calls.lock() += 1;
        let config_uuid = config_uuid.parse().map_err(|_err| {
            ConfigManagerError::InvalidConfigUuid { uuid: config_uuid.to_owned() }
        })?;
        let maybe_uuid = self.active_configs.lock().take(&config_uuid);
        if maybe_uuid.is_some() {
            return Ok(());
        }
        let maybe_uuid = self.inactive_configs.lock().take(&config_uuid);
        if maybe_uuid.is_some() {
            return Ok(());
        }
        Err(ConfigManagerError::InvalidConfigUuid { uuid: config_uuid.to_string() })
    }

    async fn remove_all_metrics_configs(&self) -> Result<(), ConfigManagerError> {
        *self.num_remove_all_config_calls.lock() += 1;
        Ok(())
    }

    async fn get_active_metrics_config_uuids(&self) -> Vec<MetricsConfigUuid> {
        *self.num_get_active_config_calls.lock() += 1;

        let config_list = self.active_configs.lock().clone().into_iter().collect();

        config_list
    }

    async fn get_inactive_metrics_config_uuids(&self) -> Vec<MetricsConfigUuid> {
        *self.num_get_inactive_config_calls.lock() += 1;

        let config_list = self.inactive_configs.lock().clone().into_iter().collect();

        config_list
    }
}
