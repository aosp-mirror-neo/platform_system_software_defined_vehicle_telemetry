// Copyright 2023 Google LLC. All Rights Reserved.

use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::metrics_config_orchestrator::MetricsConfigOrchestratorError as ConfigOrchestratorError;
use crate::{
    metrics_config_manager::MetricsConfigManagerError as ConfigManagerError, MetricsConfigManager,
};
use sdvtelemetry_proto_rust::metrics_configuration::MetricsConfig;

pub(crate) struct MockMetricsConfigManager {
    num_add_metrics_config_calls: Arc<Mutex<i32>>,
    num_activate_metrics_config_calls: Arc<Mutex<i32>>,
    num_deactivate_metrics_config_calls: Arc<Mutex<i32>>,
    num_remove_config_calls: Arc<Mutex<i32>>,
    num_remove_all_config_calls: Arc<Mutex<i32>>,
    active_configs: Mutex<HashSet<String>>,
    inactive_configs: Mutex<HashSet<String>>,
}

impl MockMetricsConfigManager {
    pub(crate) fn new(
        num_add_metrics_config_calls: Arc<Mutex<i32>>,
        num_activate_metrics_config_calls: Arc<Mutex<i32>>,
        num_deactivate_metrics_config_calls: Arc<Mutex<i32>>,
        num_remove_config_calls: Arc<Mutex<i32>>,
        num_remove_all_config_calls: Arc<Mutex<i32>>,
    ) -> Self {
        Self {
            num_add_metrics_config_calls,
            num_activate_metrics_config_calls,
            num_deactivate_metrics_config_calls,
            num_remove_config_calls,
            num_remove_all_config_calls,
            active_configs: HashSet::new().into(),
            inactive_configs: HashSet::new().into(),
        }
    }
}

#[async_trait]
impl MetricsConfigManager for MockMetricsConfigManager {
    async fn add_metrics_config(
        &self,
        metrics_config: &[u8],
    ) -> Result<String, ConfigManagerError> {
        let config: MetricsConfig = protobuf::Message::parse_from_bytes(metrics_config)
            .map_err(|_| ConfigManagerError::ConfigParseError)?;
        let mut num_calls = self.num_add_metrics_config_calls.lock().unwrap();
        *num_calls += 1;
        if self.inactive_configs.lock().unwrap().insert(config.uuid.clone()) {
            return Ok(config.uuid);
        }
        Err(ConfigManagerError::ConfigOrchestratorError {
            error: ConfigOrchestratorError::DuplicateMetricsConfigUuid { uuid: config.uuid },
        })
    }

    async fn activate_metrics_config(&self, config_uuid: &str) -> Result<(), ConfigManagerError> {
        let mut num_calls = self.num_activate_metrics_config_calls.lock().unwrap();
        *num_calls += 1;
        let maybe_uuid = self.inactive_configs.lock().unwrap().take(config_uuid);
        if let Some(uuid) = maybe_uuid {
            self.active_configs.lock().unwrap().insert(uuid);
            return Ok(());
        }

        Err(ConfigManagerError::InvalidConfigUuid { uuid: config_uuid.to_string() })
    }

    async fn deactivate_metrics_config(&self, config_uuid: &str) -> Result<(), ConfigManagerError> {
        let mut num_calls = self.num_deactivate_metrics_config_calls.lock().unwrap();
        *num_calls += 1;
        let maybe_uuid = self.active_configs.lock().unwrap().take(config_uuid);
        if let Some(uuid) = maybe_uuid {
            self.inactive_configs.lock().unwrap().insert(uuid);
            return Ok(());
        }

        Err(ConfigManagerError::InvalidConfigUuid { uuid: config_uuid.to_string() })
    }

    async fn remove_metrics_config(&self, config_uuid: &str) -> Result<(), ConfigManagerError> {
        let mut num_calls = self.num_remove_config_calls.lock().unwrap();
        *num_calls += 1;
        let maybe_uuid = self.active_configs.lock().unwrap().take(config_uuid);
        if maybe_uuid.is_some() {
            return Ok(());
        }
        let maybe_uuid = self.inactive_configs.lock().unwrap().take(config_uuid);
        if maybe_uuid.is_some() {
            return Ok(());
        }
        Err(ConfigManagerError::InvalidConfigUuid { uuid: config_uuid.to_string() })
    }

    async fn remove_all_metrics_configs(&self) -> Result<(), ConfigManagerError> {
        let mut num_calls = self.num_remove_all_config_calls.lock().unwrap();
        *num_calls += 1;
        Ok(())
    }
}
