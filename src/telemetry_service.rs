// Copyright 2023 Google LLC

//! This module implements the ITelemetryService AIDL interface

mod expr;
mod metrics_config_manager;
mod metrics_config_orchestrator;
mod metrics_report_listener;
pub mod service_publisher_accessor;
pub mod service_publisher_manager;

// External dependencies.
use async_trait::async_trait;
use google_sdv_telemetry::aidl::google::sdv::telemetry::{
    ITelemetryApiStatusListener::ITelemetryApiStatusListener,
    ITelemetryConfigListListener::ITelemetryConfigListListener,
    ITelemetryMiscStatusListener::ITelemetryMiscStatusListener,
    ITelemetryReportListener::ITelemetryReportListener,
    ITelemetryReportReadyListener::ITelemetryReportReadyListener,
    ITelemetryService::ITelemetryServiceAsyncServer,
    TelemetryServiceStatusCodes::TelemetryServiceStatusCodes as StatusCodes,
};
use google_sdv_telemetry::binder::{Interface, Result as BinderResult, Strong};
use log::{error, info, warn};
use protobuf::Message;
use std::marker::{Send, Sync};
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use tracing::instrument;

// Child module dependencies.
pub use service_publisher_accessor::ServicePublisherAccessor;

use metrics_config_manager::MetricsConfigManager;
use metrics_config_manager::{
    MetricsConfigManagerError as ConfigManagerError, MetricsConfigManagerImpl,
};

use metrics_config_orchestrator::MetricsConfigOrchestratorError as ConfigOrchestratorError;
use metrics_report_listener::MetricsReportListener;

const ACTIVE_CONFIGS_DATA_DIR: &str = "telemetry/active_configs";
const INACTIVE_CONFIGS_DATA_DIR: &str = "telemetry/inactive_configs";

fn get_error_code_and_message(err: ConfigManagerError) -> (StatusCodes, String) {
    let err_code = match err {
        ConfigManagerError::ConfigOrchestratorError { ref error } => match error {
            ConfigOrchestratorError::DuplicatePublisherDefinition { .. } => {
                StatusCodes::ADD_METRICS_CONFIG_FAILED
            }
            ConfigOrchestratorError::DuplicateTriggerDefinition { .. } => {
                StatusCodes::ADD_METRICS_CONFIG_FAILED
            }
            ConfigOrchestratorError::DuplicateMetricsReportConfigName { .. } => {
                StatusCodes::ADD_METRICS_CONFIG_FAILED
            }
            ConfigOrchestratorError::CycledPublishersTriggers => {
                StatusCodes::ADD_METRICS_CONFIG_FAILED
            }
            ConfigOrchestratorError::InvalidDependency { .. } => {
                StatusCodes::ADD_METRICS_CONFIG_FAILED
            }
            ConfigOrchestratorError::DuplicateMetricsConfigUuid { .. } => {
                StatusCodes::ADD_METRICS_CONFIG_FAILED
            }
            ConfigOrchestratorError::InvalidMetricsReportConfig { .. } => {
                StatusCodes::ADD_METRICS_CONFIG_FAILED
            }
            ConfigOrchestratorError::InvalidPublisherDefinition { .. } => {
                StatusCodes::ADD_METRICS_CONFIG_FAILED
            }
            ConfigOrchestratorError::InvalidTriggerDefinition { .. } => {
                StatusCodes::ADD_METRICS_CONFIG_FAILED
            }
            ConfigOrchestratorError::InvalidDescriptorProtos { .. } => {
                StatusCodes::ADD_METRICS_CONFIG_FAILED
            }
            ConfigOrchestratorError::InvalidConfigUuid { .. } => {
                StatusCodes::METRICS_CONFIG_INVALID_CONFIG_UUID
            }
            ConfigOrchestratorError::NoStartTriggerDefined { .. } => {
                StatusCodes::ADD_METRICS_CONFIG_FAILED
            }
            ConfigOrchestratorError::UnableToActivatePublisher { .. } => {
                StatusCodes::ACTIVATE_METRICS_CONFIG_FAILED
            }
            _ => StatusCodes::ADD_METRICS_CONFIG_FAILED,
        },
        ConfigManagerError::InvalidConfigUuid { .. } => {
            StatusCodes::METRICS_CONFIG_INVALID_CONFIG_UUID
        }
        ConfigManagerError::ConfigReadError { .. } => StatusCodes::METRICS_CONFIG_CORRUPTED,
        ConfigManagerError::ConfigRemoveError { .. } => StatusCodes::REMOVE_METRICS_CONFIG_FAILED,
        ConfigManagerError::ConfigParseError => StatusCodes::METRICS_CONFIG_PARSE_ERROR,
        ConfigManagerError::ConfigWriteError { .. } => StatusCodes::ADD_METRICS_CONFIG_FAILED,
        ConfigManagerError::FileIOError { .. } => StatusCodes::FILE_IO_ERROR,
        ConfigManagerError::PersistentStorageLimitExceeded => {
            StatusCodes::ADD_METRICS_CONFIG_FAILED
        }
    };

    (err_code, err.to_string())
}

/// This struct is defined to implement ITelemetryService AIDL interface.
pub struct SdvTelemetryService {
    /// Instance of Metrics Configuration Manager.
    metrics_config_manager: Arc<dyn MetricsConfigManager + Send + Sync>,
    /// Report Ready listener to inform the client when a report is ready
    metrics_report_listener: Arc<MetricsReportListener>,
    /// Status Listener to send back status to client
    api_status_listener: Mutex<Option<Strong<dyn ITelemetryApiStatusListener>>>,
    /// Misc Status Listener to send back status to client
    misc_status_listener: Mutex<Option<Strong<dyn ITelemetryMiscStatusListener>>>,
    /// Store for miscellaneous errors
    misc_error_store: Arc<Mutex<Vec<(StatusCodes, String)>>>,
}

impl Interface for SdvTelemetryService {}

impl SdvTelemetryService {
    /// Instantiates an instance of SdvTelemetryService
    #[instrument(skip(service_publisher_manager))]
    pub async fn new(
        root_path: &str,
        config_store_size: usize,
        service_publisher_manager: Arc<dyn ServicePublisherAccessor>,
    ) -> Option<Self> {
        let (active_data_dir, inactive_data_dir) = if !root_path.is_empty() {
            (
                format!("{}/{}", root_path, ACTIVE_CONFIGS_DATA_DIR),
                format!("{}/{}", root_path, INACTIVE_CONFIGS_DATA_DIR),
            )
        } else {
            (ACTIVE_CONFIGS_DATA_DIR.to_owned(), INACTIVE_CONFIGS_DATA_DIR.to_owned())
        };

        // Create manager and its dependencies
        let report_listener = Arc::new(MetricsReportListener::new());
        let report_listener_clone = Arc::clone(&report_listener);

        let misc_error_store = Arc::new(Mutex::new(Vec::new()));
        let misc_error_store_clone = Arc::clone(&misc_error_store);

        let config_manager_res = MetricsConfigManagerImpl::new(
            active_data_dir,
            inactive_data_dir,
            config_store_size,
            service_publisher_manager,
            move |report| report_listener.on_report_ready(report),
            move |status_code, status_message| {
                let mut vec = misc_error_store_clone.lock().unwrap();
                vec.push((status_code, status_message))
            },
        )
        .await;

        let config_manager = match config_manager_res {
            Ok(config_manager) => config_manager,
            Err(e) => {
                error!("Error in initializing config manager: {:?}", e);
                return None;
            }
        };

        Some(SdvTelemetryService {
            metrics_config_manager: config_manager,
            metrics_report_listener: report_listener_clone,
            api_status_listener: Mutex::new(None),
            misc_status_listener: Mutex::new(None),
            misc_error_store,
        })
    }

    /// Uses the listener object set by the telemetry service cpp client to send back status codes
    /// and error messages for API calls
    pub fn send_status(
        &self,
        api_call_id: i32,
        status_code: StatusCodes,
        status_message: &str,
    ) -> BinderResult<()> {
        if let Ok(listener) = self.api_status_listener.lock() {
            if let Some(listener_ref) = listener.as_ref() {
                if let Err(e) =
                    listener_ref.deref().sendStatus(api_call_id, status_code, status_message)
                {
                    warn!(
                        "Error when sending status code {:?} for API call {} to the client: {:?}",
                        status_code, api_call_id, e
                    );
                }
            }
        }
        Ok(())
    }

    /// Uses the global listener object set by the client to send back status codes
    /// and error messages for API calls
    pub fn send_misc_status(
        &self,
        status_code: StatusCodes,
        status_message: &str,
    ) -> BinderResult<()> {
        if let Ok(listener) = self.misc_status_listener.lock() {
            if let Some(listener_ref) = listener.as_ref() {
                if let Err(e) = listener_ref.deref().sendStatus(status_code, status_message) {
                    warn!(
                        "Error when sending status code {:?} to the client: {:?}",
                        status_code, e
                    );
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ITelemetryServiceAsyncServer for SdvTelemetryService {
    #[instrument(skip(self, metrics_config))]
    async fn addMetricsConfig(&self, api_call_id: i32, metrics_config: &[u8]) -> BinderResult<()> {
        // Add metrics config and report any errors to client
        let metrics_config_uuid =
            match self.metrics_config_manager.add_metrics_config(metrics_config).await {
                Ok(config_uuid) => config_uuid,
                Err(e) => {
                    error!("Invalid Metric Config");
                    let (err_code, err_msg) = get_error_code_and_message(e);
                    return self.send_status(api_call_id, err_code, &err_msg);
                }
            };

        return self.send_status(
            api_call_id,
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            &metrics_config_uuid,
        );
    }

    #[instrument(skip(self))]
    async fn activateMetricsConfig(
        &self,
        api_call_id: i32,
        metrics_config_uuid: &str,
    ) -> BinderResult<()> {
        // Activate metrics config and report any errors to client
        if let Err(e) =
            self.metrics_config_manager.activate_metrics_config(metrics_config_uuid).await
        {
            error!("Unable to activate Metric Config Err: {:?}", e);
            let (err_code, err_msg) = get_error_code_and_message(e);
            return self.send_status(api_call_id, err_code, &err_msg);
        }

        self.send_status(
            api_call_id,
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            metrics_config_uuid,
        )
    }

    #[instrument(skip(self))]
    async fn deactivateMetricsConfig(
        &self,
        api_call_id: i32,
        metrics_config_uuid: &str,
    ) -> BinderResult<()> {
        // Deactivate metrics config and report any errors to client
        if let Err(e) =
            self.metrics_config_manager.deactivate_metrics_config(metrics_config_uuid).await
        {
            error!("Unable to deactivate Metric Config Err: {:?}", e);
            let (err_code, err_msg) = get_error_code_and_message(e);
            return self.send_status(api_call_id, err_code, &err_msg);
        }

        self.send_status(
            api_call_id,
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            metrics_config_uuid,
        )
    }

    #[instrument(skip(self))]
    async fn removeMetricsConfig(
        &self,
        api_call_id: i32,
        metrics_config_uuid: &str,
    ) -> BinderResult<()> {
        // Remove metrics config and report any errors to client
        if let Err(e) = self.metrics_config_manager.remove_metrics_config(metrics_config_uuid).await
        {
            error!("Unable to remove Metric Config Err: {:?}", e);
            let (err_code, err_msg) = get_error_code_and_message(e);
            return self.send_status(api_call_id, err_code, &err_msg);
        }

        self.send_status(
            api_call_id,
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            metrics_config_uuid,
        )
    }

    #[instrument(skip(self))]
    async fn removeAllMetricsConfigs(&self, api_call_id: i32) -> BinderResult<()> {
        // Remove all metrics config and report any errors to client
        if let Err(e) = self.metrics_config_manager.remove_all_metrics_configs().await {
            error!("Unable to remove all metric configs Err: {:?}", e);
            let (err_code, err_msg) = get_error_code_and_message(e);
            return self.send_status(api_call_id, err_code, &err_msg);
        }

        self.send_status(
            api_call_id,
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            "successfully Removed All Configs",
        )
    }

    #[instrument(skip(self, config_list_listener))]
    async fn getActiveMetricsConfigUuids(
        &self,
        config_list_listener: &Strong<dyn ITelemetryConfigListListener>,
    ) -> BinderResult<()> {
        info!("getActiveMetricsConfigUuids");

        let config_list = self.metrics_config_manager.get_active_metrics_config_uuids().await;
        if let Err(e) = config_list_listener.deref().sendConfigList(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            "",
            &config_list,
        ) {
            warn!("Unable to send config list to the client: {:?}", e);
        }

        Ok(())
    }

    #[instrument(skip(self, config_list_listener))]
    async fn getInactiveMetricsConfigUuids(
        &self,
        config_list_listener: &Strong<dyn ITelemetryConfigListListener>,
    ) -> BinderResult<()> {
        info!("getActiveMetricsConfigUuids");
        let config_list = self.metrics_config_manager.get_inactive_metrics_config_uuids().await;
        if let Err(e) = config_list_listener.deref().sendConfigList(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            "",
            &config_list,
        ) {
            warn!("Unable to send config list to the client: {:?}", e);
        }

        Ok(())
    }

    #[instrument(skip(self))]
    async fn registerReportReadyListener(
        &self,
        api_call_id: i32,
        listener: &Strong<dyn ITelemetryReportReadyListener>,
    ) -> BinderResult<()> {
        info!("registerReportReadyListener");
        let (status_code, status_message) =
            self.metrics_report_listener.set_report_ready_listener(listener).map_or_else(
                |_| {
                    (
                        StatusCodes::SET_REPORT_READY_LISTENER_FAILED,
                        "Report Ready Listener already exists",
                    )
                },
                |_| {
                    (
                        StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
                        "Report Ready Listener successfully added",
                    )
                },
            );
        self.send_status(api_call_id, status_code, status_message)
    }

    #[instrument(skip(self))]
    async fn registerApiStatusListener(
        &self,
        listener: &Strong<dyn ITelemetryApiStatusListener>,
    ) -> BinderResult<()> {
        *self.api_status_listener.lock().unwrap() = Some(listener.clone());
        Ok(())
    }

    #[instrument(skip(self))]
    async fn registerMiscStatusListener(
        &self,
        api_call_id: i32,
        listener: &Strong<dyn ITelemetryMiscStatusListener>,
    ) -> BinderResult<()> {
        let (status_code, status_message) = {
            let mut guard = self.misc_status_listener.lock().unwrap();
            if guard.is_none() {
                *guard = Some(listener.clone());
                (
                    StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
                    "Status listener successfully added",
                )
            } else {
                (
                    StatusCodes::SET_MISC_STATUS_LISTENER_FAILED,
                    "Misc Status listener already exists",
                )
            }
        };
        self.send_status(api_call_id, status_code, status_message).unwrap();

        let misc_errors = self.misc_error_store.lock().unwrap();

        for (status_code, status_message) in misc_errors.iter() {
            self.send_misc_status(*status_code, status_message).unwrap();
        }

        Ok(())
    }

    #[instrument(skip(self, report_listener))]
    async fn getMetricsReport(
        &self,
        metrics_config_uuid: &str,
        report_config_name: &str,
        report_number: i32,
        report_listener: &Strong<dyn ITelemetryReportListener>,
    ) -> BinderResult<()> {
        info!("getMetricsReport: {}", metrics_config_uuid);
        if let Ok(metrics_report) = self.metrics_report_listener.get_metrics_report(
            metrics_config_uuid,
            report_config_name,
            report_number,
        ) {
            let serialized_report: &[u8] = &metrics_report.write_to_bytes().unwrap();
            if let Err(e) = report_listener.deref().onReport(
                StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
                metrics_config_uuid,
                serialized_report,
            ) {
                warn!("Unable to send report to the client: {:?}", e);
            }
        } else if let Err(e) = report_listener.deref().onReport(
            StatusCodes::GET_FINISHED_REPORT_FAILED,
            metrics_config_uuid,
            &[],
        ) {
            warn!("Unable to send report retrieval error to the client: {:?}", e);
        }

        Ok(())
    }

    #[instrument(skip(self))]
    async fn unregisterReportReadyListener(&self) -> BinderResult<()> {
        info!("unregisterReportReadyListener");
        self.metrics_report_listener.clear_report_ready_listener().unwrap();
        Ok(())
    }

    #[instrument(skip(self))]
    async fn unregisterApiStatusListener(&self) -> BinderResult<()> {
        info!("unregisterApiStatusListener");
        *self.api_status_listener.lock().unwrap() = None;
        Ok(())
    }

    #[instrument(skip(self))]
    async fn unregisterMiscStatusListener(&self) -> BinderResult<()> {
        info!("unregisterMiscStatusListener");
        *self.misc_status_listener.lock().unwrap() = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use binder::BinderFeatures;
    use google_sdv_telemetry::aidl::google::sdv::telemetry::{
        ITelemetryApiStatusListener::BnTelemetryApiStatusListener,
        ITelemetryConfigListListener::BnTelemetryConfigListListener,
        ITelemetryMiscStatusListener::BnTelemetryMiscStatusListener,
        ITelemetryReportListener::BnTelemetryReportListener,
        ITelemetryReportReadyListener::BnTelemetryReportReadyListener,
    };
    use protobuf::Message;
    use sdv_telemetry_proto_rust::metrics_configuration::MetricsConfig;
    use sdv_telemetry_proto_rust::metrics_configuration::MetricsReport;

    use super::*;
    use crate::metrics_config_manager::mock::MockMetricsConfigManager;
    use crate::service_publisher_accessor::mock::MockServicePublisherAccessor;

    const DUMMY_API_CALL_ID: i32 = -1;
    const MAX_PERSISTENT_CONFIGURATION_SIZE_MB: usize = 10 * 1024 * 1024; // 10Mbs
    const REPORT_NUMBER: i32 = 42;
    const UUID1: &str = "01bf0754-df95-46bb-bb35-522b6af66600";
    const UUID2: &str = "f47ac10b-58cc-4372-a567-0e02b2c3d479";

    /// This struct is defined to implement ITelemetryReportListener AIDL interface for unit testing purposes
    #[derive(Clone)]
    struct MockTelemetryConfigListListener {
        status_code: Arc<Mutex<StatusCodes>>,
        num_send_config_calls: Arc<Mutex<i32>>,
        metrics_config_list: Arc<Mutex<Option<Vec<String>>>>,
    }

    impl Interface for MockTelemetryConfigListListener {}

    impl MockTelemetryConfigListListener {
        fn new() -> Self {
            Self {
                status_code: Arc::new(Mutex::new(StatusCodes::TELEMETRY_SERVICE_UNKNOWN_ERROR)),
                num_send_config_calls: Arc::new(Mutex::new(0)),
                metrics_config_list: Arc::new(Mutex::new(None)),
            }
        }

        fn get_status_code(&self) -> StatusCodes {
            *self.status_code.lock().unwrap()
        }

        fn get_num_send_config_calls(&self) -> i32 {
            *self.num_send_config_calls.lock().unwrap()
        }

        fn get_metrics_config_list(&self) -> Vec<String> {
            self.metrics_config_list.lock().unwrap().clone().unwrap()
        }
    }

    impl ITelemetryConfigListListener for MockTelemetryConfigListListener {
        fn sendConfigList(
            &self,
            status_code: StatusCodes,
            _status_message: &str,
            config_list: &[String],
        ) -> BinderResult<()> {
            let mut mut_status_code = self.status_code.lock().unwrap();
            *mut_status_code = status_code;
            let mut num_send_config_calls = self.num_send_config_calls.lock().unwrap();
            *num_send_config_calls += 1;
            let mut mut_config_list = self.metrics_config_list.lock().unwrap();
            *mut_config_list = Some(config_list.to_vec());

            Ok(())
        }
    }

    /// This struct is defined to implement ITelemetryReportListener AIDL interface for unit testing purposes
    #[derive(Clone)]
    struct MockTelemetryReportListener {
        status_code: Arc<Mutex<StatusCodes>>,
        status_message: Arc<Mutex<String>>,
        num_on_result_calls: Arc<Mutex<i32>>,
        uuid: Arc<Mutex<String>>,
        report_number: Arc<Mutex<i32>>,
        metrics_report: Arc<Mutex<Option<MetricsReport>>>,
    }

    impl Interface for MockTelemetryReportListener {}

    impl MockTelemetryReportListener {
        fn new() -> Self {
            Self {
                status_code: Arc::new(Mutex::new(StatusCodes::TELEMETRY_SERVICE_UNKNOWN_ERROR)),
                status_message: Arc::new(Mutex::new(String::new())),
                num_on_result_calls: Arc::new(Mutex::new(0)),
                uuid: Arc::new(Mutex::new(String::new())),
                report_number: Arc::new(Mutex::new(0)),
                metrics_report: Arc::new(Mutex::new(None)),
            }
        }

        fn get_status_code(&self) -> StatusCodes {
            *self.status_code.lock().unwrap()
        }

        fn get_status_message(&self) -> String {
            self.status_message.lock().unwrap().to_string()
        }

        fn get_num_on_result_calls(&self) -> i32 {
            *self.num_on_result_calls.lock().unwrap()
        }

        fn get_report_number(&self) -> i32 {
            *self.report_number.lock().unwrap()
        }

        fn get_uuid(&self) -> String {
            self.uuid.lock().unwrap().to_string()
        }
    }

    impl ITelemetryReportListener for MockTelemetryReportListener {
        fn onReport(
            &self,
            status_code: StatusCodes,
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

    /// This struct is defined to implement ITelemetryReportReadyListener AIDL interface for unit
    /// testing purposes
    #[derive(Clone)]
    struct MockTelemetryReportReadyListener {
        num_on_ready_report_calls: Arc<Mutex<i32>>,
        report_number: Arc<Mutex<i32>>,
        uuid: Arc<Mutex<String>>,
    }

    impl Interface for MockTelemetryReportReadyListener {}

    impl MockTelemetryReportReadyListener {
        fn new() -> Self {
            Self {
                num_on_ready_report_calls: Arc::new(Mutex::new(0)),
                report_number: Arc::new(Mutex::new(0)),
                uuid: Arc::new(Mutex::new(String::new())),
            }
        }

        fn get_num_on_ready_report_calls(&self) -> i32 {
            *self.num_on_ready_report_calls.lock().unwrap()
        }

        fn get_report_number(&self) -> i32 {
            *self.report_number.lock().unwrap()
        }

        fn get_uuid(&self) -> String {
            self.uuid.lock().unwrap().to_string()
        }
    }

    impl ITelemetryReportReadyListener for MockTelemetryReportReadyListener {
        fn onReady(
            &self,
            metrics_config_uuid: &str,
            _report_config_name: &str,
            metrics_report_number: i32,
        ) -> BinderResult<()> {
            let mut num_calls = self.num_on_ready_report_calls.lock().unwrap();
            *num_calls += 1;
            let mut report_number = self.report_number.lock().unwrap();
            *report_number = metrics_report_number;
            let mut uuid = self.uuid.lock().unwrap();
            metrics_config_uuid.clone_into(&mut uuid);
            Ok(())
        }
    }

    /// This struct is defined to implement ITelemetryApiStatusListener AIDL interface for unit
    /// testing purposes
    #[derive(Clone)]
    struct MockTelemetryApiStatusListener {
        num_send_status_calls: Arc<Mutex<i32>>,
        status: Arc<Mutex<StatusCodes>>,
    }

    impl Interface for MockTelemetryApiStatusListener {}

    impl MockTelemetryApiStatusListener {
        fn new() -> Self {
            Self {
                num_send_status_calls: Arc::new(Mutex::new(0)),
                status: Arc::new(Mutex::new(StatusCodes::TELEMETRY_SERVICE_UNKNOWN_ERROR)),
            }
        }

        pub fn get_num_send_status_calls(&self) -> i32 {
            *self.num_send_status_calls.lock().unwrap()
        }

        pub fn get_status_code(&self) -> StatusCodes {
            *self.status.lock().unwrap()
        }
    }

    impl ITelemetryApiStatusListener for MockTelemetryApiStatusListener {
        fn sendStatus(
            &self,
            _api_call_id: i32,
            status_code: StatusCodes,
            _status_msg: &str,
        ) -> BinderResult<()> {
            let mut num_calls = self.num_send_status_calls.lock().unwrap();
            *num_calls += 1;

            let mut mut_status = self.status.lock().unwrap();
            *mut_status = status_code;

            Ok(())
        }
    }

    /// This struct is defined to implement ITelemetryMiscStatusListener AIDL interface for unit
    /// testing purposes
    #[derive(Clone)]
    struct MockTelemetryMiscStatusListener {
        num_send_status_calls: Arc<Mutex<i32>>,
        status: Arc<Mutex<StatusCodes>>,
    }

    impl Interface for MockTelemetryMiscStatusListener {}

    impl MockTelemetryMiscStatusListener {
        fn new() -> Self {
            Self {
                num_send_status_calls: Arc::new(Mutex::new(0)),
                status: Arc::new(Mutex::new(StatusCodes::TELEMETRY_SERVICE_UNKNOWN_ERROR)),
            }
        }
    }

    impl ITelemetryMiscStatusListener for MockTelemetryMiscStatusListener {
        fn sendStatus(&self, status_code: StatusCodes, _status_msg: &str) -> BinderResult<()> {
            let mut num_calls = self.num_send_status_calls.lock().unwrap();
            *num_calls += 1;

            let mut mut_status = self.status.lock().unwrap();
            *mut_status = status_code;

            Ok(())
        }
    }

    struct TestEnvironment {
        service: SdvTelemetryService,
        api_status_listener: Option<MockTelemetryApiStatusListener>,
        metrics_config_manager: Arc<MockMetricsConfigManager>,
        num_add_metrics_config_calls: Arc<Mutex<i32>>,
        num_activate_metrics_config_calls: Arc<Mutex<i32>>,
        num_deactivate_metrics_config_calls: Arc<Mutex<i32>>,
        num_remove_config_calls: Arc<Mutex<i32>>,
        num_remove_all_config_calls: Arc<Mutex<i32>>,
        num_get_active_config_calls: Arc<Mutex<i32>>,
        num_get_inactive_config_calls: Arc<Mutex<i32>>,
    }

    impl TestEnvironment {
        // Test setup
        async fn new(dir_path: String) -> TestEnvironment {
            let num_add_metrics_config_calls = Arc::new(Mutex::new(0));
            let num_activate_metrics_config_calls = Arc::new(Mutex::new(0));
            let num_deactivate_metrics_config_calls = Arc::new(Mutex::new(0));
            let num_remove_config_calls = Arc::new(Mutex::new(0));
            let num_remove_all_config_calls = Arc::new(Mutex::new(0));
            let num_get_active_config_calls = Arc::new(Mutex::new(0));
            let num_get_inactive_config_calls = Arc::new(Mutex::new(0));

            let mut service = SdvTelemetryService::new(
                dir_path.as_str(),
                MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
                Arc::new(MockServicePublisherAccessor::new()),
            )
            .await
            .unwrap();
            let metrics_config_manager = Arc::new(MockMetricsConfigManager::new(
                Arc::clone(&num_add_metrics_config_calls),
                Arc::clone(&num_activate_metrics_config_calls),
                Arc::clone(&num_deactivate_metrics_config_calls),
                Arc::clone(&num_remove_config_calls),
                Arc::clone(&num_remove_all_config_calls),
                Arc::clone(&num_get_active_config_calls),
                Arc::clone(&num_get_inactive_config_calls),
            ));

            service.metrics_config_manager = Arc::<
                metrics_config_manager::mock::MockMetricsConfigManager,
            >::clone(&metrics_config_manager);

            TestEnvironment {
                service,
                api_status_listener: None,
                metrics_config_manager,
                num_add_metrics_config_calls: Arc::clone(&num_add_metrics_config_calls),
                num_activate_metrics_config_calls: Arc::clone(&num_activate_metrics_config_calls),
                num_deactivate_metrics_config_calls: Arc::clone(
                    &num_deactivate_metrics_config_calls,
                ),
                num_remove_config_calls: Arc::clone(&num_remove_config_calls),
                num_remove_all_config_calls: Arc::clone(&num_remove_all_config_calls),
                num_get_active_config_calls: Arc::clone(&num_get_active_config_calls),
                num_get_inactive_config_calls: Arc::clone(&num_get_inactive_config_calls),
            }
        }

        pub async fn set_mock_api_status_listener(&mut self) {
            let api_status_listener = MockTelemetryApiStatusListener::new();

            let binder_status_listener = BnTelemetryApiStatusListener::new_binder(
                api_status_listener.clone(),
                BinderFeatures::default(),
            );

            self.service.registerApiStatusListener(&binder_status_listener).await.unwrap();
            self.api_status_listener = Some(api_status_listener);

            assert_eq!(StatusCodes::TELEMETRY_SERVICE_UNKNOWN_ERROR, self.get_status_code());
        }

        pub fn get_status_code(&self) -> StatusCodes {
            self.api_status_listener.as_ref().unwrap().get_status_code()
        }

        pub fn get_num_add_metrics_config_calls(&self) -> i32 {
            *self.num_add_metrics_config_calls.lock().unwrap()
        }

        pub fn get_num_activate_metrics_config_calls(&self) -> i32 {
            *self.num_activate_metrics_config_calls.lock().unwrap()
        }

        pub fn get_num_deactivate_metrics_config_calls(&self) -> i32 {
            *self.num_deactivate_metrics_config_calls.lock().unwrap()
        }

        pub fn get_num_remove_config_calls(&self) -> i32 {
            *self.num_remove_config_calls.lock().unwrap()
        }

        pub fn get_num_remove_all_config_calls(&self) -> i32 {
            *self.num_remove_all_config_calls.lock().unwrap()
        }

        pub fn get_num_get_active_config_calls(&self) -> i32 {
            *self.num_get_active_config_calls.lock().unwrap()
        }

        pub fn get_num_get_inactive_config_calls(&self) -> i32 {
            *self.num_get_inactive_config_calls.lock().unwrap()
        }
    }

    fn create_serialized_config(uuid: &str) -> Vec<u8> {
        let mut config = MetricsConfig::new();
        config.uuid = uuid.to_string();
        config.version = 2;

        info!("Serializing metric config");
        config.write_to_bytes().unwrap()
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn add_metrics_config_call_received() {
        const UUID1: &str = "01bf0754-df95-46bb-bb35-522b6af66600";

        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "add_metrics_config";

        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_api_status_listener().await;

        info!("Serializing metric config");
        let serialized_config = create_serialized_config(UUID1);

        test_environment
            .service
            .addMetricsConfig(DUMMY_API_CALL_ID, &serialized_config)
            .await
            .unwrap();
        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            test_environment.get_status_code()
        );
        assert_eq!(1, test_environment.get_num_add_metrics_config_calls());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn add_metrics_config_call_duplicate_fail() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "add_metrics_config";

        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_api_status_listener().await;

        info!("Serializing metric config");
        let serialized_config = create_serialized_config(UUID1);

        test_environment
            .service
            .addMetricsConfig(DUMMY_API_CALL_ID, &serialized_config)
            .await
            .unwrap();
        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            test_environment.get_status_code()
        );
        assert_eq!(1, test_environment.get_num_add_metrics_config_calls());

        test_environment
            .service
            .addMetricsConfig(DUMMY_API_CALL_ID, &serialized_config)
            .await
            .unwrap();
        assert_eq!(StatusCodes::ADD_METRICS_CONFIG_FAILED, test_environment.get_status_code());
        assert_eq!(2, test_environment.get_num_add_metrics_config_calls());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_metrics_config_call_received() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "activate_metrics_config";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_api_status_listener().await;

        let serialized_config = create_serialized_config(UUID1);
        test_environment
            .service
            .addMetricsConfig(DUMMY_API_CALL_ID, &serialized_config)
            .await
            .unwrap();
        assert_eq!(1, test_environment.get_num_add_metrics_config_calls());
        test_environment.service.activateMetricsConfig(DUMMY_API_CALL_ID, UUID1).await.unwrap();
        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            test_environment.get_status_code()
        );
        assert_eq!(1, test_environment.get_num_activate_metrics_config_calls());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn activate_metrics_config_fail_config_doesnt_exist() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "activate_metrics_config";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_api_status_listener().await;

        test_environment.service.activateMetricsConfig(DUMMY_API_CALL_ID, UUID1).await.unwrap();
        assert_eq!(
            StatusCodes::METRICS_CONFIG_INVALID_CONFIG_UUID,
            test_environment.get_status_code()
        );
        assert_eq!(1, test_environment.get_num_activate_metrics_config_calls());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn deactivate_metrics_config_call_received() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "deactivate_metrics_config";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_api_status_listener().await;

        let serialized_config = create_serialized_config(UUID1);
        test_environment
            .service
            .addMetricsConfig(DUMMY_API_CALL_ID, &serialized_config)
            .await
            .unwrap();
        assert_eq!(1, test_environment.get_num_add_metrics_config_calls());

        test_environment.service.activateMetricsConfig(DUMMY_API_CALL_ID, UUID1).await.unwrap();
        assert_eq!(1, test_environment.get_num_activate_metrics_config_calls());

        test_environment.service.deactivateMetricsConfig(DUMMY_API_CALL_ID, UUID1).await.unwrap();
        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            test_environment.get_status_code()
        );
        assert_eq!(1, test_environment.get_num_deactivate_metrics_config_calls());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn deactivate_metrics_config_fail_config_doesnt_exist() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "deactivate_metrics_config";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_api_status_listener().await;

        test_environment.service.deactivateMetricsConfig(DUMMY_API_CALL_ID, UUID1).await.unwrap();
        assert_eq!(
            StatusCodes::METRICS_CONFIG_INVALID_CONFIG_UUID,
            test_environment.get_status_code()
        );
        assert_eq!(1, test_environment.get_num_deactivate_metrics_config_calls());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn remove_metrics_config_success() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "remove_metrics_config";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_api_status_listener().await;

        let serialized_config = create_serialized_config(UUID1);
        test_environment
            .service
            .addMetricsConfig(DUMMY_API_CALL_ID, &serialized_config)
            .await
            .unwrap();
        assert_eq!(1, test_environment.get_num_add_metrics_config_calls());

        test_environment.service.activateMetricsConfig(DUMMY_API_CALL_ID, UUID1).await.unwrap();
        assert_eq!(1, test_environment.get_num_activate_metrics_config_calls());

        test_environment.service.removeMetricsConfig(DUMMY_API_CALL_ID, UUID1).await.unwrap();
        assert_eq!(1, test_environment.get_num_remove_config_calls());
        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            test_environment.get_status_code()
        );
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn remove_metrics_config_fail_config_doesnt_exist() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "remove_metrics_config";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_api_status_listener().await;

        test_environment.service.removeMetricsConfig(DUMMY_API_CALL_ID, UUID1).await.unwrap();
        assert_eq!(
            StatusCodes::METRICS_CONFIG_INVALID_CONFIG_UUID,
            test_environment.get_status_code()
        );
        assert_eq!(1, test_environment.get_num_remove_config_calls());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn remove_all_metrics_configs() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "remove_all_metrics_configs";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_api_status_listener().await;

        let serialized_config1 = create_serialized_config(UUID1);
        let serialized_config2 = create_serialized_config(UUID2);

        // Add configs
        test_environment
            .service
            .addMetricsConfig(DUMMY_API_CALL_ID, &serialized_config1)
            .await
            .unwrap();
        test_environment
            .service
            .addMetricsConfig(DUMMY_API_CALL_ID, &serialized_config2)
            .await
            .unwrap();
        assert_eq!(2, test_environment.get_num_add_metrics_config_calls());

        // Remove all configs
        test_environment.service.removeAllMetricsConfigs(-1).await.unwrap();
        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            test_environment.get_status_code()
        );
        assert_eq!(1, test_environment.get_num_remove_all_config_calls());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_active_metrics_config_uuids() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "get_active_metrics_config_uuids";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_api_status_listener().await;

        let config_list_listener = MockTelemetryConfigListListener::new();

        let binder_config_list_listener = BnTelemetryConfigListListener::new_binder(
            config_list_listener.clone(),
            BinderFeatures::default(),
        );

        let config_uuids = [UUID1.to_string(), UUID2.to_string()];
        test_environment.metrics_config_manager.populate_active_configs(config_uuids.to_vec());

        test_environment
            .service
            .getActiveMetricsConfigUuids(&binder_config_list_listener)
            .await
            .unwrap();

        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            config_list_listener.get_status_code()
        );
        assert_eq!(1, test_environment.get_num_get_active_config_calls());
        assert_eq!(1, config_list_listener.get_num_send_config_calls());
        assert!(config_uuids
            .iter()
            .all(|item| config_list_listener.get_metrics_config_list().contains(item)));
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_inactive_metrics_config_uuids() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "get_inactive_metrics_config_uuids";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_api_status_listener().await;

        let config_list_listener = MockTelemetryConfigListListener::new();

        let binder_config_list_listener = BnTelemetryConfigListListener::new_binder(
            config_list_listener.clone(),
            BinderFeatures::default(),
        );

        let config_uuids = [UUID1.to_string(), UUID2.to_string()];
        test_environment.metrics_config_manager.populate_inactive_configs(config_uuids.to_vec());

        test_environment
            .service
            .getInactiveMetricsConfigUuids(&binder_config_list_listener)
            .await
            .unwrap();

        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            config_list_listener.get_status_code()
        );
        assert_eq!(1, test_environment.get_num_get_inactive_config_calls());
        assert_eq!(1, config_list_listener.get_num_send_config_calls());
        assert!(config_uuids
            .iter()
            .all(|item| config_list_listener.get_metrics_config_list().contains(item)));
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn clear_report_ready_listener() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "clear_report_ready_listener";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_api_status_listener().await;

        let report_ready_listener = MockTelemetryReportReadyListener::new();

        let binder_report_ready_listener = BnTelemetryReportReadyListener::new_binder(
            report_ready_listener.clone(),
            BinderFeatures::default(),
        );

        test_environment
            .service
            .registerReportReadyListener(DUMMY_API_CALL_ID, &binder_report_ready_listener)
            .await
            .unwrap();

        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            test_environment.get_status_code()
        );

        test_environment.service.unregisterReportReadyListener().await.unwrap();

        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            test_environment.get_status_code()
        );
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn set_report_ready_listener() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "set_report_ready_listener";

        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_api_status_listener().await;

        let report_ready_listener = MockTelemetryReportReadyListener::new();

        let binder_report_ready_listener = BnTelemetryReportReadyListener::new_binder(
            report_ready_listener.clone(),
            BinderFeatures::default(),
        );

        test_environment
            .service
            .registerReportReadyListener(DUMMY_API_CALL_ID, &binder_report_ready_listener)
            .await
            .unwrap();

        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            test_environment.get_status_code()
        );
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn set_report_ready_listener_and_check_on_ready_calls() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "check_on_ready_calls";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_api_status_listener().await;

        let report_ready_listener = MockTelemetryReportReadyListener::new();

        let binder_report_ready_listener = BnTelemetryReportReadyListener::new_binder(
            report_ready_listener.clone(),
            BinderFeatures::default(),
        );

        test_environment
            .service
            .registerReportReadyListener(DUMMY_API_CALL_ID, &binder_report_ready_listener)
            .await
            .unwrap();

        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            test_environment.get_status_code()
        );

        // Call onReady on report ready listener
        report_ready_listener.onReady(UUID1, &String::default(), REPORT_NUMBER).unwrap();
        assert_eq!(1, report_ready_listener.get_num_on_ready_report_calls());
        assert_eq!(REPORT_NUMBER, report_ready_listener.get_report_number());
        assert_eq!(UUID1, &report_ready_listener.get_uuid());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn set_multiple_report_ready_listener_fails() {
        const ROOT_PATH: &str = "set_multiple_report_ready_listener";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_api_status_listener().await;

        let report_listener = BnTelemetryReportReadyListener::new_binder(
            MockTelemetryReportReadyListener::new(),
            BinderFeatures::default(),
        );

        test_environment
            .service
            .registerReportReadyListener(DUMMY_API_CALL_ID, &report_listener)
            .await
            .unwrap();

        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            test_environment.get_status_code()
        );

        test_environment
            .service
            .registerReportReadyListener(DUMMY_API_CALL_ID, &report_listener)
            .await
            .unwrap();
        assert_eq!(
            StatusCodes::SET_REPORT_READY_LISTENER_FAILED,
            test_environment.get_status_code()
        );
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn set_api_status_listener_and_check_status_code() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "set_status_listener";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        let api_status_listener = MockTelemetryApiStatusListener::new();

        let binder_listener = BnTelemetryApiStatusListener::new_binder(
            api_status_listener.clone(),
            BinderFeatures::default(),
        );

        let serialized_config = create_serialized_config(UUID1);
        test_environment.service.registerApiStatusListener(&binder_listener).await.unwrap();
        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_UNKNOWN_ERROR,
            api_status_listener.get_status_code()
        );

        test_environment
            .service
            .addMetricsConfig(DUMMY_API_CALL_ID, &serialized_config)
            .await
            .unwrap();

        assert_eq!(1, api_status_listener.get_num_send_status_calls());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn set_misc_status_listener_and_check_status_code() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "set_status_listener";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        let api_status_listener = MockTelemetryApiStatusListener::new();

        let binder_listener = BnTelemetryApiStatusListener::new_binder(
            api_status_listener.clone(),
            BinderFeatures::default(),
        );

        test_environment.service.registerApiStatusListener(&binder_listener).await.unwrap();

        let misc_status_listener = MockTelemetryMiscStatusListener::new();

        let binder_listener = BnTelemetryMiscStatusListener::new_binder(
            misc_status_listener.clone(),
            BinderFeatures::default(),
        );

        test_environment
            .service
            .registerMiscStatusListener(DUMMY_API_CALL_ID, &binder_listener)
            .await
            .unwrap();
        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            api_status_listener.get_status_code()
        );
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn set_multiple_misc_status_listener_fails() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "set_multiple_status_listener";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        let api_status_listener = MockTelemetryApiStatusListener::new();

        let binder_listener = BnTelemetryApiStatusListener::new_binder(
            api_status_listener.clone(),
            BinderFeatures::default(),
        );

        test_environment.service.registerApiStatusListener(&binder_listener).await.unwrap();

        let misc_status_listener = MockTelemetryMiscStatusListener::new();

        let binder_listener = BnTelemetryMiscStatusListener::new_binder(
            misc_status_listener.clone(),
            BinderFeatures::default(),
        );

        test_environment
            .service
            .registerMiscStatusListener(DUMMY_API_CALL_ID, &binder_listener)
            .await
            .unwrap();
        test_environment
            .service
            .registerMiscStatusListener(DUMMY_API_CALL_ID, &binder_listener)
            .await
            .unwrap();

        assert_eq!(
            StatusCodes::SET_MISC_STATUS_LISTENER_FAILED,
            api_status_listener.get_status_code()
        );
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn clear_misc_status_listener() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "clear_misc_status_listener";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_api_status_listener().await;

        let result = test_environment.service.unregisterApiStatusListener().await;
        assert!(result.is_ok());
        assert!(test_environment.service.api_status_listener.lock().unwrap().is_none());
    }

    #[sdv_telemetry_test_utils_macros::timeout]
    #[tokio::test]
    async fn get_metrics_report() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "get_metrics_report";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_api_status_listener().await;

        // Setting Report Ready Listener

        let report_ready_listener = MockTelemetryReportReadyListener::new();

        let binder_report_ready_listener = BnTelemetryReportReadyListener::new_binder(
            report_ready_listener.clone(),
            BinderFeatures::default(),
        );

        test_environment
            .service
            .registerReportReadyListener(DUMMY_API_CALL_ID, &binder_report_ready_listener)
            .await
            .unwrap();

        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
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
        report.metrics_config_uuid = UUID1.to_string();
        report.report_number = REPORT_NUMBER;

        // Call on_report_ready callback on metrics report listener
        test_environment.service.metrics_report_listener.on_report_ready(report);
        test_environment
            .service
            .getMetricsReport(UUID1, &String::default(), REPORT_NUMBER, &binder_report_listener)
            .await
            .unwrap();

        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            report_listener.get_status_code()
        );
        assert_eq!(UUID1, report_listener.get_status_message());
        assert_eq!(1, report_ready_listener.get_num_on_ready_report_calls());
        assert_eq!(1, report_listener.get_num_on_result_calls());
        assert_eq!(UUID1, report_listener.get_uuid());
        assert_eq!(REPORT_NUMBER, report_listener.get_report_number());
    }
}
