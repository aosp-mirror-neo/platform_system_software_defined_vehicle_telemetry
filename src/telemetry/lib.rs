// Copyright 2023 Google LLC

//! This module implements the ITelemetryService AIDL interface

mod expr;
mod metrics_config_manager;
mod metrics_config_orchestrator;
mod metrics_report_listener;
mod status_listener;
mod status_reporter;

pub mod service_publisher_manager;

#[cfg(test)]
mod mock_service_publisher_accessor;

// External dependencies.
use async_trait::async_trait;
use google_sdv_telemetry::aidl::google::sdv::telemetry::{
    status::TelemetryServiceStatusCodes::TelemetryServiceStatusCodes as StatusCodes,
    ITelemetryApiStatusListener::ITelemetryApiStatusListener,
    ITelemetryConfigListListener::ITelemetryConfigListListener,
    ITelemetryMiscStatusListener::ITelemetryMiscStatusListener,
    ITelemetryReadyReportsInfoListener::ITelemetryReadyReportsInfoListener,
    ITelemetryReportListener::ITelemetryReportListener,
    ITelemetryReportReadyListener::ITelemetryReportReadyListener,
    ITelemetryService::ITelemetryServiceAsyncServer, MetricsReportInfo::MetricsReportInfo,
};
use log::{error, info, warn};
use protobuf::Message;
use sdv_telemetry_platform_abstraction_layer::PlatformAbstractionLayer;
use sdv_telemetry_service_publisher_accessor::ServicePublisherAccessor;
use std::borrow::Cow;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use tracing::instrument;

// Child module dependencies.
use metrics_config_manager::{
    MetricsConfigManager, MetricsConfigManagerError as ConfigManagerError, MetricsConfigManagerImpl,
};
use metrics_config_orchestrator::MetricsConfigOrchestratorError as ConfigOrchestratorError;
use metrics_report_listener::MetricsReportListener;
use status_listener::{StatusListener, StatusListenerImpl};
use status_reporter::StatusReporterImpl;

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
        ConfigManagerError::DuplicateMetricsConfigUuid { .. } => {
            StatusCodes::ADD_METRICS_CONFIG_FAILED
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
    /// Instance of Metrics Configuration Manager
    metrics_config_manager: Arc<dyn MetricsConfigManager + Send + Sync>,
    /// Report Ready listener to inform the client when a report is ready
    metrics_report_listener: Arc<MetricsReportListener>,
    /// Listener of status events
    status_listener: Arc<dyn StatusListener>,
}

impl binder::Interface for SdvTelemetryService {}

impl SdvTelemetryService {
    /// Instantiates an instance of SdvTelemetryService
    #[instrument(skip(service_publisher_manager))]
    pub async fn new(
        root_path: &str,
        config_store_size: usize,
        service_publisher_manager: Arc<dyn ServicePublisherAccessor>,
        pal: Arc<dyn PlatformAbstractionLayer>,
    ) -> Option<Self> {
        let (active_dir_path, inactive_dir_path) = if !root_path.is_empty() {
            (
                Path::new(root_path).join(ACTIVE_CONFIGS_DATA_DIR),
                Path::new(root_path).join(INACTIVE_CONFIGS_DATA_DIR),
            )
        } else {
            (
                Path::new(ACTIVE_CONFIGS_DATA_DIR).to_owned(),
                Path::new(INACTIVE_CONFIGS_DATA_DIR).to_owned(),
            )
        };

        // Create manager and its dependencies
        let report_listener = Arc::new(MetricsReportListener::new());
        let report_listener_clone = Arc::clone(&report_listener);

        let status_listener = Arc::new(StatusListenerImpl::new());
        let status_listener_clone = Arc::clone(&status_listener);

        let status_reporter = Arc::new(StatusReporterImpl::new(move |status| {
            status_listener_clone.send_misc_status(status);
        }));

        let config_manager_res = MetricsConfigManagerImpl::new(
            active_dir_path,
            inactive_dir_path,
            config_store_size,
            service_publisher_manager,
            pal,
            move |report| report_listener.on_report_ready(report),
            status_reporter,
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
            status_listener,
        })
    }
}

#[async_trait]
impl ITelemetryServiceAsyncServer for SdvTelemetryService {
    #[instrument(skip(self, metrics_config))]
    async fn addMetricsConfig(
        &self,
        api_call_id: i32,
        metrics_config: &[u8],
    ) -> binder::Result<()> {
        // Add metrics config and report any errors to client
        let metrics_config_uuid =
            match self.metrics_config_manager.add_metrics_config(metrics_config).await {
                Ok(config_uuid) => config_uuid,
                Err(e) => {
                    error!("Invalid Metric Config");
                    let (err_code, err_msg) = get_error_code_and_message(e);
                    self.status_listener.send_api_status(api_call_id, err_code, &err_msg);
                    return Ok(());
                }
            };

        self.status_listener.send_api_status(
            api_call_id,
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            &metrics_config_uuid,
        );
        Ok(())
    }

    #[instrument(skip(self))]
    async fn activateMetricsConfig(
        &self,
        api_call_id: i32,
        metrics_config_uuid: &str,
    ) -> binder::Result<()> {
        // Activate metrics config and report any errors to client
        if let Err(e) =
            self.metrics_config_manager.activate_metrics_config(metrics_config_uuid).await
        {
            error!("Unable to activate Metric Config Err: {:?}", e);
            let (err_code, err_msg) = get_error_code_and_message(e);
            self.status_listener.send_api_status(api_call_id, err_code, &err_msg);
            return Ok(());
        }

        self.status_listener.send_api_status(
            api_call_id,
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            metrics_config_uuid,
        );
        Ok(())
    }

    #[instrument(skip(self))]
    async fn deactivateMetricsConfig(
        &self,
        api_call_id: i32,
        metrics_config_uuid: &str,
    ) -> binder::Result<()> {
        // Deactivate metrics config and report any errors to client
        if let Err(e) =
            self.metrics_config_manager.deactivate_metrics_config(metrics_config_uuid).await
        {
            error!("Unable to deactivate Metric Config Err: {:?}", e);
            let (err_code, err_msg) = get_error_code_and_message(e);
            self.status_listener.send_api_status(api_call_id, err_code, &err_msg);
            return Ok(());
        }

        self.status_listener.send_api_status(
            api_call_id,
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            metrics_config_uuid,
        );
        Ok(())
    }

    #[instrument(skip(self))]
    async fn removeMetricsConfig(
        &self,
        api_call_id: i32,
        metrics_config_uuid: &str,
    ) -> binder::Result<()> {
        // Remove metrics config and report any errors to client
        if let Err(e) = self.metrics_config_manager.remove_metrics_config(metrics_config_uuid).await
        {
            error!("Unable to remove Metric Config Err: {:?}", e);
            let (err_code, err_msg) = get_error_code_and_message(e);
            self.status_listener.send_api_status(api_call_id, err_code, &err_msg);
            return Ok(());
        }

        self.status_listener.send_api_status(
            api_call_id,
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            metrics_config_uuid,
        );
        Ok(())
    }

    #[instrument(skip(self))]
    async fn removeAllMetricsConfigs(&self, api_call_id: i32) -> binder::Result<()> {
        // Remove all metrics config and report any errors to client
        if let Err(e) = self.metrics_config_manager.remove_all_metrics_configs().await {
            error!("Unable to remove all metric configs Err: {:?}", e);
            let (err_code, err_msg) = get_error_code_and_message(e);
            self.status_listener.send_api_status(api_call_id, err_code, &err_msg);
            return Ok(());
        }

        self.status_listener.send_api_status(
            api_call_id,
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            "successfully Removed All Configs",
        );
        Ok(())
    }

    #[instrument(skip(self, config_list_listener))]
    async fn getActiveMetricsConfigUuids(
        &self,
        config_list_listener: &binder::Strong<dyn ITelemetryConfigListListener>,
    ) -> binder::Result<()> {
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
        config_list_listener: &binder::Strong<dyn ITelemetryConfigListListener>,
    ) -> binder::Result<()> {
        info!("getInactiveMetricsConfigUuids");
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
        listener: &binder::Strong<dyn ITelemetryReportReadyListener>,
    ) -> binder::Result<()> {
        info!("registerReportReadyListener");
        let (status_code, status_message) =
            self.metrics_report_listener.set_report_ready_listener(listener).map_or_else(
                |err| (StatusCodes::SET_REPORT_READY_LISTENER_FAILED, Cow::Owned(err.to_string())),
                |()| {
                    (
                        StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
                        Cow::Borrowed("Report Ready Listener successfully added"),
                    )
                },
            );
        self.status_listener.send_api_status(api_call_id, status_code, &status_message);
        Ok(())
    }

    #[instrument(skip(self))]
    async fn registerApiStatusListener(
        &self,
        new_listener: &binder::Strong<dyn ITelemetryApiStatusListener>,
    ) -> binder::Result<()> {
        Arc::clone(&self.status_listener).set_api_status_listener(new_listener)
    }

    #[instrument(skip(self))]
    async fn registerMiscStatusListener(
        &self,
        api_call_id: i32,
        new_listener: &binder::Strong<dyn ITelemetryMiscStatusListener>,
    ) -> binder::Result<()> {
        Arc::clone(&self.status_listener).set_misc_status_listener(api_call_id, new_listener)
    }

    #[instrument(skip(self, report_listener))]
    async fn getMetricsReport(
        &self,
        metrics_report_info: &MetricsReportInfo,
        report_listener: &binder::Strong<dyn ITelemetryReportListener>,
    ) -> binder::Result<()> {
        info!("getMetricsReport: {}", metrics_report_info.metricsConfigUuid);
        if let Ok(metrics_report) =
            self.metrics_report_listener.get_metrics_report(metrics_report_info)
        {
            let serialized_report: &[u8] = &metrics_report.write_to_bytes().unwrap();
            if let Err(e) = report_listener.deref().onReport(
                StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
                &metrics_report_info.metricsConfigUuid,
                serialized_report,
            ) {
                warn!("Unable to send report to the client: {:?}", e);
            }
        } else if let Err(e) = report_listener.deref().onReport(
            StatusCodes::GET_FINISHED_REPORT_FAILED,
            &metrics_report_info.metricsConfigUuid,
            &[],
        ) {
            warn!("Unable to send report retrieval error to the client: {:?}", e);
        }

        Ok(())
    }

    #[instrument(skip(self, report_listener))]
    async fn getReadyReportsInfo(
        &self,
        report_listener: &binder::Strong<dyn ITelemetryReadyReportsInfoListener>,
    ) -> binder::Result<()> {
        info!("getReadyReportsInfo");
        let ready_reports_info = self.metrics_report_listener.get_ready_reports_info();

        if let Err(e) = report_listener.deref().sendReadyReportsInfo(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
            format!("Number of ready reports: {}", ready_reports_info.len()).as_str(),
            &ready_reports_info,
        ) {
            warn!("Unable to send info of ready reports to the client: {:?}", e);
        }
        Ok(())
    }

    #[instrument(skip(self))]
    async fn unregisterReportReadyListener(&self) -> binder::Result<()> {
        info!("unregisterReportReadyListener");
        self.metrics_report_listener.clear_report_ready_listener();
        Ok(())
    }

    #[instrument(skip(self))]
    async fn unregisterApiStatusListener(&self) -> binder::Result<()> {
        info!("unregisterApiStatusListener");
        self.status_listener.clear_api_status_listener();
        Ok(())
    }

    #[instrument(skip(self))]
    async fn unregisterMiscStatusListener(&self) -> binder::Result<()> {
        info!("unregisterMiscStatusListener");
        self.status_listener.clear_misc_status_listener();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use binder::BinderFeatures;
    use google_sdv_telemetry::aidl::google::sdv::telemetry::{
        status::TelemetryServiceStatus::TelemetryServiceStatus,
        ITelemetryApiStatusListener::BnTelemetryApiStatusListener,
        ITelemetryConfigListListener::BnTelemetryConfigListListener,
        ITelemetryMiscStatusListener::BnTelemetryMiscStatusListener,
        ITelemetryReadyReportsInfoListener::BnTelemetryReadyReportsInfoListener,
        ITelemetryReportListener::BnTelemetryReportListener,
        ITelemetryReportReadyListener::BnTelemetryReportReadyListener,
        MetricsReportInfo::MetricsReportInfo,
    };
    use parking_lot::Mutex;
    use protobuf::Message;
    use sdv_telemetry_platform_abstraction_layer::new_stub_pal;
    use sdv_telemetry_proto_rust::metrics_configuration::MetricsConfig;
    use sdv_telemetry_proto_rust::metrics_configuration::MetricsReport;

    use crate::metrics_config_manager::mock::MockMetricsConfigManager;
    use crate::metrics_report_listener::tests::create_default_metrics_report_listener;
    use crate::mock_service_publisher_accessor::MockServicePublisherAccessor;

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

    impl binder::Interface for MockTelemetryConfigListListener {}

    impl MockTelemetryConfigListListener {
        fn new() -> Self {
            Self {
                status_code: Arc::new(Mutex::new(StatusCodes::TELEMETRY_SERVICE_UNKNOWN_ERROR)),
                num_send_config_calls: Arc::new(Mutex::new(0)),
                metrics_config_list: Arc::new(Mutex::new(None)),
            }
        }

        fn get_status_code(&self) -> StatusCodes {
            *self.status_code.lock()
        }

        fn get_num_send_config_calls(&self) -> i32 {
            *self.num_send_config_calls.lock()
        }

        fn get_metrics_config_list(&self) -> Vec<String> {
            self.metrics_config_list.lock().as_ref().unwrap().clone()
        }
    }

    impl ITelemetryConfigListListener for MockTelemetryConfigListListener {
        fn sendConfigList(
            &self,
            status_code: StatusCodes,
            _status_message: &str,
            config_list: &[String],
        ) -> binder::Result<()> {
            *self.status_code.lock() = status_code;
            *self.num_send_config_calls.lock() += 1;
            self.metrics_config_list.lock().replace(config_list.to_vec());
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

    impl binder::Interface for MockTelemetryReportListener {}

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
            *self.status_code.lock()
        }

        fn get_status_message(&self) -> String {
            self.status_message.lock().to_string()
        }

        fn get_num_on_result_calls(&self) -> i32 {
            *self.num_on_result_calls.lock()
        }

        fn get_report_number(&self) -> i32 {
            *self.report_number.lock()
        }

        fn get_uuid(&self) -> String {
            self.uuid.lock().to_string()
        }
    }

    impl ITelemetryReportListener for MockTelemetryReportListener {
        fn onReport(
            &self,
            status_code: StatusCodes,
            status_message: &str,
            serialized_report: &[u8],
        ) -> binder::Result<()> {
            // bytes now contains the contents of the file
            let metrics_report: MetricsReport =
                match protobuf::Message::parse_from_bytes(serialized_report) {
                    Ok(report) => report,
                    Err(_) => return Err(binder::StatusCode::BAD_VALUE.into()),
                };

            let mut mut_status_code = self.status_code.lock();
            *mut_status_code = status_code;
            let mut mut_status_message = self.status_message.lock();
            *mut_status_message = status_message.to_string();
            let mut num_on_result_calls = self.num_on_result_calls.lock();
            *num_on_result_calls += 1;
            let mut report_number = self.report_number.lock();
            *report_number = metrics_report.report_number;
            let mut uuid = self.uuid.lock();
            *uuid = metrics_report.clone().metrics_config_uuid;
            let mut report = self.metrics_report.lock();
            *report = Some(metrics_report);

            Ok(())
        }
    }

    /// This struct is defined to implement ITelemetryReportReadyListener AIDL interface for unit testing purposes
    #[derive(Clone)]
    struct MockTelemetryReadyReportsInfoListener {
        status_code: Arc<Mutex<StatusCodes>>,
        status_message: Arc<Mutex<String>>,
        num_on_ready_reports_info_calls: Arc<Mutex<i32>>,
        ready_reports_info: Arc<Mutex<Vec<MetricsReportInfo>>>,
    }

    impl binder::Interface for MockTelemetryReadyReportsInfoListener {}

    impl MockTelemetryReadyReportsInfoListener {
        fn new() -> Self {
            Self {
                status_code: Arc::new(Mutex::new(StatusCodes::TELEMETRY_SERVICE_UNKNOWN_ERROR)),
                status_message: Arc::new(Mutex::new(String::new())),
                num_on_ready_reports_info_calls: Arc::new(Mutex::new(0)),
                ready_reports_info: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn get_status_code(&self) -> StatusCodes {
            *self.status_code.lock()
        }

        fn get_status_message(&self) -> String {
            self.status_message.lock().to_string()
        }

        fn get_num_on_ready_reports_info_calls(&self) -> i32 {
            *self.num_on_ready_reports_info_calls.lock()
        }

        fn get_ready_reports_info(&self) -> Vec<MetricsReportInfo> {
            self.ready_reports_info.lock().clone()
        }
    }

    impl ITelemetryReadyReportsInfoListener for MockTelemetryReadyReportsInfoListener {
        fn sendReadyReportsInfo(
            &self,
            status_code: StatusCodes,
            status_message: &str,
            ready_reports_info: &[MetricsReportInfo],
        ) -> binder::Result<()> {
            let mut mut_status_code = self.status_code.lock();
            *mut_status_code = status_code;

            let mut mut_status_message = self.status_message.lock();
            *mut_status_message = status_message.to_string();

            let mut num_on_ready_reports_info_calls = self.num_on_ready_reports_info_calls.lock();
            *num_on_ready_reports_info_calls += 1;

            let mut mut_ready_reports_info = self.ready_reports_info.lock();
            *mut_ready_reports_info = ready_reports_info.to_vec();

            Ok(())
        }
    }

    /// This struct is defined to implement ITelemetryReportReadyListener AIDL interface for unit
    /// testing purposes
    #[derive(Clone)]
    struct MockTelemetryReportReadyListener {
        num_on_ready_report_calls: Arc<Mutex<i32>>,
        metrics_report_info: Arc<Mutex<MetricsReportInfo>>,
    }

    impl binder::Interface for MockTelemetryReportReadyListener {}

    impl MockTelemetryReportReadyListener {
        fn new() -> Self {
            Self {
                num_on_ready_report_calls: Arc::new(Mutex::new(0)),
                metrics_report_info: Arc::new(Mutex::new(MetricsReportInfo::default())),
            }
        }

        fn get_num_on_ready_report_calls(&self) -> i32 {
            *self.num_on_ready_report_calls.lock()
        }

        fn get_report_number(&self) -> i32 {
            self.metrics_report_info.lock().reportNumber
        }

        fn get_uuid(&self) -> String {
            self.metrics_report_info.lock().metricsConfigUuid.clone()
        }
    }

    impl ITelemetryReportReadyListener for MockTelemetryReportReadyListener {
        fn onReady(&self, metrics_report_info: &MetricsReportInfo) -> binder::Result<()> {
            let mut num_calls = self.num_on_ready_report_calls.lock();
            *num_calls += 1;
            let mut report_info = self.metrics_report_info.lock();
            *report_info = metrics_report_info.clone();
            Ok(())
        }
    }

    /// This struct is defined to implement ITelemetryApiStatusListener AIDL interface for unit
    /// testing purposes
    struct MockTelemetryApiStatusListener {}

    impl binder::Interface for MockTelemetryApiStatusListener {}

    impl ITelemetryApiStatusListener for MockTelemetryApiStatusListener {
        fn sendStatus(
            &self,
            _api_call_id: i32,
            _status_code: StatusCodes,
            _status_msg: &str,
        ) -> binder::Result<()> {
            Ok(())
        }
    }

    /// This struct is defined to implement ITelemetryMiscStatusListener AIDL interface for unit
    /// testing purposes
    struct MockTelemetryMiscStatusListener {}

    impl binder::Interface for MockTelemetryMiscStatusListener {}

    impl ITelemetryMiscStatusListener for MockTelemetryMiscStatusListener {
        fn sendStatus(&self, _status: &TelemetryServiceStatus) -> binder::Result<()> {
            Ok(())
        }
    }

    struct MockStatusListener {
        api_status_code: Arc<Mutex<Option<StatusCodes>>>,
        api_status_message: Arc<Mutex<Option<String>>>,
        api_listener: Arc<Mutex<Option<binder::Strong<dyn ITelemetryApiStatusListener>>>>,
        misc_listener: Arc<Mutex<Option<(i32, binder::Strong<dyn ITelemetryMiscStatusListener>)>>>,
    }

    impl MockStatusListener {
        fn new() -> Self {
            Self {
                api_status_code: Arc::new(Mutex::new(None)),
                api_status_message: Arc::new(Mutex::new(None)),
                api_listener: Arc::new(Mutex::new(None)),
                misc_listener: Arc::new(Mutex::new(None)),
            }
        }
    }

    impl StatusListener for MockStatusListener {
        fn send_api_status(
            &self,
            _api_call_id: i32,
            status_code: StatusCodes,
            status_message: &str,
        ) {
            self.api_status_code.lock().replace(status_code);
            self.api_status_message.lock().replace(status_message.to_owned());
        }

        fn send_misc_status(&self, _status: TelemetryServiceStatus) {}

        fn clear_api_status_listener(&self) {
            *self.api_listener.lock() = None;
        }

        fn clear_misc_status_listener(&self) {
            *self.misc_listener.lock() = None;
        }

        fn set_api_status_listener(
            self: Arc<Self>,
            new_listener: &binder::Strong<dyn ITelemetryApiStatusListener>,
        ) -> binder::Result<()> {
            self.api_listener.lock().replace(new_listener.clone());
            Ok(())
        }

        fn set_misc_status_listener(
            self: Arc<Self>,
            api_call_id: i32,
            new_listener: &binder::Strong<dyn ITelemetryMiscStatusListener>,
        ) -> binder::Result<()> {
            self.misc_listener.lock().replace((api_call_id, new_listener.clone()));
            Ok(())
        }
    }

    struct TestEnvironment {
        service: SdvTelemetryService,
        status_listener: Arc<MockStatusListener>,
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
                new_stub_pal(),
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
            service.metrics_report_listener = create_default_metrics_report_listener().into();

            let status_listener = Arc::new(MockStatusListener::new());
            service.status_listener = Arc::<MockStatusListener>::clone(&status_listener);

            TestEnvironment {
                service,
                status_listener,
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

        pub fn get_status_code(&self) -> StatusCodes {
            self.status_listener.api_status_code.lock().expect("Status code should be set")
        }

        pub fn get_status_message(&self) -> String {
            self.status_listener
                .api_status_message
                .lock()
                .take()
                .expect("Status message should be set")
        }

        pub fn get_num_add_metrics_config_calls(&self) -> i32 {
            *self.num_add_metrics_config_calls.lock()
        }

        pub fn get_num_activate_metrics_config_calls(&self) -> i32 {
            *self.num_activate_metrics_config_calls.lock()
        }

        pub fn get_num_deactivate_metrics_config_calls(&self) -> i32 {
            *self.num_deactivate_metrics_config_calls.lock()
        }

        pub fn get_num_remove_config_calls(&self) -> i32 {
            *self.num_remove_config_calls.lock()
        }

        pub fn get_num_remove_all_config_calls(&self) -> i32 {
            *self.num_remove_all_config_calls.lock()
        }

        pub fn get_num_get_active_config_calls(&self) -> i32 {
            *self.num_get_active_config_calls.lock()
        }

        pub fn get_num_get_inactive_config_calls(&self) -> i32 {
            *self.num_get_inactive_config_calls.lock()
        }
    }

    fn create_serialized_config(uuid: &str) -> Vec<u8> {
        let mut config = MetricsConfig::new();
        config.uuid = uuid.to_string();
        config.version = 2;

        info!("Serializing metric config");
        config.write_to_bytes().unwrap()
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn add_metrics_config_call_received() {
        const UUID1: &str = "01bf0754-df95-46bb-bb35-522b6af66600";

        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "add_metrics_config";

        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn add_metrics_config_call_duplicate_fail() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "add_metrics_config";

        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_metrics_config_call_received() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "activate_metrics_config";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_metrics_config_fail_config_doesnt_exist() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "activate_metrics_config";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        test_environment.service.activateMetricsConfig(DUMMY_API_CALL_ID, UUID1).await.unwrap();
        assert_eq!(
            StatusCodes::METRICS_CONFIG_INVALID_CONFIG_UUID,
            test_environment.get_status_code()
        );
        assert_eq!(1, test_environment.get_num_activate_metrics_config_calls());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn deactivate_metrics_config_call_received() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "deactivate_metrics_config";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn deactivate_metrics_config_fail_config_doesnt_exist() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "deactivate_metrics_config";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        test_environment.service.deactivateMetricsConfig(DUMMY_API_CALL_ID, UUID1).await.unwrap();

        assert_eq!(
            StatusCodes::METRICS_CONFIG_INVALID_CONFIG_UUID,
            test_environment.get_status_code()
        );
        assert_eq!(1, test_environment.get_num_deactivate_metrics_config_calls());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn remove_metrics_config_success() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "remove_metrics_config";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn remove_metrics_config_fail_config_doesnt_exist() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "remove_metrics_config";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        test_environment.service.removeMetricsConfig(DUMMY_API_CALL_ID, UUID1).await.unwrap();

        assert_eq!(
            StatusCodes::METRICS_CONFIG_INVALID_CONFIG_UUID,
            test_environment.get_status_code()
        );
        assert_eq!(1, test_environment.get_num_remove_config_calls());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn remove_all_metrics_configs() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "remove_all_metrics_configs";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_active_metrics_config_uuids() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "get_active_metrics_config_uuids";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_inactive_metrics_config_uuids() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "get_inactive_metrics_config_uuids";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn clear_report_ready_listener() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "clear_report_ready_listener";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn set_report_ready_listener() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "set_report_ready_listener";

        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn set_report_ready_listener_and_check_on_ready_calls() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "check_on_ready_calls";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
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

        let report_info = MetricsReportInfo {
            metricsConfigUuid: UUID1.to_string(),
            reportConfigName: String::default(),
            reportNumber: REPORT_NUMBER,
        };
        // Call onReady on report ready listener
        report_ready_listener.onReady(&report_info).unwrap();
        assert_eq!(1, report_ready_listener.get_num_on_ready_report_calls());
        assert_eq!(REPORT_NUMBER, report_ready_listener.get_report_number());
        assert_eq!(UUID1, &report_ready_listener.get_uuid());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn set_multiple_report_ready_listener_fails() {
        const ROOT_PATH: &str = "set_multiple_report_ready_listener";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn register_api_status_listener_success() {
        const ROOT_PATH: &str = "register_api_status_listener";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        let mock_api_listener = BnTelemetryApiStatusListener::new_binder(
            MockTelemetryApiStatusListener {},
            BinderFeatures::default(),
        );

        test_environment.service.registerApiStatusListener(&mock_api_listener).await.unwrap();

        assert!(test_environment.status_listener.api_listener.lock().is_some());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn register_misc_status_listener_success() {
        const ROOT_PATH: &str = "register_misc_status_listener";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        let misc_status_listener = MockTelemetryMiscStatusListener {};
        let binder_listener = BnTelemetryMiscStatusListener::new_binder(
            misc_status_listener,
            BinderFeatures::default(),
        );

        test_environment.service.registerMiscStatusListener(123, &binder_listener).await.unwrap();

        let listener = test_environment.status_listener.misc_listener.lock();
        assert!(listener.is_some());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn unregister_api_status_listener_success() {
        const ROOT_PATH: &str = "unregister_api_status_listener";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        let api_status_listener = MockTelemetryApiStatusListener {};
        let binder_listener = BnTelemetryApiStatusListener::new_binder(
            api_status_listener,
            BinderFeatures::default(),
        );

        // Register the listener first
        test_environment.service.registerApiStatusListener(&binder_listener).await.unwrap();

        // Then unregister
        test_environment.service.unregisterApiStatusListener().await.unwrap();

        assert!(test_environment.status_listener.api_listener.lock().is_none());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn unregister_misc_status_listener_success() {
        const ROOT_PATH: &str = "unregister_misc_status_listener";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        let misc_status_listener = MockTelemetryMiscStatusListener {};
        let binder_listener = BnTelemetryMiscStatusListener::new_binder(
            misc_status_listener,
            BinderFeatures::default(),
        );

        // Register the listener first
        test_environment.service.registerMiscStatusListener(123, &binder_listener).await.unwrap();

        // Then unregister
        test_environment.service.unregisterMiscStatusListener().await.unwrap();

        assert!(test_environment.status_listener.misc_listener.lock().is_none());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_metrics_report() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "get_metrics_report";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

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
            test_environment.get_status_code(),
            "Unexpected status: ({:?}, {})",
            test_environment.get_status_code(),
            test_environment.get_status_message()
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
        let report_info = MetricsReportInfo {
            metricsConfigUuid: UUID1.to_string(),
            reportConfigName: String::default(),
            reportNumber: REPORT_NUMBER,
        };

        // Call on_report_ready callback on metrics report listener
        test_environment.service.metrics_report_listener.on_report_ready(report);
        test_environment
            .service
            .getMetricsReport(&report_info, &binder_report_listener)
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

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_ready_reports_info() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "get_ready_reports_info";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

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

        // Setting Ready Report Ids Listener
        let ready_reports_info_listener = MockTelemetryReadyReportsInfoListener::new();
        let binder_ready_reports_info_listener = BnTelemetryReadyReportsInfoListener::new_binder(
            ready_reports_info_listener.clone(),
            BinderFeatures::default(),
        );

        // Send report to metric report listener
        let mut report = MetricsReport::new();
        report.metrics_config_uuid = UUID1.to_string();
        report.report_number = REPORT_NUMBER;
        let report_info = MetricsReportInfo {
            metricsConfigUuid: UUID1.to_string(),
            reportConfigName: String::default(),
            reportNumber: REPORT_NUMBER,
        };

        test_environment.service.metrics_report_listener.on_report_ready(report);

        // Validate sent report readiness
        test_environment
            .service
            .getReadyReportsInfo(&binder_ready_reports_info_listener)
            .await
            .unwrap();
        assert_eq!(
            ready_reports_info_listener.get_status_code(),
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS
        );
        assert_eq!(ready_reports_info_listener.get_status_message(), "Number of ready reports: 1");
        assert_eq!(ready_reports_info_listener.get_num_on_ready_reports_info_calls(), 1);
        let ready_reports_info = ready_reports_info_listener.get_ready_reports_info();
        assert_eq!(ready_reports_info.len(), 1);
        assert_eq!(ready_reports_info[0].metricsConfigUuid, UUID1);
        assert_eq!(ready_reports_info[0].reportNumber, REPORT_NUMBER);

        // Setting Report Listener
        let report_listener = MockTelemetryReportListener::new();
        let binder_report_listener = BnTelemetryReportListener::new_binder(
            report_listener.clone(),
            BinderFeatures::default(),
        );
        // Extract the report
        test_environment
            .service
            .getMetricsReport(&report_info, &binder_report_listener)
            .await
            .unwrap();

        // Validate no reports are ready
        test_environment
            .service
            .getReadyReportsInfo(&binder_ready_reports_info_listener)
            .await
            .unwrap();
        assert_eq!(
            ready_reports_info_listener.get_status_code(),
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS
        );
        assert_eq!(ready_reports_info_listener.get_status_message(), "Number of ready reports: 0");
        assert_eq!(ready_reports_info_listener.get_num_on_ready_reports_info_calls(), 2);
        assert!(ready_reports_info_listener.get_ready_reports_info().is_empty());
    }
}
