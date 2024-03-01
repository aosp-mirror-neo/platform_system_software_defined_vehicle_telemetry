// Copyright 2023 Google LLC. All Rights Reserved.

//! This module implements the ITelemetryService AIDL interface

mod expr;
mod metrics_config_manager;
mod metrics_config_orchestrator;
mod metrics_report_listener;
pub mod service_publisher_manager;

// External dependencies.
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
use std::marker::{Send, Sync};
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use tracing::instrument;

// Child module dependencies.
pub use service_publisher_manager::{
    ServicePublisherManager, ServicePublisherManagerSubscribeError,
};

use metrics_config_manager::MetricsConfigManager;
use metrics_config_manager::{
    MetricsConfigManagerError as ConfigManagerError, MetricsConfigManagerImpl,
};

use metrics_config_orchestrator::MetricsConfigOrchestratorError as ConfigOrchestratorError;
use metrics_report_listener::MetricsReportListener;

static ACTIVE_CONFIGS_DATA_DIR: &str = "telemetry/active_configs";
static INACTIVE_CONFIGS_DATA_DIR: &str = "telemetry/inactive_configs";

const GLOBAL_STATUS_LISTENER_API_CALL_ID: i32 = 0;

fn get_error_code_and_message(err: ConfigManagerError) -> (i32, String) {
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

    (err_code.0, err.to_string())
}

/// This struct is defined to implement ITelemetryService AIDL interface.
pub struct SdvTelemetryService {
    /// Instance of Metrics Configuration Manager.
    metrics_config_manager: Arc<dyn MetricsConfigManager + Send + Sync>,
    /// Report Ready listener to inform the client when a report is ready
    metrics_report_listener: Arc<MetricsReportListener>,
    /// Status Listener to send back status to client
    status_listener: Mutex<Option<Strong<dyn ITelemetryStatusListener>>>,
}

impl Interface for SdvTelemetryService {}

impl SdvTelemetryService {
    /// Instantiates an instance of SdvTelemetryService
    #[instrument(skip(service_publisher_manager))]
    pub async fn new(
        root_path: &str,
        config_store_size: usize,
        service_publisher_manager: Arc<dyn ServicePublisherManager>,
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
        let report_listener_clone = report_listener.clone(); // Clone the Arc to share ownership
        let config_manager = match MetricsConfigManagerImpl::new(
            active_data_dir,
            inactive_data_dir,
            config_store_size,
            service_publisher_manager,
            move |report| report_listener.on_report_ready(report),
        )
        .await
        {
            Ok(config_manager) => config_manager,
            Err(e) => {
                error!("Error in initializing config manager: {:?}", e);
                return None;
            }
        };

        Some(SdvTelemetryService {
            metrics_config_manager: config_manager,
            metrics_report_listener: report_listener_clone,
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
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
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
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
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
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
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
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
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
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
            "Successfully Removed All Configs",
        )
    }

    #[instrument(skip(self))]
    async fn setReportReadyListener(
        &self,
        listener: &Strong<dyn ITelemetryReportReadyListener>,
    ) -> BinderResult<()> {
        info!("setReportReadyListener");
        let (status_code, status_message) =
            self.metrics_report_listener.set_report_ready_listener(listener).map_or_else(
                |_| {
                    (
                        StatusCodes::SET_REPORT_READY_LISTENER_FAILED.0,
                        "Report Ready Listener already exists",
                    )
                },
                |_| {
                    (
                        StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
                        "Report Ready Listener successfully added",
                    )
                },
            );
        self.send_status(GLOBAL_STATUS_LISTENER_API_CALL_ID, status_code, status_message)
    }

    #[instrument(skip(self))]
    async fn setStatusListener(
        &self,
        listener: &Strong<dyn ITelemetryStatusListener>,
    ) -> BinderResult<()> {
        let (status_code, status_message) = {
            let mut guard = self.status_listener.lock().unwrap();
            if guard.is_none() {
                *guard = Some(listener.clone());
                (
                    StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
                    "Status listener successfully added",
                )
            } else {
                (StatusCodes::SET_STATUS_LISTENER_FAILED.0, "Status listener already exists")
            }
        };
        self.send_status(GLOBAL_STATUS_LISTENER_API_CALL_ID, status_code, status_message)
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
            let _ = report_listener.deref().onReport(
                StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
                metrics_config_uuid,
                serialized_report,
            );
        } else {
            let _ = report_listener.deref().onReport(
                StatusCodes::GET_FINISHED_REPORT_FAILED.0,
                metrics_config_uuid,
                &[],
            );
        }

        Ok(())
    }

    #[instrument(skip(self))]
    async fn clearReportReadyListener(&self) -> BinderResult<()> {
        info!("clearReportReadyListener");
        let (status_code, status_message) = {
            match self.metrics_report_listener.clear_report_ready_listener() {
                Ok(()) => (
                    StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
                    "Report listener successfully cleared",
                ),
                Err(_) => {
                    (StatusCodes::CLEAR_REPORT_READY_LISTENER_FAILED.0, "Report listener not set")
                }
            }
        };
        self.send_status(GLOBAL_STATUS_LISTENER_API_CALL_ID, status_code, status_message)
    }

    #[instrument(skip(self))]
    async fn clearStatusListener(&self) -> BinderResult<()> {
        info!("clearStatusListener");
        *self.status_listener.lock().unwrap() = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use android_sdv_telemetry::aidl::android::sdv::telemetry::{
        ITelemetryReportListener::BnTelemetryReportListener,
        ITelemetryReportReadyListener::BnTelemetryReportReadyListener,
        ITelemetryStatusListener::BnTelemetryStatusListener,
    };
    use binder::BinderFeatures;
    use protobuf::Message;
    use sdvtelemetry_proto_rust::metrics_configuration::MetricsConfig;
    use sdvtelemetry_proto_rust::metrics_configuration::MetricsReport;

    use super::*;
    use crate::metrics_config_manager::mock::MockMetricsConfigManager;
    use crate::service_publisher_manager::mock::MockServicePublisherManager;

    const MAX_PERSISTENT_CONFIGURATION_SIZE_MB: usize = 10 * 1024 * 1024; // 10Mbs
    const REPORT_NUMBER: i32 = 42;

    /// This struct is defined to implement ITelemetryReportListener AIDL interface for unit testing purposes
    #[derive(Clone)]
    struct MockTelemetryReportListener {
        status_code: Arc<Mutex<i32>>,
        status_message: Arc<Mutex<String>>,
        num_on_result_calls: Arc<Mutex<i32>>,
        uuid: Arc<Mutex<String>>,
        report_number: Arc<Mutex<i32>>,
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
        report_number: Arc<Mutex<i32>>,
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
            *uuid = metrics_config_uuid.to_owned();
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
        service: SdvTelemetryService,
        status_listener: Option<MockTelemetryStatusListener>,
        num_add_metrics_config_calls: Arc<Mutex<i32>>,
        num_activate_metrics_config_calls: Arc<Mutex<i32>>,
        num_deactivate_metrics_config_calls: Arc<Mutex<i32>>,
        num_remove_config_calls: Arc<Mutex<i32>>,
        num_remove_all_config_calls: Arc<Mutex<i32>>,
    }

    impl TestEnvironment {
        // Test setup
        async fn new(dir_path: String) -> TestEnvironment {
            let num_add_metrics_config_calls = Arc::new(Mutex::new(0));
            let num_activate_metrics_config_calls = Arc::new(Mutex::new(0));
            let num_deactivate_metrics_config_calls = Arc::new(Mutex::new(0));
            let num_remove_config_calls = Arc::new(Mutex::new(0));
            let num_remove_all_config_calls = Arc::new(Mutex::new(0));

            let mut service = SdvTelemetryService::new(
                dir_path.as_str(),
                MAX_PERSISTENT_CONFIGURATION_SIZE_MB,
                Arc::new(MockServicePublisherManager::default()),
            )
            .await
            .unwrap();
            service.metrics_config_manager = Arc::new(MockMetricsConfigManager::new(
                num_add_metrics_config_calls.clone(),
                num_activate_metrics_config_calls.clone(),
                num_deactivate_metrics_config_calls.clone(),
                num_remove_config_calls.clone(),
                num_remove_all_config_calls.clone(),
            ));

            TestEnvironment {
                service,
                status_listener: None,
                num_add_metrics_config_calls: num_add_metrics_config_calls.clone(),
                num_activate_metrics_config_calls: num_activate_metrics_config_calls.clone(),
                num_deactivate_metrics_config_calls: num_deactivate_metrics_config_calls.clone(),
                num_remove_config_calls: num_remove_config_calls.clone(),
                num_remove_all_config_calls: num_remove_all_config_calls.clone(),
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

            assert_eq!(StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0, self.get_status_code());
        }

        pub fn get_status_code(&self) -> i32 {
            self.status_listener.as_ref().unwrap().get_status_code()
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
    }

    fn create_serialized_config(uuid: &str) -> Vec<u8> {
        let mut config = MetricsConfig::new();
        config.uuid = uuid.to_string();
        config.version = 2;

        info!("Serializing metric config");
        config.write_to_bytes().unwrap()
    }

    #[tokio::test]
    async fn add_metrics_config_call_received() {
        const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";

        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "add_metrics_config";

        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_status_listener().await;

        info!("Serializing metric config");
        let serialized_config = create_serialized_config(UUID);

        test_environment.service.addMetricsConfig(-1, &serialized_config).await.unwrap();
        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
            test_environment.get_status_code()
        );
        assert_eq!(1, test_environment.get_num_add_metrics_config_calls());
    }

    #[tokio::test]
    async fn add_metrics_config_call_duplicate_fail() {
        const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";

        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "add_metrics_config";

        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_status_listener().await;

        info!("Serializing metric config");
        let serialized_config = create_serialized_config(UUID);

        test_environment.service.addMetricsConfig(-1, &serialized_config).await.unwrap();
        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
            test_environment.get_status_code()
        );
        assert_eq!(1, test_environment.get_num_add_metrics_config_calls());

        test_environment.service.addMetricsConfig(-1, &serialized_config).await.unwrap();
        assert_eq!(StatusCodes::ADD_METRICS_CONFIG_FAILED.0, test_environment.get_status_code());
        assert_eq!(2, test_environment.get_num_add_metrics_config_calls());
    }

    #[tokio::test]
    async fn activate_metrics_config_call_received() {
        const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";

        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "activate_metrics_config";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_status_listener().await;

        let serialized_config = create_serialized_config(UUID);
        test_environment.service.addMetricsConfig(-1, &serialized_config).await.unwrap();
        assert_eq!(1, test_environment.get_num_add_metrics_config_calls());
        let _ = test_environment.service.activateMetricsConfig(-1, UUID).await;
        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
            test_environment.get_status_code()
        );
        assert_eq!(1, test_environment.get_num_activate_metrics_config_calls());
    }

    #[tokio::test]
    async fn activate_metrics_config_fail_config_doesnt_exist() {
        const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";

        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "activate_metrics_config";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_status_listener().await;

        let _ = test_environment.service.activateMetricsConfig(-1, UUID).await;
        assert_eq!(
            StatusCodes::METRICS_CONFIG_INVALID_CONFIG_UUID.0,
            test_environment.get_status_code()
        );
        assert_eq!(1, test_environment.get_num_activate_metrics_config_calls());
    }

    #[tokio::test]
    async fn deactivate_metrics_config_call_received() {
        const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";

        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "deactivate_metrics_config";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_status_listener().await;

        let serialized_config = create_serialized_config(UUID);
        test_environment.service.addMetricsConfig(-1, &serialized_config).await.unwrap();
        assert_eq!(1, test_environment.get_num_add_metrics_config_calls());

        let _ = test_environment.service.activateMetricsConfig(-1, UUID).await;
        assert_eq!(1, test_environment.get_num_activate_metrics_config_calls());

        let _ = test_environment.service.deactivateMetricsConfig(-1, UUID).await;
        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
            test_environment.get_status_code()
        );
        assert_eq!(1, test_environment.get_num_deactivate_metrics_config_calls());
    }

    #[tokio::test]
    async fn deactivate_metrics_config_fail_config_doesnt_exist() {
        const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";

        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "deactivate_metrics_config";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_status_listener().await;

        let _ = test_environment.service.deactivateMetricsConfig(-1, UUID).await;
        assert_eq!(
            StatusCodes::METRICS_CONFIG_INVALID_CONFIG_UUID.0,
            test_environment.get_status_code()
        );
        assert_eq!(1, test_environment.get_num_deactivate_metrics_config_calls());
    }

    #[tokio::test]
    async fn remove_metrics_config_success() {
        const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";

        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "remove_metrics_config";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_status_listener().await;

        let serialized_config = create_serialized_config(UUID);
        test_environment.service.addMetricsConfig(-1, &serialized_config).await.unwrap();
        assert_eq!(1, test_environment.get_num_add_metrics_config_calls());

        let _ = test_environment.service.activateMetricsConfig(-1, UUID).await;
        assert_eq!(1, test_environment.get_num_activate_metrics_config_calls());

        let _ = test_environment.service.removeMetricsConfig(-1, UUID).await;
        assert_eq!(1, test_environment.get_num_remove_config_calls());
        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
            test_environment.get_status_code()
        );
    }

    #[tokio::test]
    async fn remove_metrics_config_fail_config_doesnt_exist() {
        const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";

        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "remove_metrics_config";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_status_listener().await;

        let _ = test_environment.service.removeMetricsConfig(-1, UUID).await;
        assert_eq!(
            StatusCodes::METRICS_CONFIG_INVALID_CONFIG_UUID.0,
            test_environment.get_status_code()
        );
        assert_eq!(1, test_environment.get_num_remove_config_calls());
    }

    #[tokio::test]
    async fn remove_all_metrics_configs() {
        const UUID1: &str = "01bf0754-df95-46bb-bb35-522b6af66600";
        const UUID2: &str = "f47ac10b-58cc-4372-a567-0e02b2c3d479";

        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "remove_all_metrics_configs";
        let mut test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;
        test_environment.set_mock_status_listener().await;

        let serialized_config1 = create_serialized_config(UUID1);
        let serialized_config2 = create_serialized_config(UUID2);

        // Add configs
        test_environment.service.addMetricsConfig(-1, &serialized_config1).await.unwrap();
        test_environment.service.addMetricsConfig(-1, &serialized_config2).await.unwrap();
        assert_eq!(2, test_environment.get_num_add_metrics_config_calls());

        // Remove all configs
        let _ = test_environment.service.removeAllMetricsConfigs(-1).await;
        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
            test_environment.get_status_code()
        );
        assert_eq!(1, test_environment.get_num_remove_all_config_calls());
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
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
            test_environment.get_status_code()
        );

        let _ = test_environment.service.clearReportReadyListener().await;

        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
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
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
            test_environment.get_status_code()
        );
    }

    #[tokio::test]
    async fn set_report_ready_listener_and_check_on_ready_calls() {
        const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";

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
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
            test_environment.get_status_code()
        );

        // Call onReady on report ready listener
        let _ = report_ready_listener.onReady(UUID, &String::default(), REPORT_NUMBER);
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
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
            test_environment.get_status_code()
        );

        let _ = test_environment.service.setReportReadyListener(&report_listener).await;
        assert_eq!(
            StatusCodes::SET_REPORT_READY_LISTENER_FAILED.0,
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
        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
            status_listener.get_status_code()
        );

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
        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
            status_listener.get_status_code()
        );

        let _ = test_environment.service.setStatusListener(&binder_listener).await;
        assert_eq!(StatusCodes::SET_STATUS_LISTENER_FAILED.0, status_listener.get_status_code());
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
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
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
            .getMetricsReport(UUID, &String::default(), REPORT_NUMBER, &binder_report_listener)
            .await;

        assert_eq!(
            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS.0,
            report_listener.get_status_code()
        );
        assert_eq!(UUID, report_listener.get_status_message());
        assert_eq!(1, report_ready_listener.get_num_on_ready_report_calls());
        assert_eq!(1, report_listener.get_num_on_result_calls());
        assert_eq!(UUID, report_listener.get_uuid());
        assert_eq!(REPORT_NUMBER, report_listener.get_report_number());
    }
}
