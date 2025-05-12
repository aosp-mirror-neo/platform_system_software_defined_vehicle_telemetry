// Copyright 2023 Google LLC

//! This module implements the ITelemetryService AIDL interface

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
    ITelemetryService::ITelemetryServiceAsyncServer,
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
    data: Arc<SdvTelemetryServiceData>,
}

struct SdvTelemetryServiceData {
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
            data: Arc::new(SdvTelemetryServiceData {
                metrics_config_manager: config_manager,
                metrics_report_listener: report_listener_clone,
                status_listener,
            }),
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
        let metrics_config = metrics_config.to_owned();
        let data_reference = Arc::downgrade(&self.data);
        tokio::spawn(async move {
            // Add metrics config and report any errors to client
            let Some(data) = data_reference.upgrade() else { return };
            let (status_code, status_message) =
                data.metrics_config_manager.add_metrics_config(&metrics_config).await.map_or_else(
                    |e| {
                        error!("Unable to add Metrics Config: {e:?}");
                        get_error_code_and_message(e)
                    },
                    |config_uuid| (StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS, config_uuid),
                );

            data.status_listener.send_api_status(api_call_id, status_code, &status_message);
        });

        Ok(())
    }

    #[instrument(skip(self))]
    async fn activateMetricsConfig(
        &self,
        api_call_id: i32,
        metrics_config_uuid: &str,
    ) -> binder::Result<()> {
        let metrics_config_uuid = metrics_config_uuid.to_owned();
        let data_reference = Arc::downgrade(&self.data);
        tokio::spawn(async move {
            // Activate metrics config and report any errors to client
            let Some(data) = data_reference.upgrade() else { return };
            let (status_code, status_message) = data
                .metrics_config_manager
                .activate_metrics_config(&metrics_config_uuid)
                .await
                .map_or_else(
                    |e| {
                        error!("Unable to activate Metrics Config: {e:?}");
                        get_error_code_and_message(e)
                    },
                    |()| (StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS, metrics_config_uuid),
                );

            data.status_listener.send_api_status(api_call_id, status_code, &status_message);
        });

        Ok(())
    }

    #[instrument(skip(self))]
    async fn deactivateMetricsConfig(
        &self,
        api_call_id: i32,
        metrics_config_uuid: &str,
    ) -> binder::Result<()> {
        let metrics_config_uuid = metrics_config_uuid.to_owned();
        let data_reference = Arc::downgrade(&self.data);
        tokio::spawn(async move {
            // Deactivate metrics config and report any errors to client
            let Some(data) = data_reference.upgrade() else { return };
            let (status_code, status_message) = data
                .metrics_config_manager
                .deactivate_metrics_config(&metrics_config_uuid)
                .await
                .map_or_else(
                    |e| {
                        error!("Unable to deactivate Metrics Config: {e:?}");
                        get_error_code_and_message(e)
                    },
                    |()| (StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS, metrics_config_uuid),
                );

            data.status_listener.send_api_status(api_call_id, status_code, &status_message);
        });

        Ok(())
    }

    #[instrument(skip(self))]
    async fn removeMetricsConfig(
        &self,
        api_call_id: i32,
        metrics_config_uuid: &str,
    ) -> binder::Result<()> {
        let metrics_config_uuid = metrics_config_uuid.to_owned();
        let data_reference = Arc::downgrade(&self.data);
        tokio::spawn(async move {
            // Remove metrics config and report any errors to client
            let Some(data) = data_reference.upgrade() else { return };
            let (status_code, status_message) = data
                .metrics_config_manager
                .remove_metrics_config(&metrics_config_uuid)
                .await
                .map_or_else(
                    |e| {
                        error!("Unable to remove Metrics Config: {e:?}");
                        get_error_code_and_message(e)
                    },
                    |()| (StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS, metrics_config_uuid),
                );

            data.status_listener.send_api_status(api_call_id, status_code, &status_message);
        });

        Ok(())
    }

    #[instrument(skip(self))]
    async fn removeAllMetricsConfigs(&self, api_call_id: i32) -> binder::Result<()> {
        let data_reference = Arc::downgrade(&self.data);
        tokio::spawn(async move {
            // Remove all metrics config and report any errors to client
            let Some(data) = data_reference.upgrade() else { return };
            let (status_code, status_message) =
                data.metrics_config_manager.remove_all_metrics_configs().await.map_or_else(
                    |e| {
                        error!("Unable to remove all Metrics Configs: {e:?}");
                        let (code, message) = get_error_code_and_message(e);
                        (code, Cow::from(message))
                    },
                    |()| {
                        (
                            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
                            Cow::from("Successfully removed all Metrics Configs"),
                        )
                    },
                );

            data.status_listener.send_api_status(api_call_id, status_code, &status_message);
        });

        Ok(())
    }

    #[instrument(skip(self, config_list_listener))]
    async fn getActiveMetricsConfigUuids(
        &self,
        config_list_listener: &binder::Strong<dyn ITelemetryConfigListListener>,
    ) -> binder::Result<()> {
        info!("getActiveMetricsConfigUuids");

        let config_list_listener = config_list_listener.clone();
        let data_reference = Arc::downgrade(&self.data);
        tokio::spawn(async move {
            let Some(data) = data_reference.upgrade() else { return };
            let config_list = data.metrics_config_manager.get_active_metrics_config_uuids().await;
            if let Err(e) = config_list_listener.deref().sendConfigList(
                StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
                "",
                &config_list,
            ) {
                warn!("Unable to send config list to the client: {:?}", e);
            }
        });

        Ok(())
    }

    #[instrument(skip(self, config_list_listener))]
    async fn getInactiveMetricsConfigUuids(
        &self,
        config_list_listener: &binder::Strong<dyn ITelemetryConfigListListener>,
    ) -> binder::Result<()> {
        info!("getInactiveMetricsConfigUuids");

        let config_list_listener = config_list_listener.clone();
        let data_reference = Arc::downgrade(&self.data);
        tokio::spawn(async move {
            let Some(data) = data_reference.upgrade() else { return };
            let config_list = data.metrics_config_manager.get_inactive_metrics_config_uuids().await;
            if let Err(e) = config_list_listener.deref().sendConfigList(
                StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
                "",
                &config_list,
            ) {
                warn!("Unable to send config list to the client: {:?}", e);
            }
        });

        Ok(())
    }

    #[instrument(skip(self))]
    async fn registerReportReadyListener(
        &self,
        api_call_id: i32,
        listener: &binder::Strong<dyn ITelemetryReportReadyListener>,
    ) -> binder::Result<()> {
        info!("registerReportReadyListener");

        let listener = listener.clone();
        let data_reference = Arc::downgrade(&self.data);
        tokio::spawn(async move {
            let Some(data) = data_reference.upgrade() else { return };
            let (status_code, status_message) =
                data.metrics_report_listener.set_report_ready_listener(listener).map_or_else(
                    |err| {
                        (StatusCodes::SET_REPORT_READY_LISTENER_FAILED, Cow::from(err.to_string()))
                    },
                    |()| {
                        (
                            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
                            Cow::from("Report Ready Listener successfully added"),
                        )
                    },
                );

            data.status_listener.send_api_status(api_call_id, status_code, &status_message);
        });

        Ok(())
    }

    #[instrument(skip(self))]
    async fn registerApiStatusListener(
        &self,
        new_listener: &binder::Strong<dyn ITelemetryApiStatusListener>,
    ) -> binder::Result<()> {
        Arc::clone(&self.data.status_listener).set_api_status_listener(new_listener)
    }

    #[instrument(skip(self))]
    async fn registerMiscStatusListener(
        &self,
        api_call_id: i32,
        new_listener: &binder::Strong<dyn ITelemetryMiscStatusListener>,
    ) -> binder::Result<()> {
        let new_listener = new_listener.clone();
        let data_reference = Arc::downgrade(&self.data);
        tokio::spawn(async move {
            let Some(data) = data_reference.upgrade() else { return };
            let (status_code, status_message) = Arc::clone(&data.status_listener)
                .set_misc_status_listener(new_listener)
                .map_or_else(
                    |e| {
                        (
                            StatusCodes::SET_MISC_STATUS_LISTENER_FAILED,
                            Cow::from(format!("Unable to set misc status listener: {e:?}")),
                        )
                    },
                    |()| {
                        (
                            StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
                            Cow::from("Misc Status listener successfully set"),
                        )
                    },
                );

            data.status_listener.send_api_status(api_call_id, status_code, &status_message);
        });

        Ok(())
    }

    #[instrument(skip(self, report_listener))]
    async fn getMetricsReport(
        &self,
        report_uuid: &str,
        report_listener: &binder::Strong<dyn ITelemetryReportListener>,
    ) -> binder::Result<()> {
        info!("getMetricsReport: {}", report_uuid);

        let report_uuid = report_uuid.to_owned();
        let report_listener = report_listener.clone();
        let data_reference = Arc::downgrade(&self.data);
        tokio::spawn(async move {
            let Some(data) = data_reference.upgrade() else { return };
            match data.metrics_report_listener.get_metrics_report(&report_uuid) {
                Ok(metrics_report) => {
                    let serialized_report: &[u8] = &metrics_report
                        .write_to_bytes()
                        .expect("Report serialization should be successful");
                    if let Err(e) = report_listener.deref().onReport(
                        StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
                        &report_uuid,
                        serialized_report,
                    ) {
                        warn!("Unable to send report to the client: {:?}", e);
                    }
                }
                Err(e) => {
                    if let Err(e) = report_listener.deref().onReport(
                        StatusCodes::GET_FINISHED_REPORT_FAILED,
                        &format!("Unable to retrieve report: {:?}", e),
                        &[],
                    ) {
                        warn!("Unable to send report retrieval error to the client: {:?}", e);
                    }
                }
            }
        });

        Ok(())
    }

    #[instrument(skip(self, report_listener))]
    async fn getReadyReportsInfo(
        &self,
        report_listener: &binder::Strong<dyn ITelemetryReadyReportsInfoListener>,
    ) -> binder::Result<()> {
        info!("getReadyReportsInfo");

        let report_listener = report_listener.clone();
        let data_reference = Arc::downgrade(&self.data);
        tokio::spawn(async move {
            let Some(data) = data_reference.upgrade() else { return };
            let ready_reports_info = data.metrics_report_listener.get_ready_reports_info();

            if let Err(e) = report_listener.deref().sendReadyReportsInfo(
                StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS,
                format!("Number of ready reports: {}", ready_reports_info.len()).as_str(),
                &ready_reports_info,
            ) {
                warn!("Unable to send info of ready reports to the client: {:?}", e);
            }
        });

        Ok(())
    }

    #[instrument(skip(self))]
    async fn unregisterReportReadyListener(&self) -> binder::Result<()> {
        info!("unregisterReportReadyListener");
        self.data.metrics_report_listener.clear_report_ready_listener();
        Ok(())
    }

    #[instrument(skip(self))]
    async fn unregisterApiStatusListener(&self) -> binder::Result<()> {
        info!("unregisterApiStatusListener");
        self.data.status_listener.clear_api_status_listener();
        Ok(())
    }

    #[instrument(skip(self))]
    async fn unregisterMiscStatusListener(&self) -> binder::Result<()> {
        info!("unregisterMiscStatusListener");
        self.data.status_listener.clear_misc_status_listener();
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
    use sdv_telemetry_utils::collection_extension::HashMapExtension;
    use std::collections::HashMap;
    use std::future::Future;
    use std::sync::atomic::{AtomicI32, Ordering};
    use tokio::sync::oneshot;

    use crate::metrics_config_manager::mock::MockMetricsConfigManager;
    use crate::metrics_report_listener::tests::create_default_metrics_report_listener;
    use crate::mock_service_publisher_accessor::MockServicePublisherAccessor;
    use crate::status_listener::SetMiscStatusListenerError;

    const MAX_PERSISTENT_CONFIGURATION_SIZE_MB: usize = 10 * 1024 * 1024; // 10Mbs
    const UUID1: &str = "01bf0754-df95-46bb-bb35-522b6af66600";
    const UUID2: &str = "f47ac10b-58cc-4372-a567-0e02b2c3d479";

    /// This struct is defined to implement ITelemetryReportListener AIDL interface for unit testing purposes
    #[derive(Clone)]
    struct MockTelemetryConfigListListener {
        metrics_config_list: Arc<Mutex<Option<Vec<String>>>>,
        status_sender: Arc<Mutex<Option<oneshot::Sender<(StatusCodes, String)>>>>,
    }

    impl binder::Interface for MockTelemetryConfigListListener {}

    impl MockTelemetryConfigListListener {
        fn new() -> Self {
            Self {
                metrics_config_list: Arc::new(Mutex::new(None)),
                status_sender: Arc::new(Mutex::new(None)),
            }
        }

        fn expect_call(&self) -> oneshot::Receiver<(StatusCodes, String)> {
            let (sender, receiver) = oneshot::channel();
            assert!(self.status_sender.lock().replace(sender).is_none());
            receiver
        }

        fn get_metrics_config_list(&self) -> Vec<String> {
            self.metrics_config_list.lock().as_ref().unwrap().clone()
        }
    }

    impl ITelemetryConfigListListener for MockTelemetryConfigListListener {
        fn sendConfigList(
            &self,
            status_code: StatusCodes,
            status_message: &str,
            config_list: &[String],
        ) -> binder::Result<()> {
            self.metrics_config_list.lock().replace(config_list.to_owned());
            self.status_sender
                .lock()
                .take()
                .expect("Status sender should be set")
                .send((status_code, status_message.to_owned()))
                .unwrap();

            Ok(())
        }
    }

    /// This struct is defined to implement ITelemetryReportListener AIDL interface for unit testing purposes
    #[derive(Clone)]
    struct MockTelemetryReportListener {
        metrics_report: Arc<Mutex<Option<MetricsReport>>>,
        status_sender: Arc<Mutex<Option<oneshot::Sender<(StatusCodes, String)>>>>,
    }

    impl binder::Interface for MockTelemetryReportListener {}

    impl MockTelemetryReportListener {
        fn new() -> Self {
            Self {
                metrics_report: Arc::new(Mutex::new(None)),
                status_sender: Arc::new(Mutex::new(None)),
            }
        }

        fn expect_call(&self) -> oneshot::Receiver<(StatusCodes, String)> {
            let (sender, receiver) = oneshot::channel();
            assert!(self.status_sender.lock().replace(sender).is_none());
            receiver
        }

        fn get_metrics_report(&self) -> Option<MetricsReport> {
            self.metrics_report.lock().take()
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
            let metrics_report = match MetricsReport::parse_from_bytes(serialized_report) {
                Ok(report) => report,
                Err(_) => return Err(binder::StatusCode::BAD_VALUE.into()),
            };

            assert!(self.metrics_report.lock().replace(metrics_report).is_none());
            self.status_sender
                .lock()
                .take()
                .expect("Status sender should be set")
                .send((status_code, status_message.to_owned()))
                .unwrap();

            Ok(())
        }
    }

    /// This struct is defined to implement ITelemetryReportReadyListener AIDL interface for unit testing purposes
    #[derive(Clone)]
    struct MockTelemetryReadyReportsInfoListener {
        ready_reports_info: Arc<Mutex<Option<Vec<String>>>>,
        status_sender: Arc<Mutex<Option<oneshot::Sender<(StatusCodes, String)>>>>,
    }

    impl binder::Interface for MockTelemetryReadyReportsInfoListener {}

    impl MockTelemetryReadyReportsInfoListener {
        fn new() -> Self {
            Self {
                ready_reports_info: Arc::new(Mutex::new(None)),
                status_sender: Arc::new(Mutex::new(None)),
            }
        }

        fn expect_call(&self) -> oneshot::Receiver<(StatusCodes, String)> {
            let (sender, receiver) = oneshot::channel();
            assert!(self.status_sender.lock().replace(sender).is_none());
            receiver
        }

        fn get_ready_reports_info(&self) -> Option<Vec<String>> {
            self.ready_reports_info.lock().take()
        }
    }

    impl ITelemetryReadyReportsInfoListener for MockTelemetryReadyReportsInfoListener {
        fn sendReadyReportsInfo(
            &self,
            status_code: StatusCodes,
            status_message: &str,
            ready_reports_info: &[MetricsReportInfo],
        ) -> binder::Result<()> {
            let info =
                ready_reports_info.iter().map(|item| item.reportUuid.clone()).collect::<Vec<_>>();
            assert!(self.ready_reports_info.lock().replace(info).is_none());
            self.status_sender
                .lock()
                .take()
                .expect("Status sender should be set")
                .send((status_code, status_message.to_owned()))
                .unwrap();

            Ok(())
        }
    }

    /// This struct is defined to implement ITelemetryReportReadyListener AIDL interface for unit
    /// testing purposes
    #[derive(Clone)]
    struct MockTelemetryReportReadyListener {
        num_on_ready_report_calls: Arc<Mutex<i32>>,
        report_uuid: Arc<Mutex<String>>,
    }

    impl binder::Interface for MockTelemetryReportReadyListener {}

    impl MockTelemetryReportReadyListener {
        fn new() -> Self {
            Self {
                num_on_ready_report_calls: Arc::new(Mutex::new(0)),
                report_uuid: Arc::new(Mutex::new(String::default())),
            }
        }

        fn get_num_on_ready_report_calls(&self) -> i32 {
            *self.num_on_ready_report_calls.lock()
        }

        fn get_uuid(&self) -> String {
            self.report_uuid.lock().clone()
        }
    }

    impl ITelemetryReportReadyListener for MockTelemetryReportReadyListener {
        fn onReady(&self, report_info: &MetricsReportInfo) -> binder::Result<()> {
            *self.num_on_ready_report_calls.lock() += 1;
            *self.report_uuid.lock() = report_info.reportUuid.clone();
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
        num_clear_api_status_listener_calls: Mutex<u32>,
        num_clear_misc_status_listener_calls: Mutex<u32>,
        num_set_api_status_listener_calls: Mutex<u32>,
        num_set_misc_status_listener_calls: Mutex<i32>,
        status_senders: Mutex<HashMap<i32, oneshot::Sender<(StatusCodes, String)>>>,
    }

    impl MockStatusListener {
        fn new() -> Self {
            Self {
                num_clear_api_status_listener_calls: 0.into(),
                num_clear_misc_status_listener_calls: 0.into(),
                num_set_api_status_listener_calls: 0.into(),
                num_set_misc_status_listener_calls: 0.into(),
                status_senders: HashMap::new().into(),
            }
        }

        fn expect_call(&self, api_call_id: i32) -> oneshot::Receiver<(StatusCodes, String)> {
            let (sender, receiver) = oneshot::channel();
            self.status_senders
                .lock()
                .try_insert_value(api_call_id, sender)
                .expect("API call ids should be unique");
            receiver
        }
    }

    impl StatusListener for MockStatusListener {
        fn send_api_status(
            &self,
            api_call_id: i32,
            status_code: StatusCodes,
            status_message: &str,
        ) {
            let Some(status_sender) = self.status_senders.lock().remove(&api_call_id) else {
                panic!("Received unexpected API call id: {api_call_id}");
            };
            status_sender.send((status_code, status_message.to_owned())).unwrap();
        }

        fn send_misc_status(&self, _status: TelemetryServiceStatus) {}

        fn clear_api_status_listener(&self) {
            *self.num_clear_api_status_listener_calls.lock() += 1;
        }

        fn clear_misc_status_listener(&self) {
            *self.num_clear_misc_status_listener_calls.lock() += 1;
        }

        fn set_api_status_listener(
            self: Arc<Self>,
            _new_listener: &binder::Strong<dyn ITelemetryApiStatusListener>,
        ) -> binder::Result<()> {
            *self.num_set_api_status_listener_calls.lock() += 1;

            Ok(())
        }

        fn set_misc_status_listener(
            self: Arc<Self>,
            _new_listener: binder::Strong<dyn ITelemetryMiscStatusListener>,
        ) -> Result<(), SetMiscStatusListenerError> {
            *self.num_set_misc_status_listener_calls.lock() += 1;

            Ok(())
        }
    }

    struct TestEnvironment {
        service: SdvTelemetryService,
        status_listener: Arc<MockStatusListener>,
        metrics_config_manager: Arc<MockMetricsConfigManager>,
        next_api_call_id: AtomicI32,
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
            let status_listener = Arc::new(MockStatusListener::new());
            service.data = Arc::new(SdvTelemetryServiceData {
                metrics_config_manager: Arc::<MockMetricsConfigManager>::clone(
                    &metrics_config_manager,
                ),
                metrics_report_listener: create_default_metrics_report_listener().into(),
                status_listener: Arc::<MockStatusListener>::clone(&status_listener),
            });

            TestEnvironment {
                service,
                status_listener,
                metrics_config_manager,
                next_api_call_id: 1.into(),
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

        fn get_num_add_metrics_config_calls(&self) -> i32 {
            *self.num_add_metrics_config_calls.lock()
        }

        fn get_num_activate_metrics_config_calls(&self) -> i32 {
            *self.num_activate_metrics_config_calls.lock()
        }

        fn get_num_deactivate_metrics_config_calls(&self) -> i32 {
            *self.num_deactivate_metrics_config_calls.lock()
        }

        fn get_num_remove_config_calls(&self) -> i32 {
            *self.num_remove_config_calls.lock()
        }

        fn get_num_remove_all_config_calls(&self) -> i32 {
            *self.num_remove_all_config_calls.lock()
        }

        fn get_num_get_active_config_calls(&self) -> i32 {
            *self.num_get_active_config_calls.lock()
        }

        fn get_num_get_inactive_config_calls(&self) -> i32 {
            *self.num_get_inactive_config_calls.lock()
        }

        async fn make_async_call<'a, Fut>(
            &'a self,
            f: impl FnOnce(&'a SdvTelemetryService, i32) -> Fut,
        ) -> Result<(), StatusCodes>
        where
            Fut: Future<Output = binder::Result<()>> + 'a,
        {
            let api_call_id = self.next_api_call_id.fetch_add(1, Ordering::AcqRel);
            let status_receiver = self.status_listener.expect_call(api_call_id);

            f(&self.service, api_call_id)
                .await
                .expect("API should return successful Binder result");

            let (status_code, _status_message) = status_receiver.await.unwrap();
            if status_code == StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS {
                Ok(())
            } else {
                Err(status_code)
            }
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
    async fn add_metrics_config_success() {
        const UUID1: &str = "01bf0754-df95-46bb-bb35-522b6af66600";

        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "add_metrics_config";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        info!("Serializing metric config");
        let serialized_config = create_serialized_config(UUID1);

        test_environment
            .make_async_call(|service, api_call_id| {
                service.addMetricsConfig(api_call_id, &serialized_config)
            })
            .await
            .unwrap();

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
            .make_async_call(|service, api_call_id| {
                service.addMetricsConfig(api_call_id, &serialized_config)
            })
            .await
            .unwrap();

        let status_code = test_environment
            .make_async_call(|service, api_call_id| {
                service.addMetricsConfig(api_call_id, &serialized_config)
            })
            .await
            .unwrap_err();

        assert_eq!(StatusCodes::ADD_METRICS_CONFIG_FAILED, status_code);
        assert_eq!(2, test_environment.get_num_add_metrics_config_calls());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_metrics_config_success() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "activate_metrics_config";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        let serialized_config = create_serialized_config(UUID1);

        test_environment
            .make_async_call(|service, api_call_id| {
                service.addMetricsConfig(api_call_id, &serialized_config)
            })
            .await
            .unwrap();

        test_environment
            .make_async_call(|service, api_call_id| {
                service.activateMetricsConfig(api_call_id, UUID1)
            })
            .await
            .unwrap();

        assert_eq!(1, test_environment.get_num_add_metrics_config_calls());
        assert_eq!(1, test_environment.get_num_activate_metrics_config_calls());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn activate_metrics_config_config_doesnt_exist_fails() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "activate_metrics_config";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        let status_code = test_environment
            .make_async_call(|service, api_call_id| {
                service.activateMetricsConfig(api_call_id, UUID1)
            })
            .await
            .unwrap_err();

        assert_eq!(StatusCodes::METRICS_CONFIG_INVALID_CONFIG_UUID, status_code);
        assert_eq!(1, test_environment.get_num_activate_metrics_config_calls());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn deactivate_metrics_config_success() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "deactivate_metrics_config";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        let serialized_config = create_serialized_config(UUID1);

        test_environment
            .make_async_call(|service, api_call_id| {
                service.addMetricsConfig(api_call_id, &serialized_config)
            })
            .await
            .unwrap();

        test_environment
            .make_async_call(|service, api_call_id| {
                service.activateMetricsConfig(api_call_id, UUID1)
            })
            .await
            .unwrap();

        test_environment
            .make_async_call(|service, api_call_id| {
                service.deactivateMetricsConfig(api_call_id, UUID1)
            })
            .await
            .unwrap();

        assert_eq!(1, test_environment.get_num_add_metrics_config_calls());
        assert_eq!(1, test_environment.get_num_activate_metrics_config_calls());
        assert_eq!(1, test_environment.get_num_deactivate_metrics_config_calls());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn deactivate_metrics_config_config_doesnt_exist_fails() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "deactivate_metrics_config";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        let status_code = test_environment
            .make_async_call(|service, api_call_id| {
                service.deactivateMetricsConfig(api_call_id, UUID1)
            })
            .await
            .unwrap_err();

        assert_eq!(StatusCodes::METRICS_CONFIG_INVALID_CONFIG_UUID, status_code);
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
            .make_async_call(|service, api_call_id| {
                service.addMetricsConfig(api_call_id, &serialized_config)
            })
            .await
            .unwrap();

        test_environment
            .make_async_call(|service, api_call_id| {
                service.activateMetricsConfig(api_call_id, UUID1)
            })
            .await
            .unwrap();

        test_environment
            .make_async_call(|service, api_call_id| service.removeMetricsConfig(api_call_id, UUID1))
            .await
            .unwrap();

        assert_eq!(1, test_environment.get_num_add_metrics_config_calls());
        assert_eq!(1, test_environment.get_num_activate_metrics_config_calls());
        assert_eq!(1, test_environment.get_num_remove_config_calls());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn remove_metrics_config_config_doesnt_exist_fails() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "remove_metrics_config";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        let status_code = test_environment
            .make_async_call(|service, api_call_id| service.removeMetricsConfig(api_call_id, UUID1))
            .await
            .unwrap_err();

        assert_eq!(StatusCodes::METRICS_CONFIG_INVALID_CONFIG_UUID, status_code);
        assert_eq!(1, test_environment.get_num_remove_config_calls());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn remove_all_metrics_configs_success() {
        // Creating a separate directory for each unit test to avoid overlapping file
        // IO operations in the same directories which can cause test flakiness.
        const ROOT_PATH: &str = "remove_all_metrics_configs";
        let test_environment = TestEnvironment::new(ROOT_PATH.to_owned()).await;

        let serialized_config1 = create_serialized_config(UUID1);
        let serialized_config2 = create_serialized_config(UUID2);

        // Add configs
        test_environment
            .make_async_call(|service, api_call_id| {
                service.addMetricsConfig(api_call_id, &serialized_config1)
            })
            .await
            .unwrap();
        test_environment
            .make_async_call(|service, api_call_id| {
                service.addMetricsConfig(api_call_id, &serialized_config2)
            })
            .await
            .unwrap();

        // Remove all configs
        test_environment
            .make_async_call(|service, api_call_id| service.removeAllMetricsConfigs(api_call_id))
            .await
            .unwrap();

        assert_eq!(2, test_environment.get_num_add_metrics_config_calls());
        assert_eq!(1, test_environment.get_num_remove_all_config_calls());
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_active_metrics_config_uuids_success() {
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
        let status_receiver = config_list_listener.expect_call();

        test_environment
            .service
            .getActiveMetricsConfigUuids(&binder_config_list_listener)
            .await
            .unwrap();

        let (status_code, _status_message) = status_receiver.await.unwrap();
        assert_eq!(StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS, status_code);
        assert_eq!(1, test_environment.get_num_get_active_config_calls());
        assert!(config_uuids
            .iter()
            .all(|item| config_list_listener.get_metrics_config_list().contains(item)));
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_inactive_metrics_config_uuids_success() {
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
        let status_receiver = config_list_listener.expect_call();

        test_environment
            .service
            .getInactiveMetricsConfigUuids(&binder_config_list_listener)
            .await
            .unwrap();

        let (status_code, _status_message) = status_receiver.await.unwrap();
        assert_eq!(StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS, status_code);
        assert_eq!(1, test_environment.get_num_get_inactive_config_calls());
        assert!(config_uuids
            .iter()
            .all(|item| config_list_listener.get_metrics_config_list().contains(item)));
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn clear_report_ready_listener_success() {
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
            .make_async_call(|service, api_call_id| {
                service.registerReportReadyListener(api_call_id, &binder_report_ready_listener)
            })
            .await
            .unwrap();

        test_environment.service.unregisterReportReadyListener().await.unwrap();
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn set_report_ready_listener_success() {
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
            .make_async_call(|service, api_call_id| {
                service.registerReportReadyListener(api_call_id, &binder_report_ready_listener)
            })
            .await
            .unwrap();
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn set_report_ready_listener_receives_ready_calls() {
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
            .make_async_call(|service, api_call_id| {
                service.registerReportReadyListener(api_call_id, &binder_report_ready_listener)
            })
            .await
            .unwrap();

        let report_info = MetricsReportInfo { reportUuid: UUID1.to_string(), ..Default::default() };
        // Call onReady on report ready listener
        report_ready_listener.onReady(&report_info).unwrap();

        assert_eq!(1, report_ready_listener.get_num_on_ready_report_calls());
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
            .make_async_call(|service, api_call_id| {
                service.registerReportReadyListener(api_call_id, &report_listener)
            })
            .await
            .unwrap();

        let status_code = test_environment
            .make_async_call(|service, api_call_id| {
                service.registerReportReadyListener(api_call_id, &report_listener)
            })
            .await
            .unwrap_err();

        assert_eq!(StatusCodes::SET_REPORT_READY_LISTENER_FAILED, status_code);
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

        assert_eq!(*test_environment.status_listener.num_set_api_status_listener_calls.lock(), 1);
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

        test_environment
            .make_async_call(|service, api_call_id| {
                service.registerMiscStatusListener(api_call_id, &binder_listener)
            })
            .await
            .unwrap();

        assert_eq!(*test_environment.status_listener.num_set_misc_status_listener_calls.lock(), 1);
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

        assert_eq!(*test_environment.status_listener.num_set_api_status_listener_calls.lock(), 1);
        assert_eq!(*test_environment.status_listener.num_clear_api_status_listener_calls.lock(), 1);
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
        test_environment
            .make_async_call(|service, api_call_id| {
                service.registerMiscStatusListener(api_call_id, &binder_listener)
            })
            .await
            .unwrap();

        // Then unregister
        test_environment.service.unregisterMiscStatusListener().await.unwrap();

        assert_eq!(*test_environment.status_listener.num_set_misc_status_listener_calls.lock(), 1);
        assert_eq!(
            *test_environment.status_listener.num_clear_misc_status_listener_calls.lock(),
            1
        );
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_metrics_report_success() {
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
            .make_async_call(|service, api_call_id| {
                service.registerReportReadyListener(api_call_id, &binder_report_ready_listener)
            })
            .await
            .unwrap();

        // Setting Report Listener
        let report_listener = MockTelemetryReportListener::new();
        let binder_report_listener = BnTelemetryReportListener::new_binder(
            report_listener.clone(),
            BinderFeatures::default(),
        );
        let status_receiver = report_listener.expect_call();

        // Send report to metric report listener
        let mut report = MetricsReport::new();
        report.report_uuid = UUID1.to_string();

        // Call on_report_ready callback on metrics report listener
        test_environment.service.data.metrics_report_listener.on_report_ready(report);
        test_environment.service.getMetricsReport(UUID1, &binder_report_listener).await.unwrap();

        let (status_code, status_message) = status_receiver.await.unwrap();
        assert_eq!(
            (StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS, UUID1),
            (status_code, status_message.as_str())
        );
        let metrics_report =
            report_listener.get_metrics_report().expect("Metrics report should be set");
        assert_eq!(UUID1, metrics_report.report_uuid);
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_ready_reports_info_success() {
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
            .make_async_call(|service, api_call_id| {
                service.registerReportReadyListener(api_call_id, &binder_report_ready_listener)
            })
            .await
            .unwrap();

        // Setting Ready Report Ids Listener
        let ready_reports_info_listener = MockTelemetryReadyReportsInfoListener::new();
        let binder_ready_reports_info_listener = BnTelemetryReadyReportsInfoListener::new_binder(
            ready_reports_info_listener.clone(),
            BinderFeatures::default(),
        );
        let status_receiver = ready_reports_info_listener.expect_call();

        // Send report to metric report listener
        let report = MetricsReport { report_uuid: UUID1.to_owned(), ..Default::default() };
        test_environment.service.data.metrics_report_listener.on_report_ready(report);

        // Validate sent report readiness
        test_environment
            .service
            .getReadyReportsInfo(&binder_ready_reports_info_listener)
            .await
            .unwrap();

        let (status_code, status_message) = status_receiver.await.unwrap();
        assert_eq!(status_code, StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS);
        assert_eq!(status_message, "Number of ready reports: 1");
        let ready_reports_info = ready_reports_info_listener
            .get_ready_reports_info()
            .expect("Ready reports info should be set");
        assert_eq!(ready_reports_info.len(), 1);
        assert_eq!(ready_reports_info[0], UUID1);

        // Setting Report Listener
        let report_listener = MockTelemetryReportListener::new();
        let binder_report_listener = BnTelemetryReportListener::new_binder(
            report_listener.clone(),
            BinderFeatures::default(),
        );
        let status_receiver = report_listener.expect_call();

        // Extract the report
        test_environment.service.getMetricsReport(UUID1, &binder_report_listener).await.unwrap();

        let (status_code, _status_message) = status_receiver.await.unwrap();
        assert_eq!(status_code, StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS);

        // Validate no reports are ready
        let status_receiver = ready_reports_info_listener.expect_call();
        test_environment
            .service
            .getReadyReportsInfo(&binder_ready_reports_info_listener)
            .await
            .unwrap();

        let (status_code, status_message) = status_receiver.await.unwrap();
        assert_eq!(status_code, StatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS);
        assert_eq!(status_message, "Number of ready reports: 0");
        assert!(ready_reports_info_listener
            .get_ready_reports_info()
            .expect("Ready reports info should be set")
            .is_empty());
    }
}
