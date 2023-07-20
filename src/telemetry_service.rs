// Copyright 2023 Google LLC. All Rights Reserved.

//! This module implements the ITelemetryService AIDL interface
#![allow(non_snake_case)]

mod collection_extension;
mod expression_evaluation;
mod file_io_utils;
mod metrics_config_orchestrator;

use android_sdv_telemetry::aidl::android::sdv::telemetry::{
    ITelemetryReportListener::ITelemetryReportListener,
    ITelemetryReportReadyListener::ITelemetryReportReadyListener,
    ITelemetryService::ITelemetryServiceAsyncServer,
};

use android_sdv_telemetry::binder::{Interface, Result as BinderResult, Status, Strong};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use log::info;
use sdvtelemetry_proto_rust::metrics_configuration::MetricsConfig;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::{self};
use std::marker::{Send, Sync};
use std::path::{Path, PathBuf};

use metrics_config_orchestrator::MetricsConfigOrchestratorImpl;

pub use metrics_config_orchestrator::MetricsConfigOrchestrator;

static DATA_DIR: &str = "telemetry";

/// Convert a Result<T, E> to BinderResult<T> to allow it to be returned from a binder RPC,
/// preserving the content as far as possible.
/// Also log the error if there is one.
pub fn to_binder_result<T, E: Debug>(result: Result<T, E>) -> BinderResult<T> {
    result.map_err(|e| {
        let message = format!("{:?}", e);
        info!("Returning binder error: {}", &message);
        Status::new_service_specific_error_str(-1, Some(message))
    })
}

/// This struct is defined to implement ITelemetryService AIDL interface.
pub struct SdvTelemetryService {
    data_dir: String,
    /// Instance of Metrics Configuration Orchestrator.
    pub metrics_config_orchestrator: Box<dyn MetricsConfigOrchestrator + Send + Sync>,
    /// Hashmap to store the metricsConfigs passed by the client.
    pub metrics_configs_store: Arc<Mutex<HashMap<String, MetricsConfig>>>,
}

impl Interface for SdvTelemetryService {}

impl SdvTelemetryService {
    #[allow(dead_code)]
    /// Instantiates an instance of SdvTelemetryService
    pub async fn new(root_path: &str) -> Self {
        let mut data_dir = DATA_DIR.to_string();
        if !root_path.is_empty() {
            data_dir = format!("{}/{}", root_path, DATA_DIR);
        }

        let dir_path = Path::new(&data_dir);

        let configs_store = Arc::new(Mutex::new(HashMap::new()));

        let mut configs_loaded = 0;

        if dir_path.is_dir() {
            // Read the contents of each file in the directory into a byte array
            for entry_result in fs::read_dir(dir_path).unwrap() {
                let entry = entry_result.unwrap();
                let path = entry.path();
                if path.is_file() {
                    let bytes = file_io_utils::read_from_file(&path).await.unwrap();
                    // bytes now contains the bytes of the file

                    let config: MetricsConfig =
                        protobuf::Message::parse_from_bytes(&bytes).unwrap();
                    let uuid = config.uuid.clone();
                    {
                        let mut db = configs_store.lock().unwrap();
                        db.insert(uuid, config);
                        configs_loaded += 1;
                    }
                }
            }
        } else {
            file_io_utils::create_dir(&dir_path.to_path_buf()).await.unwrap();
        }

        info!("Telemetry service initialized with {:} configs loaded from memory", configs_loaded);
        SdvTelemetryService {
            data_dir,
            metrics_config_orchestrator: Box::new(MetricsConfigOrchestratorImpl::new()),
            metrics_configs_store: configs_store,
        }
    }
}

#[async_trait]
impl ITelemetryServiceAsyncServer for SdvTelemetryService {
    async fn addMetricsConfig(&self, metrics_config: &[u8]) -> BinderResult<()> {
        let config: MetricsConfig = protobuf::Message::parse_from_bytes(metrics_config).unwrap();
        info!("addMetricsConfig: {:?}\n", config);

        let res = self.metrics_config_orchestrator.initialize_config(&config);

        // Validates and initializes metrics config
        if res.is_err() {
            info!("Invalid Metric Config");
            return to_binder_result(res);
        }

        let bytes = metrics_config.to_vec();
        let path = PathBuf::from(self.data_dir.clone() + "/" + &config.uuid.clone());

        //TODO (b/290638883): Handle errors with file IO
        file_io_utils::write_to_file(&path, &bytes).await.unwrap();
        let mut db = self.metrics_configs_store.lock().unwrap();
        db.insert(config.uuid.clone(), config);

        Ok(())
    }

    async fn removeMetricsConfig(&self, metrics_config_uuid: &str) -> BinderResult<()> {
        info!("removeMetricsConfig: {}", metrics_config_uuid);
        let res = self.metrics_config_orchestrator.finish_config(metrics_config_uuid);
        if res.is_err() {
            info!("Invalid Metric Config UUID");
            return to_binder_result(res);
        }

        let uuid = String::from(metrics_config_uuid);
        let delete_file = {
            let mut db = self.metrics_configs_store.lock().unwrap();
            db.remove(&uuid).is_some()
        };

        if delete_file {
            let path = PathBuf::from(self.data_dir.clone() + "/" + &uuid);

            //TODO: Handle errors with file IO b/290638883
            file_io_utils::remove_file(&path).await.unwrap();
        } else {
            info!("Config not found {:}", uuid);
        }

        Ok(())
    }

    #[allow(unused_variables)]
    async fn removeAllMetricsConfigs(&self) -> BinderResult<()> {
        info!("removeAllMetricsConfigs");

        for entry in fs::read_dir(self.data_dir.clone()).unwrap() {
            file_io_utils::remove_file(&entry.unwrap().path()).await.unwrap();
        }
        let mut db = self.metrics_configs_store.lock().unwrap();
        db.clear();
        Ok(())
    }

    #[allow(unused_variables)]
    async fn getFinishedReport(
        &self,
        metrics_config_uuid: &str,
        listener: &Strong<(dyn ITelemetryReportListener)>,
    ) -> BinderResult<()> {
        info!("getFinishedReport: {}", metrics_config_uuid);
        Ok(())
    }

    async fn getAllFinishedReports(
        &self,
        _listener: &Strong<dyn ITelemetryReportListener>,
    ) -> BinderResult<()> {
        info!("getAllFinishedReports");
        Ok(())
    }

    async fn setReportReadyListener(
        &self,
        listener: &Strong<dyn ITelemetryReportReadyListener>,
    ) -> BinderResult<()> {
        info!("setReportReadyListener");
        let _ = listener.onReady("Received CB from Telemetry Binder service");
        Ok(())
    }

    async fn clearReportReadyListener(&self) -> BinderResult<()> {
        info!("clearReportReadyListener");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    extern crate protobuf;
    use super::*;
    use binder::ParcelFileDescriptor;
    use protobuf::Message;

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
        // behind a reference
        let service: Box<dyn ITelemetryServiceAsyncServer> =
            Box::new(SdvTelemetryService::new("").await);
        println!("Running add metrics configs");

        let mut config = MetricsConfig::new();
        config.uuid = "01bf0754-df95-46bb-bb35-522b6af66600".to_string();
        config.version = 2;
        config.script = "Sample Script".to_string();

        info!("Serializing metric config");
        let serialized_config: &[u8] = &config.write_to_bytes().unwrap();

        let result = service.addMetricsConfig(serialized_config).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn remove_metrics_config() {
        const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";
        // behind a reference
        let service: Box<dyn ITelemetryServiceAsyncServer> =
            Box::new(SdvTelemetryService::new("").await);

        let mut config = MetricsConfig::new();
        config.uuid = UUID.to_owned();
        service.addMetricsConfig(&config.write_to_bytes().unwrap()).await.unwrap();

        let result = service.removeMetricsConfig(UUID).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn remove_all_metrics_configs() {
        // behind a reference
        let service: Box<dyn ITelemetryServiceAsyncServer> =
            Box::new(SdvTelemetryService::new("").await);

        let result = service.removeAllMetricsConfigs().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn clear_report_ready_listener() {
        // behind a reference
        let service: Box<dyn ITelemetryServiceAsyncServer> =
            Box::new(SdvTelemetryService::new("").await);

        let result = service.clearReportReadyListener().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn set_report_ready_listener() {
        // behind a reference
        let service: Box<dyn ITelemetryServiceAsyncServer> =
            Box::new(SdvTelemetryService::new("").await);

        let listener: Strong<dyn ITelemetryReportReadyListener> =
            Strong::new(Box::new(TelemetryReportReadyListener));

        let result = service.setReportReadyListener(&listener).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn get_finished_report() {
        // behind a reference
        let service: Box<dyn ITelemetryServiceAsyncServer> =
            Box::new(SdvTelemetryService::new("").await);

        let listener: Strong<dyn ITelemetryReportListener> =
            Strong::new(Box::new(TelemetryReportListener));

        let result = service.getFinishedReport("TestConfig", &listener).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn get_all_finished_report() {
        // behind a reference
        let service: Box<dyn ITelemetryServiceAsyncServer> =
            Box::new(SdvTelemetryService::new("").await);

        let listener: Strong<dyn ITelemetryReportListener> =
            Strong::new(Box::new(TelemetryReportListener));

        let result = service.getAllFinishedReports(&listener).await;
        assert!(result.is_ok());
    }
}
