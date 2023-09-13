// Copyright 2023 Google LLC. All Rights Reserved.

use android_sdv_telemetry::aidl::android::sdv::telemetry::{
    ITelemetryReportListener::ITelemetryReportListener,
    ITelemetryReportReadyListener::ITelemetryReportReadyListener,
};
use sdvtelemetry_proto_rust::metrics_configuration::MetricsReport;

use android_sdv_telemetry::binder::Strong;
use protobuf::Message;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use thiserror::Error;

pub struct MetricsReportListener {
    /// Hashmap to store the MetricsReport until the client asks for it.
    metrics_report_store: Mutex<HashMap<(String, i32), MetricsReport>>,
    /// Report Ready listener to inform the client when a report is ready
    report_listener: Mutex<Option<Strong<dyn ITelemetryReportListener>>>,
    /// Report Ready listener to inform the client when a report is ready
    report_ready_listener: Mutex<Option<Strong<dyn ITelemetryReportReadyListener>>>,
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum MetricsReportListenerError {
    #[error("Report Listener already exists")]
    ReportListenerExists,
    #[error("Report Listener not set")]
    ReportListenerNotSet,
    #[error("Report Ready Listener already exists")]
    ReportReadyListenerExists,
    #[error("Report Ready Listener not set")]
    ReportReadyListenerNotSet,
    #[error("Metrics Report not found")]
    MetricsReportNotFound,
}

impl MetricsReportListener {
    pub fn new() -> Self {
        MetricsReportListener {
            metrics_report_store: Mutex::new(HashMap::new()),
            report_ready_listener: Mutex::new(None),
            report_listener: Mutex::new(None),
        }
    }

    pub fn on_report_ready(self: &Arc<Self>, metrics_report: MetricsReport) {
        let uuid = metrics_report.metrics_config_uuid.clone();
        let report_number = metrics_report.report_number as i32;

        self.metrics_report_store
            .lock()
            .unwrap()
            .insert((uuid.clone(), report_number), metrics_report);
        let listener = self.report_ready_listener.lock().unwrap();
        if let Some(listener_ref) = listener.as_ref() {
            let _ = listener_ref.deref().onReady(&uuid, report_number);
        }
    }

    pub fn get_finished_report(
        &self,
        metrics_report_uuid: &str,
        report_number: i32,
    ) -> Result<(), MetricsReportListenerError> {
        let report = {
            let mut report_container = self.metrics_report_store.lock().unwrap();
            report_container.remove(&(metrics_report_uuid.to_string(), report_number))
        };
        let Some(metrics_report) = report else {
            return Err(MetricsReportListenerError::MetricsReportNotFound);
        };

        let serialized_report: &[u8] = &metrics_report.write_to_bytes().unwrap();
        let listener = self.report_listener.lock().unwrap();
        if let Some(listener_ref) = listener.as_ref() {
            let _ = listener_ref.deref().onResult(serialized_report);
        } else {
            return Err(MetricsReportListenerError::ReportListenerNotSet);
        }

        Ok(())
    }

    pub fn set_report_listener(
        &self,
        listener: &Strong<dyn ITelemetryReportListener>,
    ) -> Result<(), MetricsReportListenerError> {
        let mut guard = self.report_listener.lock().unwrap();
        if guard.is_none() {
            *guard = Some(listener.clone());
            Ok(())
        } else {
            Err(MetricsReportListenerError::ReportListenerExists)
        }
    }

    pub fn clear_report_listener(&self) -> Result<(), MetricsReportListenerError> {
        let mut listener = self.report_listener.lock().unwrap();
        if listener.is_none() {
            return Err(MetricsReportListenerError::ReportListenerNotSet);
        }
        *listener = None;
        Ok(())
    }

    pub fn set_report_ready_listener(
        &self,
        listener: &Strong<dyn ITelemetryReportReadyListener>,
    ) -> Result<(), MetricsReportListenerError> {
        let mut guard = self.report_ready_listener.lock().unwrap();
        if guard.is_none() {
            *guard = Some(listener.clone());
            Ok(())
        } else {
            Err(MetricsReportListenerError::ReportReadyListenerExists)
        }
    }

    pub fn clear_report_ready_listener(&self) -> Result<(), MetricsReportListenerError> {
        let mut listener = self.report_ready_listener.lock().unwrap();
        if listener.is_none() {
            return Err(MetricsReportListenerError::ReportReadyListenerNotSet);
        }
        *listener = None;
        Ok(())
    }
}
