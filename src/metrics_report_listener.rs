// Copyright 2023 Google LLC. All Rights Reserved.

use android_sdv_telemetry::aidl::android::sdv::telemetry::ITelemetryReportReadyListener::ITelemetryReportReadyListener;
use sdvtelemetry_proto_rust::metrics_configuration::MetricsReport;

use android_sdv_telemetry::binder::Strong;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use thiserror::Error;

pub struct MetricsReportListener {
    /// Hashmap to store the MetricsReport until the client asks for it.
    metrics_report_store: Mutex<HashMap<String, MetricsReport>>,
    /// Report Ready listener to inform the client when a report is ready
    report_ready_listener: Mutex<Option<Strong<dyn ITelemetryReportReadyListener>>>,
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum MetricsReportListenerError {
    #[error("Report Ready Listener already exists")]
    ReportReadyListenerExists,
    #[error("Report Ready Listener not set")]
    ReportReadyListenerNotSet,
}

impl MetricsReportListener {
    pub fn new() -> Self {
        MetricsReportListener {
            metrics_report_store: Mutex::new(HashMap::new()),
            report_ready_listener: Mutex::new(None),
        }
    }

    pub fn on_report_ready(self: &Arc<Self>, metrics_report: MetricsReport) {
        let uuid = metrics_report.uuid.clone();
        self.metrics_report_store.lock().unwrap().insert(uuid.clone(), metrics_report);
        let listener = self.report_ready_listener.lock().unwrap();
        if let Some(listener_ref) = listener.as_ref() {
            let _ = listener_ref.deref().onReady(&uuid);
        }
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
