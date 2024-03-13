// Copyright 2023 Google LLC. All Rights Reserved.

use android_sdv_telemetry::aidl::android::sdv::telemetry::ITelemetryReportReadyListener::ITelemetryReportReadyListener;
use android_sdv_telemetry::binder::Strong;
use sdv_telemetry_utils::collection_extension::HashMapExtension;
use sdvtelemetry_proto_rust::metrics_configuration::MetricsReport;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use thiserror::Error;

pub(crate) struct MetricsReportListener {
    /// Hashmap to store the MetricsReport until the client asks for it.
    metrics_report_store: Mutex<HashMap<(String, String, i32), MetricsReport>>,
    /// Report Ready listener to inform the client when a report is ready
    report_ready_listener: Mutex<Option<Strong<dyn ITelemetryReportReadyListener>>>,
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub(crate) enum MetricsReportListenerError {
    #[error("Report Ready Listener already exists")]
    ReportReadyListenerExists,
    #[error("Report Ready Listener not set")]
    ReportReadyListenerNotSet,
    #[error("Metrics Report not found")]
    MetricsReportNotFound,
}

impl MetricsReportListener {
    pub(crate) fn new() -> Self {
        Self {
            metrics_report_store: Mutex::new(HashMap::new()),
            report_ready_listener: Mutex::new(None),
        }
    }

    pub(crate) fn on_report_ready(self: &Arc<Self>, metrics_report: MetricsReport) {
        let metrics_config_uuid = metrics_report.metrics_config_uuid.clone();
        let report_config_name = metrics_report.report_config_name.clone();
        let report_number = metrics_report.report_number;

        let report_key = (metrics_config_uuid.clone(), report_config_name.clone(), report_number);
        // Insert the report and panic if report with the same key already exists.
        self.metrics_report_store
            .lock()
            .unwrap()
            .try_insert_value(report_key, metrics_report)
            .unwrap();

        let listener = self.report_ready_listener.lock().unwrap();
        if let Some(listener_ref) = listener.as_ref() {
            let _ = listener_ref.deref().onReady(
                &metrics_config_uuid,
                &report_config_name,
                report_number,
            );
        }
    }

    pub(crate) fn get_metrics_report(
        &self,
        metrics_config_uuid: &str,
        report_config_name: &str,
        report_number: i32,
    ) -> Result<MetricsReport, MetricsReportListenerError> {
        let report = {
            let mut report_container = self.metrics_report_store.lock().unwrap();
            report_container.remove(&(
                metrics_config_uuid.to_owned(),
                report_config_name.to_owned(),
                report_number,
            ))
        };
        let Some(metrics_report) = report else {
            return Err(MetricsReportListenerError::MetricsReportNotFound);
        };

        Ok(metrics_report)
    }

    pub(crate) fn set_report_ready_listener(
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

    pub(crate) fn clear_report_ready_listener(&self) -> Result<(), MetricsReportListenerError> {
        let mut listener = self.report_ready_listener.lock().unwrap();
        if listener.is_none() {
            return Err(MetricsReportListenerError::ReportReadyListenerNotSet);
        }
        *listener = None;
        Ok(())
    }
}
