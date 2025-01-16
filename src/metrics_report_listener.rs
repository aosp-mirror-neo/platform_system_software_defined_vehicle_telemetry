// Copyright 2023 Google LLC

use google_sdv_telemetry::aidl::google::sdv::telemetry::{
    ITelemetryReportReadyListener::ITelemetryReportReadyListener,
    MetricsReportInfo::MetricsReportInfo,
};
use google_sdv_telemetry::binder::Strong;
use log::warn;
use parking_lot::Mutex;
use sdv_telemetry_proto_rust::metrics_configuration::MetricsReport;
use sdv_telemetry_utils::collection_extension::HashMapExtension;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use thiserror::Error;

pub(crate) struct MetricsReportListener {
    /// Hashmap to store the MetricsReport until the client asks for it.
    metrics_report_store: Mutex<HashMap<MetricsReportInfo, MetricsReport>>,
    /// Report Ready listener to inform the client when a report is ready
    report_ready_listener: Mutex<Option<Strong<dyn ITelemetryReportReadyListener>>>,
}

#[derive(Error, Debug)]
pub(crate) enum MetricsReportListenerError {
    #[error("Report Ready Listener already exists")]
    ReportReadyListenerExists,
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
        let report_info = MetricsReportInfo {
            metricsConfigUuid: metrics_report.metrics_config_uuid.clone(),
            reportConfigName: metrics_report.report_config_name.clone(),
            reportNumber: metrics_report.report_number,
        };

        // Insert the report and panic if report with the same key already exists.
        self.metrics_report_store
            .lock()
            .try_insert_value(report_info.clone(), metrics_report)
            .unwrap();

        let listener = self.report_ready_listener.lock();
        if let Some(listener_ref) = listener.as_ref() {
            if let Err(e) = listener_ref.deref().onReady(&report_info) {
                warn!("Error when trying to notify client about a new report: {:?}", e);
            }
        }
    }

    pub(crate) fn get_metrics_report(
        &self,
        report_info: &MetricsReportInfo,
    ) -> Result<MetricsReport, MetricsReportListenerError> {
        let report = {
            let mut report_container = self.metrics_report_store.lock();
            report_container.remove(report_info)
        };
        let Some(metrics_report) = report else {
            return Err(MetricsReportListenerError::MetricsReportNotFound);
        };

        Ok(metrics_report)
    }

    pub(crate) fn get_ready_reports_info(&self) -> Vec<MetricsReportInfo> {
        self.metrics_report_store.lock().keys().cloned().collect()
    }

    pub(crate) fn set_report_ready_listener(
        &self,
        listener: &Strong<dyn ITelemetryReportReadyListener>,
    ) -> Result<(), MetricsReportListenerError> {
        let mut guard = self.report_ready_listener.lock();
        if guard.is_none() {
            *guard = Some(listener.clone());
            Ok(())
        } else {
            Err(MetricsReportListenerError::ReportReadyListenerExists)
        }
    }

    pub(crate) fn clear_report_ready_listener(&self) -> Result<(), MetricsReportListenerError> {
        let mut listener = self.report_ready_listener.lock();
        *listener = None;
        Ok(())
    }
}
