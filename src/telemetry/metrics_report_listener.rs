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
use std::collections::{HashMap, VecDeque};
use std::ops::Deref;
use std::sync::Arc;

use sdv_telemetry_binder_utils::{BinderAdapter, BinderAdapterImpl};

const MAX_METRICS_REPORT_STORE_SIZE: usize = 1024;

pub(super) struct MetricsReportListener {
    /// Mapping of Metrics Report UUID to Metrics Config UUID
    report_to_config_map: Mutex<HashMap<String, String>>,
    /// Hashmap to store the MetricsReport until the client asks for it.
    metrics_report_store: Mutex<HashMap<String, MetricsReport>>,
    /// Queue to keep track of the order of report insertion for FIFO eviction.
    insertion_order: Mutex<VecDeque<String>>,
    /// Report Ready listener to inform the client when a report is ready
    report_ready_listener:
        Mutex<Option<(Strong<dyn ITelemetryReportReadyListener>, binder::DeathRecipient)>>,
    /// Adapter to perform operations on binders.
    listener_binder_adapter: Box<dyn BinderAdapter<dyn ITelemetryReportReadyListener>>,
}

#[derive(thiserror::Error, Debug)]
pub(super) enum MetricsReportListenerError {
    #[error("Binder Error: {code}")]
    BinderError {
        #[from]
        code: binder::StatusCode,
    },
    #[error("Report Ready Listener already exists")]
    ReportReadyListenerExists,
    #[error("Metrics Report not found")]
    MetricsReportNotFound,
}

impl MetricsReportListener {
    pub(super) fn new() -> Self {
        Self {
            report_to_config_map: Mutex::new(HashMap::new()),
            metrics_report_store: Mutex::new(HashMap::new()),
            insertion_order: Mutex::new(VecDeque::new()),
            report_ready_listener: Mutex::new(None),
            listener_binder_adapter: Box::new(BinderAdapterImpl {}),
        }
    }

    pub(super) fn on_report_ready(self: &Arc<Self>, metrics_report: MetricsReport) {
        let mut store = self.metrics_report_store.lock();
        let mut mapping = self.report_to_config_map.lock();
        let mut insertion_order = self.insertion_order.lock();

        if store.len() == MAX_METRICS_REPORT_STORE_SIZE {
            // Remove the oldest added report if the number of stored reports reached the limit.
            // Since insertion_order is not updated when a report is extracted, we have to skip
            // elements from queue beginning until we find uuid which still exists.
            while let Some(oldest_report_uuid) = insertion_order.pop_front() {
                if store.remove(&oldest_report_uuid).is_some() {
                    mapping
                        .remove(&oldest_report_uuid)
                        .expect("Mapping should contain key if store contained key");
                    break;
                }
            }
        }

        let config_uuid = metrics_report.metrics_config_uuid.clone();
        let report_uuid = metrics_report.report_uuid.clone();

        // Insert the report and panic if report with the same key already exists.
        store
            .try_insert_value(report_uuid.clone(), metrics_report)
            .expect("Metrics Report UUIDs should be unique");
        mapping
            .try_insert_value(report_uuid.clone(), config_uuid.clone())
            .expect("Metrics Report UUIDs should be unique");
        insertion_order.push_back(report_uuid.clone());

        if let Some((listener, _)) = self.report_ready_listener.lock().as_ref() {
            let report_info =
                MetricsReportInfo { configUuid: config_uuid, reportUuid: report_uuid };
            if let Err(e) = listener.deref().onReady(&report_info) {
                warn!("Error when trying to notify client about a new report: {e:?}");
            }
        }
    }

    pub(super) fn get_metrics_report(
        &self,
        report_uuid: &str,
    ) -> Result<MetricsReport, MetricsReportListenerError> {
        // We deliberately do not update insertion_order in this function in order to keep constant
        // time complexity and not overcomplicate the implementation with LRUCache-like data
        // structures. We instead use the fact all UUIDs are unique, and do insertion_order cleanup
        // only when the number of objects stored reaches the limit.
        let Some(metrics_report) = self.metrics_report_store.lock().remove(report_uuid) else {
            return Err(MetricsReportListenerError::MetricsReportNotFound);
        };
        let Some(_) = self.report_to_config_map.lock().remove(report_uuid) else {
            return Err(MetricsReportListenerError::MetricsReportNotFound);
        };
        Ok(metrics_report)
    }

    pub(super) fn get_ready_reports_info(&self) -> Vec<MetricsReportInfo> {
        self.report_to_config_map
            .lock()
            .clone()
            .into_iter()
            .map(|(report_uuid, config_uuid)| MetricsReportInfo {
                configUuid: config_uuid,
                reportUuid: report_uuid,
            })
            .collect()
    }

    pub(super) fn set_report_ready_listener(
        self: &Arc<Self>,
        listener: Strong<dyn ITelemetryReportReadyListener>,
    ) -> Result<(), MetricsReportListenerError> {
        let mut guard = self.report_ready_listener.lock();
        if guard.is_none() {
            let self_reference = Arc::downgrade(self);
            let death_recipient = self.listener_binder_adapter.link_to_death(
                &listener,
                Box::new(move || {
                    if let Some(self_reference) = self_reference.upgrade() {
                        self_reference.clear_report_ready_listener();
                    }
                }),
            )?;
            *guard = Some((listener, death_recipient));
            Ok(())
        } else {
            Err(MetricsReportListenerError::ReportReadyListenerExists)
        }
    }

    pub(super) fn clear_report_ready_listener(&self) {
        *self.report_ready_listener.lock() = None;
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    use google_sdv_telemetry::aidl::google::sdv::telemetry::ITelemetryReportReadyListener::BnTelemetryReportReadyListener;
    use parking_lot::Mutex;
    use sdv_telemetry_proto_rust::metrics_configuration::MetricsReport;
    use uuid::Uuid;

    #[derive(Clone)]
    struct MockTelemetryReportReadyListener {
        last_call: Arc<Mutex<Option<String>>>,
    }

    impl binder::Interface for MockTelemetryReportReadyListener {}

    impl MockTelemetryReportReadyListener {
        fn new() -> Self {
            Self { last_call: Arc::new(Mutex::new(None)) }
        }

        pub fn get_last_call(&self) -> String {
            self.last_call.lock().take().unwrap()
        }
    }

    impl ITelemetryReportReadyListener for MockTelemetryReportReadyListener {
        fn onReady(&self, report_info: &MetricsReportInfo) -> binder::Result<()> {
            self.last_call.lock().replace(report_info.reportUuid.clone());
            Ok(())
        }
    }

    #[derive(Clone)]
    struct MockBinderAdapter {
        callback: Arc<Mutex<Option<Box<dyn Fn() + Send + Sync>>>>,
    }

    impl<T: binder::FromIBinder + ?Sized> BinderAdapter<T> for MockBinderAdapter {
        fn link_to_death(
            &self,
            _binder: &binder::Strong<T>,
            callback: Box<dyn Fn() + Send + Sync>,
        ) -> Result<binder::DeathRecipient, binder::StatusCode> {
            self.callback.lock().replace(callback);
            Ok(binder::DeathRecipient::new(|| {}))
        }
    }

    impl MockBinderAdapter {
        fn new() -> Self {
            Self { callback: Arc::new(Mutex::new(None)) }
        }
    }

    fn create_metrics_report_listener(
        binder_adapter: Option<MockBinderAdapter>,
    ) -> MetricsReportListener {
        let mut report_listener = MetricsReportListener::new();
        report_listener.listener_binder_adapter =
            Box::new(binder_adapter.unwrap_or_else(MockBinderAdapter::new));

        report_listener
    }

    pub(crate) fn create_default_metrics_report_listener() -> MetricsReportListener {
        create_metrics_report_listener(None)
    }

    fn create_metrics_report() -> MetricsReport {
        MetricsReport {
            report_uuid: Uuid::new_v4().to_string(),
            metrics_config_uuid: Uuid::new_v4().to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn on_report_ready_notifies_listener() {
        let report_listener = Arc::new(create_default_metrics_report_listener());
        let mock_listener = MockTelemetryReportReadyListener::new();
        let binder_listener = BnTelemetryReportReadyListener::new_binder(
            mock_listener.clone(),
            binder::BinderFeatures::default(),
        );
        report_listener.set_report_ready_listener(binder_listener).unwrap();
        let metrics_report = create_metrics_report();

        report_listener.on_report_ready(metrics_report.clone());

        assert_eq!(mock_listener.get_last_call(), metrics_report.report_uuid);
    }

    #[test]
    #[should_panic]
    fn on_report_ready_same_report_panics() {
        let report_listener = Arc::new(create_default_metrics_report_listener());
        let report = create_metrics_report();

        report_listener.on_report_ready(report.clone());
        report_listener.on_report_ready(report.clone());
    }

    #[test]
    fn on_report_ready_overflow_fifo() {
        let report_listener = Arc::new(create_default_metrics_report_listener());
        let mut report_uuids = Vec::new();

        for i in 0..MAX_METRICS_REPORT_STORE_SIZE + 1 {
            let metrics_report = MetricsReport {
                report_uuid: format!("report-{i}"),
                metrics_config_uuid: Uuid::new_v4().to_string(),
                ..Default::default()
            };
            report_uuids.push(metrics_report.report_uuid.clone());
            report_listener.on_report_ready(metrics_report);
        }

        assert_eq!(
            report_listener.metrics_report_store.lock().len(),
            MAX_METRICS_REPORT_STORE_SIZE
        );

        // Check that the FIFO report was removed.
        let fifo_report_uuid = report_uuids.first().unwrap();
        let err = report_listener.get_metrics_report(fifo_report_uuid).unwrap_err();
        assert!(matches!(err, MetricsReportListenerError::MetricsReportNotFound));

        // Check that the last added report still exists.
        let last_report_uuid = report_uuids.last().unwrap();
        let report = report_listener.get_metrics_report(last_report_uuid).unwrap();
        assert_eq!(report.report_uuid, *last_report_uuid);
    }

    #[test]
    fn on_report_ready_limits_size_after_report_extraction() {
        let report_listener = Arc::new(create_default_metrics_report_listener());
        let mut report_uuids = Vec::new();

        for i in 0..MAX_METRICS_REPORT_STORE_SIZE {
            let metrics_report = MetricsReport {
                report_uuid: format!("report-{i}"),
                metrics_config_uuid: Uuid::new_v4().to_string(),
                ..Default::default()
            };
            report_uuids.push(metrics_report.report_uuid.clone());
            report_listener.on_report_ready(metrics_report);
        }

        assert_eq!(
            report_listener.metrics_report_store.lock().len(),
            MAX_METRICS_REPORT_STORE_SIZE
        );

        // Remove the first-in report
        let uuid = &report_uuids[0];
        let _ = report_listener.get_metrics_report(uuid).unwrap();

        // Add two new reports with the last one overflowing the limit
        for i in 0..2 {
            let metrics_report = MetricsReport {
                report_uuid: format!("report-{}", i + MAX_METRICS_REPORT_STORE_SIZE),
                metrics_config_uuid: Uuid::new_v4().to_string(),
                ..Default::default()
            };
            report_uuids.push(metrics_report.report_uuid.clone());
            report_listener.on_report_ready(metrics_report);
        }

        // Check that the second-in report was removed.
        let uuid = &report_uuids[1];
        let err = report_listener.get_metrics_report(uuid).unwrap_err();
        assert!(matches!(err, MetricsReportListenerError::MetricsReportNotFound));
        assert_eq!(
            report_listener.metrics_report_store.lock().len(),
            MAX_METRICS_REPORT_STORE_SIZE
        );
    }

    #[test]
    fn get_metrics_report_success() {
        let report_listener = Arc::new(create_default_metrics_report_listener());
        let metrics_report = create_metrics_report();
        report_listener.on_report_ready(metrics_report.clone());

        let retrieved_report =
            report_listener.get_metrics_report(&metrics_report.report_uuid).unwrap();

        assert_eq!(retrieved_report, metrics_report);
    }

    #[test]
    fn get_metrics_report_not_found() {
        let report_listener = Arc::new(create_default_metrics_report_listener());
        let metrics_report = create_metrics_report();

        let err = report_listener.get_metrics_report(&metrics_report.report_uuid).unwrap_err();

        assert!(
            matches!(err, MetricsReportListenerError::MetricsReportNotFound),
            "Unexpected error: {err:?}"
        );
    }

    #[test]
    fn get_ready_reports_info_success() {
        let report_listener = Arc::new(create_default_metrics_report_listener());
        let metrics_report = create_metrics_report();
        report_listener.on_report_ready(metrics_report.clone());

        let ready_reports_info = report_listener.get_ready_reports_info();

        assert_eq!(ready_reports_info.len(), 1);
        assert_eq!(ready_reports_info[0].reportUuid, metrics_report.report_uuid);
        assert_eq!(ready_reports_info[0].configUuid, metrics_report.metrics_config_uuid);
    }

    #[test]
    fn set_report_ready_listener_twice_fails() {
        let report_listener = Arc::new(create_default_metrics_report_listener());
        let binder_listener = BnTelemetryReportReadyListener::new_binder(
            MockTelemetryReportReadyListener::new(),
            binder::BinderFeatures::default(),
        );
        report_listener.set_report_ready_listener(binder_listener.clone()).unwrap();

        let err = report_listener.set_report_ready_listener(binder_listener).unwrap_err();

        assert!(
            matches!(err, MetricsReportListenerError::ReportReadyListenerExists),
            "Unexpected error: {err:?}"
        );
    }

    #[test]
    fn set_report_ready_listener_cleared_on_client_death() {
        let mock_adapter = MockBinderAdapter::new();
        let report_listener = Arc::new(create_metrics_report_listener(Some(mock_adapter.clone())));

        let mock_listener = MockTelemetryReportReadyListener::new();
        let binder_listener = BnTelemetryReportReadyListener::new_binder(
            mock_listener,
            binder::BinderFeatures::default(),
        );

        report_listener.set_report_ready_listener(binder_listener).unwrap();

        mock_adapter.callback.lock().as_ref().unwrap()();

        assert!(report_listener.report_ready_listener.lock().is_none());
    }

    #[test]
    fn clear_report_ready_listener_success() {
        let report_listener = Arc::new(create_default_metrics_report_listener());
        let binder_listener = BnTelemetryReportReadyListener::new_binder(
            MockTelemetryReportReadyListener::new(),
            binder::BinderFeatures::default(),
        );
        report_listener.set_report_ready_listener(binder_listener).unwrap();

        report_listener.clear_report_ready_listener();

        assert!(report_listener.report_ready_listener.lock().is_none());
    }
}
