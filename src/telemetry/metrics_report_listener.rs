// Copyright 2023 Google LLC

use google_sdv_telemetry::aidl::google::sdv::telemetry::{
    ITelemetryReportReadyListener::ITelemetryReportReadyListener,
    MetricsReportInfo::MetricsReportInfo,
};
use google_sdv_telemetry::binder::Strong;
use log::warn;
use parking_lot::Mutex;
use sdv_telemetry_proto_rust::metrics_configuration::MetricsReport;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use sdv_telemetry_binder_utils::{BinderAdapter, BinderAdapterImpl};

pub(super) struct MetricsReportListener {
    /// Hashmap to store the MetricsReport until the client asks for it.
    metrics_report_store: Mutex<HashMap<MetricsReportInfo, MetricsReport>>,
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
            metrics_report_store: Mutex::new(HashMap::new()),
            report_ready_listener: Mutex::new(None),
            listener_binder_adapter: Box::new(BinderAdapterImpl {}),
        }
    }

    pub(super) fn on_report_ready(self: &Arc<Self>, metrics_report: MetricsReport) {
        let report_info = MetricsReportInfo {
            metricsConfigUuid: metrics_report.metrics_config_uuid.clone(),
            reportConfigName: metrics_report.report_config_name.clone(),
            reportNumber: metrics_report.report_number,
        };

        // TODO(b/384783248): use surrogate key and don't overwrite reports.
        self.metrics_report_store.lock().insert(report_info.clone(), metrics_report);

        if let Some((listener, _)) = self.report_ready_listener.lock().as_ref() {
            if let Err(e) = listener.deref().onReady(&report_info) {
                warn!("Error when trying to notify client about a new report: {:?}", e);
            }
        }
    }

    pub(super) fn get_metrics_report(
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

    pub(super) fn get_ready_reports_info(&self) -> Vec<MetricsReportInfo> {
        self.metrics_report_store.lock().keys().cloned().collect()
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
    use protobuf::{reflect::ReflectEqMode, MessageFull};
    use sdv_telemetry_proto_rust::metrics_configuration::MetricsReport;

    const UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";
    const REPORT_NAME: &str = "report1";
    const REPORT_NUMBER: i32 = 42;

    #[derive(Clone)]
    struct MockTelemetryReportReadyListener {
        last_call: Arc<Mutex<Option<MetricsReportInfo>>>,
    }

    impl binder::Interface for MockTelemetryReportReadyListener {}

    impl MockTelemetryReportReadyListener {
        fn new() -> Self {
            Self { last_call: Arc::new(Mutex::new(None)) }
        }

        pub fn get_last_call(&self) -> MetricsReportInfo {
            self.last_call.lock().take().unwrap()
        }
    }

    impl ITelemetryReportReadyListener for MockTelemetryReportReadyListener {
        fn onReady(&self, report_info: &MetricsReportInfo) -> binder::Result<()> {
            self.last_call.lock().replace(report_info.clone());
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
        let mut metrics_report = MetricsReport::new();
        metrics_report.metrics_config_uuid = UUID.to_owned();
        metrics_report.report_config_name = REPORT_NAME.to_owned();
        metrics_report.report_number = REPORT_NUMBER;
        metrics_report
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

        report_listener.on_report_ready(create_metrics_report());

        assert_eq!(
            mock_listener.get_last_call(),
            MetricsReportInfo {
                metricsConfigUuid: UUID.to_owned(),
                reportConfigName: REPORT_NAME.to_owned(),
                reportNumber: REPORT_NUMBER
            }
        );
    }

    #[test]
    fn on_report_ready_overwrites_report() {
        let report_listener = Arc::new(create_default_metrics_report_listener());
        let report = create_metrics_report();

        report_listener.on_report_ready(report.clone());
        report_listener.on_report_ready(report.clone());

        let stored_reports = report_listener
            .metrics_report_store
            .lock()
            .values()
            .take(2)
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(stored_reports.len(), 1);
        assert!(
            stored_reports[0].reflect_eq(&report, &ReflectEqMode::default()),
            "Unexpected stored report: {:?}",
            stored_reports[0]
        );
    }

    #[test]
    fn get_metrics_report_success() {
        let report_listener = Arc::new(create_default_metrics_report_listener());
        let metrics_report = create_metrics_report();
        let report_info = MetricsReportInfo {
            metricsConfigUuid: metrics_report.metrics_config_uuid.clone(),
            reportConfigName: metrics_report.report_config_name.clone(),
            reportNumber: metrics_report.report_number,
        };
        report_listener.on_report_ready(metrics_report.clone());

        let retrieved_report = report_listener.get_metrics_report(&report_info).unwrap();

        assert_eq!(retrieved_report, metrics_report);
    }

    #[test]
    fn get_metrics_report_not_found() {
        let report_listener = Arc::new(create_default_metrics_report_listener());
        let metrics_report = create_metrics_report();
        let report_info = MetricsReportInfo {
            metricsConfigUuid: metrics_report.metrics_config_uuid.clone(),
            reportConfigName: metrics_report.report_config_name.clone(),
            reportNumber: metrics_report.report_number,
        };

        let err = report_listener.get_metrics_report(&report_info).unwrap_err();

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
        let report_info = MetricsReportInfo {
            metricsConfigUuid: metrics_report.metrics_config_uuid.clone(),
            reportConfigName: metrics_report.report_config_name.clone(),
            reportNumber: metrics_report.report_number,
        };

        let ready_reports_info = report_listener.get_ready_reports_info();

        assert_eq!(ready_reports_info, vec![report_info]);
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
