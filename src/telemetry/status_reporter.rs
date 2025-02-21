// Copyright 2025 Google LLC

use google_sdv_telemetry::aidl::google::sdv::telemetry::status::{
    StatusDetails::StatusDetails, TelemetryServiceStatus::TelemetryServiceStatus,
    TelemetryServiceStatusCodes::TelemetryServiceStatusCodes as StatusCodes,
};
use sdv_telemetry_utils::atomic::SequenceNumber;

type StatusId = i64;

// Status Reporter wraps the callback for reporting errors additionally providing unique
// identifiers to statuses using SequenceNumber.
pub(super) trait StatusReporter: Send + Sync {
    fn report(&self, code: StatusCodes, message: String, details: StatusDetails) -> StatusId;
}

pub(super) struct StatusReporterImpl {
    status_ids_provider: SequenceNumber,
    status_callback: Box<dyn Fn(TelemetryServiceStatus) + Send + Sync>,
}

impl StatusReporter for StatusReporterImpl {
    fn report(&self, code: StatusCodes, message: String, details: StatusDetails) -> StatusId {
        let id = self.status_ids_provider.next() as StatusId;
        (self.status_callback)(TelemetryServiceStatus { id, code, message, details });
        id
    }
}

impl StatusReporterImpl {
    pub(super) fn new(callback: impl Fn(TelemetryServiceStatus) + Send + Sync + 'static) -> Self {
        Self { status_ids_provider: SequenceNumber::new(), status_callback: Box::new(callback) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use sdv_telemetry_test_utils::oneshot_callback;
    use google_sdv_telemetry::aidl::google::sdv::telemetry::status::StatusDetails::AggregationPublisherErrorDetails::AggregationPublisherErrorDetails;

    const CONFIG_UUID: &str = "01bf0754-df95-46bb-bb35-522b6af66600";
    const PUBLISHER_NAME: &str = "publisher";
    const FIELD_NAME: &str = "field";
    const ERROR_MESSAGE: &str = "Evaluation failed";

    fn create_status_data() -> (StatusCodes, String, StatusDetails) {
        let code = StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR;
        let message = ERROR_MESSAGE.to_owned();
        let details =
            StatusDetails::AggregationPublisherErrorDetails(AggregationPublisherErrorDetails {
                metricsConfigUUID: CONFIG_UUID.to_owned(),
                publisherName: PUBLISHER_NAME.to_owned(),
                fieldName: FIELD_NAME.to_owned(),
            });
        (code, message, details)
    }

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn report_status_success() {
        let (tx, rx) = oneshot_callback();
        let reporter = StatusReporterImpl::new(tx);

        let (code, message, details) = create_status_data();
        let status_id = reporter.report(code, message, details);
        assert_eq!(status_id, 0);

        let status = rx.await;
        assert_eq!(
            status.code,
            StatusCodes::METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR
        );
        assert_eq!(status.message, ERROR_MESSAGE);
    }

    #[test]
    fn report_single_reporter_generates_sequential_ids() {
        let reporter = StatusReporterImpl::new(|_| {});

        let (code, message, details) = create_status_data();
        let status_id = reporter.report(code, message, details);
        assert_eq!(status_id, 0);

        let (code, message, details) = create_status_data();
        let status_id = reporter.report(code, message, details);
        assert_eq!(status_id, 1);

        let (code, message, details) = create_status_data();
        let status_id = reporter.report(code, message, details);
        assert_eq!(status_id, 2);
    }

    #[test]
    fn report_reporter_clone_generates_sequential_ids() {
        let reporter = Arc::new(StatusReporterImpl::new(|_| {}));
        let reporter2 = Arc::clone(&reporter);

        // Reporter #2 generates status id = 0
        let (code, message, details) = create_status_data();
        let status_id = reporter2.report(code, message, details);
        assert_eq!(status_id, 0);

        // Reporter #1 generates status id = 1
        let (code, message, details) = create_status_data();
        let status_id = reporter.report(code, message, details);
        assert_eq!(status_id, 1);

        // Reporter #1 generates status id = 2
        let (code, message, details) = create_status_data();
        let status_id = reporter.report(code, message, details);
        assert_eq!(status_id, 2);

        // Reporter #2 generates status id = 3
        let (code, message, details) = create_status_data();
        let status_id = reporter2.report(code, message, details);
        assert_eq!(status_id, 3);
    }
}
