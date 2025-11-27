// Copyright 2025 Google LLC

package google.sdv.telemetry.status;

import google.sdv.telemetry.status.StatusDetails;
import google.sdv.telemetry.status.TelemetryServiceStatusCodes;

/**
 * TelemetryServiceStatus contains details of Telemetry Service statuses
 * which are sent back to the client
 */

@VintfStability
parcelable TelemetryServiceStatus {
    // Unique status ID
    long id;

    // Associated status code providing additional context
    TelemetryServiceStatusCodes code;

    // Human-readable status message
    @utf8InCpp String message;

    /**
     * Structured status details.
     *
     * The following types of status details are possible depending on status code:
     *
     * - FILE_IO_ERROR:
     *     - FileIOErrorDetails
     *
     * - METRICS_CONFIG_PARSE_ERROR:
     *     - FileIOErrorDetails
     *
     * - ACTIVATE_METRICS_CONFIG_FAILED:
     *     - FileIOErrorDetails
     *
     * - METRICS_CONFIG_CORRUPTED:
     *     - FileIOErrorDetails
     *
     * - METRICS_CONFIG_RUNTIME_AGGREGATION_ERROR:
     *     - AggregationPublisherErrorDetails
     *     - MetricsReportGeneratorErrorDetails
     *
     * - METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR:
     *     - AggregationPublisherErrorDetails
     *     - ConditionalTriggerErrorDetails
     *     - MetricsReportGeneratorErrorDetails
     *
     * - METRICS_CONFIG_RUNTIME_SERVICE_CONNECTION_ERROR:
     *     - ServicePublisherErrorDetails
     *
     * - METRICS_CONFIG_RUNTIME_SERVICE_METADATA_PARSING_ERROR:
     *     - ServicePublisherErrorDetails
     *
     * - METRICS_CONFIG_RUNTIME_SERVICE_MESSAGE_RETRIEVAL_ERROR:
     *     - AggregationPublisherErrorDetails
     *     - ConditionalTriggerErrorDetails
     *     - MetricsReportGeneratorErrorDetails
     *     - ServiceErrorDetails
     *
     * - METRICS_CONFIG_RUNTIME_SERVICE_MESSAGE_PARSING_ERROR:
     *     - AggregationPublisherErrorDetails
     *     - ConditionalTriggerErrorDetails
     *     - MetricsReportGeneratorErrorDetails
     *     - ServiceErrorDetails
     *
     * No other status code is currently sent with TelemetryServiceStatus.
     */

    StatusDetails details;
}
