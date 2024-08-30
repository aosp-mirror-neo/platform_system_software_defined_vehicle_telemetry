// Copyright 2023 Google LLC

package google.sdv.telemetry;

import google.sdv.telemetry.TelemetryServiceStatusCodes;

/**
 * Interface implemented by Data logger to receive telemetry reports.
 */

oneway interface ITelemetryReportListener {
    /**
     * Method called by telemetry service during getMetricsReport() API call to send metrics report
     * back to the client. MetricsReport format sent by client as part of Metrics Configuration
     *
     * metricsReport is not size checked currently. However it is expected to be small (<10Kb).
     * If this API needs to pass large metricsReport, it has to be modified to use pipes or similar
     * technique.
     *
     * @param statusCode status code indicating the outcome of the getMetricsReport() API call
     * @param statusMessage status message adding more context to the outcome of the API call.
     * @param metricsReport serialized MetricsReport
     *
     */
    void onReport(in TelemetryServiceStatusCodes statusCode,
            in @utf8InCpp String statusMessage, in byte[] metricsReport);
}
