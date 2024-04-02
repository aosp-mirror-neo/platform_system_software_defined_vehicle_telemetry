// Copyright 2023 Google LLC. All Rights Reserved.

package android.sdv.telemetry;

/**
 * Interface implemented by Data logger to receive telemetry reports.
 */
@VintfStability
oneway interface ITelemetryReportListener {
    /**
     * Method called by telemetry service during getMetricsReport() API call to send metrics report
     * back to the client. MetricsReport format sent by client as part of Metrics Configuration
     *
     * @param statusCode status code indicating the outcome of the getMetricsReport() API call
     * @param statusMessage status message adding more context to the outcome of the API call.
     * @param metricsReport serialized MetricsReport
     *
     */
    void onReport(
            in int statusCode, in @utf8InCpp String statusMessage, in byte[] metricsReport);
}
