// Copyright 2023 Google LLC. All Rights Reserved.

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
     * @param statusCode status code indicating the outcome of the getMetricsReport() API call
     * @param statusMessage status message adding more context to the outcome of the API call.
     * @param metricsReport serialized MetricsReport
     *
     */
    void onReport(in TelemetryServiceStatusCodes statusCode,
            in @utf8InCpp String statusMessage, in byte[] metricsReport);
}
