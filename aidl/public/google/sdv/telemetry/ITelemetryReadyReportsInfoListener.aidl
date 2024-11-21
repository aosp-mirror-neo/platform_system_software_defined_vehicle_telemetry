// Copyright 2024 Google LLC

package google.sdv.telemetry;

import google.sdv.telemetry.MetricsReportInfo;
import google.sdv.telemetry.TelemetryServiceStatusCodes;

/**
 * Enables clients to receive info of ready telemetry reports.
 */

@VintfStability
oneway interface ITelemetryReadyReportsInfoListener {
    /**
     * Called by telemetry service to provide finished reports info.
     *
     * @param statusCode status code indicating the outcome of the sendReadyReportsInfo() API call
     * @param statusMessage status message adding more context to the outcome of the API call.
     * @param readyReportsInfo List of MetricsReportInfo representing reports which are ready.
     */
    void sendReadyReportsInfo(in TelemetryServiceStatusCodes statusCode,
            in @utf8InCpp String statusMessage, in MetricsReportInfo[] readyReportsInfo);
}
