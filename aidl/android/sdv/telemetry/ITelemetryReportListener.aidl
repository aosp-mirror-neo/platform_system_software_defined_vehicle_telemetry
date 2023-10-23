// Copyright 2023 Google LLC. All Rights Reserved.

package android.sdv.telemetry;

/**
 * Binder interface that enables sending telemetry reports
 * from telemetry service to client.
 *
 * @hide
 */
oneway interface ITelemetryReportListener {
    void onReport(
            in int statusCode, in @utf8InCpp String statusMessage, in byte[] metricsReport);
}
