// Copyright 2023 Google LLC. All Rights Reserved.

package android.sdv.telemetry;

/**
 * Binder interface that enables sending telemetry reports
 * from telemetry service to client.
 *
 * @hide
 */
oneway interface ITelemetryReportListener {
    void onReport(in int status_code, in @utf8InCpp String status_message,
            in byte[] metricsReport);
}
