// Copyright 2023 Google LLC. All Rights Reserved.

package android.sdv.telemetry;

/**
 * Enables clients to receive telemetry report ready notifications.
 *
 * @hide
 */
oneway interface ITelemetryReportReadyListener {
    /**
     * Called by telemetry service to provide notifications on finished reports or script
     * runtime error.
     */
    void onReady(in @utf8InCpp String metricsConfigUuid, in int reportNumber);
}
