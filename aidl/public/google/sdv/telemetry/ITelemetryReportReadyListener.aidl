// Copyright 2023 Google LLC

package google.sdv.telemetry;

/**
 * Enables clients to receive telemetry report ready notifications.
 */
@VintfStability
oneway interface ITelemetryReportReadyListener {
    /**
     * Called by telemetry service to provide notifications on finished reports.
     * Reports are numbered using a monotonously increasing id number that is independent for each
     * metrics UUID
     * @param metricsConfigUUID UUID of the Metrics Config for which the report is ready.
     * @param metricsReportConfigName the name of the Metrics Report Config for which the
     *         report is ready.
     * @param reportNumber report number of the report that is ready.
     */
    void onReady(in @utf8InCpp String metricsConfigUuid,
            in @utf8InCpp String metricsReportConfigName, in int reportNumber);
}
