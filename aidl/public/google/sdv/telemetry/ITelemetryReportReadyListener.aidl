// Copyright 2023 Google LLC

package google.sdv.telemetry;

import google.sdv.telemetry.MetricsReportInfo;

/**
 * Enables clients to receive telemetry report ready notifications.
 */
@VintfStability
oneway interface ITelemetryReportReadyListener {
    /**
     * Called by telemetry service to provide notifications on finished reports.
     * @param metrics_report_info info of the ready report.
     */
    void onReady(in MetricsReportInfo metrics_report_info);
}
