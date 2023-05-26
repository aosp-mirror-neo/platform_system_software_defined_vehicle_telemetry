// Copyright 2023 Google LLC. All Rights Reserved.

package android.sdv.telemetry;

import android.sdv.telemetry.ITelemetryReportListener;
import android.sdv.telemetry.ITelemetryReportReadyListener;

/**
 * Internal binder interface for telemetry service.
 *
 * @hide
 */
interface ITelemetryService {
    /**
     * Adds telemetry MetricsConfigs to TelemetryService.
     */
    void addMetricsConfig(in String metricsConfigName, in byte[] metricsConfig);

    /**
     * Removes a MetricsConfig based on the name. This will also remove outputs produced by the
     * MetricsConfig.
     */
    void removeMetricsConfig(in String metricsConfigName);

    /**
     * Removes all MetricsConfigs. This will also remove all MetricsConfig outputs.
     */
    void removeAllMetricsConfigs();

    /**
     * Sends finished telemetry reports or errors associated with the given name using the
     * {@code ITelemetryServiceListener}.
     */
    void getFinishedReport(in String metricsConfigName, in ITelemetryReportListener listener);

    /**
     * Sends all finished telemetry reports or errors using the {@code ITelemetryReportListener}.
     */
    void getAllFinishedReports(in ITelemetryReportListener listener);

    /**
     * Registers a listener for receiving notifications when a report or telemetry error is ready.
     */
    void setReportReadyListener(in ITelemetryReportReadyListener listener);

    /**
     * Clears listener to stop receiving notifications when a report or telemetry error is ready.
     */
    void clearReportReadyListener();
}
