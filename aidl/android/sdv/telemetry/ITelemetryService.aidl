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
    void addMetricsConfig(in byte[] metricsConfig);

    /**
     * Activates a MetricsConfig based on uuid. This will also create objects
     * related to the MetricsConfig.
     */
    void activateMetricsConfig(in @utf8InCpp String metricsConfigUuid);

    /**
     * Deactivates a MetricsConfig based on uuid. This will also destroy objects
     * related to the MetricsConfig.
     */
    void deactivateMetricsConfig(in @utf8InCpp String metricsConfigUuid);

    /**
     * Removes a MetricsConfig based on uuid. This will also remove outputs produced by the
     * MetricsConfig.
     */
    void removeMetricsConfig(in @utf8InCpp String metricsConfigUuid);

    /**
     * Removes all MetricsConfigs. This will also remove all MetricsConfig outputs.
     */
    void removeAllMetricsConfigs();

    /**
     * Sends finished telemetry reports or errors associated with the given uuid using the
     * {@code ITelemetryReportListener}.
     */
    void getFinishedReport(
            in @utf8InCpp String metricsConfigUuid, in ITelemetryReportListener listener);

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
