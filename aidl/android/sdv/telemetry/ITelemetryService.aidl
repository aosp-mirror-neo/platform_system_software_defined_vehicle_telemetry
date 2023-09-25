// Copyright 2023 Google LLC. All Rights Reserved.

package android.sdv.telemetry;

import android.sdv.telemetry.ITelemetryReportListener;
import android.sdv.telemetry.ITelemetryReportReadyListener;
import android.sdv.telemetry.ITelemetryStatusListener;

/**
 * Internal binder interface for telemetry service.
 *
 * @hide
 */
interface ITelemetryService {
    /**
     * Adds telemetry MetricsConfigs to TelemetryService.
     */
    void addMetricsConfig(in int api_call_id, in byte[] metricsConfig);

    /**
     * Activates a MetricsConfig based on uuid. This will also create objects
     * related to the MetricsConfig.
     */
    void activateMetricsConfig(in int api_call_id, in @utf8InCpp String metricsConfigUuid);

    /**
     * Deactivates a MetricsConfig based on uuid. This will also destroy objects
     * related to the MetricsConfig.
     */
    void deactivateMetricsConfig(
            in int api_call_id, in @utf8InCpp String metricsConfigUuid);

    /**
     * Removes a MetricsConfig based on uuid. This will also remove outputs produced by the
     * MetricsConfig.
     */
    void removeMetricsConfig(in int api_call_id, in @utf8InCpp String metricsConfigUuid);

    /**
     * Removes all MetricsConfigs. This will also remove all MetricsConfig outputs.
     */
    void removeAllMetricsConfigs(in int api_call_id);

    /**
     * Sends finished telemetry reports or errors associated with the given uuid using the
     * {@code ITelemetryReportListener}.
     */
    void getMetricsReport(in int report_number, in @utf8InCpp String metricsConfigUuid,
            in ITelemetryReportListener listener);

    /**
     * Registers a listener for receiving notifications when a report or telemetry error is ready.
     */
    void setReportReadyListener(in ITelemetryReportReadyListener listener);

    /**
     * Registers a callback for receiving status codes for API calls.
     */
    void setStatusListener(in ITelemetryStatusListener listener);

    /**
     * Clears listener to stop receiving notifications when a report or telemetry error is ready.
     */
    void clearReportReadyListener();

    /**
     * Clears listener to stop receiving status codes.
     */
    void clearStatusListener();
}
