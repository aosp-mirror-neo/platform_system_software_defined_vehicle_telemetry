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
     *
     * @param apiCallId: unique integer id assigned to each API call
     * @param metricsConfig: serialized metrics config byte array
     */
    void addMetricsConfig(in int apiCallId, in byte[] metricsConfig);

    /**
     * Activates a MetricsConfig based on uuid. This will also create objects
     * related to the MetricsConfig.
     *
     * @param apiCallId: unique integer id assigned to each API call
     * @param metricsConfigUuid: uuid of the metricConfig that needs to be activated
     */
    void activateMetricsConfig(in int apiCallId, in @utf8InCpp String metricsConfigUuid);

    /**
     * Deactivates a MetricsConfig based on uuid. This will also destroy objects
     * related to the MetricsConfig.
     *
     * @param apiCallId: unique integer id assigned to each API call
     * @param metricsConfigUuid: uuid of the metricConfig that needs to be deactivated
     */
    void deactivateMetricsConfig(in int apiCallId, in @utf8InCpp String metricsConfigUuid);

    /**
     * Removes a MetricsConfig based on uuid. This will also remove outputs produced by the
     * MetricsConfig.
     *
     * @param apiCallId: unique integer id assigned to each API call
     * @param metricsConfigUuid: uuid of the metricConfig that needs to be removed
     */
    void removeMetricsConfig(in int apiCallId, in @utf8InCpp String metricsConfigUuid);

    /**
     * Removes all MetricsConfigs. This will also remove all MetricsConfig outputs.
     *
     * @param apiCallId: unique integer id assigned to each API call
     */
    void removeAllMetricsConfigs(in int apiCallId);

    /**
     * Sends finished telemetry reports or errors associated with the given uuid using the
     * {@code ITelemetryReportListener}.
     *
     * @param report_number: report number of the metrics report requested
     * @param metricsConfigUuid: uuid of the metricConfig for which the generated report is
     *         requested
     * @param listener: implementation of ITelemetryReportListener, that is used to send the report
     *         back
     */
    void getMetricsReport(in int report_number, in @utf8InCpp String metricsConfigUuid,
            in ITelemetryReportListener listener);

    /**
     * Registers a listener for receiving notifications when a metrics report is ready.
     *
     * @param listener: implementation of ITelemetryReportReadyListener, responsible for informing
     * the client about reports which are ready for consumption
     */
    void setReportReadyListener(in ITelemetryReportReadyListener listener);

    /**
     * Registers a callback for receiving status codes for API calls.
     *
     * @param listener: implementation of ITelemetryStatusListener, responsible for sending
     * status codes back to the client
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
