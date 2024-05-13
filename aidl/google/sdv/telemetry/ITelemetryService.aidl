// Copyright 2023 Google LLC. All Rights Reserved.

package google.sdv.telemetry;

import google.sdv.telemetry.ITelemetryApiStatusListener;
import google.sdv.telemetry.ITelemetryMiscStatusListener;
import google.sdv.telemetry.ITelemetryReportListener;
import google.sdv.telemetry.ITelemetryReportReadyListener;

/**
 * Telemetry system for software defined vehicle has three main components: Telemetry service, Data
 * logger (the application that controls telemetry service and receive metrics reports) and Data
 * publishers. This is the primary interface exposed by the telemetry service for communicating with
 * the Data logger
 *
 */
@VintfStability
interface ITelemetryService {
    /**
     * Adds telemetry MetricsConfigs to TelemetryService.
     *
     * @param apiCallId unique integer id assigned to each API call
     * @param metricsConfig serialized metrics config byte array
     */
    void addMetricsConfig(in int apiCallId, in byte[] metricsConfig);

    /**
     * Activates a MetricsConfig based on uuid. This will also create objects
     * related to the MetricsConfig.
     *
     * @param apiCallId unique integer id assigned to each API call
     * @param metricsConfigUuid uuid of the metricsConfig that needs to be activated
     */
    void activateMetricsConfig(in int apiCallId, in @utf8InCpp String metricsConfigUuid);

    /**
     * Deactivates a MetricsConfig based on uuid. This will also destroy objects
     * related to the MetricsConfig.
     *
     * @param apiCallId unique integer id assigned to each API call
     * @param metricsConfigUuid uuid of the metricsConfig that needs to be deactivated
     */
    void deactivateMetricsConfig(in int apiCallId, in @utf8InCpp String metricsConfigUuid);

    /**
     * Removes a MetricsConfig based on uuid. This will also remove outputs produced by the
     * MetricsConfig.
     *
     * @param apiCallId unique integer id assigned to each API call
     * @param metricsConfigUuid uuid of the metricsConfig that needs to be removed
     */
    void removeMetricsConfig(in int apiCallId, in @utf8InCpp String metricsConfigUuid);

    /**
     * Removes all MetricsConfigs. This will also remove all MetricsConfig outputs.
     *
     * @param apiCallId unique integer id assigned to each API call
     */
    void removeAllMetricsConfigs(in int apiCallId);

    /**
     * Sends finished telemetry report with the specified metricsConfigUuid,
     * metricsReportConfigName and reportNumber, or errors associated with it using the specified
     *
     * @param metricsConfigUuid uuid of the Metrics Config for which the generated report is
     *         requested.
     * @param metricsReportConfigName the name of the Metrics Report Config for which the
     *         generated report is requested.
     * @param reportNumber report number of the metrics report requested.
     * @param listener Listener that is called when the requested report is ready
     */
    void getMetricsReport(in @utf8InCpp String metricsConfigUuid,
            in @utf8InCpp String metricsReportConfigName, in int reportNumber,
            in ITelemetryReportListener listener);

    /**
     * Registers a listener for receiving notifications when a metrics report is ready. If a
     * listener is already registered an error is returned.
     *
     * @param listener implementation of ITelemetryReportReadyListener, responsible for informing
     * the client about reports which are ready for consumption
     */
    void registerReportReadyListener(in int apiCallId, in ITelemetryReportReadyListener listener);

    /**
     * Registers a callback for receiving status codes for API calls. If a listener is already
     * registered an error is returned.
     *
     * @param listener implementation of ITelemetryApiStatusListener, responsible for sending
     * status codes back to the client
     */
    void registerApiStatusListener(in ITelemetryApiStatusListener listener);

    /**
     * Registers a callback for receiving misc status codes for telemetry service.
     *
     * @param listener implementation of ITelemetryMiscStatusListener, responsible for sending
     * status codes back to the client
     */
    void registerMiscStatusListener(in int apiCallId, in ITelemetryMiscStatusListener listener);

    /**
     * Clears listener to stop receiving notifications when a report or telemetry error is ready. If
     * a listener is already unregistered, the call has no effect.
     */
    void unregisterReportReadyListener();

    /**
     * Clears listener to stop receiving status codes. If a listener is already unregistered, the
     * call has no effect.
     */
    void unregisterApiStatusListener();

    /**
     * Clears misc status listener to stop receiving status codes. If a listener is already
     * unregistered, the call has no effect.
     */
    void unregisterMiscStatusListener();
}
