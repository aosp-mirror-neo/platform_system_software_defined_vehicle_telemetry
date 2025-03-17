// Copyright 2023 Google LLC

package google.sdv.telemetry;

import google.sdv.telemetry.ITelemetryApiStatusListener;
import google.sdv.telemetry.ITelemetryConfigListListener;
import google.sdv.telemetry.ITelemetryMiscStatusListener;
import google.sdv.telemetry.ITelemetryReadyReportsInfoListener;
import google.sdv.telemetry.ITelemetryReportListener;
import google.sdv.telemetry.ITelemetryReportReadyListener;
import google.sdv.telemetry.MetricsReportInfo;

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
     * This API takes a byte buffer as a parameter, however this is safe as metrics configurations
     * are simple serialized protobuffers without descriptors (expected to be less than 10KB).
     *
     * This method is asynchronous and always returns immediately.
     * Execution result is reported through ITelemetryApiStatusListener using provided apiCallId.
     *
     * @param apiCallId unique integer id assigned to each API call
     * @param metricsConfig serialized metrics config byte array
     */
    void addMetricsConfig(in int apiCallId, in byte[] metricsConfig);

    /**
     * Activates a MetricsConfig based on uuid. This will also create objects
     * related to the MetricsConfig.
     *
     * This method is asynchronous and always returns immediately.
     * Execution result is reported through ITelemetryApiStatusListener using provided apiCallId.
     *
     * @param apiCallId unique integer id assigned to each API call
     * @param metricsConfigUuid uuid of the metricsConfig that needs to be activated
     */
    void activateMetricsConfig(in int apiCallId, in @utf8InCpp String metricsConfigUuid);

    /**
     * Deactivates a MetricsConfig based on uuid. This will also destroy objects
     * related to the MetricsConfig.
     *
     * This method is asynchronous and always returns immediately.
     * Execution result is reported through ITelemetryApiStatusListener using provided apiCallId.
     *
     * @param apiCallId unique integer id assigned to each API call
     * @param metricsConfigUuid uuid of the metricsConfig that needs to be deactivated
     */
    void deactivateMetricsConfig(in int apiCallId, in @utf8InCpp String metricsConfigUuid);

    /**
     * Removes a MetricsConfig based on uuid. This will also remove outputs produced by the
     * MetricsConfig.
     *
     * This method is asynchronous and always returns immediately.
     * Execution result is reported through ITelemetryApiStatusListener using provided apiCallId.
     *
     * @param apiCallId unique integer id assigned to each API call
     * @param metricsConfigUuid uuid of the metricsConfig that needs to be removed
     */
    void removeMetricsConfig(in int apiCallId, in @utf8InCpp String metricsConfigUuid);

    /**
     * Removes all MetricsConfigs. This will also remove all MetricsConfig outputs.
     *
     * This method is asynchronous and always returns immediately.
     * Execution result is reported through ITelemetryApiStatusListener using provided apiCallId.
     *
     * @param apiCallId unique integer id assigned to each API call
     */
    void removeAllMetricsConfigs(in int apiCallId);

    /**
     * Returns a list of all metrics config currently in the active state. The list is returned as
     * a serialized protobuf message by calling the callback function of the listener sent as a
     * function argument.
     *
     * This method is asynchronous and always returns immediately.
     * Execution result is reported using provided listener.
     *
     * @param listener callback used by telemetry service to send back list of active configs
     */
    void getActiveMetricsConfigUuids(in ITelemetryConfigListListener listener);

    /**
     * Returns a list of all metrics config currently in the inactive state. The list is returned as
     * a serialized protobuf message by calling the callback function of the listener sent as a
     * function argument.
     *
     * This method is asynchronous and always returns immediately.
     * Execution result is reported using provided listener.
     *
     * @param listener callback used by telemetry service to send back list of inactive configs
     */
    void getInactiveMetricsConfigUuids(in ITelemetryConfigListListener listener);

    /**
     * Sends finished telemetry report with the specified metrics report info.
     *
     * This method is asynchronous and always returns immediately.
     * Execution result is reported using provided listener.
     *
     * @param metricsReportInfo info of the generated report which is requested
     * @param listener listener that is called when the requested report is ready
     */
    void getMetricsReport(
            in MetricsReportInfo metricsReportInfo, in ITelemetryReportListener listener);

    /**
     * Sends finished telemetry reports info: metricsConfigUuid, reportConfigName and reportNumber
     *
     * This method is asynchronous and always returns immediately.
     * Execution result is reported using provided listener.
     *
     * @param listener Listener that is used to send IDs of ready reports
     */
    void getReadyReportsInfo(in ITelemetryReadyReportsInfoListener listener);

    /**
     * Registers a listener for receiving notifications when a metrics report is ready. If a
     * listener is already registered an error is returned.
     *
     * This method is asynchronous and always returns immediately.
     * Execution result is reported through ITelemetryApiStatusListener using provided apiCallId.
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
     * This method is asynchronous and always returns immediately.
     * Execution result is reported through ITelemetryApiStatusListener using provided apiCallId.
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
