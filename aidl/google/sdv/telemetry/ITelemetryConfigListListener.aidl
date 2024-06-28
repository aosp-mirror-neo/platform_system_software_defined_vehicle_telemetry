// Copyright 2024 Google LLC

package google.sdv.telemetry;

import google.sdv.telemetry.TelemetryServiceStatusCodes;

/**
 * Enables clients to receive list of active or inactive configs currently part of the service.
 */

oneway interface ITelemetryConfigListListener {
    /**
     * Called by telemetry service to send back list of configs to client
     *
     * @param statusCode status code indicating the outcome of the sendConfigList() API call
     * @param statusMessage status message adding more context to the outcome of the API call.
     * @param metricsReport List of metrics config uuids maintained by telemetry service in either
     *                      active or inactive state
     */
    void sendConfigList(in TelemetryServiceStatusCodes statusCode,
            in @utf8InCpp String statusMessage, in List<String> configList);
}
