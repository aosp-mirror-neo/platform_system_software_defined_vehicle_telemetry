// Copyright 2023 Google LLC

package google.sdv.telemetry;

import google.sdv.telemetry.TelemetryServiceStatusCodes;

/**
 * Enables clients to receive status codes and messages for API calls
 */
@VintfStability
oneway interface ITelemetryApiStatusListener {
    /**
     * Called by telemetry service to send back status codes and messages for APIs
     */
    void sendStatus(in int apiCallId, in TelemetryServiceStatusCodes statusCode,
            in @utf8InCpp String statusMessage);
}
