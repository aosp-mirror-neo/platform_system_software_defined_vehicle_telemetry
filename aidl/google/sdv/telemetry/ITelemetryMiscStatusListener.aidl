// Copyright 2024 Google LLC. All Rights Reserved.

package google.sdv.telemetry;

/**
 * Enables clients to receive error codes and messages for miscellaneous errors
 */
@VintfStability
oneway interface ITelemetryMiscStatusListener {
    /**
     * Called by telemetry service to send back miscellaneous errors to client
     */
    void sendStatus(in int statusCode, in @utf8InCpp String statusMessage);
}
