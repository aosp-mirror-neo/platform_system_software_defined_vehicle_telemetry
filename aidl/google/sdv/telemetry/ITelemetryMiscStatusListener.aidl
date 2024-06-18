// Copyright 2024 Google LLC. All Rights Reserved.

package google.sdv.telemetry;

import google.sdv.telemetry.TelemetryServiceStatusCodes;

/**
 * Enables clients to receive error codes and messages for miscellaneous errors
 */

oneway interface ITelemetryMiscStatusListener {
    /**
     * Called by telemetry service to send back miscellaneous errors to client
     */
    void sendStatus(
            in TelemetryServiceStatusCodes statusCode, in @utf8InCpp String statusMessage);
}
