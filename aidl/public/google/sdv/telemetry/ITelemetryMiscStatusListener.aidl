// Copyright 2024 Google LLC

package google.sdv.telemetry;

import google.sdv.telemetry.status.TelemetryServiceStatus;

/**
 * Enables clients to receive miscellaneous statuses
 */
@VintfStability
oneway interface ITelemetryMiscStatusListener {
    /**
     * Called by telemetry service to send miscellaneous statuses back to client
     */
    void sendStatus(in TelemetryServiceStatus status);
}
