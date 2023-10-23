// Copyright 2023 Google LLC. All Rights Reserved.

package android.sdv.telemetry;

/**
 * Enables clients to receive telemetry report ready notifications.
 *
 * @hide
 */
oneway interface ITelemetryStatusListener {
    /**
     * Called by telemetry service to provide notifications on finished reports or script
     * runtime error.
     */
    void sendStatus(
            in int apiCallId, in int statusCode, in @utf8InCpp String statusMessage);
}
