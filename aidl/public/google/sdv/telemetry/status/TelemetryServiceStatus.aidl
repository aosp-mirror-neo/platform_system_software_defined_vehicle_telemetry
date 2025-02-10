// Copyright 2025 Google LLC

package google.sdv.telemetry.status;

import google.sdv.telemetry.status.TelemetryServiceStatusCodes;

/**
 * TelemetryServiceStatus contains details of Telemetry Service statuses
 * which are sent back to the client
 */

@RustDerive(Clone=true)
@VintfStability
parcelable TelemetryServiceStatus {
    // Associated status code providing additional context
    TelemetryServiceStatusCodes code;

    // Human-readable status message
    @utf8InCpp String message;
}
