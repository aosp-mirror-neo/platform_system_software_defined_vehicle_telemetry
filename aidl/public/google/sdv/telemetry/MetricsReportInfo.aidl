// Copyright 2025 Google LLC

package google.sdv.telemetry;

/**
 * Represents Telemetry Service Metrics Report information
 */

@VintfStability
parcelable MetricsReportInfo {
    // UUID of the Metrics Config
    @utf8InCpp String configUuid;

    // UUID of the Metrics Report
    @utf8InCpp String reportUuid;
}
