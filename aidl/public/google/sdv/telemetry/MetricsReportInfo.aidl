// Copyright 2024 Google LLC

package google.sdv.telemetry;

/**
 * Information of telemetry metrics reports
 */

@RustDerive(Clone=true, Hash=true, PartialEq=true, Eq=true)
@VintfStability
parcelable MetricsReportInfo {
    // UUID of the Metrics Config
    @utf8InCpp String metricsConfigUuid;

    // Name of the Metrics Report Config
    @utf8InCpp String reportConfigName;

    // Report number of the finished report. Reports are numbered using a monotonously increasing id
    // number that is independent for each metrics UUID
    int reportNumber;
}
