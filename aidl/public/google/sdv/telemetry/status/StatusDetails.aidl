// Copyright 2025 Google LLC

package google.sdv.telemetry.status;

/**
 * Information on statuses which are being sent back to the client
 */

@RustDerive(Clone=true)
@VintfStability
union StatusDetails {
    // Aggregation Publisher error details
    @RustDerive(Clone=true)
    @VintfStability
    parcelable AggregationPublisherErrorDetails {
        @utf8InCpp String metricsConfigUUID;
        @utf8InCpp String publisherName;
        @utf8InCpp String fieldName;
    }

    // Service Publisher error details
    @RustDerive(Clone=true)
    @VintfStability
    parcelable ServicePublisherErrorDetails {
        @utf8InCpp String metricsConfigUUID;
        @utf8InCpp String publisherName;
    }

    // Trigger error details
    @RustDerive(Clone=true)
    @VintfStability
    parcelable ConditionalTriggerErrorDetails {
        @utf8InCpp String metricsConfigUUID;
        @utf8InCpp String triggerName;
    }

    // Metrics Report Generator error details
    @RustDerive(Clone=true)
    @VintfStability
    parcelable MetricsReportGeneratorErrorDetails {
        @utf8InCpp String metricsConfigUUID;
        @utf8InCpp String metricsReportName;
        @utf8InCpp String fieldName;
    }

    // Service error details
    @RustDerive(Clone=true)
    @VintfStability
    parcelable ServiceErrorDetails {
        @utf8InCpp String serviceName;
    }

    // File IO error details
    @RustDerive(Clone=true)
    @VintfStability
    parcelable FileIOErrorDetails {
        @utf8InCpp String path;
    }

    AggregationPublisherErrorDetails aggregationPublisherErrorDetails;
    ServicePublisherErrorDetails servicePublisherErrorDetails;
    ConditionalTriggerErrorDetails conditionalTriggerErrorDetails;
    MetricsReportGeneratorErrorDetails metricsReportGeneratorErrorDetails;
    ServiceErrorDetails serviceErrorDetails;
    FileIOErrorDetails fileIOErrorDetails;
}
