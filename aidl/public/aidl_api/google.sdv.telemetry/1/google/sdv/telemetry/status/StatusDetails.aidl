// Copyright 2025 Google LLC
///////////////////////////////////////////////////////////////////////////////
// THIS FILE IS IMMUTABLE. DO NOT EDIT IN ANY CASE.                          //
///////////////////////////////////////////////////////////////////////////////

// This file is a snapshot of an AIDL file. Do not edit it manually. There are
// two cases:
// 1). this is a frozen version file - do not edit this in any case.
// 2). this is a 'current' file. If you make a backwards compatible change to
//     the interface (from the latest frozen version), the build system will
//     prompt you to update this file with `m <name>-update-api`.
//
// You must not make a backward incompatible change to any AIDL file built
// with the aidl_interface module type with versions property set. The module
// type is used to build AIDL files in a way that they can be used across
// independently updatable components of the system. If a device is shipped
// with such a backward incompatible change, it has a high risk of breaking
// later when a module using the interface is updated, e.g., Mainline modules.

package google.sdv.telemetry.status;
@VintfStability
union StatusDetails {
  google.sdv.telemetry.status.StatusDetails.AggregationPublisherErrorDetails aggregationPublisherErrorDetails;
  google.sdv.telemetry.status.StatusDetails.ServicePublisherErrorDetails servicePublisherErrorDetails;
  google.sdv.telemetry.status.StatusDetails.ConditionalTriggerErrorDetails conditionalTriggerErrorDetails;
  google.sdv.telemetry.status.StatusDetails.MetricsReportGeneratorErrorDetails metricsReportGeneratorErrorDetails;
  google.sdv.telemetry.status.StatusDetails.ServiceErrorDetails serviceErrorDetails;
  google.sdv.telemetry.status.StatusDetails.FileIOErrorDetails fileIOErrorDetails;
  @VintfStability
  parcelable AggregationPublisherErrorDetails {
    @utf8InCpp String metricsConfigUUID;
    @utf8InCpp String publisherName;
    @utf8InCpp String fieldName;
  }
  @VintfStability
  parcelable ServicePublisherErrorDetails {
    @utf8InCpp String metricsConfigUUID;
    @utf8InCpp String publisherName;
  }
  @VintfStability
  parcelable ConditionalTriggerErrorDetails {
    @utf8InCpp String metricsConfigUUID;
    @utf8InCpp String triggerName;
  }
  @VintfStability
  parcelable MetricsReportGeneratorErrorDetails {
    @utf8InCpp String metricsConfigUUID;
    @utf8InCpp String metricsReportName;
    @utf8InCpp String fieldName;
  }
  @VintfStability
  parcelable ServiceErrorDetails {
    @utf8InCpp String serviceName;
  }
  @VintfStability
  parcelable FileIOErrorDetails {
    @utf8InCpp String path;
  }
}
