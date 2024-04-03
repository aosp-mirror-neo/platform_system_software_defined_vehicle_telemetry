// Copyright 2023 Google LLC. All Rights Reserved.
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

package google.sdv.telemetry;
@VintfStability
interface ITelemetryService {
  void addMetricsConfig(in int apiCallId, in byte[] metricsConfig);
  void activateMetricsConfig(in int apiCallId, in @utf8InCpp String metricsConfigUuid);
  void deactivateMetricsConfig(in int apiCallId, in @utf8InCpp String metricsConfigUuid);
  void removeMetricsConfig(in int apiCallId, in @utf8InCpp String metricsConfigUuid);
  void removeAllMetricsConfigs(in int apiCallId);
  void getMetricsReport(in @utf8InCpp String metricsConfigUuid, in @utf8InCpp String metricsReportConfigName, in int reportNumber, in google.sdv.telemetry.ITelemetryReportListener listener);
  void registerReportReadyListener(in google.sdv.telemetry.ITelemetryReportReadyListener listener);
  void registerStatusListener(in google.sdv.telemetry.ITelemetryStatusListener listener);
  void unregisterReportReadyListener();
  void unregisterStatusListener();
}
