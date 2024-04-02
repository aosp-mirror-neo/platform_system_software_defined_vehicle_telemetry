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
@Backing(type="int") @VintfStability
enum TelemetryServiceStatusCodes {
  TELEMETRY_SERVICE_API_CALL_SUCCESS = 200,
  METRICS_CONFIG_INVALID_CONFIG_UUID = 400,
  METRICS_CONFIG_VERSION_TOO_OLD = 401,
  METRICS_CONFIG_PARSE_ERROR = 402,
  METRICS_CONFIG_CORRUPTED = 403,
  FILE_IO_ERROR = 404,
  ADD_METRICS_CONFIG_FAILED = 405,
  ACTIVATE_METRICS_CONFIG_FAILED = 406,
  REMOVE_METRICS_CONFIG_FAILED = 407,
  SET_REPORT_READY_LISTENER_FAILED = 408,
  SET_REPORT_LISTENER_FAILED = 409,
  SET_STATUS_LISTENER_FAILED = 410,
  GET_FINISHED_REPORT_FAILED = 411,
  API_CALL_FAILED_LISTENER_NOT_SET = 412,
  API_CALL_FAILED_CALLBACK_FUNCTION_NOT_SET = 413,
  TELEMETRY_SERVICE_UNKNOWN_ERROR = 499,
}
