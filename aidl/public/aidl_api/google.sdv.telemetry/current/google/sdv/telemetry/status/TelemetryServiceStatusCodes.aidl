//
// Copyright (C) 2023 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
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
@Backing(type="int")
@VintfStability
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
    SET_MISC_STATUS_LISTENER_FAILED = 410,
    GET_FINISHED_REPORT_FAILED = 411,
    API_CALL_FAILED_LISTENER_NOT_SET = 412,
    API_CALL_FAILED_CALLBACK_FUNCTION_NOT_SET = 413,
    GET_CONFIG_LIST_FAILED = 414,
    API_CALL_FAILED_INTERNAL_ERROR = 415,
    METRICS_CONFIG_RUNTIME_AGGREGATION_ERROR = 416,
    METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR = 417,
    METRICS_CONFIG_RUNTIME_SERVICE_DISCOVERY_AMBIGUOUS_SERVICE_NAME_ERROR = 418,
    METRICS_CONFIG_RUNTIME_SERVICE_DISCOVERY_INTERNAL_ERROR = 419,
    METRICS_CONFIG_RUNTIME_SERVICE_CONNECTION_ERROR = 420,
    METRICS_CONFIG_RUNTIME_SERVICE_METADATA_PARSING_ERROR = 421,
    METRICS_CONFIG_RUNTIME_SERVICE_MESSAGE_RETRIEVAL_ERROR = 422,
    METRICS_CONFIG_RUNTIME_SERVICE_MESSAGE_PARSING_ERROR = 423,
    TELEMETRY_SERVICE_UNKNOWN_ERROR = 499,
}
