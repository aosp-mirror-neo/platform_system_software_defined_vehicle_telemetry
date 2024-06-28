// Copyright 2023 Google LLC

#ifndef TELEMETRY_CLIENTLIB_INCLUDE_STATUS_CODES_H
#define TELEMETRY_CLIENTLIB_INCLUDE_STATUS_CODES_H

#include <stdint.h>

enum class TelemetryServiceStatusCodes : int32_t {

    /**
     * Status code for successful API calls
     */
    TELEMETRY_SERVICE_API_CALL_SUCCESS = 200,

    /**
     * Common error messages during API calls
     */
    METRICS_CONFIG_INVALID_CONFIG_UUID = 400,
    METRICS_CONFIG_VERSION_TOO_OLD = 401,
    METRICS_CONFIG_PARSE_ERROR = 402,
    METRICS_CONFIG_CORRUPTED = 403,
    FILE_IO_ERROR = 404,

    /**
     * Errors specific to add metrics config
     */
    ADD_METRICS_CONFIG_FAILED = 405,

    /**
     * Errors specific to metrics config activation
     */
    ACTIVATE_METRICS_CONFIG_FAILED = 406,

    /**
     * Errors specific to remove metrics config
     */
    REMOVE_METRICS_CONFIG_FAILED = 407,

    /**
     * Errors specific to set report ready listener
     */
    SET_REPORT_READY_LISTENER_FAILED = 408,

    /**
     * Errors specific to set report listener
     */
    SET_REPORT_LISTENER_FAILED = 409,

    /**
     * Errors specific to set status listener
     */
    SET_MISC_STATUS_LISTENER_FAILED = 410,

    /**
     * Errors specific to get finished report
     */
    GET_FINISHED_REPORT_FAILED = 411,

    /**
     * Errors with params sent to API calls
     */
    API_CALL_FAILED_LISTENER_NOT_SET = 412,
    API_CALL_FAILED_CALLBACK_FUNCTION_NOT_SET = 413,

    /**
     * Arbitrary Error used as a placeholder for errors not yet defined
     */
    TELEMETRY_SERVICE_UNKNOWN_ERROR = 499,
};

#endif  // TELEMETRY_CLIENTLIB_INCLUDE_STATUS_CODES_H
