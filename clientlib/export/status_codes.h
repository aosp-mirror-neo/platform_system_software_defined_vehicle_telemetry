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

    /**
     * No Callback Function sent to telemetry service API Call
     */
    API_CALL_FAILED_CALLBACK_FUNCTION_NOT_SET = 413,

    /**
     * Errors specific to get active and inactive configs
     */
    GET_CONFIG_LIST_FAILED = 414,

    /**
     * Binder-related errors
     */
    API_CALL_FAILED_INTERNAL_ERROR = 415,

    /**
     * Errors during runtime aggregation of metrics.
     */
    METRICS_CONFIG_RUNTIME_AGGREGATION_ERROR = 416,

    /**
     * Errors during evaluation of expression nodes in the metrics
       configuration.
     */
    METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR = 417,

    /**
     * Ambiguous service name encountered during service discovery.
     */
    METRICS_CONFIG_RUNTIME_SERVICE_DISCOVERY_AMBIGUOUS_SERVICE_NAME_ERROR = 418,

    /**
     * Internal service discovery error occurred during service discovery.
     */
    METRICS_CONFIG_RUNTIME_SERVICE_DISCOVERY_INTERNAL_ERROR = 419,

    /**
     * Error occurred during service connection.
     */
    METRICS_CONFIG_RUNTIME_SERVICE_CONNECTION_ERROR = 420,

    /**
     * Error occurred while parsing service metadata.
     */
    METRICS_CONFIG_RUNTIME_SERVICE_METADATA_PARSING_ERROR = 421,

    /**
     * Error occurred while retrieving service messages.
     */
    METRICS_CONFIG_RUNTIME_SERVICE_MESSAGE_RETRIEVAL_ERROR = 422,

    /**
     * Error occurred while parsing service messages.
     */
    METRICS_CONFIG_RUNTIME_SERVICE_MESSAGE_PARSING_ERROR = 423,

    /**
     * Arbitrary Error used as a placeholder for errors not yet defined
     */
    TELEMETRY_SERVICE_UNKNOWN_ERROR = 499,
};

#endif  // TELEMETRY_CLIENTLIB_INCLUDE_STATUS_CODES_H
