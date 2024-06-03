// Copyright 2023 Google LLC. All Rights Reserved.

package google.sdv.telemetry;

/**
 * Exhaustive list of all status codes supported by the telemetry service.
 */
@Backing(type="int")
@VintfStability
enum TelemetryServiceStatusCodes {

    /**
     * Status code for successful API calls
     */
    TELEMETRY_SERVICE_API_CALL_SUCCESS = 200,

    /**
     * Common error messages during API calls
     */

    /**
     * Invalid metric config uuid send to API call
     */
    METRICS_CONFIG_INVALID_CONFIG_UUID = 400,

    /**
     * Metric Config version used is deprecated and
     * telemetry service can not understand it.
     */
    METRICS_CONFIG_VERSION_TOO_OLD = 401,

    /**
     * Error occurred when trying to parse Metric Config.
     * Metric Config is either passed during add metrics config
     * or loaded from disk.
     */
    METRICS_CONFIG_PARSE_ERROR = 402,

    /**
     * Error occurred while trying to read metric config from disk.
     */
    METRICS_CONFIG_CORRUPTED = 403,

    /**
     * Error occurred while performing a file operation (delete, move, etc)
     */
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
     * Errors specific to set misc status listener
     */
    SET_MISC_STATUS_LISTENER_FAILED = 410,

    /**
     * Errors specific to get finished report
     */
    GET_FINISHED_REPORT_FAILED = 411,

    /**
     * No Listener object sent to telemetry service API Call
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
     * Arbitrary Error used as a placeholder for errors not yet defined
     */
    TELEMETRY_SERVICE_UNKNOWN_ERROR = 499,
}
