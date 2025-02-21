// Copyright 2025 Google LLC

#ifndef TELEMETRY_CLIENTLIB_INCLUDE_STATUS_H
#define TELEMETRY_CLIENTLIB_INCLUDE_STATUS_H

#include <stdint.h>

// Details structure serving as a fallback
struct UnknownDetails {};

// Aggregation Publisher error details
struct AggregationPublisherErrorDetails {
    // Metrics config UUID
    const char* metrics_config_uuid;
    // Aggregation publisher name
    const char* publisher_name;
    // Field name
    const char* field_name;
};

// Service Publisher error details
struct ServicePublisherErrorDetails {
    // Metrics config UUID
    const char* metrics_config_uuid;
    // Service publisher name
    const char* publisher_name;
};

// Conditional Trigger error details
struct ConditionalTriggerErrorDetails {
    // Metrics config UUID
    const char* metrics_config_uuid;
    // Conditional trigger name
    const char* trigger_name;
};

// Metrics Report Generator error details
struct MetricsReportGeneratorErrorDetails {
    // Metrics config UUID
    const char* metrics_config_uuid;
    // Report name
    const char* metrics_report_name;
    // Field name
    const char* field_name;
};

struct ServiceErrorDetails {
    // Service name
    const char* service_name;
};

struct FileIOErrorDetails {
    // File or directory path
    const char* path;
};

enum StatusDetailsTag {
    UnknownDetailsTag,
    AggregationPublisherErrorDetailsTag,
    ServicePublisherErrorDetailsTag,
    ConditionalTriggerErrorDetailsTag,
    MetricsReportGeneratorErrorDetailsTag,
    ServiceErrorDetailsTag,
    FileIOErrorDetailsTag,
};

union StatusDetails {
    // Details structure serving as a fallback
    struct UnknownDetails unknown_details;
    // Aggregation Publisher error details
    struct AggregationPublisherErrorDetails aggregation_publisher_error_details;
    // Service Publisher error details
    struct ServicePublisherErrorDetails service_publisher_error_details;
    // Conditional Trigger error details
    struct ConditionalTriggerErrorDetails conditional_trigger_error_details;
    // Metrics Report Generator error details
    struct MetricsReportGeneratorErrorDetails metrics_report_generator_error_details;
    // Service error details
    struct ServiceErrorDetails service_error_details;
    // File IO error details
    struct FileIOErrorDetails file_io_error_details;
};

struct StatusDetailsWithTag {
    // Tag for determining which union field is set
    enum StatusDetailsTag tag;
    // Union of status details
    union StatusDetails details;
};

// TelemetryServiceStatus contains details of Telemetry Service statuses
// which are sent back to the client
struct TelemetryServiceStatus {
    // Unique status ID
    int64_t id;
    // Associated status code providing additional context
    int32_t code;
    // Human-readable status message
    const char* message;
    // Structured status details
    const struct StatusDetailsWithTag* details;
};

#endif  // TELEMETRY_CLIENTLIB_INCLUDE_STATUS_H