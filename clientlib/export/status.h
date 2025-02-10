// Copyright 2025 Google LLC

#ifndef TELEMETRY_CLIENTLIB_INCLUDE_STATUS_H
#define TELEMETRY_CLIENTLIB_INCLUDE_STATUS_H

#include <stdint.h>

// TelemetryServiceStatus contains details of Telemetry Service statuses
// which are sent back to the client
struct TelemetryServiceStatus {
    // Associated status code providing additional context
    int32_t code;

    // Human-readable status message
    const char* message;
};

#endif  // TELEMETRY_CLIENTLIB_INCLUDE_STATUS_H
