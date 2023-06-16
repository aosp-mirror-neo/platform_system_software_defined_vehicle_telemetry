// Copyright 2023 Google LLC. All Rights Reserved.

#ifndef TELEMETRY_CLIENTLIB_INCLUDE_LIB_H
#define TELEMETRY_CLIENTLIB_INCLUDE_LIB_H

#include <stddef.h>
#include <stdint.h>

// extern C functions should not be combined with namespaces, because extern "C" functions in
// different namespaces refer to the same entity and only one definition is allowed

#ifdef __cplusplus
extern "C" {
#endif
    // Opaque Pointer holding an instance of the telemetry_service defined in cpp.
    typedef struct ServiceHolder ServiceHolder;
    ServiceHolder* start_telemetry_service();
    void stop_telemetry_service(ServiceHolder* holder);
    void addMetricsConfig(ServiceHolder* holder, uint8_t* metrics_config, size_t size);
    void removeAllMetricsConfigs(ServiceHolder* holder);
    void setReportReadyListener(
        ServiceHolder* holder, void* listener, void (*cbFunc)(void*, const char*, size_t));

#ifdef __cplusplus
}
#endif

#endif // TELEMETRY_CLIENTLIB_INCLUDE_LIB_H
