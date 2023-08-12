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
    ServiceHolder* get_telemetry_service();
    void stop_telemetry_service(ServiceHolder* holder);
    void add_metrics_config(ServiceHolder* holder, uint8_t* metrics_config, size_t size);
    void activate_metrics_config(ServiceHolder* holder, const char* uuid);
    void deactivate_metrics_config(ServiceHolder* holder, const char* uuid);
    void remove_metrics_config(ServiceHolder* holder, const char* uuid);
    void remove_all_metrics_configs(ServiceHolder* holder);
    void set_report_ready_listener(ServiceHolder* holder, void* listener,
                                   void (*cbFunc)(void*, const char*));

#ifdef __cplusplus
}
#endif

#endif // TELEMETRY_CLIENTLIB_INCLUDE_LIB_H
