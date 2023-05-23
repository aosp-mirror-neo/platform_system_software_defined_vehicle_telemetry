// Copyright 2023 Google LLC. All Rights Reserved.

#include "lib.h"
#include "telemetry_service.h"

ServiceHolder* start_telemetry_service() {
    // The current assumption is that the lifecycle of the TelemetryService is equal
    // to the lifecycle of the client application, hence it is defined as function static
    // here. This makes it more convenient to implement thread-safety when multiple threads
    // are calling the start_telemetry_service.
    static android::sdv::telemetry::TelemetryService* service =
                new android::sdv::telemetry::TelemetryService();
    return reinterpret_cast<ServiceHolder*>(service);
}

void stop_telemetry_service(ServiceHolder* holder){
    // Note: Kept for future use. In case variables need to be freed explicitly.
}

void addMetricsConfig(
    ServiceHolder* holder, char* config_name, uint8_t* metrics_config, size_t size)
{
    android::sdv::telemetry::TelemetryService* service =
                reinterpret_cast<android::sdv::telemetry::TelemetryService*>(holder);
    std::vector<uint8_t> vec(metrics_config, metrics_config+size);
    service->addMetricsConfig(android::String16(config_name), vec);
}

void removeAllMetricsConfigs(ServiceHolder* holder){
    android::sdv::telemetry::TelemetryService* service =
                reinterpret_cast<android::sdv::telemetry::TelemetryService*>(holder);
    service->removeAllMetricsConfigs();
}

void setReportReadyListener(
    ServiceHolder* holder, void* listener, void (*cbFunc)(void*, const char*, size_t))
{
    android::sdv::telemetry::TelemetryService* service =
                reinterpret_cast<android::sdv::telemetry::TelemetryService*>(holder);
    service->setReportReadyListener(listener,cbFunc);
}
