// Copyright 2023 Google LLC. All Rights Reserved.

#include "lib.h"
#include "telemetry_service.h"

ServiceHolder* get_telemetry_service() {
    // The current assumption is that the lifecycle of the TelemetryService is equal
    // to the lifecycle of the client application, hence it is defined as function static
    // here. This makes it more convenient to implement thread-safety when multiple threads
    // are calling the get_telemetry_service.
    static android::sdv::telemetry::TelemetryService* service =
                new android::sdv::telemetry::TelemetryService();
    return reinterpret_cast<ServiceHolder*>(service);
}

int stop_telemetry_service(ServiceHolder* holder) {
    // Note: Kept for future use. In case variables need to be freed explicitly.
    return 0;
}

int add_metrics_config(ServiceHolder* holder, uint8_t* metrics_config, size_t size, void* listener,
                       StatusCallbackFunction cbFunc) {
    android::sdv::telemetry::TelemetryService* service =
                reinterpret_cast<android::sdv::telemetry::TelemetryService*>(holder);
    std::vector<uint8_t> vec(metrics_config, metrics_config+size);
    return static_cast<int>(service->add_metrics_config(vec, listener, cbFunc));
}

int activate_metrics_config(ServiceHolder* holder, const char* config_uuid, void* listener,
                            StatusCallbackFunction cbFunc) {
    android::sdv::telemetry::TelemetryService* service =
            reinterpret_cast<android::sdv::telemetry::TelemetryService*>(holder);
    return static_cast<int>(service->activate_metrics_config(config_uuid, listener, cbFunc));
}

int deactivate_metrics_config(ServiceHolder* holder, const char* config_uuid, void* listener,
                              StatusCallbackFunction cbFunc) {
    android::sdv::telemetry::TelemetryService* service =
            reinterpret_cast<android::sdv::telemetry::TelemetryService*>(holder);
    return static_cast<int>(service->deactivate_metrics_config(config_uuid, listener, cbFunc));
}

int remove_metrics_config(ServiceHolder* holder, const char* config_uuid, void* listener,
                          StatusCallbackFunction cbFunc) {
    android::sdv::telemetry::TelemetryService* service =
            reinterpret_cast<android::sdv::telemetry::TelemetryService*>(holder);
    return static_cast<int>(service->remove_metrics_config(config_uuid, listener, cbFunc));
}

int remove_all_metrics_configs(ServiceHolder* holder, void* listener,
                               StatusCallbackFunction cbFunc) {
    android::sdv::telemetry::TelemetryService* service =
            reinterpret_cast<android::sdv::telemetry::TelemetryService*>(holder);
    return static_cast<int>(service->remove_all_metrics_configs(listener, cbFunc));
}

int set_report_ready_listener(ServiceHolder* holder, void* listener,
                              StatusCallbackFunction cbFunc) {
    android::sdv::telemetry::TelemetryService* service =
                reinterpret_cast<android::sdv::telemetry::TelemetryService*>(holder);
    return static_cast<int>(service->set_report_ready_listener(listener, cbFunc));
}

int clear_report_ready_listener(ServiceHolder* holder) {
    android::sdv::telemetry::TelemetryService* service =
            reinterpret_cast<android::sdv::telemetry::TelemetryService*>(holder);
    return static_cast<int>(service->clear_report_ready_listener());
}

int get_metrics_report(ServiceHolder* holder, int32_t report_number, const char* report_uuid,
                       void* listener, GetReportCallbackFunction cbFunc) {
    android::sdv::telemetry::TelemetryService* service =
            reinterpret_cast<android::sdv::telemetry::TelemetryService*>(holder);
    return static_cast<int>(
            service->get_metrics_report(report_number, report_uuid, listener, cbFunc));
}

int set_status_listener(ServiceHolder* holder, void* listener, StatusCallbackFunction cbFunc) {
    android::sdv::telemetry::TelemetryService* service =
            reinterpret_cast<android::sdv::telemetry::TelemetryService*>(holder);
    return static_cast<int>(service->set_status_listener(listener, cbFunc));
}

int clear_status_listener(ServiceHolder* holder) {
    android::sdv::telemetry::TelemetryService* service =
            reinterpret_cast<android::sdv::telemetry::TelemetryService*>(holder);
    return static_cast<int>(service->clear_status_listener());
}
