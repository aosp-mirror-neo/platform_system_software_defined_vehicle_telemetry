// Copyright 2023 Google LLC

#include "lib.h"

#include "telemetry_service_client.h"

#include <android-base/logging.h>

#include <memory>
#include <mutex>
#include <span>
#include <string_view>
#include <utility>

namespace {

template <typename F>
int32_t callClient(const std::string& name, ServiceHolder* holder, F&& fn) {
    LOG(DEBUG) << name;

    google::sdv::telemetry::TelemetryServiceClient* client =
            reinterpret_cast<google::sdv::telemetry::TelemetryServiceClient*>(holder);
    google::sdv::telemetry::Status status = fn(*client);

    LOG_IF(ERROR, status.code != TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS)
            << name << " failed, error is " << ::aidl::google::sdv::telemetry::toString(status.code)
            << ": " << status.message;

    return std::to_underlying(status.code);
}

}  // namespace

ServiceHolder* get_telemetry_service() {
    // The current assumption is that the lifecycle of the TelemetryServiceClient is equal
    // to the lifecycle of the client application, hence it is defined as function static
    // here.
    static std::unique_ptr<google::sdv::telemetry::TelemetryServiceClient> client;
    static std::mutex mutex;

    std::lock_guard<std::mutex> guard(mutex);
    if (!client) {
        auto maybeClient = google::sdv::telemetry::TelemetryServiceClient::Create();
        if (maybeClient.has_value()) {
            client = std::move(maybeClient.value());
        } else {
            LOG(ERROR) << "Failed to create TelemetryServiceClient: " << maybeClient.error();
        }
    }

    return client ? reinterpret_cast<ServiceHolder*>(client.get()) : nullptr;
}

int32_t add_metrics_config(ServiceHolder* holder, uint8_t* metrics_config,
                           size_t metrics_config_size, void* listener,
                           StatusCallbackFunction cbFunc) {
    return callClient("addMetricsConfig", holder, [&](auto& client) {
        return client.add_metrics_config(std::vector(metrics_config,
                                                     metrics_config + metrics_config_size),
                                         listener, cbFunc);
    });
}

int32_t activate_metrics_config(ServiceHolder* holder, const char* config_uuid, void* listener,
                                StatusCallbackFunction cbFunc) {
    return callClient("activateMetricsConfig", holder, [&](auto& client) {
        return client.activate_metrics_config(config_uuid, listener, cbFunc);
    });
}

int32_t deactivate_metrics_config(ServiceHolder* holder, const char* config_uuid, void* listener,
                                  StatusCallbackFunction cbFunc) {
    return callClient("deactivateMetricsConfig", holder, [&](auto& client) {
        return client.deactivate_metrics_config(config_uuid, listener, cbFunc);
    });
}

int32_t get_active_metrics_config_uuids(ServiceHolder* holder, void* listener,
                                        ConfigListCallbackFunction cbFunc) {
    return callClient("getActiveMetricsConfigUuids", holder, [&](auto& client) {
        return client.get_active_metrics_config_uuids(listener, cbFunc);
    });
}

int32_t get_inactive_metrics_config_uuids(ServiceHolder* holder, void* listener,
                                          ConfigListCallbackFunction cbFunc) {
    return callClient("getInactiveMetricsConfigUuids", holder, [&](auto& client) {
        return client.get_inactive_metrics_config_uuids(listener, cbFunc);
    });
}

int32_t remove_metrics_config(ServiceHolder* holder, const char* config_uuid, void* listener,
                              StatusCallbackFunction cbFunc) {
    return callClient("removeMetricsConfig", holder, [&](auto& client) {
        return client.remove_metrics_config(config_uuid, listener, cbFunc);
    });
}

int32_t remove_all_metrics_configs(ServiceHolder* holder, void* listener,
                                   StatusCallbackFunction cbFunc) {
    return callClient("removeAllMetricsConfigs", holder, [&](auto& client) {
        return client.remove_all_metrics_configs(listener, cbFunc);
    });
}

int32_t set_report_ready_listener(ServiceHolder* holder, void* reportListener,
                                  ReportReadyCallbackFunction reportReadyCbFunc, void* apiListener,
                                  StatusCallbackFunction cbFunc, void** outPreviousReportListener) {
    return callClient("registerReportReadyListener", holder, [&](auto& client) {
        return client.set_report_ready_listener(reportListener, reportReadyCbFunc, apiListener,
                                                cbFunc, outPreviousReportListener);
    });
}

int32_t clear_report_ready_listener(ServiceHolder* holder, void** outPreviousReportListener) {
    return callClient("unregisterReportReadyListener", holder, [&](auto& client) {
        return client.clear_report_ready_listener(outPreviousReportListener);
    });
}

int32_t get_metrics_report(ServiceHolder* holder, const char* metrics_config_uuid,
                           const char* report_config_name, int32_t report_number, void* listener,
                           GetSerializedDataCallbackFunction cbFunc) {
    return callClient("getMetricsReport", holder, [&](auto& client) {
        return client.get_metrics_report(metrics_config_uuid, report_config_name, report_number,
                                         listener, cbFunc);
    });
}

int32_t set_misc_status_listener(ServiceHolder* holder, void* status_listener,
                                 StatusCallbackFunction statusCbFunc, void* api_listener,
                                 StatusCallbackFunction apiCbFunc,
                                 void** outPreviousStatusListener) {
    return callClient("registerMiscStatusListener", holder, [&](auto& client) {
        return client.set_misc_status_listener(status_listener, statusCbFunc, api_listener,
                                               apiCbFunc, outPreviousStatusListener);
    });
}

int32_t clear_misc_status_listener(ServiceHolder* holder, void** outPreviousStatusListener) {
    return callClient("unregisterMiscStatusListener", holder, [&](auto& client) {
        return client.clear_misc_status_listener(outPreviousStatusListener);
    });
}
