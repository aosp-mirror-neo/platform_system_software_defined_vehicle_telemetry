// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "lib.h"

#include "synchronized.h"
#include "telemetry_service_client.h"

#include <android-base/logging.h>

#include <memory>
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
            << name << " failed, error is "
            << ::aidl::google::sdv::telemetry::status::toString(status.code) << ": "
            << status.message;

    return std::to_underlying(status.code);
}

}  // namespace

ServiceHolder* get_telemetry_service() {
    // The current assumption is that the lifecycle of the TelemetryServiceClient is equal
    // to the lifecycle of the client application, hence it is defined as function static
    // here.
    static Synchronized<std::unique_ptr<google::sdv::telemetry::TelemetryServiceClient>> client;

    return client.lock([](auto& client) {
        if (!client) {
            auto maybeClient = google::sdv::telemetry::TelemetryServiceClient::Create();
            if (maybeClient.has_value()) {
                client = std::move(maybeClient.value());
            } else {
                LOG(ERROR) << "Failed to create TelemetryServiceClient: " << maybeClient.error();
            }
        }

        return client ? reinterpret_cast<ServiceHolder*>(client.get()) : nullptr;
    });
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

int32_t get_metrics_report(ServiceHolder* holder, const char* report_uuid, void* listener,
                           GetSerializedDataCallbackFunction cbFunc) {
    return callClient("getMetricsReport", holder, [&](auto& client) {
        return client.get_metrics_report(report_uuid, listener, cbFunc);
    });
}

int32_t get_ready_reports_info(ServiceHolder* holder, void* listener,
                               ReadyReportsInfoCallbackFunction cbFunc) {
    return callClient("getReadyReportsInfo", holder, [&](auto& client) {
        return client.get_ready_reports_info(listener, cbFunc);
    });
}

int32_t set_misc_status_listener(ServiceHolder* holder, void* misc_status_listener,
                                 MiscStatusCallbackFunction miscStatusCbFunc, void* api_listener,
                                 StatusCallbackFunction apiCbFunc,
                                 void** outPreviousStatusListener) {
    return callClient("registerMiscStatusListener", holder, [&](auto& client) {
        return client.set_misc_status_listener(misc_status_listener, miscStatusCbFunc, api_listener,
                                               apiCbFunc, outPreviousStatusListener);
    });
}

int32_t clear_misc_status_listener(ServiceHolder* holder, void** outPreviousStatusListener) {
    return callClient("unregisterMiscStatusListener", holder, [&](auto& client) {
        return client.clear_misc_status_listener(outPreviousStatusListener);
    });
}
