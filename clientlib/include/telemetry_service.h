// Copyright 2023 Google LLC. All Rights Reserved.

#ifndef TELEMETRY_CLIENTLIB_INCLUDE_TELEMETRY_SERVICE_H
#define TELEMETRY_CLIENTLIB_INCLUDE_TELEMETRY_SERVICE_H

#include "api_status_listener_impl.h"
#include "callback_container.h"
#include "misc_status_listener_impl.h"
#include "report_listener_impl.h"
#include "report_ready_listener_impl.h"

#include <aidl/google/sdv/telemetry/BnTelemetryService.h>
#include <aidl/google/sdv/telemetry/TelemetryServiceStatusCodes.h>
#include <android-base/expected.h>
#include <android-base/stringprintf.h>
#include <android/binder_manager.h>
#include <utils/Log.h>

#include <getopt.h>
#include <stdlib.h>
#include <sysexits.h>
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <iostream>
#include <memory>
#include <vector>

using aidl::google::sdv::telemetry::ITelemetryService;
using aidl::google::sdv::telemetry::TelemetryServiceStatusCodes;

namespace google::sdv::telemetry {

// Acts as the mediator between the OEM client and the telemetry service.
class TelemetryService {
public:
    static android::base::expected<std::unique_ptr<TelemetryService>, ndk::ScopedAStatus> Create();

    TelemetryService(const TelemetryService&) = delete;
    TelemetryService& operator=(const TelemetryService&) = delete;

    TelemetryServiceStatusCodes activate_metrics_config(const std::string& metricsConfigUuid,
                                                        void* listener,
                                                        StatusCallbackFunction cbFunc);
    TelemetryServiceStatusCodes deactivate_metrics_config(const std::string& metricsConfigUuid,
                                                          void* listener,
                                                          StatusCallbackFunction cbFunc);
    TelemetryServiceStatusCodes add_metrics_config(const std::vector<uint8_t>& metricConfig,
                                                   void* listener, StatusCallbackFunction cbFunc);
    TelemetryServiceStatusCodes remove_all_metrics_configs(void* listener,
                                                           StatusCallbackFunction cbFunc);
    TelemetryServiceStatusCodes remove_metrics_config(const std::string& metricsConfigUuid,
                                                      void* listener,
                                                      StatusCallbackFunction cbFunc);

    TelemetryServiceStatusCodes set_report_ready_listener(
            void* report_listener, ReportReadyCallbackFunction reportReadyCbFunc,
            void* api_listener, StatusCallbackFunction cbFunc);
    TelemetryServiceStatusCodes clear_report_ready_listener();
    TelemetryServiceStatusCodes get_metrics_report(const char* metrics_config_uuid,
                                                   const char* report_config_name,
                                                   int32_t report_number, void* listener,
                                                   GetReportCallbackFunction cbFunc);
    TelemetryServiceStatusCodes set_misc_status_listener(void* status_listener,
                                                         StatusCallbackFunction statusCbFunc,
                                                         void* api_listener,
                                                         StatusCallbackFunction apiCbFunc);
    TelemetryServiceStatusCodes clear_misc_status_listener();

private:
    using ApiId = int;

    explicit TelemetryService(std::shared_ptr<ITelemetryService> binder_service);

    ndk::ScopedAStatus registerListeners();

    ApiId getNextApiId();

    std::atomic<ApiId> mLastApiId = 1;
    StatusCallbackContainer mStatusCallbackContainer;
    std::shared_ptr<ApiStatusListenerImpl> mApiStatusListenerImpl =
            ndk::SharedRefBase::make<ApiStatusListenerImpl>(mStatusCallbackContainer);

    std::shared_ptr<ITelemetryService> mBinderService;
};
}  // namespace google::sdv::telemetry

#endif // TELEMETRY_CLIENTLIB_INCLUDE_TELEMETRY_SERVICE_H
