// Copyright 2023 Google LLC. All Rights Reserved.

#ifndef TELEMETRY_CLIENTLIB_INCLUDE_TELEMETRY_SERVICE_H
#define TELEMETRY_CLIENTLIB_INCLUDE_TELEMETRY_SERVICE_H

#include "callback_container.h"
#include "report_listener_impl.h"
#include "report_ready_listener_impl.h"
#include "status_listener_impl.h"

#include <android-base/expected.h>
#include <android-base/stringprintf.h>
#include <binder/IServiceManager.h>
#include <google/sdv/telemetry/BnTelemetryService.h>
#include <google/sdv/telemetry/TelemetryServiceStatusCodes.h>
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

using ::google::sdv::telemetry::ITelemetryService;

namespace google::sdv::telemetry {

// Acts as the mediator between the OEM client and the telemetry service.
class TelemetryService {
public:
    static android::base::expected<std::unique_ptr<TelemetryService>, android::binder::Status>
    Create();

    TelemetryService(const TelemetryService&) = delete;
    TelemetryService& operator=(const TelemetryService&) = delete;

    TelemetryServiceStatusCodes activate_metrics_config(const std::string& metricsConfigUuid,
                                                        void* listener, CbFunction cbFunc);
    TelemetryServiceStatusCodes deactivate_metrics_config(const std::string& metricsConfigUuid,
                                                          void* listener, CbFunction cbFunc);
    TelemetryServiceStatusCodes add_metrics_config(const std::vector<uint8_t>& metricsConfig,
                                                   void* listener, CbFunction cbFunc);
    TelemetryServiceStatusCodes remove_all_metrics_configs(void* listener, CbFunction cbFunc);
    TelemetryServiceStatusCodes remove_metrics_config(const std::string& metricsConfigUuid,
                                                      void* listener, CbFunction cbFunc);

    TelemetryServiceStatusCodes set_report_ready_listener(void* listener,
                                                          ReportReadyCallbackFunction cbFunc);
    TelemetryServiceStatusCodes clear_report_ready_listener();
    TelemetryServiceStatusCodes get_metrics_report(const char* metrics_config_uuid,
                                                   const char* report_config_name,
                                                   int32_t report_number, void* listener,
                                                   GetReportCallbackFunction cbFunc);
    TelemetryServiceStatusCodes set_status_listener(void* listener, CbFunction cbFunc);
    TelemetryServiceStatusCodes clear_status_listener();

private:
    using ApiId = int;

    explicit TelemetryService(
            android::sp<google::sdv::telemetry::ITelemetryService> binder_service);

    android::binder::Status registerListeners();

    ApiId getNextApiId();

    std::atomic<ApiId> mLastApiId = 1;
    CallbackContainer mCallbackContainer;
    android::sp<StatusListenerImpl> mStatusListenerImpl =
            android::sp<StatusListenerImpl>::make(mCallbackContainer);
    android::sp<ReportReadyListenerImpl> mReportReadyListener =
            android::sp<ReportReadyListenerImpl>::make();

    android::sp<google::sdv::telemetry::ITelemetryService> mBinderService;
};
}  // namespace google::sdv::telemetry

#endif // TELEMETRY_CLIENTLIB_INCLUDE_TELEMETRY_SERVICE_H
