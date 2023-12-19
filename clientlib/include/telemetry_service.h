// Copyright 2023 Google LLC. All Rights Reserved.

#ifndef TELEMETRY_CLIENTLIB_INCLUDE_TELEMETRY_SERVICE_H
#define TELEMETRY_CLIENTLIB_INCLUDE_TELEMETRY_SERVICE_H

#include "callback_container.h"
#include "report_listener_impl.h"
#include "report_ready_listener_impl.h"
#include "status_listener_impl.h"

#include <android-base/stringprintf.h>
#include <android/sdv/telemetry/BnTelemetryService.h>
#include <android/sdv/telemetry/TelemetryServiceStatusCodes.h>
#include <binder/IServiceManager.h>
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

using ::android::sdv::telemetry::ITelemetryService;

namespace android::sdv::telemetry {

// Acts as the mediator between the OEM client and the telemetry service.
class TelemetryService {
public:
    TelemetryService();

    TelemetryServiceStatusCodes activate_metrics_config(const std::string& metricsConfigUuid,
                                                        void* listener, CbFunction cbFunc);
    TelemetryServiceStatusCodes deactivate_metrics_config(const std::string& metricsConfigUuid,
                                                          void* listener, CbFunction cbFunc);
    TelemetryServiceStatusCodes add_metrics_config(const std::vector<uint8_t>& metricConfig,
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
    std::atomic<int> api_id_;
    android::sp<CallbackContainer> cb_container_;
    android::sp<StatusListenerImpl> mStatusListenerImpl_;
    android::sp<ReportReadyListenerImpl> mReportReadyListener_;

    // holds a reference to the binder service
    android::sp<android::sdv::telemetry::ITelemetryService> binder_service_;
};
}  // namespace android::sdv::telemetry

#endif // TELEMETRY_CLIENTLIB_INCLUDE_TELEMETRY_SERVICE_H
