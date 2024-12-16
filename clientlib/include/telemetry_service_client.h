// Copyright 2023 Google LLC

#pragma once

#include "api_status_listener_impl.h"
#include "callback_container.h"
#include "callback_functions.h"
#include "config_list_listener_impl.h"
#include "misc_status_listener_impl.h"
#include "report_listener_impl.h"
#include "report_ready_listener_impl.h"

#include <aidl/google/sdv/telemetry/BnTelemetryService.h>
#include <aidl/google/sdv/telemetry/TelemetryServiceStatusCodes.h>
#include <android-base/expected.h>
#include <android/binder_manager.h>

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

// Wraps a `TelemetryServiceStatusCodes` and an optional `std::string` message.
struct Status {
    static Status ok();
    static Status fromBinderStatus(const ndk::ScopedAStatus& status);

    explicit Status(TelemetryServiceStatusCodes code, std::string message = "");

    TelemetryServiceStatusCodes code;
    std::string message;
};

// Acts as the mediator between the OEM client and the telemetry service.
class TelemetryServiceClient {
public:
    static android::base::expected<std::unique_ptr<TelemetryServiceClient>, ndk::ScopedAStatus>
    Create();

    TelemetryServiceClient(const TelemetryServiceClient&) = delete;
    TelemetryServiceClient& operator=(const TelemetryServiceClient&) = delete;
    ~TelemetryServiceClient();

    Status activate_metrics_config(const std::string& metricsConfigUuid, void* statusCbData,
                                   StatusCallbackFunction statusCb);
    Status deactivate_metrics_config(const std::string& metricsConfigUuid, void* cbData,
                                     StatusCallbackFunction statusCb);

    Status add_metrics_config(const std::vector<uint8_t>& metricConfig, void* statusCbData,
                              StatusCallbackFunction statusCb);
    Status remove_all_metrics_configs(void* statusCbData, StatusCallbackFunction statusCb);
    Status remove_metrics_config(const std::string& metricsConfigUuid, void* statusCbData,
                                 StatusCallbackFunction statusCb);

    Status get_active_metrics_config_uuids(void* configListCbData,
                                           ConfigListCallbackFunction configListCb);

    Status get_inactive_metrics_config_uuids(void* configListCbData,
                                             ConfigListCallbackFunction configListCb);

    Status set_report_ready_listener(void* reportReadyCbData,
                                     ReportReadyCallbackFunction reportReadyCb, void* statusCbData,
                                     StatusCallbackFunction statusCb,
                                     void** outPreviousReportReadyCbData);
    Status clear_report_ready_listener(void** outPreviousReportReadyCbData);

    Status get_metrics_report(const MetricsReportInfo* metricsReportInfo, void* metricsReportCbData,
                              GetSerializedDataCallbackFunction metricsReportCb);

    Status get_ready_reports_info(void* readyReportsInfoCbData,
                                  ReadyReportsInfoCallbackFunction readyReportsInfoCb);

    Status set_misc_status_listener(void* miscStatusCbData, StatusCallbackFunction miscStatusCb,
                                    void* statusCbData, StatusCallbackFunction statusCb,
                                    void** outPreviousMiscStatusCbData);
    Status clear_misc_status_listener(void** outPreviousMiscStatusCbData);

private:
    using ApiCallId = int;

    explicit TelemetryServiceClient(std::shared_ptr<ITelemetryService> binderService);

    ndk::ScopedAStatus registerListeners();

    template <typename T, typename F>
    Status callWithApiCallId(void* cbData, T&& cb, F&& callFn) {
        ApiCallId apiCallId = getNextApiCallId();
        bool success = mCallbackContainer.insert(apiCallId, Callback(cbData, std::move(cb)));
        if (!success) {
            return Status(TelemetryServiceStatusCodes::API_CALL_FAILED_INTERNAL_ERROR,
                          "Inserting status callback failed.");
        }
        ndk::ScopedAStatus status = callFn(apiCallId);
        return Status::fromBinderStatus(status);
    }

    ApiCallId getNextApiCallId();

    std::atomic<ApiCallId> mLastApiCallId = 1;
    CallbackContainer<StatusCallbackFunction> mCallbackContainer;
    std::shared_ptr<ApiStatusListenerImpl> mApiStatusListenerImpl =
            ndk::SharedRefBase::make<ApiStatusListenerImpl>(mCallbackContainer);
    std::shared_ptr<ReportReadyListenerImpl> mReportReadyListener;
    std::shared_ptr<MiscStatusListenerImpl> mMiscStatusListener;

    std::shared_ptr<ITelemetryService> mBinderService;
};

}  // namespace google::sdv::telemetry
