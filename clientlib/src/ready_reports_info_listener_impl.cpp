// Copyright 2024 Google LLC

#include "ready_reports_info_listener_impl.h"

#include "aidl/google/sdv/telemetry/MetricsReportInfo.h"
#include "lib.h"

#include <android-base/logging.h>

#include <cstdint>

namespace google::sdv::telemetry {

ReadyReportsInfoListenerImpl::ReadyReportsInfoListenerImpl(
        Callback<ReadyReportsInfoCallbackFunction> callback) :
      mCallback(std::move(callback)) {
    LOG(INFO) << "ReadyReportsInfoListenerImpl Created";
}

ndk::ScopedAStatus ReadyReportsInfoListenerImpl::sendReadyReportsInfo(
        TelemetryServiceStatusCodes statusCode, const std::string& statusMessage,
        const std::vector<aidl::google::sdv::telemetry::MetricsReportInfo>& ready_reports_info) {
    std::vector<MetricsReportInfo> converted_info;
    converted_info.reserve(ready_reports_info.size());
    for (const auto& report_info : ready_reports_info) {
        converted_info.emplace_back(MetricsReportInfo{report_info.metricsConfigUuid.c_str(),
                                                      report_info.reportConfigName.c_str(),
                                                      report_info.reportNumber});
    }
    mCallback.run(std::to_underlying(statusCode), statusMessage.c_str(), converted_info.data(),
                  converted_info.size());
    return ndk::ScopedAStatus::ok();
}

}  // namespace google::sdv::telemetry
