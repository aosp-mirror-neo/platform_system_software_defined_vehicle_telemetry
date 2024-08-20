// Copyright 2023 Google LLC

#include "report_ready_listener_impl.h"

#include <android-base/logging.h>

namespace google::sdv::telemetry {

ReportReadyListenerImpl::ReportReadyListenerImpl(Callback<ReportReadyCallbackFunction> callback) :
      mCallback(std::move(callback)) {
    LOG(INFO) << "ReportReadyListenerImpl Created";
}

ndk::ScopedAStatus ReportReadyListenerImpl::onReady(const std::string& metricsConfigUuid,
                                                    const std::string& metricsReportConfigName,
                                                    int32_t reportNumber) {
    const char* configUuid = metricsConfigUuid.c_str();
    const char* reportConfigName = metricsReportConfigName.c_str();
    LOG(DEBUG) << "ReportReadyListenerImpl::onReady, UUID: " << configUuid
               << ", report config name: " << reportConfigName
               << ", report number: " << reportNumber;
    mCallback.run(configUuid, reportConfigName, reportNumber);
    return ndk::ScopedAStatus::ok();
}

}  // namespace google::sdv::telemetry
