// Copyright 2023 Google LLC

#define LOG_TAG "ReportReadyListenerImpl"

#include "report_ready_listener_impl.h"

namespace google::sdv::telemetry {

ReportReadyListenerImpl::ReportReadyListenerImpl(void* clientCb,
                                                 ReportReadyCallbackFunction cbFunc) :
      clientCb_(clientCb), cbFunc_(cbFunc) {
    ALOGI("Created");
}

ndk::ScopedAStatus ReportReadyListenerImpl::onReady(const std::string& metricsConfigUuid,
                                                    const std::string& metricsReportConfigName,
                                                    int32_t reportNumber) {
    const char* configUuid = metricsConfigUuid.c_str();
    const char* reportConfigName = metricsReportConfigName.c_str();
    ALOGD("onReady %s %s report number: %d", configUuid, reportConfigName, reportNumber);
    cbFunc_(clientCb_, configUuid, reportConfigName, reportNumber);
    return ndk::ScopedAStatus::ok();
}
}  // namespace google::sdv::telemetry
