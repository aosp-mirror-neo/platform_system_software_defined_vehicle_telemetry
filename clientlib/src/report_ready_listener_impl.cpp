// Copyright 2023 Google LLC. All Rights Reserved.

#define LOG_TAG "ReportReadyListenerImpl"

#include "report_ready_listener_impl.h"

namespace google::sdv::telemetry {

void ReportReadyListenerImpl::set_report_ready_listener(void* cbObject,
                                                        ReportReadyCallbackFunction cbFunc) {
    clientCb_ = cbObject;
    cbFuncPtr_ = cbFunc;
}

ndk::ScopedAStatus ReportReadyListenerImpl::onReady(const std::string& metricsConfigUuid,
                                                    const std::string& metricsReportConfigName,
                                                    int32_t reportNumber) {
    const char* configUuid = metricsConfigUuid.c_str();
    const char* reportConfigName = metricsReportConfigName.c_str();
    ALOGD("onReady %s %s report number: %d", configUuid, reportConfigName, reportNumber);
    cbFuncPtr_(clientCb_, configUuid, reportConfigName, reportNumber);
    return ndk::ScopedAStatus::ok();
}
}  // namespace google::sdv::telemetry