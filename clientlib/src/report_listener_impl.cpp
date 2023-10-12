// Copyright 2023 Google LLC. All Rights Reserved.

#define LOG_TAG "ReportListenerImpl"

#include "report_listener_impl.h"

namespace android::sdv::telemetry {

ReportListenerImpl::ReportListenerImpl(void* cb, GetReportCallbackFunction cbFunc) :
      clientReportListener_(cb), cbFuncPtr_(cbFunc) {
    ALOGI("Created");
}

android::binder::Status ReportListenerImpl::onReport(int32_t status_code,
                                                     const std::string& status_message,
                                                     const std::vector<uint8_t>& report) {
    cbFuncPtr_(clientReportListener_, status_code, status_message.c_str(), &report[0],
               report.size());
    return android::binder::Status::ok();
}
}  // namespace android::sdv::telemetry