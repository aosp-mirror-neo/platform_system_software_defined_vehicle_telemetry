// Copyright 2023 Google LLC. All Rights Reserved.

#define LOG_TAG "ReportListenerImpl"

#include "report_listener_impl.h"

namespace google::sdv::telemetry {

ReportListenerImpl::ReportListenerImpl(void* cb, GetSerializedDataCallbackFunction cbFunc) :
      clientReportListener_(cb), cbFuncPtr_(cbFunc) {
    ALOGI("Created");
}

ndk::ScopedAStatus ReportListenerImpl::onReport(int32_t status_code,
                                                const std::string& status_message,
                                                const std::vector<uint8_t>& report) {
    cbFuncPtr_(clientReportListener_, status_code, status_message.c_str(), report.data(),
               report.size());
    return ndk::ScopedAStatus::ok();
}
}  // namespace google::sdv::telemetry