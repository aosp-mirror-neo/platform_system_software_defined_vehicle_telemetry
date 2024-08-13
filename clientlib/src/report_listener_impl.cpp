// Copyright 2023 Google LLC

#include "report_listener_impl.h"

#include <android-base/logging.h>

namespace google::sdv::telemetry {

ReportListenerImpl::ReportListenerImpl(Callback<GetSerializedDataCallbackFunction> callback) :
      mCallback(std::move(callback)) {
    LOG(INFO) << "ReportListenerImpl created";
}

ndk::ScopedAStatus ReportListenerImpl::onReport(TelemetryServiceStatusCodes statusCode,
                                                const std::string& statusMessage,
                                                const std::vector<uint8_t>& report) {
    mCallback.run(std::to_underlying(statusCode), statusMessage.c_str(), report.data(),
                  report.size());
    return ndk::ScopedAStatus::ok();
}

}  // namespace google::sdv::telemetry
