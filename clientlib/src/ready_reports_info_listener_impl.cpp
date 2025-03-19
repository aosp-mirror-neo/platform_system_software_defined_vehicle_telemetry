// Copyright 2024 Google LLC

#include "ready_reports_info_listener_impl.h"

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
        const std::vector<std::string>& report_uuids) {
    std::vector<const char*> uuid_c_strings;
    uuid_c_strings.reserve(report_uuids.size());
    std::transform(report_uuids.begin(), report_uuids.end(), std::back_inserter(uuid_c_strings),
                   [](auto& uuid) { return uuid.c_str(); });
    mCallback.run(std::to_underlying(statusCode), statusMessage.c_str(), uuid_c_strings.data(),
                  uuid_c_strings.size());
    return ndk::ScopedAStatus::ok();
}

}  // namespace google::sdv::telemetry
