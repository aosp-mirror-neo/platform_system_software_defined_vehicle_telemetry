// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
        const std::vector<aidl::google::sdv::telemetry::MetricsReportInfo>& ready_reports_info) {
    std::vector<MetricsReportInfo> info;
    info.reserve(ready_reports_info.size());
    for (const auto& report_info : ready_reports_info) {
        info.emplace_back(MetricsReportInfo{.config_uuid = report_info.configUuid.c_str(),
                                            .report_uuid = report_info.reportUuid.c_str()});
    }
    mCallback.run(std::to_underlying(statusCode), statusMessage.c_str(), info.data(), info.size());
    return ndk::ScopedAStatus::ok();
}

}  // namespace google::sdv::telemetry
