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

#include "report_ready_listener_impl.h"

#include "lib.h"

#include <android-base/logging.h>

namespace google::sdv::telemetry {

ReportReadyListenerImpl::ReportReadyListenerImpl(Callback<ReportReadyCallbackFunction> callback) :
      mCallback(std::move(callback)) {
    LOG(INFO) << "ReportReadyListenerImpl Created";
}

ndk::ScopedAStatus ReportReadyListenerImpl::onReady(
        const aidl::google::sdv::telemetry::MetricsReportInfo& metricsReportInfo) {
    LOG(DEBUG) << "ReportReadyListenerImpl::onReady, " << metricsReportInfo.toString();

    const char* config_uuid = metricsReportInfo.configUuid.c_str();
    const char* report_uuid = metricsReportInfo.reportUuid.c_str();

    MetricsReportInfo info{.config_uuid = config_uuid, .report_uuid = report_uuid};
    mCallback.run(&info);
    return ndk::ScopedAStatus::ok();
}

}  // namespace google::sdv::telemetry
