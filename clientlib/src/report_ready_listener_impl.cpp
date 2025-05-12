// Copyright 2023 Google LLC

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
