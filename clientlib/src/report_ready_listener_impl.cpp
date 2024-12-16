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

    const char* metrics_config_uuid = metricsReportInfo.metricsConfigUuid.c_str();
    const char* report_config_name = metricsReportInfo.reportConfigName.c_str();
    const int32_t report_number = metricsReportInfo.reportNumber;

    MetricsReportInfo info{.metrics_config_uuid = metrics_config_uuid,
                           .report_config_name = report_config_name,
                           .report_number = report_number};
    mCallback.run(&info);
    return ndk::ScopedAStatus::ok();
}

}  // namespace google::sdv::telemetry
