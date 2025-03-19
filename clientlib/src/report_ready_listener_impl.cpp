// Copyright 2023 Google LLC

#include "report_ready_listener_impl.h"

#include "lib.h"

#include <android-base/logging.h>

namespace google::sdv::telemetry {

ReportReadyListenerImpl::ReportReadyListenerImpl(Callback<ReportReadyCallbackFunction> callback) :
      mCallback(std::move(callback)) {
    LOG(INFO) << "ReportReadyListenerImpl Created";
}

ndk::ScopedAStatus ReportReadyListenerImpl::onReady(const std::string& reportUuid) {
    LOG(DEBUG) << "ReportReadyListenerImpl::onReady, " << reportUuid;

    mCallback.run(reportUuid.c_str());
    return ndk::ScopedAStatus::ok();
}

}  // namespace google::sdv::telemetry
