// Copyright 2023 Google LLC

#include "misc_status_listener_impl.h"

#include "status.h"

#include <android-base/logging.h>

namespace google::sdv::telemetry {

MiscStatusListenerImpl::MiscStatusListenerImpl(Callback<MiscStatusCallbackFunction> callback) :
      mCallback(std::move(callback)) {
    LOG(INFO) << "MiscStatusListenerImpl created";
}

ndk::ScopedAStatus MiscStatusListenerImpl::sendStatus(
        const ::aidl::google::sdv::telemetry::status::TelemetryServiceStatus& status) {
    LOG(DEBUG) << "Sending misc status back to client. Status: "
               << ::aidl::google::sdv::telemetry::status::toString(status.code)
               << ", Message: " << status.message;

    const auto telemetry_status = TelemetryServiceStatus{
            .code = std::to_underlying(status.code),
            .message = status.message.c_str(),
    };
    mCallback.run(&telemetry_status);
    return ndk::ScopedAStatus::ok();
}

}  // namespace google::sdv::telemetry
