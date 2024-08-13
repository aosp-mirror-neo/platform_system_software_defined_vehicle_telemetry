// Copyright 2023 Google LLC

#include "misc_status_listener_impl.h"

#include <android-base/logging.h>

namespace google::sdv::telemetry {

MiscStatusListenerImpl::MiscStatusListenerImpl(Callback<StatusCallbackFunction> callback) :
      mCallback(std::move(callback)) {
    LOG(INFO) << "MiscStatusListenerImpl created";
}

ndk::ScopedAStatus MiscStatusListenerImpl::sendStatus(TelemetryServiceStatusCodes statusCode,
                                                      const std::string& statusMessage) {
    LOG(DEBUG) << "Sending misc status back to client. Status: "
               << ::aidl::google::sdv::telemetry::toString(statusCode)
               << ", Message: " << statusMessage;

    mCallback.run(std::to_underlying(statusCode), statusMessage.c_str());
    return ndk::ScopedAStatus::ok();
}

}  // namespace google::sdv::telemetry
