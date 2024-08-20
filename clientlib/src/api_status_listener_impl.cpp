// Copyright 2023 Google LLC

#include "api_status_listener_impl.h"

#include <android-base/logging.h>

namespace google::sdv::telemetry {

ApiStatusListenerImpl::ApiStatusListenerImpl(
        CallbackContainer<StatusCallbackFunction>& callbackContainer) :
      mCallbackContainer(callbackContainer) {
    LOG(INFO) << "ApiStatusListenerImpl created";
}

ndk::ScopedAStatus ApiStatusListenerImpl::sendStatus(int apiCallId,
                                                     TelemetryServiceStatusCodes statusCode,
                                                     const std::string& statusMessage) {
    LOG(DEBUG) << "Sending API status back to client. API call id: " << apiCallId
               << ", Status: " << ::aidl::google::sdv::telemetry::toString(statusCode)
               << ", Message: " << statusMessage;

    auto callback = mCallbackContainer.extract(apiCallId);
    if (callback) {
        callback->run(std::to_underlying(statusCode), statusMessage.c_str());
    } else {
        LOG(ERROR) << "Callback not found for API call id: " << apiCallId;
    }

    return ndk::ScopedAStatus::ok();
}
}  // namespace google::sdv::telemetry
