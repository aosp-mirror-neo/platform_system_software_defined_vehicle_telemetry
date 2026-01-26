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
               << ", Status: " << ::aidl::google::sdv::telemetry::status::toString(statusCode)
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
