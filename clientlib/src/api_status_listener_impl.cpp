// Copyright 2023 Google LLC. All Rights Reserved.

#define LOG_TAG "ApiStatusListenerImpl"

#include "api_status_listener_impl.h"

namespace google::sdv::telemetry {

ApiStatusListenerImpl::ApiStatusListenerImpl(StatusCallbackContainer& cb_container) :
      cb_container_(cb_container) {
    ALOGI("Created");
}

ndk::ScopedAStatus ApiStatusListenerImpl::sendStatus(int api_call_id,
                                                     TelemetryServiceStatusCodes status_code,
                                                     const std::string& status_message) {
    int int_status_code =
            static_cast<std::underlying_type_t<TelemetryServiceStatusCodes>>(status_code);

    ALOGD("sending status back to client. API call id: %d, Status: %s, Message: %s", api_call_id,
          ::aidl::google::sdv::telemetry::toString(status_code).c_str(), status_message.c_str());

    auto cb_data = cb_container_.extract_callback(api_call_id);
    if (cb_data) {
        StatusCallbackFunction func = cb_data->cb_function;
        func(cb_data->cb_object, int_status_code, status_message.c_str());
    } else {
        ALOGE("Callback not found for API id: %d", api_call_id);
    }

    return ndk::ScopedAStatus::ok();
}
}  // namespace google::sdv::telemetry