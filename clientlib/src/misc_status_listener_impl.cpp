// Copyright 2023 Google LLC

#define LOG_TAG "MiscStatusListenerImpl"

#include "misc_status_listener_impl.h"

namespace google::sdv::telemetry {

MiscStatusListenerImpl::MiscStatusListenerImpl(void* cb_object, StatusCallbackFunction cbFunc) :
      clientCb_(cb_object), cbFunc_(cbFunc) {
    ALOGI("Created");
}

ndk::ScopedAStatus MiscStatusListenerImpl::sendStatus(TelemetryServiceStatusCodes status_code,
                                                      const std::string& status_message) {
    int int_status_code =
            static_cast<std::underlying_type_t<TelemetryServiceStatusCodes>>(status_code);
    ALOGD("sending misc status back to client. Status: %s, Message: %s",
          ::aidl::google::sdv::telemetry::toString(status_code).c_str(), status_message.c_str());

    cbFunc_(clientCb_, int_status_code, status_message.c_str());
    return ndk::ScopedAStatus::ok();
}
}  // namespace google::sdv::telemetry
