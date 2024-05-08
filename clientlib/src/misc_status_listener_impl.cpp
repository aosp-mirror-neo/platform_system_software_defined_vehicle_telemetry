// Copyright 2023 Google LLC. All Rights Reserved.

#define LOG_TAG "MiscStatusListenerImpl"

#include "misc_status_listener_impl.h"

namespace google::sdv::telemetry {

MiscStatusListenerImpl::MiscStatusListenerImpl(void* cb_object, StatusCallbackFunction cbFunc) :
      mCbObject(cb_object), mCBFunc(cbFunc) {
    ALOGI("Created");
}

ndk::ScopedAStatus MiscStatusListenerImpl::sendStatus(int status_code,
                                                      const std::string& status_message) {
    ALOGD("sending misc status back to client. Status: %d, Message: %s", status_code,
          status_message.c_str());

    mCBFunc(mCbObject, status_code, status_message.c_str());
    return ndk::ScopedAStatus::ok();
}
}  // namespace google::sdv::telemetry