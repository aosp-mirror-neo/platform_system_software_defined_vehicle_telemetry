// Copyright 2023 Google LLC. All Rights Reserved.

#define LOG_TAG "StatusListenerImpl"

#include "status_listener_impl.h"

namespace android::sdv::telemetry {

StatusListenerImpl::StatusListenerImpl(void (*cbFunc)(int, const char*)) : cbFuncPtr(cbFunc) {
    ALOGI("Created");
}

StatusListenerImpl::~StatusListenerImpl() {
    ALOGI("Destroyed");
}

android::binder::Status StatusListenerImpl::sendStatus(int status_code,
                                                       const std::string& status_message) {
    ALOGI("sending status back to client. Status: %d, Message: %s", status_code,
          status_message.c_str());
    cbFuncPtr(status_code, status_message.c_str());
    return android::binder::Status::ok();
}
}  // namespace android::sdv::telemetry