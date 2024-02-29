// Copyright 2023 Google LLC. All Rights Reserved.

#define LOG_TAG "StatusListenerImpl"

#include "status_listener_impl.h"

namespace android::sdv::telemetry {

const int GLOBAL_STATUS_LISTENER_UNIQUE_ID = 0;

StatusListenerImpl::StatusListenerImpl(android::sp<CallbackContainer> cb_container) :
      cb_container_(cb_container) {
    ALOGI("Created");
}

void StatusListenerImpl::set_status_listener(void* listener, CbFunction cbFunc) {
    ALOGI("set_status_listener");
    cb_container_->insert_callback(GLOBAL_STATUS_LISTENER_UNIQUE_ID,
                                   CallbackData{listener, cbFunc});
    sendStatus(GLOBAL_STATUS_LISTENER_UNIQUE_ID,
               static_cast<int>(TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS),
               "Status listener successfully added");
}

android::binder::Status StatusListenerImpl::sendStatus(int api_call_id, int status_code,
                                                       const std::string& status_message) {
    ALOGD("sending status back to client. API call id: %d, Status: %d, Message: %s", api_call_id,
          status_code, status_message.c_str());

    auto cb_data = cb_container_->extract_callback(api_call_id);
    if (cb_data) {
        CbFunction func = cb_data->cb_function;
        func(cb_data->cb_object, status_code, status_message.c_str());
    }  // TODO: Handle case when callback data not found

    return android::binder::Status::ok();
}
}  // namespace android::sdv::telemetry