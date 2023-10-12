// Copyright 2023 Google LLC. All Rights Reserved.

#include "callback_container.h"

#include <android/sdv/telemetry/BnTelemetryStatusListener.h>
#include <android/sdv/telemetry/TelemetryServiceStatusCodes.h>
#include <utils/Log.h>

namespace android::sdv::telemetry {

class StatusListenerImpl : public android::sdv::telemetry::BnTelemetryStatusListener {
public:
    explicit StatusListenerImpl(android::sp<CallbackContainer> cb_container);
    void set_status_listener(void* listener, CbFunction cbFunc);
    android::binder::Status sendStatus(int api_call_id, int status_code,
                                       const std::string& message) override;

private:
    android::sp<CallbackContainer> cb_container_;
};

}  // namespace android::sdv::telemetry