// Copyright 2023 Google LLC. All Rights Reserved.

#include "callback_container.h"

#include <aidl/google/sdv/telemetry/BnTelemetryStatusListener.h>
#include <aidl/google/sdv/telemetry/TelemetryServiceStatusCodes.h>
#include <utils/Log.h>

namespace google::sdv::telemetry {

class StatusListenerImpl : public aidl::google::sdv::telemetry::BnTelemetryStatusListener {
public:
    explicit StatusListenerImpl(std::shared_ptr<CallbackContainer> cb_container);
    explicit StatusListenerImpl(CallbackContainer& cb_container);
    void set_status_listener(void* listener, CbFunction cbFunc);
    ndk::ScopedAStatus sendStatus(int api_call_id, int status_code,
                                  const std::string& message) override;

private:
    CallbackContainer& cb_container_;
};

}  // namespace google::sdv::telemetry
