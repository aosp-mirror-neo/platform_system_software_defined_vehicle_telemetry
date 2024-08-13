// Copyright 2023 Google LLC

#pragma once

#include "callback_container.h"
#include "callback_functions.h"

#include <aidl/google/sdv/telemetry/BnTelemetryApiStatusListener.h>
#include <aidl/google/sdv/telemetry/TelemetryServiceStatusCodes.h>

#include <string>

using ::aidl::google::sdv::telemetry::TelemetryServiceStatusCodes;

namespace google::sdv::telemetry {

class ApiStatusListenerImpl : public aidl::google::sdv::telemetry::BnTelemetryApiStatusListener {
public:
    explicit ApiStatusListenerImpl(CallbackContainer<StatusCallbackFunction>& callbackContainer);

    ndk::ScopedAStatus sendStatus(int apiCallId, TelemetryServiceStatusCodes statusCode,
                                  const std::string& statusMessage) override;

private:
    CallbackContainer<StatusCallbackFunction>& mCallbackContainer;
};

}  // namespace google::sdv::telemetry
