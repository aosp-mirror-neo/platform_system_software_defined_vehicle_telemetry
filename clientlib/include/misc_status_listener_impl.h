// Copyright 2024 Google LLC

#pragma once

#include "callback_container.h"
#include "callback_functions.h"

#include <aidl/google/sdv/telemetry/BnTelemetryMiscStatusListener.h>
#include <aidl/google/sdv/telemetry/TelemetryServiceStatusCodes.h>

using ::aidl::google::sdv::telemetry::TelemetryServiceStatusCodes;

namespace google::sdv::telemetry {

class MiscStatusListenerImpl : public aidl::google::sdv::telemetry::BnTelemetryMiscStatusListener {
public:
    explicit MiscStatusListenerImpl(Callback<StatusCallbackFunction> callback);

    ndk::ScopedAStatus sendStatus(TelemetryServiceStatusCodes statusCode,
                                  const std::string& statusMessage) override;

    void* getData() { return mCallback.data; }

private:
    Callback<StatusCallbackFunction> mCallback;
};

}  // namespace google::sdv::telemetry
