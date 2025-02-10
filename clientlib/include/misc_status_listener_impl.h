// Copyright 2024 Google LLC

#pragma once

#include "callback_container.h"
#include "callback_functions.h"

#include <aidl/google/sdv/telemetry/BnTelemetryMiscStatusListener.h>
#include <aidl/google/sdv/telemetry/status/TelemetryServiceStatus.h>

namespace google::sdv::telemetry {

class MiscStatusListenerImpl : public aidl::google::sdv::telemetry::BnTelemetryMiscStatusListener {
public:
    explicit MiscStatusListenerImpl(Callback<MiscStatusCallbackFunction> callback);

    ndk::ScopedAStatus sendStatus(
            const ::aidl::google::sdv::telemetry::status::TelemetryServiceStatus& status) override;

    void* getData() { return mCallback.data; }

private:
    Callback<MiscStatusCallbackFunction> mCallback;
};

}  // namespace google::sdv::telemetry
