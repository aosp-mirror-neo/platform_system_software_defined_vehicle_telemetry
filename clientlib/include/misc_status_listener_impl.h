// Copyright 2024 Google LLC. All Rights Reserved.

#include "callback_function.h"

#include <aidl/google/sdv/telemetry/BnTelemetryMiscStatusListener.h>
#include <aidl/google/sdv/telemetry/TelemetryServiceStatusCodes.h>
#include <utils/Log.h>

namespace google::sdv::telemetry {

class MiscStatusListenerImpl : public aidl::google::sdv::telemetry::BnTelemetryMiscStatusListener {
public:
    explicit MiscStatusListenerImpl(void* cb_object, StatusCallbackFunction cbFunc);
    ndk::ScopedAStatus sendStatus(int status_code, const std::string& message) override;

private:
    void* mCbObject;
    StatusCallbackFunction mCBFunc;
};

}  // namespace google::sdv::telemetry