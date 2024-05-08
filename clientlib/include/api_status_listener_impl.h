// Copyright 2023 Google LLC. All Rights Reserved.

#include "callback_container.h"

#include <aidl/google/sdv/telemetry/BnTelemetryApiStatusListener.h>
#include <aidl/google/sdv/telemetry/TelemetryServiceStatusCodes.h>
#include <utils/Log.h>

namespace google::sdv::telemetry {

class ApiStatusListenerImpl : public aidl::google::sdv::telemetry::BnTelemetryApiStatusListener {
public:
    explicit ApiStatusListenerImpl(StatusCallbackContainer& cb_container);
    ndk::ScopedAStatus sendStatus(int api_call_id, int status_code,
                                  const std::string& message) override;

private:
    StatusCallbackContainer& cb_container_;
};

}  // namespace google::sdv::telemetry