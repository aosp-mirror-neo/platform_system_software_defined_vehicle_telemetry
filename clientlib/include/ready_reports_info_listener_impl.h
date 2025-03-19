// Copyright 2024 Google LLC

#pragma once

#include "callback_container.h"
#include "callback_functions.h"

#include <aidl/google/sdv/telemetry/BnTelemetryReadyReportsInfoListener.h>

using ::aidl::google::sdv::telemetry::status::TelemetryServiceStatusCodes;

namespace google::sdv::telemetry {

class ReadyReportsInfoListenerImpl :
      public aidl::google::sdv::telemetry::BnTelemetryReadyReportsInfoListener {
public:
    explicit ReadyReportsInfoListenerImpl(Callback<ReadyReportsInfoCallbackFunction> callback);

    ndk::ScopedAStatus sendReadyReportsInfo(TelemetryServiceStatusCodes statusCode,
                                            const std::string& statusMessage,
                                            const std::vector<std::string>& report_uuids) override;

private:
    Callback<ReadyReportsInfoCallbackFunction> mCallback;
};

}  // namespace google::sdv::telemetry
