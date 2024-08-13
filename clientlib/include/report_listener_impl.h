// Copyright 2023 Google LLC

#pragma once

#include "callback_container.h"
#include "callback_functions.h"

#include <aidl/google/sdv/telemetry/BnTelemetryReportListener.h>
#include <aidl/google/sdv/telemetry/TelemetryServiceStatusCodes.h>

using ::aidl::google::sdv::telemetry::TelemetryServiceStatusCodes;

namespace google::sdv::telemetry {

class ReportListenerImpl : public aidl::google::sdv::telemetry::BnTelemetryReportListener {
public:
    ReportListenerImpl(Callback<GetSerializedDataCallbackFunction> callback);

    ndk::ScopedAStatus onReport(TelemetryServiceStatusCodes statusCode,
                                const std::string& statusMessage,
                                const std::vector<uint8_t>& report) override;

private:
    Callback<GetSerializedDataCallbackFunction> mCallback;
};

}  // namespace google::sdv::telemetry
