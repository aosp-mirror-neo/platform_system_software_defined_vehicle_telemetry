// Copyright 2024 Google LLC

#pragma once

#include "aidl/google/sdv/telemetry/MetricsReportInfo.h"
#include "callback_container.h"
#include "callback_functions.h"

#include <aidl/google/sdv/telemetry/BnTelemetryReadyReportsInfoListener.h>

using ::aidl::google::sdv::telemetry::TelemetryServiceStatusCodes;

namespace google::sdv::telemetry {

class ReadyReportsInfoListenerImpl :
      public aidl::google::sdv::telemetry::BnTelemetryReadyReportsInfoListener {
public:
    explicit ReadyReportsInfoListenerImpl(Callback<ReadyReportsInfoCallbackFunction> callback);

    ndk::ScopedAStatus sendReadyReportsInfo(
            TelemetryServiceStatusCodes statusCode, const std::string& statusMessage,
            const std::vector<::aidl::google::sdv::telemetry::MetricsReportInfo>&
                    ready_reports_info) override;

private:
    Callback<ReadyReportsInfoCallbackFunction> mCallback;
};

}  // namespace google::sdv::telemetry
