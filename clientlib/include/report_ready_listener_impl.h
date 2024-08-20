// Copyright 2023 Google LLC

#pragma once

#include "callback_container.h"
#include "callback_functions.h"

#include <aidl/google/sdv/telemetry/BnTelemetryReportReadyListener.h>

namespace google::sdv::telemetry {

class ReportReadyListenerImpl :
      public aidl::google::sdv::telemetry::BnTelemetryReportReadyListener {
public:
    explicit ReportReadyListenerImpl(Callback<ReportReadyCallbackFunction> callback);

    ndk::ScopedAStatus onReady(const std::string& metricsConfigUuid,
                               const std::string& metricsReportConfigName,
                               int32_t reportNumber) override;

    void* getData() { return mCallback.data; }

private:
    Callback<ReportReadyCallbackFunction> mCallback;
};

}  // namespace google::sdv::telemetry
