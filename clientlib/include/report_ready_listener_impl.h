// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "callback_container.h"
#include "callback_functions.h"

#include <aidl/google/sdv/telemetry/BnTelemetryReportReadyListener.h>

namespace google::sdv::telemetry {

class ReportReadyListenerImpl :
      public aidl::google::sdv::telemetry::BnTelemetryReportReadyListener {
public:
    explicit ReportReadyListenerImpl(Callback<ReportReadyCallbackFunction> callback);

    ndk::ScopedAStatus onReady(
            const aidl::google::sdv::telemetry::MetricsReportInfo& metricsReportInfo) override;

    void* getData() { return mCallback.data; }

private:
    Callback<ReportReadyCallbackFunction> mCallback;
};

}  // namespace google::sdv::telemetry
