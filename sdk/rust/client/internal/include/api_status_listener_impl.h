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

#include <aidl/google/sdv/telemetry/BnTelemetryApiStatusListener.h>
#include <aidl/google/sdv/telemetry/status/TelemetryServiceStatusCodes.h>

#include <string>

using ::aidl::google::sdv::telemetry::status::TelemetryServiceStatusCodes;

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
