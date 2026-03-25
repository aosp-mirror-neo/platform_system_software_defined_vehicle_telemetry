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
#include "system/software_defined_vehicle/telemetry/proto/metrics_configuration/metrics_configuration.pb.h"

#include <aidl/google/sdv/telemetry/BnTelemetryConfigListListener.h>
#include <aidl/google/sdv/telemetry/status/TelemetryServiceStatusCodes.h>

using ::aidl::google::sdv::telemetry::status::TelemetryServiceStatusCodes;

namespace google::sdv::telemetry {

class ConfigListListenerImpl : public aidl::google::sdv::telemetry::BnTelemetryConfigListListener {
public:
    ConfigListListenerImpl(Callback<ConfigListCallbackFunction> callback);

    ndk::ScopedAStatus sendConfigList(TelemetryServiceStatusCodes statusCode,
                                      const std::string& statusMessage,
                                      const std::vector<std::string>& uuids) override;

private:
    Callback<ConfigListCallbackFunction> mCallback;
};

}  // namespace google::sdv::telemetry
