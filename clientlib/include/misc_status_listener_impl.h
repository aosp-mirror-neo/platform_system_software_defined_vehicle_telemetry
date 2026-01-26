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
