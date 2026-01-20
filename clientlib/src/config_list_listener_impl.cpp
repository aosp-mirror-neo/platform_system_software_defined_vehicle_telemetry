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

#include "config_list_listener_impl.h"

#include <android-base/logging.h>

#include <cstddef>

using ::aidl::google::sdv::telemetry::status::TelemetryServiceStatusCodes;

namespace google::sdv::telemetry {

ConfigListListenerImpl::ConfigListListenerImpl(Callback<ConfigListCallbackFunction> callback) :
      mCallback(std::move(callback)) {
    LOG(INFO) << "ConfigListListenerImpl created";
}

ndk::ScopedAStatus ConfigListListenerImpl::sendConfigList(TelemetryServiceStatusCodes statusCode,
                                                          const std::string& statusMessage,
                                                          const std::vector<std::string>& uuids) {
    std::vector<const char*> c_strings;

    for (const auto& uuid : uuids) {
        c_strings.push_back(uuid.c_str());
    }

    mCallback.run(std::to_underlying(statusCode), statusMessage.c_str(), c_strings.data(),
                  c_strings.size());

    return ndk::ScopedAStatus::ok();
}

}  // namespace google::sdv::telemetry
