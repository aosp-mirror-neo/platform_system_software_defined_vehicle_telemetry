// Copyright 2023 Google LLC

#include "config_list_listener_impl.h"

#include <android-base/logging.h>

#include <cstddef>

using ::aidl::google::sdv::telemetry::TelemetryServiceStatusCodes;

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
