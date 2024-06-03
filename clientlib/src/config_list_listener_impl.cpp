// Copyright 2023 Google LLC. All Rights Reserved.

#include <cstddef>
#define LOG_TAG "ConfigListListenerImpl"

#include "config_list_listener_impl.h"

using ::aidl::google::sdv::telemetry::TelemetryServiceStatusCodes;

namespace google::sdv::telemetry {

ConfigListListenerImpl::ConfigListListenerImpl(void* cb, ConfigListCallbackFunction cbFunc) :
      clientConfigListListener_(cb), cbFuncPtr_(cbFunc) {
    ALOGI("Created");
}

ndk::ScopedAStatus ConfigListListenerImpl::sendConfigList(
        int32_t status_code, const std::string& status_message,
        const std::vector<std::string>& config_list) {
    std::vector<const char*> c_strings;

    for (const auto& uuid : config_list) {
        c_strings.push_back(uuid.c_str());
    }

    cbFuncPtr_(clientConfigListListener_, status_code, status_message.c_str(), c_strings.data(),
               c_strings.size());

    return ndk::ScopedAStatus::ok();
}
}  // namespace google::sdv::telemetry