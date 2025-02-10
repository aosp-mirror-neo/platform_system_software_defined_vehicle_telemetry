// Copyright 2024 Google LLC

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
