// Copyright 2025 Google LLC. All Rights Reserved.

#include "callback_functions.h"
#include "system/software_defined_vehicle/telemetry/proto/metrics_configuration/metrics_configuration.pb.h"

#include <aidl/google/sdv/telemetry/BnTelemetryConfigListListener.h>
#include <aidl/google/sdv/telemetry/TelemetryServiceStatusCodes.h>
#include <utils/Log.h>

using ::aidl::google::sdv::telemetry::TelemetryServiceStatusCodes;

namespace google::sdv::telemetry {

class ConfigListListenerImpl : public aidl::google::sdv::telemetry::BnTelemetryConfigListListener {
public:
    ConfigListListenerImpl(void* cb, ConfigListCallbackFunction cbFunc);
    ndk::ScopedAStatus sendConfigList(TelemetryServiceStatusCodes status_code,
                                      const std::string& status_message,
                                      const std::vector<std::string>& config_list) override;

private:
    // Void pointer storing the config_list listener object passed by the OEM client app.
    void* clientConfigListListener_;

    // Function pointer to the CB function provided by the OEM client application.
    // CB function will be used to send the metrics config_list back to the client
    ConfigListCallbackFunction cbFuncPtr_;
};

}  // namespace google::sdv::telemetry