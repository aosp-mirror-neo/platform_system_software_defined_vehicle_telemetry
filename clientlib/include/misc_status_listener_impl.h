// Copyright 2024 Google LLC

#include "callback_functions.h"

#include <aidl/google/sdv/telemetry/BnTelemetryMiscStatusListener.h>
#include <aidl/google/sdv/telemetry/TelemetryServiceStatusCodes.h>
#include <utils/Log.h>

using ::aidl::google::sdv::telemetry::TelemetryServiceStatusCodes;
namespace google::sdv::telemetry {

class MiscStatusListenerImpl : public aidl::google::sdv::telemetry::BnTelemetryMiscStatusListener {
public:
    explicit MiscStatusListenerImpl(void* cb_object, StatusCallbackFunction cbFunc);
    ndk::ScopedAStatus sendStatus(TelemetryServiceStatusCodes status_code,
                                  const std::string& message) override;
    void* getClientCb() { return clientCb_; }

private:
    void* clientCb_;
    StatusCallbackFunction cbFunc_;
};

}  // namespace google::sdv::telemetry
