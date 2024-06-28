// Copyright 2023 Google LLC

#include "callback_functions.h"

#include <aidl/google/sdv/telemetry/BnTelemetryReportListener.h>
#include <aidl/google/sdv/telemetry/TelemetryServiceStatusCodes.h>
#include <utils/Log.h>

using ::aidl::google::sdv::telemetry::TelemetryServiceStatusCodes;

namespace google::sdv::telemetry {

class ReportListenerImpl : public aidl::google::sdv::telemetry::BnTelemetryReportListener {
public:
    ReportListenerImpl(void* cb, void (*cbFunc)(void*, int, const char*, const uint8_t*, size_t));
    ndk::ScopedAStatus onReport(TelemetryServiceStatusCodes status_code,
                                const std::string& status_message,
                                const std::vector<uint8_t>& report) override;

private:
    // Void pointer storing the report listener object passed by the OEM client app.
    void* clientReportListener_;

    // Function pointer to the CB function provided by the OEM client application.
    // CB function will be used to send the metrics report back to the client
    GetSerializedDataCallbackFunction cbFuncPtr_;
};

}  // namespace google::sdv::telemetry
