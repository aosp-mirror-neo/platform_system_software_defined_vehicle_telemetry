// Copyright 2023 Google LLC. All Rights Reserved.

#include <aidl/google/sdv/telemetry/BnTelemetryReportListener.h>
#include <utils/Log.h>

namespace google::sdv::telemetry {

typedef void (*GetReportCallbackFunction)(void*, int, const char*, const uint8_t*, size_t);

class ReportListenerImpl : public aidl::google::sdv::telemetry::BnTelemetryReportListener {
public:
    ReportListenerImpl(void* cb, void (*cbFunc)(void*, int, const char*, const uint8_t*, size_t));
    ndk::ScopedAStatus onReport(int32_t status_code, const std::string& status_message,
                                const std::vector<uint8_t>& report) override;

private:
    // Void pointer storing the report listener object passed by the OEM client app.
    void* clientReportListener_;

    // Function pointer to the CB function provided by the OEM client application.
    // CB function will be used to send the metrics report back to the client
    GetReportCallbackFunction cbFuncPtr_;
};

}  // namespace google::sdv::telemetry