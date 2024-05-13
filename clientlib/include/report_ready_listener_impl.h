// Copyright 2023 Google LLC. All Rights Reserved.

#include <aidl/google/sdv/telemetry/BnTelemetryReportReadyListener.h>
#include <utils/Log.h>

namespace google::sdv::telemetry {

typedef void (*ReportReadyCallbackFunction)(void*, const char*, const char*, int32_t);

class ReportReadyListenerImpl :
      public aidl::google::sdv::telemetry::BnTelemetryReportReadyListener {
public:
    explicit ReportReadyListenerImpl(void* clientCb, ReportReadyCallbackFunction cbFunc);
    ndk::ScopedAStatus onReady(const std::string& metricsConfigUuid,
                               const std::string& metricsReportConfigName,
                               int32_t reportNumber) override;

private:
    // Void pointer storing the CB object passed by the OEM client app.
    void* clientCb_;

    // Function pointer to the CB function provided by the OEM client application.
    // Client will have its own implementation of CB interface which will be stored
    // as void* pointer in telemetry_service.h. The void* cb will be cast back to the
    // CB interfce by the client in the CB function.
    ReportReadyCallbackFunction cbFunc_;
};

}  // namespace google::sdv::telemetry