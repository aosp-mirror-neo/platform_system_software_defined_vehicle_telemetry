// Copyright 2023 Google LLC. All Rights Reserved.

#include <android/sdv/telemetry/BnTelemetryReportReadyListener.h>
#include <utils/Log.h>

namespace android::sdv::telemetry {

class ReportReadyListenerImpl :
      public android::sdv::telemetry::BnTelemetryReportReadyListener {
public:
    ReportReadyListenerImpl(void* cb, void (*cbFunc)(void*, const char*, int32_t));
    ~ReportReadyListenerImpl();
    android::binder::Status onReady(const std::string& metricConfigUuid,
                                    int32_t reportNumber) override;

private:
    // Void pointer storing the CB object passed by the OEM client app.
    void *clientCB_;

    // Function pointer to the CB function provided by the OEM client application.
    // Client will have its own implementation of CB interface which will be stored
    // as void* pointer in telemetry_service.h. The void* cb will be cast back to the
    // CB interfce by the client in the CB function.
    void (*cbFuncPtr)(void*, const char*, int32_t);
};

}