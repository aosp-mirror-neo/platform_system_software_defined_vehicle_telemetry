// Copyright 2023 Google LLC. All Rights Reserved.

#include <android/sdv/telemetry/BnTelemetryStatusListener.h>
#include <utils/Log.h>

namespace android::sdv::telemetry {

class StatusListenerImpl : public android::sdv::telemetry::BnTelemetryStatusListener {
public:
    StatusListenerImpl(void (*cbFunc)(int, const char*));
    ~StatusListenerImpl();
    android::binder::Status sendStatus(int status_code, const std::string& message) override;

private:
    // Function pointer to the CB function provided by the OEM client application.
    // Client will have its own implementation of CB interface which will be stored
    // as void* pointer in telemetry_service.h. The void* cb will be cast back to the
    // CB interfce by the client in the CB function.
    void (*cbFuncPtr)(int, const char*);
};

}  // namespace android::sdv::telemetry