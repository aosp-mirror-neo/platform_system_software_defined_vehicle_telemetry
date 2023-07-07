// Copyright 2023 Google LLC. All Rights Reserved.

#ifndef TELEMETRY_CLIENTLIB_INCLUDE_TELEMETRY_SERVICE_H
#define TELEMETRY_CLIENTLIB_INCLUDE_TELEMETRY_SERVICE_H

#include <android-base/stringprintf.h>
#include <android/sdv/telemetry/BnTelemetryService.h>
#include <binder/IServiceManager.h>
#include <utils/Log.h>

#include <getopt.h>
#include <stdlib.h>
#include <sysexits.h>
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <iostream>
#include <memory>
#include <vector>

using ::android::sdv::telemetry::ITelemetryService;

namespace android::sdv::telemetry {

// Acts as the mediator between the OEM client and the telemetry service.
class TelemetryService {
public:
    TelemetryService();
    virtual ~TelemetryService();

    void removeAllMetricsConfigs();
    void removeMetricsConfig(const std::string& metricsConfigUuid);
    void addMetricsConfig(const std::vector<uint8_t>& metricConfig);
    void setReportReadyListener(void* listener, void (*cbFunc)(void*, const char*, size_t));

private:
    // holds a reference to the binder service
    android::sp<android::sdv::telemetry::ITelemetryService> binder_service_;
};
}  // namespace android::sdv::telemetry

#endif // TELEMETRY_CLIENTLIB_INCLUDE_TELEMETRY_SERVICE_H
