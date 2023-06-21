// Copyright 2023 Google LLC. All Rights Reserved.

#define LOG_TAG "ServiceClient"

#include "telemetry_service.h"
#include "report_ready_listener_impl.h"
#include <iostream>

using ::android::base::StringPrintf;
using ::android::sdv::telemetry::ITelemetryService;

namespace android::sdv::telemetry {

    TelemetryService::TelemetryService() {

        ALOGI("TelemetryService Created");
        const std::string instance =
            StringPrintf("%s",
                         android::String8(ITelemetryService::descriptor).c_str());

        binder_service_ = android::waitForService
                <android::sdv::telemetry::ITelemetryService>
                (android::String16(instance.c_str()));
    }

    TelemetryService::~TelemetryService() {
        ALOGI("TelemetryService Destroyed");
    }

    void TelemetryService::removeAllMetricsConfigs() {
        ALOGI("remove all metrics configs inside CPP");
        binder_service_->removeAllMetricsConfigs();
    }

    void TelemetryService::removeMetricsConfig(const std::string& metricsConfigUuid) {
        ALOGI("remove metrics config inside CPP");
        binder_service_->removeMetricsConfig(metricsConfigUuid);
    }

    void TelemetryService::addMetricsConfig(const std::vector<uint8_t>& metricConfig) {
        ALOGI("addMetricsConfig in CPP");
        binder_service_->addMetricsConfig(metricConfig);
    }

    void TelemetryService::setReportReadyListener(void* listener,
                                void (*cbFunc)(void*, const char*, size_t))
    {
        android::sp<ReportReadyListenerImpl> mReportReadyListener(
            new ReportReadyListenerImpl(listener, cbFunc));
        android::binder::Status err = binder_service_->setReportReadyListener(mReportReadyListener);
        ALOGI("setReportReadyListener from CPP %s", std::string(err.toString8()).c_str());
    }
}
