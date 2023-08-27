// Copyright 2023 Google LLC. All Rights Reserved.

#define LOG_TAG "ServiceClient"

#include "telemetry_service.h"

#include "report_ready_listener_impl.h"
#include "status_listener_impl.h"

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

    void TelemetryService::add_metrics_config(const std::vector<uint8_t>& metricConfig) {
        ALOGI("addMetricsConfig in CPP");
        binder_service_->addMetricsConfig(metricConfig);
    }

    void TelemetryService::activate_metrics_config(const std::string& metricsConfigUuid) {
        ALOGI("activate metrics config inside CPP");
        binder_service_->activateMetricsConfig(metricsConfigUuid);
    }

    void TelemetryService::deactivate_metrics_config(const std::string& metricsConfigUuid) {
        ALOGI("deactivate metrics config inside CPP");
        binder_service_->deactivateMetricsConfig(metricsConfigUuid);
    }

    void TelemetryService::remove_metrics_config(const std::string& metricsConfigUuid) {
        ALOGI("remove metrics config inside CPP");
        binder_service_->removeMetricsConfig(metricsConfigUuid);
    }

    void TelemetryService::remove_all_metrics_configs() {
        ALOGI("remove all metrics configs inside CPP");
        binder_service_->removeAllMetricsConfigs();
    }

    void TelemetryService::set_report_ready_listener(void* listener,
                                                     void (*cbFunc)(void*, const char*)) {
        android::sp<ReportReadyListenerImpl> mReportReadyListener(
            new ReportReadyListenerImpl(listener, cbFunc));
        android::binder::Status err = binder_service_->setReportReadyListener(mReportReadyListener);
        ALOGI("setReportReadyListener from CPP %s", std::string(err.toString8()).c_str());
    }

    void TelemetryService::clear_report_ready_listener() {
        android::binder::Status err = binder_service_->clearReportReadyListener();
        ALOGI("clearReportReadyListener from CPP %s", std::string(err.toString8()).c_str());
    }

    void TelemetryService::set_status_listener(void (*cbFunc)(int, const char*)) {
        android::sp<StatusListenerImpl> mStatusListenerImpl(new StatusListenerImpl(cbFunc));
        android::binder::Status err = binder_service_->setStatusListener(mStatusListenerImpl);
        ALOGI("set status callback from CPP %s", std::string(err.toString8()).c_str());
    }

    void TelemetryService::clear_status_listener() {
        android::binder::Status err = binder_service_->clearStatusListener();
        ALOGI("clearStatusListener from CPP %s", std::string(err.toString8()).c_str());
    }
}
