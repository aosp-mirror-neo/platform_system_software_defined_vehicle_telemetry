// Copyright 2023 Google LLC. All Rights Reserved.

#define LOG_TAG "MetricsReportManager"

#include "metrics_report_manager.h"

MetricsReportManager::MetricsReportManager(ServiceHolder* holder) : holder_(holder) {
    ALOGI("Created");
    ::set_report_ready_listener(holder, static_cast<void*>(this),
                                &MetricsReportManager::on_report_ready);
}

MetricsReportManager::~MetricsReportManager() {
    ALOGI("Destroyed");
}

// Callback function triggered by the telemetry service. Pointer to this function
// is passed to the telemetry service.
void MetricsReportManager::on_report_ready(void* listener, int32_t report_number,
                                           const char* report_uuid) {
    std::string uuid(report_uuid);
    auto report_manager = static_cast<MetricsReportManager*>(listener);
    ::get_metrics_report(report_manager->holder_, report_number, report_uuid, listener,
                         &MetricsReportManager::on_report_received);
}

std::future<std::optional<android::sdv::telemetry::MetricsReport>>
MetricsReportManager::get_metrics_report() {
    return promise_report_received_.get_future();
}

void MetricsReportManager::on_report_received(void* listener, int32_t status_code,
                                              const char* status_message, const uint8_t* report,
                                              size_t size) {
    ALOGI("on_report_received status code: %d, status msg: %s", status_code, status_message);
    auto report_manager = static_cast<MetricsReportManager*>(listener);

    if (status_code ==
        static_cast<int>(TelemetryServiceStatusCodes::GET_FINISHED_REPORT_SUCCEEDED)) {
        android::sdv::telemetry::MetricsReport metrics_report;
        metrics_report.ParseFromArray(report, size);
        report_manager->promise_report_received_.set_value(metrics_report);
    } else {
        report_manager->promise_report_received_.set_value(std::nullopt);
    }
}
