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
    ::clear_report_ready_listener(holder_);
}

std::future<std::optional<android::sdv::telemetry::MetricsReport>>
MetricsReportManager::get_metrics_report() {
    return promise_report_received_.get_future();
}

// Callback function triggered by the telemetry service. Pointer to this function
// is passed to the telemetry service.
void MetricsReportManager::on_report_ready(void* listener, const char* metrics_config_uuid,
                                           const char* report_config_name, int32_t report_number) {
    auto report_manager = static_cast<MetricsReportManager*>(listener);
    ::get_metrics_report(report_manager->holder_, metrics_config_uuid, report_config_name,
                         report_number, listener, &MetricsReportManager::on_report_received);
}

void MetricsReportManager::on_report_received(void* listener, int32_t status_code,
                                              const char* status_message, const uint8_t* report,
                                              size_t size) {
    ALOGI("on_report_received status code: %d, status msg: %s", status_code, status_message);
    auto report_manager = static_cast<MetricsReportManager*>(listener);

    if (status_code ==
        static_cast<int>(TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS)) {
        android::sdv::telemetry::MetricsReport metrics_report;
        metrics_report.ParseFromArray(report, size);
        report_manager->promise_report_received_.set_value(metrics_report);
    } else {
        report_manager->promise_report_received_.set_value(std::nullopt);
    }
}
