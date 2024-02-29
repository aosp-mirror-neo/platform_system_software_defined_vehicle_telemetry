// Copyright 2023 Google LLC. All Rights Reserved.

#define LOG_TAG "MetricsConfigManager"

#include "metrics_config_manager.h"

MetricsConfigManager::MetricsConfigManager(ServiceHolder* holder) : holder_(holder) {
    ALOGI("Created");
}

MetricsConfigManager::~MetricsConfigManager() {
    ALOGI("Destroyed");
}

void MetricsConfigManager::on_config_added(void* listener, int32_t status_code,
                                           const char* message) {
    ALOGI("on_config_added status code: %d, status msg: %s", status_code, message);

    auto config_manager = static_cast<MetricsConfigManager*>(listener);

    if (status_code ==
        static_cast<int>(TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS)) {
        config_manager->promise_add_config_.set_value({true, std::string(message)});

    } else {
        ALOGE("Error in validating config: %s", message);
        config_manager->promise_add_config_.set_value({false, std::string(message)});
    }
}

void MetricsConfigManager::on_config_activated(void* listener, int32_t status_code,
                                               const char* message) {
    ALOGI("on_config_activated status code: %d, status msg: %s", status_code, message);

    auto config_manager = static_cast<MetricsConfigManager*>(listener);
    if (status_code ==
        static_cast<int>(TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS)) {
        config_manager->promise_activate_config_.set_value({true, std::string(message)});
    } else {
        ALOGE("Error in activating config: %s", message);
        config_manager->promise_activate_config_.set_value({true, std::string(message)});
    }
}

void MetricsConfigManager::on_config_removed(void* listener, int32_t status_code,
                                             const char* message) {
    ALOGI("on_config_removed status code: %d, status msg: %s", status_code, message);

    auto config_manager = static_cast<MetricsConfigManager*>(listener);

    if (status_code ==
        static_cast<int>(TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS)) {
        config_manager->promise_remove_config_.set_value({true, std::string(message)});
    } else {
        ALOGE("Error in removing config: %s", message);
        config_manager->promise_remove_config_.set_value({false, std::string(message)});
    }
}

std::future<std::pair<bool, std::string>> MetricsConfigManager::add_metrics_config(
        uint8_t* metrics_config, size_t size) {
    ::add_metrics_config(holder_, metrics_config, size, static_cast<void*>(this),
                         &MetricsConfigManager::on_config_added);
    return promise_add_config_.get_future();
}

std::future<std::pair<bool, std::string>> MetricsConfigManager::activate_metrics_config(
        const char* config_uuid) {
    ::activate_metrics_config(holder_, config_uuid, static_cast<void*>(this),
                              &MetricsConfigManager::on_config_activated);
    return promise_activate_config_.get_future();
}

std::future<std::pair<bool, std::string>> MetricsConfigManager::remove_metrics_config(
        const char* config_uuid) {
    ::remove_metrics_config(holder_, config_uuid, static_cast<void*>(this),
                            &MetricsConfigManager::on_config_removed);
    return promise_remove_config_.get_future();
}
