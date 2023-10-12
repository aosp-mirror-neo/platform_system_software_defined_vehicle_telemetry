// Copyright 2023 Google LLC. All Rights Reserved.

#define LOG_TAG "CPPClient"

#include "lib.h"
#include "metrics_config_manager.h"
#include "metrics_report_manager.h"
#include "status_listener.h"
#include "system/software_defined_vehicle/telemetry/proto/metrics_configuration.pb.h"

#include <binder/IPCThreadState.h>
#include <binder/IServiceManager.h>
#include <binder/ProcessState.h>
#include <utils/Log.h>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <vector>

// Callback function triggered by the telemetry service. Pointer to this function
// is passed to the telemetry service.
void trigger_status_listener(void* listener, int32_t status_code, const char* message) {
    std::string status_message(message);
    auto statusListener = static_cast<StatusListenerImpl*>(listener);
    statusListener->onStatus(status_code, status_message);
}

int main(int argc, char* argv[]) {
    android::sp<android::ProcessState> ps(android::ProcessState::self());
    ps->startThreadPool();
    ALOGI("Fetching binder telemetry service");
    // Opaque pointer holding an instance of the telemetry service
    ServiceHolder* holder = get_telemetry_service();

    android::sp<StatusListenerImpl> status_listener(new StatusListenerImpl());
    ALOGI("Setting status listener");
    set_status_listener(holder, static_cast<void*>(status_listener.get()),
                        &trigger_status_listener);

    std::unique_ptr<MetricsReportManager> report_manager =
            std::make_unique<MetricsReportManager>(holder);
    std::unique_ptr<MetricsConfigManager> config_manager =
            std::make_unique<MetricsConfigManager>(holder);

    // Vector to store the serialized config
    std::vector<uint8_t> vec;

    // Read config from file on disk if path is provided
    if (argc == 2) {
        std::string path = argv[1];
        std::cout << "Reading metric config from: " << path << std::endl;
        ALOGI("Reading metric config from %s", path.c_str());

        // Open the file in binary mode.
        std::ifstream file(path, std::ios::binary);
        if (!file) {
            ALOGE("Error: Couldn't open the file!");
            return 1;
        }
        // Get the size of the file.
        file.seekg(0, std::ios::end);
        std::streamsize size = file.tellg();
        file.seekg(0, std::ios::beg);

        ALOGI("Size of metric config %td", size);

        // Read the file into a vector<uint8_t>.
        vec.resize(static_cast<int>(size));

        if (!file.read(reinterpret_cast<char*>(vec.data()), size)) {
            std::cerr << "Failed to read the file!" << std::endl;
            return 1;
        }

    } else {
        std::cerr << "No input metric config provided" << std::endl;
        return 1;
    }

    std::future<std::pair<bool, std::string>> future_add_config =
            config_manager->add_metrics_config(&vec[0], vec.size());
    std::pair<bool, std::string> add_config_result = future_add_config.get();

    if (add_config_result.first) {
        ALOGI("Successfully added config %s", add_config_result.second.c_str());
    } else {
        ALOGE("Error in adding config");
        return 1;
    }

    std::future<std::pair<bool, std::string>> future_activate_config =
            config_manager->activate_metrics_config(add_config_result.second.c_str());
    std::pair<bool, std::string> activate_config_result = future_activate_config.get();

    // Wait for the callback to complete
    if (activate_config_result.first) {
        ALOGI("Successfully activated config %s", activate_config_result.second.c_str());
    } else {
        ALOGE("Error in activating config");
        return 1;
    }

    std::future<std::optional<android::sdv::telemetry::MetricsReport>> future_report_received =
            report_manager->get_metrics_report();
    std::optional<android::sdv::telemetry::MetricsReport> metrics_report =
            future_report_received.get();

    if (metrics_report) {
        ALOGI("Metrics Report Received %s", metrics_report->DebugString().c_str());
    } else {
        ALOGI("Error in receiing metrics report");
    }

    std::future<std::pair<bool, std::string>> future_remove_config =
            config_manager->remove_metrics_config(metrics_report->metrics_config_uuid().c_str());
    std::pair<bool, std::string> remove_config_result = future_remove_config.get();
    if (remove_config_result.first) {
        ALOGI("Successfully removed config %s", remove_config_result.second.c_str());
    } else {
        ALOGE("Error in removing config");
        return 1;
    }
}