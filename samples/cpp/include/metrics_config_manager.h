// Copyright 2023 Google LLC. All Rights Reserved.

#include "lib.h"
#include "status_codes.h"

#include <utils/Log.h>
#include <utils/RefBase.h>

#include <stdio.h>
#include <stdlib.h>

#include <future>
#include <string>

class MetricsConfigManager final : public android::RefBase {
public:
    explicit MetricsConfigManager(ServiceHolder* holder);
    ~MetricsConfigManager() override;

    std::future<std::pair<bool, std::string>> add_metrics_config(uint8_t* metrics_config,
                                                                 size_t size);
    std::future<std::pair<bool, std::string>> activate_metrics_config(const char* config_uuid);
    std::future<std::pair<bool, std::string>> remove_metrics_config(const char*);

private:
    ServiceHolder* holder_;

    std::promise<std::pair<bool, std::string>> promise_add_config_;
    std::promise<std::pair<bool, std::string>> promise_activate_config_;
    std::promise<std::pair<bool, std::string>> promise_remove_config_;

    static void on_config_added(void* config_manager, int32_t status_code, const char* message);
    static void on_config_activated(void* config_manager, int32_t status_code, const char* message);
    static void on_config_removed(void* config_manager, int32_t status_code, const char* message);
};
