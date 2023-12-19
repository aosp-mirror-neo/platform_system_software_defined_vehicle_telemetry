// Copyright 2023 Google LLC. All Rights Reserved.

#include "lib.h"
#include "status_codes.h"
#include "system/software_defined_vehicle/telemetry/proto/metrics_configuration.pb.h"

#include <utils/Log.h>
#include <utils/RefBase.h>

#include <stdio.h>
#include <stdlib.h>

#include <future>
#include <string>
#include <vector>

class MetricsReportManager final : public android::RefBase {
public:
    explicit MetricsReportManager(ServiceHolder* holder);
    ~MetricsReportManager() override;

    std::future<std::optional<android::sdv::telemetry::MetricsReport>> get_metrics_report();

private:
    std::promise<std::optional<android::sdv::telemetry::MetricsReport>> promise_report_received_;

    ServiceHolder* holder_;

    static void on_report_received(void* listener, int32_t status_code, const char* status_message,
                                   const uint8_t* report, size_t size);
    static void on_report_ready(void* listener, const char* metrics_report_uuid,
                                const char* report_config_name, int32_t report_number);
};
