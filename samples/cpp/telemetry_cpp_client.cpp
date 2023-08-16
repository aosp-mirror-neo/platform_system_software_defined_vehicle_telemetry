// Copyright 2023 Google LLC. All Rights Reserved.

#define LOG_TAG "CPPClient"

#include "IReportReadyListener.h"
#include "lib.h"
#include "system/software_defined_vehicle/telemetry/proto/metrics_configuration.pb.h"

#include <binder/IPCThreadState.h>
#include <binder/IServiceManager.h>
#include <binder/ProcessState.h>
#include <utils/Log.h>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <vector>

const char* UUID = "01bf0754-df95-46bb-bb35-522b6af66600";

// Callback function triggered by the telemetry service. Pointer to this function
// is passed to the telemetry service.
void trigger_report_ready_listener(void* listener, const char* data) {
    std::string str(data);
    auto reportReadListener = static_cast<IReportReadyListener*>(listener);
    reportReadListener->onReady(str);
}

// Callback function triggered by the telemetry service. Pointer to this function
// is passed to the telemetry service.
void trigger_status_listener(int status_code, const char* message) {
    ALOGI("receiving status from service. Status: %d, Message: %s", status_code, message);
}

int main() {

  android::sp<android::ProcessState> ps(android::ProcessState::self());
  ps->startThreadPool();
  ALOGI("Fetching binder telemetry service");
  // Opaque pointer holding an instance of the telemetry service
  ServiceHolder* holder = get_telemetry_service();

  set_status_listener(holder, &trigger_status_listener);

  android::sdv::telemetry::MetricsConfig config;
  config.set_uuid(UUID);
  config.set_version(2);
  config.set_script("Sample Script");

  std::string serialData;

  if (config.SerializeToString(&serialData)) {
      ALOGI("Successfully Serialized Data");
  } else {
      ALOGE("Could not serialize metric config");
      return -1;
  }

  std::vector<uint8_t> vec(serialData.begin(), serialData.end());
  add_metrics_config(holder, &vec[0], vec.size());
  activate_metrics_config(holder, UUID);

  android::sp<ReportReadyImpl> listener(new ReportReadyImpl());
  ALOGI("Setting report ready listener");
  set_report_ready_listener(holder, static_cast<void*>(listener.get()),
                            &trigger_report_ready_listener);

  deactivate_metrics_config(holder, UUID);
  remove_all_metrics_configs(holder);

  ALOGI("Releasing binder service");
  stop_telemetry_service(holder);
  return 0;
}
