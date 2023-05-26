// Copyright 2023 Google LLC. All Rights Reserved.

#define LOG_TAG "CPPClient"

#include <stdlib.h>
#include <stdio.h>
#include <vector>
#include <unistd.h>

#include <utils/String16.h>
#include <utils/Log.h>

#include <binder/IPCThreadState.h>
#include <binder/IServiceManager.h>
#include <binder/ProcessState.h>

#include "lib.h"
#include "IReportReadyListener.h"
#include "system/software_defined_vehicle/telemetry/proto/metrics_configuration.pb.h"

// Callback function triggered by the telemetry service. Pointer to this function
// is passed to the telemetry service.
void trigger_report_ready_listener(void* listener, const char* data, size_t len) {
    std::string str(data, len);
    auto reportReadListener = static_cast<IReportReadyListener*>(listener);
    reportReadListener->onReady(str);
}

int main() {

  android::sp<android::ProcessState> ps(android::ProcessState::self());
  ps->startThreadPool();
  ALOGI("Fetching binder telemetry service");
  // Opaque pointer holding an instance of the telemetry service
  ServiceHolder* holder =
        start_telemetry_service();

  android::sdv::telemetry::MetricsConfig config;
  config.set_name("Test Metric");
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
  char config_name[] = "TestConfig";
  addMetricsConfig(holder, &config_name[0], &vec[0], vec.size());

  android::sp<ReportReadyImpl> listener(new ReportReadyImpl());
  ALOGI("Setting report ready listener");
  setReportReadyListener(
                holder, static_cast<void*>(listener.get()), &trigger_report_ready_listener);

  removeAllMetricsConfigs(holder);

  // adding sleep for 3 seconds to let async callback finish.
  // Used only for testing purposes.
  // TODO fix b/284363036 and replace sleep with sync primitives
  sleep(3);

  ALOGI("Releasing binder service");
  stop_telemetry_service(holder);
  return 0;
}
