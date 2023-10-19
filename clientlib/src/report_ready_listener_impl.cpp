// Copyright 2023 Google LLC. All Rights Reserved.

#define LOG_TAG "ReportReadyListenerImpl"

#include "report_ready_listener_impl.h"

namespace android::sdv::telemetry {

void ReportReadyListenerImpl::set_report_ready_listener(void* cbObject,
                                                        ReportReadyCallbackFunction cbFunc) {
    clientCb_ = cbObject;
    cbFuncPtr_ = cbFunc;
}

android::binder::Status ReportReadyListenerImpl::onReady(int32_t report_number,
                                                         const std::string& metricConfigUuid) {
    const char* configUuid = metricConfigUuid.c_str();
    ALOGD("onReady %s report number: %d", configUuid, report_number);
    cbFuncPtr_(clientCb_, report_number, configUuid);
    return android::binder::Status::ok();
}
}