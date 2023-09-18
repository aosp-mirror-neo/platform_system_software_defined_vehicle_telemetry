// Copyright 2023 Google LLC. All Rights Reserved.

#define LOG_TAG "ReportReadyListenerImpl"

#include "report_ready_listener_impl.h"

namespace android::sdv::telemetry {

ReportReadyListenerImpl::ReportReadyListenerImpl(void* cb,
                                                 void (*cbFunc)(void*, const char*, int32_t)) :
      clientCB_(cb), cbFuncPtr(cbFunc) {
    ALOGI("Created");
}

ReportReadyListenerImpl::~ReportReadyListenerImpl() {
    ALOGI("Destroyed");
}

android::binder::Status ReportReadyListenerImpl::onReady(const std::string& metricConfigUuid,
                                                         int32_t report_number) {
    const char* configUuid = metricConfigUuid.c_str();
    ALOGI("onReady %s", configUuid);
    cbFuncPtr(clientCB_, configUuid, report_number);
    return android::binder::Status::ok();
}
}