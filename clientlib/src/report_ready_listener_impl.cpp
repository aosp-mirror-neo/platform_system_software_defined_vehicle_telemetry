// Copyright 2023 Google LLC. All Rights Reserved.

#define LOG_TAG "ReportReadyListenerImpl"

#include "report_ready_listener_impl.h"

namespace android::sdv::telemetry {

    ReportReadyListenerImpl::ReportReadyListenerImpl(
        void *cb, void (*cbFunc)(void*, const char*, size_t)):
        clientCB_(cb),
        cbFuncPtr(cbFunc)
    {
        ALOGI("Created");
    }

    ReportReadyListenerImpl::~ReportReadyListenerImpl() {
        ALOGI("Destroyed");
    }

    android::binder::Status ReportReadyListenerImpl::onReady(
        const ::android::String16& metricConfigName)
    {
        const char* configName = android::String8(metricConfigName).c_str();
        ALOGI("onReady %s", configName);
        cbFuncPtr(clientCB_, configName,strlen(configName));
        return android::binder::Status::ok();
    }

}