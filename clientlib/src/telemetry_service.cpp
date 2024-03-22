// Copyright 2023 Google LLC. All Rights Reserved.

#define LOG_TAG "ServiceClient"

#include "telemetry_service.h"

#include <iostream>

using ::android::base::StringPrintf;
using ::android::sdv::telemetry::ITelemetryService;
using ::android::sdv::telemetry::TelemetryServiceStatusCodes;

namespace android::sdv::telemetry {

template <typename T>
std::optional<TelemetryServiceStatusCodes> validate_params(void* listener, T cbFunc) {
    if (listener == nullptr) {
        return TelemetryServiceStatusCodes::API_CALL_FAILED_LISTENER_NOT_SET;
    }

    if (cbFunc == nullptr) {
        return TelemetryServiceStatusCodes::API_CALL_FAILED_CALLBACK_FUNCTION_NOT_SET;
    }

    return std::nullopt;
}

TelemetryService::TelemetryService() {
    api_id_ = 1;
    const std::string instance =
            StringPrintf("%s/default", android::String8(ITelemetryService::descriptor).c_str());

    binder_service_ = android::waitForService<android::sdv::telemetry::ITelemetryService>(
            android::String16(instance.c_str()));
    cb_container_ = new CallbackContainer();

    mStatusListenerImpl_ = new StatusListenerImpl(cb_container_);
    binder_service_->registerStatusListener(mStatusListenerImpl_);

    mReportReadyListener_ = new ReportReadyListenerImpl();
    binder_service_->registerReportReadyListener(mReportReadyListener_);

    ALOGI("TelemetryService Created");
}

TelemetryServiceStatusCodes TelemetryService::add_metrics_config(
        const std::vector<uint8_t>& metricConfig, void* listener, CbFunction cbFunc) {
    ALOGI("addMetricsConfig in CPP");

    std::optional<TelemetryServiceStatusCodes> maybe_error = validate_params(listener, cbFunc);
    if (maybe_error) {
        return *maybe_error;
    }

    int cur_api_id = ++api_id_;
    cb_container_->insert_callback(cur_api_id, CallbackData{listener, cbFunc});
    binder_service_->addMetricsConfig(cur_api_id, metricConfig);
    return TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS;
}

TelemetryServiceStatusCodes TelemetryService::activate_metrics_config(
        const std::string& metricsConfigUuid, void* listener, CbFunction cbFunc) {
    ALOGI("activate metrics config inside CPP");

    std::optional<TelemetryServiceStatusCodes> maybe_error = validate_params(listener, cbFunc);
    if (maybe_error) {
        return *maybe_error;
    }

    int cur_api_id = ++api_id_;
    cb_container_->insert_callback(cur_api_id, CallbackData{listener, cbFunc});
    binder_service_->activateMetricsConfig(cur_api_id, metricsConfigUuid);
    return TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS;
}

TelemetryServiceStatusCodes TelemetryService::deactivate_metrics_config(
        const std::string& metricsConfigUuid, void* listener, CbFunction cbFunc) {
    ALOGI("deactivate metrics config inside CPP");
    int cur_api_id = ++api_id_;

    std::optional<TelemetryServiceStatusCodes> maybe_error = validate_params(listener, cbFunc);
    if (maybe_error) {
        return *maybe_error;
    }

    cb_container_->insert_callback(cur_api_id, CallbackData{listener, cbFunc});
    binder_service_->deactivateMetricsConfig(cur_api_id, metricsConfigUuid);
    return TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS;
}

TelemetryServiceStatusCodes TelemetryService::remove_metrics_config(
        const std::string& metricsConfigUuid, void* listener, CbFunction cbFunc) {
    ALOGI("remove metrics config inside CPP");
    int cur_api_id = ++api_id_;

    std::optional<TelemetryServiceStatusCodes> maybe_error = validate_params(listener, cbFunc);
    if (maybe_error) {
        return *maybe_error;
    }

    cb_container_->insert_callback(cur_api_id, CallbackData{listener, cbFunc});
    binder_service_->removeMetricsConfig(cur_api_id, metricsConfigUuid);
    return TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS;
}

TelemetryServiceStatusCodes TelemetryService::remove_all_metrics_configs(void* listener,
                                                                         CbFunction cbFunc) {
    ALOGI("remove all metrics configs inside CPP");
    int cur_api_id = ++api_id_;

    std::optional<TelemetryServiceStatusCodes> maybe_error = validate_params(listener, cbFunc);
    if (maybe_error) {
        return *maybe_error;
    }
    cb_container_->insert_callback(cur_api_id, CallbackData{listener, cbFunc});

    binder_service_->removeAllMetricsConfigs(cur_api_id);
    return TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS;
}

TelemetryServiceStatusCodes TelemetryService::set_report_ready_listener(
        void* listener, ReportReadyCallbackFunction cbFunc) {
    std::optional<TelemetryServiceStatusCodes> maybe_error = validate_params(listener, cbFunc);
    if (maybe_error) {
        return *maybe_error;
    }

    mReportReadyListener_->set_report_ready_listener(listener, cbFunc);
    ALOGI("registerReportReadyListener from CPP");

    return TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS;
}

TelemetryServiceStatusCodes TelemetryService::clear_report_ready_listener() {
    android::binder::Status err = binder_service_->unregisterReportReadyListener();
    ALOGI("unregisterReportReadyListener from CPP %s", std::string(err.toString8()).c_str());
    return TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS;
}

TelemetryServiceStatusCodes TelemetryService::get_metrics_report(const char* metrics_config_uuid,
                                                                 const char* report_config_name,
                                                                 int32_t report_number,
                                                                 void* listener,
                                                                 GetReportCallbackFunction cbFunc) {
    if (listener == nullptr) {
        return TelemetryServiceStatusCodes::API_CALL_FAILED_LISTENER_NOT_SET;
    }

    // Had to explicitly check since the function signature is different
    if (cbFunc == nullptr) {
        return TelemetryServiceStatusCodes::API_CALL_FAILED_CALLBACK_FUNCTION_NOT_SET;
    }

    android::sp<ReportListenerImpl> mReportListener(new ReportListenerImpl(listener, cbFunc));
    android::binder::Status err =
            binder_service_->getMetricsReport(metrics_config_uuid, report_config_name,
                                              report_number, mReportListener);
    ALOGI("get_metrics_report from CPP %s", std::string(err.toString8()).c_str());
    return TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS;
}

TelemetryServiceStatusCodes TelemetryService::set_status_listener(void* listener,
                                                                  CbFunction cbFunc) {
    std::optional<TelemetryServiceStatusCodes> maybe_error = validate_params(listener, cbFunc);
    if (maybe_error) {
        return *maybe_error;
    }

    mStatusListenerImpl_->set_status_listener(listener, cbFunc);
    ALOGI("set status callback from CPP");

    return TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS;
}

TelemetryServiceStatusCodes TelemetryService::clear_status_listener() {
    android::binder::Status err = binder_service_->unregisterStatusListener();
    ALOGI("unregisterStatusListener from CPP %s", std::string(err.toString8()).c_str());
    return TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS;
}
}  // namespace android::sdv::telemetry