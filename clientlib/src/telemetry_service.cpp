// Copyright 2023 Google LLC. All Rights Reserved.

#include "android-base/expected.h"
#define LOG_TAG "ServiceClient"

#include "telemetry_service.h"

#include <iostream>

using ::aidl::google::sdv::telemetry::ITelemetryService;
using ::aidl::google::sdv::telemetry::TelemetryServiceStatusCodes;
using ::android::base::StringPrintf;

namespace google::sdv::telemetry {

namespace {

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

}  // namespace

//  static
android::base::expected<std::unique_ptr<TelemetryService>, ndk::ScopedAStatus>
TelemetryService::Create() {
    const auto descriptor = std::string(ITelemetryService::descriptor) + "/default";
    ALOGI("Waiting for service %s", descriptor.c_str());
    auto binder_service = ITelemetryService::fromBinder(
            ndk::SpAIBinder(AServiceManager_waitForService(descriptor.data())));

    if (!binder_service) {
        ALOGE("Failed to bind service %s", descriptor.c_str());
        return android::base::unexpected(ndk::ScopedAStatus::fromExceptionCode(EX_NULL_POINTER));
    }

    auto service =
            std::unique_ptr<TelemetryService>(new TelemetryService(std::move(binder_service)));
    auto status = service->registerListeners();
    if (!status.isOk()) {
        ALOGE("Failed to register listeners");
        return android::base::unexpected(
                ndk::ScopedAStatus::fromExceptionCode(status.getExceptionCode()));
    }

    ALOGI("TelemetryService Created");
    return service;
}

TelemetryService::TelemetryService(std::shared_ptr<ITelemetryService> binder_service) :
      mBinderService(std::move(binder_service)) {}

ndk::ScopedAStatus TelemetryService::registerListeners() {
    {
        ALOGI("Registering status listener");
        ndk::ScopedAStatus status = mBinderService->registerApiStatusListener(mStatusListenerImpl);
        if (!status.isOk()) {
            ALOGE("Failed to register status listener: %s", status.getDescription().c_str());
            return status;
        }
    }

    {
        ALOGI("Registering report ready listener");
        // The callback id is added in the follow up CL part of the chain
        ndk::ScopedAStatus status =
                mBinderService->registerReportReadyListener(-1, mReportReadyListener);
        if (!status.isOk()) {
            ALOGE("Failed to register report ready listener: %s", status.getDescription().c_str());
            return status;
        }
    }

    return ndk::ScopedAStatus::ok();
}

TelemetryServiceStatusCodes TelemetryService::add_metrics_config(
        const std::vector<uint8_t>& metricsConfig, void* listener, CbFunction cbFunc) {
    ALOGI("addMetricsConfig in CPP");

    std::optional<TelemetryServiceStatusCodes> maybe_error = validate_params(listener, cbFunc);
    if (maybe_error) {
        return *maybe_error;
    }

    ApiId cur_api_id = getNextApiId();
    mCallbackContainer.insert_callback(cur_api_id, CallbackData{listener, cbFunc});
    mBinderService->addMetricsConfig(cur_api_id, metricsConfig);
    return TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS;
}

TelemetryServiceStatusCodes TelemetryService::activate_metrics_config(
        const std::string& metricsConfigUuid, void* listener, CbFunction cbFunc) {
    ALOGI("activate metrics config inside CPP");

    std::optional<TelemetryServiceStatusCodes> maybe_error = validate_params(listener, cbFunc);
    if (maybe_error) {
        return *maybe_error;
    }

    ApiId cur_api_id = getNextApiId();
    mCallbackContainer.insert_callback(cur_api_id, CallbackData{listener, cbFunc});
    mBinderService->activateMetricsConfig(cur_api_id, metricsConfigUuid);
    return TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS;
}

TelemetryServiceStatusCodes TelemetryService::deactivate_metrics_config(
        const std::string& metricsConfigUuid, void* listener, CbFunction cbFunc) {
    ALOGI("deactivate metrics config inside CPP");
    ApiId cur_api_id = getNextApiId();

    std::optional<TelemetryServiceStatusCodes> maybe_error = validate_params(listener, cbFunc);
    if (maybe_error) {
        return *maybe_error;
    }

    mCallbackContainer.insert_callback(cur_api_id, CallbackData{listener, cbFunc});
    mBinderService->deactivateMetricsConfig(cur_api_id, metricsConfigUuid);
    return TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS;
}

TelemetryServiceStatusCodes TelemetryService::remove_metrics_config(
        const std::string& metricsConfigUuid, void* listener, CbFunction cbFunc) {
    ALOGI("remove metrics config inside CPP");
    ApiId cur_api_id = getNextApiId();

    std::optional<TelemetryServiceStatusCodes> maybe_error = validate_params(listener, cbFunc);
    if (maybe_error) {
        return *maybe_error;
    }

    mCallbackContainer.insert_callback(cur_api_id, CallbackData{listener, cbFunc});
    mBinderService->removeMetricsConfig(cur_api_id, metricsConfigUuid);
    return TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS;
}

TelemetryServiceStatusCodes TelemetryService::remove_all_metrics_configs(void* listener,
                                                                         CbFunction cbFunc) {
    ALOGI("remove all metrics configs inside CPP");
    ApiId cur_api_id = getNextApiId();

    std::optional<TelemetryServiceStatusCodes> maybe_error = validate_params(listener, cbFunc);
    if (maybe_error) {
        return *maybe_error;
    }
    mCallbackContainer.insert_callback(cur_api_id, CallbackData{listener, cbFunc});

    mBinderService->removeAllMetricsConfigs(cur_api_id);
    return TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS;
}

TelemetryServiceStatusCodes TelemetryService::set_report_ready_listener(
        void* listener, ReportReadyCallbackFunction cbFunc) {
    std::optional<TelemetryServiceStatusCodes> maybe_error = validate_params(listener, cbFunc);
    if (maybe_error) {
        return *maybe_error;
    }

    mReportReadyListener->set_report_ready_listener(listener, cbFunc);
    ALOGI("registerReportReadyListener from CPP");

    return TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS;
}

TelemetryServiceStatusCodes TelemetryService::clear_report_ready_listener() {
    ndk::ScopedAStatus err = mBinderService->unregisterReportReadyListener();
    ALOGI("unregisterReportReadyListener from CPP %s", err.getDescription().c_str());
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

    std::shared_ptr<ReportListenerImpl> mReportListener =
            ndk::SharedRefBase::make<ReportListenerImpl>(listener, cbFunc);
    ndk::ScopedAStatus err =
            mBinderService->getMetricsReport(metrics_config_uuid, report_config_name, report_number,
                                             mReportListener);
    ALOGI("get_metrics_report from CPP %s", err.getDescription().c_str());
    return TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS;
}

TelemetryServiceStatusCodes TelemetryService::set_status_listener(void* listener,
                                                                  CbFunction cbFunc) {
    std::optional<TelemetryServiceStatusCodes> maybe_error = validate_params(listener, cbFunc);
    if (maybe_error) {
        return *maybe_error;
    }

    mStatusListenerImpl->set_status_listener(listener, cbFunc);
    ALOGI("set status callback from CPP");

    return TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS;
}

TelemetryServiceStatusCodes TelemetryService::clear_status_listener() {
    ndk::ScopedAStatus err = mBinderService->unregisterApiStatusListener();
    ALOGI("unregisterStatusListener from CPP %s", err.getDescription().c_str());
    return TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS;
}

TelemetryService::ApiId TelemetryService::getNextApiId() {
    return ++mLastApiId;
}

}  // namespace google::sdv::telemetry
