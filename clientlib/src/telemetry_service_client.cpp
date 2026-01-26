// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "telemetry_service_client.h"

#include "callback_container.h"
#include "callback_functions.h"
#include "lib.h"
#include "ready_reports_info_listener_impl.h"

#include <aidl/google/sdv/telemetry/status/TelemetryServiceStatusCodes.h>
#include <android-base/expected.h>
#include <android-base/logging.h>

#include <iostream>

using ::aidl::google::sdv::telemetry::ITelemetryService;
using ::aidl::google::sdv::telemetry::status::TelemetryServiceStatusCodes;

namespace google::sdv::telemetry {

namespace {

template <typename T>
android::base::expected<void, Status> validateParams(void* data, T callback) {
    if (data == nullptr) {
        return android::base::unexpected(
                Status(TelemetryServiceStatusCodes::API_CALL_FAILED_LISTENER_NOT_SET));
    }
    if (callback == nullptr) {
        return android::base::unexpected(
                Status(TelemetryServiceStatusCodes::API_CALL_FAILED_CALLBACK_FUNCTION_NOT_SET));
    }
    return {};
}

#define RETURN_IF_ERROR(expr)     \
    {                             \
        const auto& val = (expr); \
        if (!val.ok()) {          \
            return val.error();   \
        }                         \
    }

}  // namespace

//  static
Status Status::ok() {
    return Status(TelemetryServiceStatusCodes::TELEMETRY_SERVICE_API_CALL_SUCCESS);
}

//  static
Status Status::fromBinderStatus(const ndk::ScopedAStatus& status) {
    if (!status.isOk()) {
        return Status(TelemetryServiceStatusCodes::API_CALL_FAILED_INTERNAL_ERROR,
                      "Binder error: " + status.getDescription());
    }
    return Status::ok();
}

Status::Status(TelemetryServiceStatusCodes code, std::string message) :
      code(code), message(std::move(message)) {}

//  static
android::base::expected<std::unique_ptr<TelemetryServiceClient>, ndk::ScopedAStatus>
TelemetryServiceClient::Create() {
    const auto descriptor = std::string(ITelemetryService::descriptor) + "/default";
    LOG(DEBUG) << "Waiting for service " << descriptor;
    auto binderService = ITelemetryService::fromBinder(
            ndk::SpAIBinder(AServiceManager_waitForService(descriptor.data())));

    if (!binderService) {
        LOG(ERROR) << "Failed to bind service " << descriptor;
        return android::base::unexpected(ndk::ScopedAStatus::fromExceptionCode(EX_NULL_POINTER));
    }

    auto service = std::unique_ptr<TelemetryServiceClient>(
            new TelemetryServiceClient(std::move(binderService)));
    auto status = service->registerListeners();
    if (!status.isOk()) {
        LOG(ERROR) << "Failed to register listeners: " << status;
        return android::base::unexpected(std::move(status));
    }

    LOG(INFO) << "TelemetryServiceClient created";
    return service;
}

TelemetryServiceClient::TelemetryServiceClient(std::shared_ptr<ITelemetryService> binderService) :
      mBinderService(std::move(binderService)) {}

TelemetryServiceClient::~TelemetryServiceClient() {
    LOG(DEBUG) << "Unregistering api status listener";
    ndk::ScopedAStatus status = mBinderService->unregisterApiStatusListener();
    LOG_IF(ERROR, !status.isOk()) << "Failed to unregister API status listener: " << status;

    LOG(DEBUG) << "Unregistering misc status listener";
    status = mBinderService->unregisterMiscStatusListener();
    LOG_IF(ERROR, !status.isOk()) << "Failed to unregister misc status listener: " << status;

    LOG(DEBUG) << "Unregistering report ready listener";
    status = mBinderService->unregisterReportReadyListener();
    LOG_IF(ERROR, !status.isOk()) << "Failed to unregister report ready listener: " << status;

    LOG(INFO) << "TelemetryServiceClient destroyed";
}

ndk::ScopedAStatus TelemetryServiceClient::registerListeners() {
    {
        LOG(DEBUG) << "Registering API status listener";
        ndk::ScopedAStatus status =
                mBinderService->registerApiStatusListener(mApiStatusListenerImpl);
        if (!status.isOk()) {
            LOG(ERROR) << "Failed to register API status listener: " << status;
            return status;
        }
    }

    return ndk::ScopedAStatus::ok();
}

Status TelemetryServiceClient::add_metrics_config(const std::vector<uint8_t>& metricsConfig,
                                                  void* statusCbData,
                                                  StatusCallbackFunction statusCb) {
    RETURN_IF_ERROR(validateParams(statusCbData, statusCb));

    return callWithApiCallId(statusCbData, statusCb, [&](ApiCallId apiCallId) {
        return mBinderService->addMetricsConfig(apiCallId, metricsConfig);
    });
}

Status TelemetryServiceClient::activate_metrics_config(const std::string& metricsConfigUuid,
                                                       void* statusCbData,
                                                       StatusCallbackFunction statusCb) {
    RETURN_IF_ERROR(validateParams(statusCbData, statusCb));

    return callWithApiCallId(statusCbData, statusCb, [&](ApiCallId apiCallId) {
        return mBinderService->activateMetricsConfig(apiCallId, metricsConfigUuid);
    });
}

Status TelemetryServiceClient::deactivate_metrics_config(const std::string& metricsConfigUuid,
                                                         void* statusCbData,
                                                         StatusCallbackFunction statusCb) {
    RETURN_IF_ERROR(validateParams(statusCbData, statusCb));

    return callWithApiCallId(statusCbData, statusCb, [&](ApiCallId apiCallId) {
        return mBinderService->deactivateMetricsConfig(apiCallId, metricsConfigUuid);
    });
}

Status TelemetryServiceClient::remove_metrics_config(const std::string& metricsConfigUuid,
                                                     void* statusCbData,
                                                     StatusCallbackFunction statusCb) {
    RETURN_IF_ERROR(validateParams(statusCbData, statusCb));

    return callWithApiCallId(statusCbData, statusCb, [&](ApiCallId apiCallId) {
        return mBinderService->removeMetricsConfig(apiCallId, metricsConfigUuid);
    });
}

Status TelemetryServiceClient::remove_all_metrics_configs(void* statusCbData,
                                                          StatusCallbackFunction statusCb) {
    RETURN_IF_ERROR(validateParams(statusCbData, statusCb));

    return callWithApiCallId(statusCbData, statusCb, [&](ApiCallId apiCallId) {
        return mBinderService->removeAllMetricsConfigs(apiCallId);
    });
}

Status TelemetryServiceClient::get_active_metrics_config_uuids(
        void* configListCbData, ConfigListCallbackFunction configListCb) {
    RETURN_IF_ERROR(validateParams(configListCbData, configListCb));

    auto config_list_listener = ndk::SharedRefBase::make<ConfigListListenerImpl>(
            Callback<ConfigListCallbackFunction>(configListCbData, std::move(configListCb)));
    ndk::ScopedAStatus status = mBinderService->getActiveMetricsConfigUuids(config_list_listener);
    return Status::fromBinderStatus(status);
}

Status TelemetryServiceClient::get_inactive_metrics_config_uuids(
        void* configListCbData, ConfigListCallbackFunction configListCb) {
    RETURN_IF_ERROR(validateParams(configListCbData, configListCb));

    auto config_list_listener = ndk::SharedRefBase::make<ConfigListListenerImpl>(
            Callback<ConfigListCallbackFunction>(configListCbData, std::move(configListCb)));
    ndk::ScopedAStatus status = mBinderService->getInactiveMetricsConfigUuids(config_list_listener);
    return Status::fromBinderStatus(status);
}

Status TelemetryServiceClient::set_report_ready_listener(void* reportReadyCbData,
                                                         ReportReadyCallbackFunction reportReadyCb,
                                                         void* statusCbData,
                                                         StatusCallbackFunction statusCb,
                                                         void** outPreviousReportReadyCbData) {
    RETURN_IF_ERROR(validateParams(statusCbData, statusCb));
    RETURN_IF_ERROR(validateParams(reportReadyCbData, reportReadyCb));

    if (outPreviousReportReadyCbData != nullptr && mReportReadyListener) {
        *outPreviousReportReadyCbData = mReportReadyListener->getData();
    }
    mReportReadyListener = ndk::SharedRefBase::make<ReportReadyListenerImpl>(
            Callback<ReportReadyCallbackFunction>(reportReadyCbData, std::move(reportReadyCb)));

    return callWithApiCallId(statusCbData, statusCb, [&](ApiCallId apiCallId) {
        return mBinderService->registerReportReadyListener(apiCallId, mReportReadyListener);
    });
}

Status TelemetryServiceClient::clear_report_ready_listener(void** outPreviousReportReadyCbData) {
    if (outPreviousReportReadyCbData != nullptr && mReportReadyListener) {
        *outPreviousReportReadyCbData = mReportReadyListener->getData();
    }
    mReportReadyListener = nullptr;

    ndk::ScopedAStatus status = mBinderService->unregisterReportReadyListener();
    return Status::fromBinderStatus(status);
}

Status TelemetryServiceClient::get_metrics_report(
        const std::string& reportUuid, void* metricsReportCbData,
        GetSerializedDataCallbackFunction metricsReportCb) {
    RETURN_IF_ERROR(validateParams(metricsReportCbData, metricsReportCb));

    std::shared_ptr<ReportListenerImpl> mReportListener =
            ndk::SharedRefBase::make<ReportListenerImpl>(
                    Callback<GetSerializedDataCallbackFunction>(metricsReportCbData,
                                                                std::move(metricsReportCb)));

    ndk::ScopedAStatus status = mBinderService->getMetricsReport(reportUuid, mReportListener);
    return Status::fromBinderStatus(status);
}

Status TelemetryServiceClient::get_ready_reports_info(
        void* readyReportsInfoCbData, ReadyReportsInfoCallbackFunction readyReportsInfoCb) {
    std::shared_ptr<ReadyReportsInfoListenerImpl> mReportListener =
            ndk::SharedRefBase::make<ReadyReportsInfoListenerImpl>(
                    Callback<ReadyReportsInfoCallbackFunction>(readyReportsInfoCbData,
                                                               std::move(readyReportsInfoCb)));
    ndk::ScopedAStatus status = mBinderService->getReadyReportsInfo(mReportListener);
    return Status::fromBinderStatus(status);
}

Status TelemetryServiceClient::set_misc_status_listener(void* miscStatusCbData,
                                                        MiscStatusCallbackFunction miscStatusCb,
                                                        void* statusCbData,
                                                        StatusCallbackFunction statusCb,
                                                        void** outPreviousMiscStatusCbData) {
    RETURN_IF_ERROR(validateParams(statusCbData, statusCb));
    RETURN_IF_ERROR(validateParams(miscStatusCbData, miscStatusCb));

    if (outPreviousMiscStatusCbData != nullptr && mMiscStatusListener) {
        *outPreviousMiscStatusCbData = mMiscStatusListener->getData();
    }
    mMiscStatusListener = ndk::SharedRefBase::make<MiscStatusListenerImpl>(
            Callback<MiscStatusCallbackFunction>(miscStatusCbData, std::move(miscStatusCb)));

    return callWithApiCallId(statusCbData, statusCb, [&](ApiCallId apiCallId) {
        return mBinderService->registerMiscStatusListener(apiCallId, mMiscStatusListener);
    });
}

Status TelemetryServiceClient::clear_misc_status_listener(void** outPreviousMiscStatusCbData) {
    if (outPreviousMiscStatusCbData != nullptr && mMiscStatusListener) {
        *outPreviousMiscStatusCbData = mMiscStatusListener->getData();
    }
    mMiscStatusListener = nullptr;

    ndk::ScopedAStatus status = mBinderService->unregisterMiscStatusListener();
    return Status::fromBinderStatus(status);
}

TelemetryServiceClient::ApiCallId TelemetryServiceClient::getNextApiCallId() {
    return ++mLastApiCallId;
}

}  // namespace google::sdv::telemetry
