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

#include "misc_status_listener_impl.h"

#include "aidl/google/sdv/telemetry/status/StatusDetails.h"
#include "status.h"

#include <android-base/logging.h>

namespace google::sdv::telemetry {

namespace {

using StatusDetailsAIDL = ::aidl::google::sdv::telemetry::status::StatusDetails;
using StatusDetailsAIDLTag = StatusDetailsAIDL::Tag;

AggregationPublisherErrorDetails convert(
        const StatusDetailsAIDL::AggregationPublisherErrorDetails& details) {
    return AggregationPublisherErrorDetails{
            .metrics_config_uuid = details.metricsConfigUUID.c_str(),
            .publisher_name = details.publisherName.c_str(),
            .field_name = details.fieldName.c_str(),
    };
}

ServicePublisherErrorDetails convert(
        const StatusDetailsAIDL::ServicePublisherErrorDetails& details) {
    return ServicePublisherErrorDetails{
            .metrics_config_uuid = details.metricsConfigUUID.c_str(),
            .publisher_name = details.publisherName.c_str(),
    };
}

ConditionalTriggerErrorDetails convert(
        const StatusDetailsAIDL::ConditionalTriggerErrorDetails& details) {
    return ConditionalTriggerErrorDetails{
            .metrics_config_uuid = details.metricsConfigUUID.c_str(),
            .trigger_name = details.triggerName.c_str(),
    };
}

MetricsReportGeneratorErrorDetails convert(
        const StatusDetailsAIDL::MetricsReportGeneratorErrorDetails& details) {
    return MetricsReportGeneratorErrorDetails{
            .metrics_config_uuid = details.metricsConfigUUID.c_str(),
            .metrics_report_name = details.metricsReportName.c_str(),
            .field_name = details.fieldName.c_str(),
    };
}

ServiceErrorDetails convert(const StatusDetailsAIDL::ServiceErrorDetails& details) {
    return ServiceErrorDetails{
            .service_name = details.serviceName.c_str(),
    };
}

FileIOErrorDetails convert(const StatusDetailsAIDL::FileIOErrorDetails& details) {
    return FileIOErrorDetails{
            .path = details.path.c_str(),
    };
}

StatusDetailsWithTag convert(const StatusDetailsAIDL& status_details) {
    StatusDetailsWithTag result;
    result.tag = StatusDetailsTag::UnknownDetailsTag;
    result.details.unknown_details = UnknownDetails{};

    switch (status_details.getTag()) {
        case StatusDetailsAIDLTag::aggregationPublisherErrorDetails: {
            const auto& details =
                    status_details.get<StatusDetailsAIDLTag::aggregationPublisherErrorDetails>();
            result.tag = StatusDetailsTag::AggregationPublisherErrorDetailsTag;
            result.details.aggregation_publisher_error_details = convert(details);
            return result;
        }

        case StatusDetailsAIDLTag::servicePublisherErrorDetails: {
            const auto& details =
                    status_details.get<StatusDetailsAIDLTag::servicePublisherErrorDetails>();
            result.tag = StatusDetailsTag::ServicePublisherErrorDetailsTag;
            result.details.service_publisher_error_details = convert(details);
            return result;
        }

        case StatusDetailsAIDLTag::conditionalTriggerErrorDetails: {
            const auto& details =
                    status_details.get<StatusDetailsAIDLTag::conditionalTriggerErrorDetails>();
            result.tag = StatusDetailsTag::ConditionalTriggerErrorDetailsTag;
            result.details.conditional_trigger_error_details = convert(details);
            return result;
        }

        case StatusDetailsAIDLTag::metricsReportGeneratorErrorDetails: {
            const auto& details =
                    status_details.get<StatusDetailsAIDLTag::metricsReportGeneratorErrorDetails>();
            result.tag = StatusDetailsTag::MetricsReportGeneratorErrorDetailsTag;
            result.details.metrics_report_generator_error_details = convert(details);
            return result;
        }

        case StatusDetailsAIDLTag::serviceErrorDetails: {
            const auto& details = status_details.get<StatusDetailsAIDLTag::serviceErrorDetails>();
            result.tag = StatusDetailsTag::ServiceErrorDetailsTag;
            result.details.service_error_details = convert(details);
            return result;
        }

        case StatusDetailsAIDLTag::fileIOErrorDetails: {
            const auto& details = status_details.get<StatusDetailsAIDLTag::fileIOErrorDetails>();
            result.tag = StatusDetailsTag::FileIOErrorDetailsTag;
            result.details.file_io_error_details = convert(details);
            return result;
        }
    }
    return result;
}

}  //  namespace

MiscStatusListenerImpl::MiscStatusListenerImpl(Callback<MiscStatusCallbackFunction> callback) :
      mCallback(std::move(callback)) {
    LOG(INFO) << "MiscStatusListenerImpl created";
}

ndk::ScopedAStatus MiscStatusListenerImpl::sendStatus(
        const ::aidl::google::sdv::telemetry::status::TelemetryServiceStatus& status) {
    LOG(DEBUG) << "Sending misc status back to client. Status: " << status.toString();

    const auto telemetry_status_details = convert(status.details);
    const auto telemetry_status = TelemetryServiceStatus{
            .id = status.id,
            .code = std::to_underlying(status.code),
            .message = status.message.c_str(),
            .details = &telemetry_status_details,
    };
    mCallback.run(&telemetry_status);
    return ndk::ScopedAStatus::ok();
}

}  // namespace google::sdv::telemetry
