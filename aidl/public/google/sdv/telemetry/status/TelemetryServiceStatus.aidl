//
// Copyright (C) 2025 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package google.sdv.telemetry.status;

import google.sdv.telemetry.status.StatusDetails;
import google.sdv.telemetry.status.TelemetryServiceStatusCodes;

/**
 * TelemetryServiceStatus contains details of Telemetry Service statuses
 * which are sent back to the client
 */

@VintfStability
parcelable TelemetryServiceStatus {
    // Unique status ID
    long id;

    // Associated status code providing additional context
    TelemetryServiceStatusCodes code;

    // Human-readable status message
    @utf8InCpp String message;

    /**
     * Structured status details.
     *
     * The following types of status details are possible depending on status code:
     *
     * - FILE_IO_ERROR:
     *     - FileIOErrorDetails
     *
     * - METRICS_CONFIG_PARSE_ERROR:
     *     - FileIOErrorDetails
     *
     * - ACTIVATE_METRICS_CONFIG_FAILED:
     *     - FileIOErrorDetails
     *
     * - METRICS_CONFIG_CORRUPTED:
     *     - FileIOErrorDetails
     *
     * - METRICS_CONFIG_RUNTIME_AGGREGATION_ERROR:
     *     - AggregationPublisherErrorDetails
     *     - MetricsReportGeneratorErrorDetails
     *
     * - METRICS_CONFIG_RUNTIME_EXPRESSION_NODE_EVALUATION_ERROR:
     *     - AggregationPublisherErrorDetails
     *     - ConditionalTriggerErrorDetails
     *     - MetricsReportGeneratorErrorDetails
     *
     * - METRICS_CONFIG_RUNTIME_SERVICE_CONNECTION_ERROR:
     *     - ServicePublisherErrorDetails
     *
     * - METRICS_CONFIG_RUNTIME_SERVICE_METADATA_PARSING_ERROR:
     *     - ServicePublisherErrorDetails
     *
     * - METRICS_CONFIG_RUNTIME_SERVICE_MESSAGE_RETRIEVAL_ERROR:
     *     - AggregationPublisherErrorDetails
     *     - ConditionalTriggerErrorDetails
     *     - MetricsReportGeneratorErrorDetails
     *     - ServiceErrorDetails
     *
     * - METRICS_CONFIG_RUNTIME_SERVICE_MESSAGE_PARSING_ERROR:
     *     - AggregationPublisherErrorDetails
     *     - ConditionalTriggerErrorDetails
     *     - MetricsReportGeneratorErrorDetails
     *     - ServiceErrorDetails
     *
     * No other status code is currently sent with TelemetryServiceStatus.
     */

    StatusDetails details;
}
