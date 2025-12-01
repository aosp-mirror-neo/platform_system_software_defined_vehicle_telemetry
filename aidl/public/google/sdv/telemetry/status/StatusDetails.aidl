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

/**
 * Information on statuses which are being sent back to the client
 */

@VintfStability
union StatusDetails {
    // Aggregation Publisher error details
    @VintfStability
    parcelable AggregationPublisherErrorDetails {
        @utf8InCpp String metricsConfigUUID;
        @utf8InCpp String publisherName;
        @utf8InCpp String fieldName;
    }

    // Service Publisher error details
    @VintfStability
    parcelable ServicePublisherErrorDetails {
        @utf8InCpp String metricsConfigUUID;
        @utf8InCpp String publisherName;
    }

    // Trigger error details
    @VintfStability
    parcelable ConditionalTriggerErrorDetails {
        @utf8InCpp String metricsConfigUUID;
        @utf8InCpp String triggerName;
    }

    // Metrics Report Generator error details
    @VintfStability
    parcelable MetricsReportGeneratorErrorDetails {
        @utf8InCpp String metricsConfigUUID;
        @utf8InCpp String metricsReportName;
        @utf8InCpp String fieldName;
    }

    // Service error details
    @VintfStability
    parcelable ServiceErrorDetails {
        @utf8InCpp String serviceName;
    }

    // File IO error details
    @VintfStability
    parcelable FileIOErrorDetails {
        @utf8InCpp String path;
    }

    AggregationPublisherErrorDetails aggregationPublisherErrorDetails;
    ServicePublisherErrorDetails servicePublisherErrorDetails;
    ConditionalTriggerErrorDetails conditionalTriggerErrorDetails;
    MetricsReportGeneratorErrorDetails metricsReportGeneratorErrorDetails;
    ServiceErrorDetails serviceErrorDetails;
    FileIOErrorDetails fileIOErrorDetails;
}
