//
// Copyright (C) 2023 The Android Open Source Project
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

package google.sdv.telemetry;

import google.sdv.telemetry.status.TelemetryServiceStatusCodes;

/**
 * Interface implemented by Data logger to receive telemetry reports.
 */
@VintfStability
oneway interface ITelemetryReportListener {
    /**
     * Method called by telemetry service during getMetricsReport() API call to send metrics report
     * back to the client. MetricsReport format sent by client as part of Metrics Configuration
     *
     * metricsReport is not size checked currently. However it is expected to be small (<10Kb).
     * If this API needs to pass large metricsReport, it has to be modified to use pipes or similar
     * technique.
     *
     * @param statusCode status code indicating the outcome of the getMetricsReport() API call
     * @param statusMessage status message adding more context to the outcome of the API call.
     * @param metricsReport serialized MetricsReport
     *
     */
    void onReport(in TelemetryServiceStatusCodes statusCode,
            in @utf8InCpp String statusMessage, in byte[] metricsReport);
}
