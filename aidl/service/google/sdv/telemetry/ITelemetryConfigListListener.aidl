//
// Copyright (C) 2024 The Android Open Source Project
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
 * Enables clients to receive list of active or inactive configs currently part of the service.
 */
@VintfStability
oneway interface ITelemetryConfigListListener {
    /**
     * Called by telemetry service to send back list of configs to client
     *
     * @param statusCode status code indicating the outcome of the sendConfigList() API call
     * @param statusMessage status message adding more context to the outcome of the API call.
     * @param metricsReport List of metrics config uuids maintained by telemetry service in either
     *                      active or inactive state
     */
    void sendConfigList(in TelemetryServiceStatusCodes statusCode,
            in @utf8InCpp String statusMessage, in List<String> configList);
}
