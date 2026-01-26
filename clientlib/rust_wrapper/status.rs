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

use sdv_telemetry_status_codes_bindgen::TelemetryServiceStatusCodes;

/// Aggregation Publisher error details
#[derive(Debug)]
pub struct AggregationPublisherErrorDetails {
    /// Metrics config UUID
    pub metrics_config_uuid: String,
    /// Aggregation publisher name
    pub publisher_name: String,
    /// Field name
    pub field_name: String,
}

/// Service Publisher error details
#[derive(Debug)]
pub struct ServicePublisherErrorDetails {
    /// Metrics config UUID
    pub metrics_config_uuid: String,
    /// Service publisher name
    pub publisher_name: String,
}

/// Conditional Trigger error details
#[derive(Debug)]
pub struct ConditionalTriggerErrorDetails {
    /// Metrics config UUID
    pub metrics_config_uuid: String,
    /// Conditional trigger name
    pub trigger_name: String,
}

/// Metrics Report Generator error details
#[derive(Debug)]
pub struct MetricsReportGeneratorErrorDetails {
    /// Metrics config UUID
    pub metrics_config_uuid: String,
    /// Report name
    pub metrics_report_name: String,
    /// Field name
    pub field_name: String,
}

/// Service error details
#[derive(Debug)]
pub struct ServiceErrorDetails {
    /// Service name
    pub service_name: String,
}

/// File IO error details
#[derive(Debug)]
pub struct FileIOErrorDetails {
    /// File or directory path
    pub path: String,
}

/// Enum of status details
#[derive(Debug)]
pub enum StatusDetails {
    /// Aggregation Publisher error details
    AggregationPublisherErrorDetails(AggregationPublisherErrorDetails),
    /// Service Publisher error details
    ServicePublisherErrorDetails(ServicePublisherErrorDetails),
    /// Conditional Trigger error details
    ConditionalTriggerErrorDetails(ConditionalTriggerErrorDetails),
    /// Metrics Report Generator error details
    MetricsReportGeneratorErrorDetails(MetricsReportGeneratorErrorDetails),
    /// Service error details
    ServiceErrorDetails(ServiceErrorDetails),
    /// File IO error details
    FileIOErrorDetails(FileIOErrorDetails),
}

/// Structure containing details of Telemetry Service statuses which are sent back to the client
#[derive(Debug)]
pub struct TelemetryServiceStatus {
    /// Unique status ID
    pub id: usize,
    /// Associated status code providing additional context
    pub code: TelemetryServiceStatusCodes,
    /// Human-readable status message
    pub message: String,
    /// Structured status details.
    pub details: Option<StatusDetails>,
}
