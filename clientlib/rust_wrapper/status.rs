// Copyright 2025 Google LLC

use sdv_telemetry_status_codes_bindgen::TelemetryServiceStatusCodes;

/// Structure containing details of Telemetry Service statuses which are sent back to the client
#[derive(Clone, Debug)]
pub struct TelemetryServiceStatus {
    /// Associated status code providing additional context
    pub code: TelemetryServiceStatusCodes,

    /// Human-readable status message
    pub message: String,
}
