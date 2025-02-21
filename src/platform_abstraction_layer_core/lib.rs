// Copyright 2025 Google LLC

//! Implements `CorePal`, which is an implementation of the `PlatformAbstractionLayer` trait for SDV
//! Core.

mod vpm;

use sdv_telemetry_platform_abstraction_layer::PlatformAbstractionLayer;
use sdv_telemetry_time_wrapper::Microseconds;

use crate::vpm::VpmClient;

/// A `PlatformAbstractionLayer` implementation for SDV Core.
#[derive(Debug)]
pub struct CorePal {
    vpm_client: VpmClient,
}

impl PlatformAbstractionLayer for CorePal {
    fn get_resumed_at(&self) -> Option<Microseconds> {
        self.vpm_client.get_resumed_at()
    }
}

impl Default for CorePal {
    fn default() -> Self {
        Self::new()
    }
}

impl CorePal {
    /// Create a new instance of a `CorePal`.
    pub fn new() -> Self {
        Self { vpm_client: VpmClient::default() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[sdv_telemetry_test_utils::timeout]
    #[tokio::test]
    async fn get_resumed_at_returns_none() {
        let pal = CorePal::new();
        assert_eq!(pal.get_resumed_at(), None);
    }
}
