// Copyright 2025 Google LLC

//! Implements `CorePal`, which is an implementation of the `PlatformAbstractionLayer` trait for SDV
//! Core.

use sdv_telemetry_platform_abstraction_layer::PlatformAbstractionLayer;
use sdv_telemetry_time_wrapper::Microseconds;

/// A `PlatformAbstractionLayer` implementation for SDV Core.
#[derive(Debug)]
pub struct CorePal {}

// TODO: b/353466210 - Implement for Core.
impl PlatformAbstractionLayer for CorePal {
    fn get_resumed_at(&self) -> Option<Microseconds> {
        log::warn!(
            "CorePal::get_resumed_at called, which is not yet implemented. Returning `None`."
        );

        None
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
        CorePal {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_resumed_at_returns_none() {
        let pal = CorePal::new();
        assert_eq!(pal.get_resumed_at(), None);
    }
}
