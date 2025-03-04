// Copyright 2025 Google LLC

//! Implements `CorePal`, which is an implementation of the `PlatformAbstractionLayer` trait for SDV
//! Core.

mod resumed_at;

use sdv_telemetry_platform_abstraction_layer::PlatformAbstractionLayer;

use crate::resumed_at::{ResumedAt, ResumedAtTracker};

/// A `PlatformAbstractionLayer` implementation for SDV Core.
#[derive(Debug)]
pub struct CorePal {
    resumed_at_tracker: ResumedAtTracker,
}

impl PlatformAbstractionLayer for CorePal {
    fn get_resumed_at(&self) -> ResumedAt {
        self.resumed_at_tracker.get_resumed_at()
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
        Self { resumed_at_tracker: ResumedAtTracker::default() }
    }
}
