// Copyright 2023 Google LLC

//! Utility functions for unit tests.
use protobuf::MessageDyn;

/// Creates a Box with a callback that accepts a message and does nothing.
pub fn create_empty_fn_box() -> Box<dyn FnMut(Box<dyn MessageDyn>) + Send + Sync> {
    Box::new(|_| {})
}
