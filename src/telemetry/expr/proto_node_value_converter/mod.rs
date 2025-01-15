// Copyright 2024 Google LLC

/// Modules that allow conversion between NodeValue and protobuf reflections.
///
/// Useful for:
/// - Extracting a field from a protobuf message and storing it as a NodeValue.
/// - Using a NodeValue to populate a field in a protobuf message.
pub(crate) mod node_value_to_protobuf_reflection;
pub(crate) mod protobuf_reflection_to_node_value;
