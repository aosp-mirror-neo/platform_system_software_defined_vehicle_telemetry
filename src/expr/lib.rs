// Copyright 2024 Google LLC

//! Module for building and traversing the abstract syntax tree.

#![expect(
    missing_docs,
    reason = "This code has only recently been migrated into a standalone crate."
)]

pub mod mock;
pub mod node;
pub mod node_error;
pub mod node_evaluation_context;
pub mod node_factory;
pub mod node_value;
pub mod node_value_type;
pub mod proto_node_value_converter;

mod node_evaluation_state;
mod node_evaluation_state_ext;
