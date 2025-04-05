// Copyright 2024 Google LLC

//! Module for building and traversing the abstract syntax tree.

#![expect(
    missing_docs,
    reason = "This code has only recently been migrated into a standalone crate."
)]

pub mod dynamic_message;
pub mod mock;
pub mod node;
pub mod node_error;
pub mod node_evaluation_context;
pub mod node_evaluation_state;
pub mod node_factory;
pub mod node_value;
pub mod node_value_type;

mod node_evaluation_state_ext;
