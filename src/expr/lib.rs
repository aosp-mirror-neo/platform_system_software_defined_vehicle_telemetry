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
