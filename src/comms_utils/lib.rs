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

//! This module includes utility functions used in Telemetry for interacting with the new Comm's
//! stack.

mod service_name;

use log::info;
use sdv_comms::id::ServiceFqin;
use sdv_comms::ContextRef;
use sdv_comms_ctx::wait_for_agents;
use tokio::time::{sleep, Duration};

pub use service_name::ServiceName;

/// Waits for SDV agents to be ready and then converts the provided FQIN into a `ContextRef`
/// object.
pub async fn wait_for_sdv_agents_and_create_context(fqin: ServiceFqin) -> ContextRef {
    wait_for_sdv_agents().await;
    // This just crashes if the Service Discovery is not yet ready.
    ContextRef::create(fqin)
}

async fn wait_for_sdv_agents() {
    loop {
        let Err(err) = tokio::task::spawn_blocking(wait_for_agents).await else {
            return;
        };
        info!("Retrying wait for service discovery: {err:?}");
        sleep(Duration::from_secs(10)).await;
    }
}

/// The `Debug` implementation for `ServiceUnitDefinition` just prints an opaque address, which
/// isn't all that helpful. This helper macro converts a `ServiceUnitDefinition` into a more verbose
/// representation.
#[macro_export]
macro_rules! debug_format_service_unit_definition {
    ($service_unit_definition:expr) => {
        format_args!(
            "{{ self: {:?}, unit_identifier: {}, unit_type: {} }}",
            $service_unit_definition,
            $crate::debug_format_unit_identifier!($service_unit_definition.get_unit_identifier()),
            $service_unit_definition.get_unit_type()
        )
    };
}

/// The `Debug` implementation for `UnitIdentifier` prints strings as vectors, which makes it really
/// hard to read. This helper macro converts a `UnitIdentifier` into a better debug representation.
#[macro_export]
macro_rules! debug_format_unit_identifier {
    ($unit_identifier:expr) => {
        format_args!(
            "{{ sid: {}, fqin: {}, unit_name: {} }}",
            $unit_identifier.get_sid(),
            $unit_identifier.get_fqin(),
            $unit_identifier.get_unit_name(),
        )
    };
}
