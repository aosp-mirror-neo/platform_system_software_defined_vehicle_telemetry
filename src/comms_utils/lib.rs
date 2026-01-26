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
use tokio::time::{sleep, Duration};

use sdv_comms::id::ServiceFqin;
use sdv_comms::ContextRef;
use sdv_comms_ctx::wait_for_agents;

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
