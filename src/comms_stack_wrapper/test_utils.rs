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

//! Test Utils for Comms Stack

#![cfg(test)]

use futures::StreamExt;
use protobuf::Message;
use sdv_comms::dt;
use sdv_comms::id::ServiceFqin;
use sdv_comms::rpc;
use sdv_comms::sd::ServiceDiscoveryManager;
use sdv_comms::sd::ServiceUnitDefinition;
use sdv_comms::sd::{
    PublisherUnitMetadata, ServerUnitMetadata, UnitMetadata, UnitRegistrationArgs, UnitType,
};
use sdv_comms::ContextRef;
use sdv_proto_reflection_rs::message_metadata::MessageMetadata;
use sdv_status_rs::SdvResult;
use sdv_telemetry_comms_utils::ServiceName;
use sdv_telemetry_test_utils::unique_dt_topic_name;
use std::{collections::HashMap, str::FromStr};

pub(super) const UNIT_NAME: &str = "default";

pub(super) async fn make_context(fqin_extra: u8) -> ContextRef {
    use sdv_telemetry_comms_utils::wait_for_sdv_agents_and_create_context;
    use tokio::sync::OnceCell;

    static CONTEXT_REF: OnceCell<ContextRef> = OnceCell::const_new();
    *CONTEXT_REF
        .get_or_init(|| async {
            // Tests may run in multiple parallel processes, therefore we need to ensure that the FQINs
            // created by the processes do not conflict with each other. This is achieved by embedding
            // the process ID into the FQIN.
            let fqin = ServiceFqin::from_str(&format!(
                "vm:com.google.sdv.telemetry.tests.DataTunnelSubscriberTest{}{}/instance",
                std::process::id(),
                fqin_extra
            ))
            .unwrap();
            // The FQIN of the created context is actually silently updated with the correct VM
            // name, so we must not rely on `fqin`, but instead query it from the Context via
            // `ctx.get_self_fqin()`.
            wait_for_sdv_agents_and_create_context(fqin).await
        })
        .await
}

pub(super) fn make_unique_service_name() -> String {
    format!("{}|{UNIT_NAME}", make_unique_unit_type())
}

pub(super) fn make_unique_unit_type() -> UnitType {
    UnitType::from_str(&format!(
        "com.google.sdv.telemetry.tests.DataTunnelSubscriberIntegrationTest#{}",
        unique_dt_topic_name()
    ))
    .unwrap()
}

pub(super) async fn discover_service(
    ctx: ContextRef,
    unit_type: &UnitType,
) -> ServiceUnitDefinition {
    let sd = ServiceDiscoveryManager::new(ctx);
    let service_units = sd.list_units_by_type(unit_type).collect::<Vec<_>>().await;
    assert_eq!(service_units.len(), 1);
    service_units[0].clone()
}

pub(super) async fn register_topic(
    ctx: ContextRef,
    service_name: &str,
    publisher_metadata: MessageMetadata,
) -> SdvResult<dt::Publisher> {
    register_topic_with_message_queue_size(ctx, service_name, publisher_metadata, 2).await
}

pub(super) async fn register_topic_with_message_queue_size(
    ctx: ContextRef,
    service_name: &str,
    publisher_metadata: MessageMetadata,
    message_queue_size: u32,
) -> SdvResult<dt::Publisher> {
    let mut unit_registration_args =
        create_registration_args(service_name, Some(publisher_metadata));

    let mut publisher_unit_metadata = PublisherUnitMetadata::new();
    publisher_unit_metadata.set_message_size(3);
    publisher_unit_metadata.set_message_count(message_queue_size);
    unit_registration_args.set_unit_metadata(&UnitMetadata::Publisher(publisher_unit_metadata));

    dt::Publisher::create(ctx, unit_registration_args).await
}

pub(super) async fn register_server(
    ctx: ContextRef,
    service_name: &str,
    publisher_metadata: Option<MessageMetadata>,
    methods_map: HashMap<String, rpc::RpcMethod>,
) -> SdvResult<rpc::Server> {
    let mut unit_registration_args = create_registration_args(service_name, publisher_metadata);

    let mut server_unit_metadata = ServerUnitMetadata::new();
    server_unit_metadata.set_version(1);
    unit_registration_args.set_unit_metadata(&UnitMetadata::Server(server_unit_metadata));

    rpc::Server::create_with_generic_handlers(
        ctx,
        unit_registration_args,
        methods_map,
        &rpc::ServerOptions::default(),
    )
    .await
}

fn create_registration_args(
    service_name: &str,
    publisher_metadata: Option<MessageMetadata>,
) -> UnitRegistrationArgs {
    let ServiceName::UnitTypeBased { unit_type, unit_name: Some(unit_name) } =
        ServiceName::parse(service_name).unwrap()
    else {
        unreachable!();
    };

    let mut unit_registration_args = UnitRegistrationArgs::new();
    unit_registration_args.set_unit_type(&unit_type);
    unit_registration_args.set_unit_name(&unit_name);

    if let Some(publisher_metadata) = publisher_metadata {
        let app_metadata =
            publisher_metadata.write_to_bytes().expect("Metadata serialization should succeed");
        unit_registration_args.set_application_metadata(&app_metadata);
    }

    unit_registration_args
}
