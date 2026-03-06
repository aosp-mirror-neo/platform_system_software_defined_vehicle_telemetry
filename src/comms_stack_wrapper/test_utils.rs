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

use anyhow::Context;
use futures::StreamExt;
use protobuf::well_known_types::wrappers::Int64Value;
use protobuf::Message;
use protobuf::MessageFull;
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
use sdv_telemetry_test_utils::match_unwrap;
use sdv_telemetry_test_utils::unique_dt_topic_name;
use std::io::{BufRead, BufReader, Lines, Write};
use std::process::{Child, ChildStdout};
use std::{collections::HashMap, str::FromStr};

use crate::metadata::Metadata;
use crate::metadata::MetadataGeneration;
use crate::metadata::MetadataServiceType;

pub(super) const UNIT_NAME: &str = "default";

pub(super) async fn make_context() -> ContextRef {
    use sdv_telemetry_comms_utils::wait_for_sdv_agents_and_create_context;
    use tokio::sync::OnceCell;

    static CONTEXT_REF: OnceCell<ContextRef> = OnceCell::const_new();
    *CONTEXT_REF
        .get_or_init(|| async {
            // Tests may run in multiple parallel processes, therefore we need to ensure that the
            // FQINs created by the processes do not conflict with each other. This is achieved by
            // embedding the process ID into the FQIN.
            let fqin = ServiceFqin::from_str(&format!(
                "vm:com.google.sdv.telemetry.tests.DataTunnelSubscriberTest{}/instance",
                std::process::id(),
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
    let (unit_type, unit_name) = match_unwrap!(
        ServiceName::parse(service_name).unwrap(),
        ServiceName::UnitTypeBased { unit_type, unit_name: Some(unit_name) } => (unit_type, unit_name)
    );

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

pub(super) struct TestPublisherProxy {
    child: Child,
    pub(super) fqin: ServiceFqin,
    child_stdout: Lines<BufReader<ChildStdout>>,
}

impl TestPublisherProxy {
    pub(super) fn new(
        child: Child,
        fqin: ServiceFqin,
        child_stdout: Lines<BufReader<ChildStdout>>,
    ) -> Self {
        Self { child, fqin, child_stdout }
    }

    pub(super) fn register(&mut self) {
        let mut stdin = self.child.stdin.as_ref().unwrap();
        stdin.write_all(b"r").unwrap();
        stdin.flush().unwrap();
        self.child_stdout.find(|line| line.as_ref().unwrap() == "done").unwrap().unwrap();
    }

    pub(super) fn unregister(&mut self) {
        let mut stdin = self.child.stdin.as_ref().unwrap();
        stdin.write_all(b"u").unwrap();
        stdin.flush().unwrap();
        self.child_stdout.find(|line| line.as_ref().unwrap() == "done").unwrap().unwrap();
    }

    pub(super) fn publish(&mut self, value: u8) {
        let mut stdin = self.child.stdin.as_ref().unwrap();
        stdin.write_all(&[b'm', value]).unwrap();
        stdin.flush().unwrap();
        self.child_stdout.find(|line| line.as_ref().unwrap() == "done").unwrap().unwrap();
    }
}

impl Drop for TestPublisherProxy {
    fn drop(&mut self) {
        self.unregister();

        drop(self.child.stdin.take());
        assert!(self.child.wait().unwrap().success());
    }
}

pub(super) enum PublisherType {
    DataTunnel,
    Rpc,
}

pub(super) fn create_publisher_in_new_process(
    service_name: String,
    publisher_type: PublisherType,
) -> TestPublisherProxy {
    use std::process::Command;
    use std::process::Stdio;
    use std::str::FromStr;

    let (unit_type, unit_name) = match_unwrap!(
        ServiceName::parse(&service_name).unwrap(),
        ServiceName::UnitTypeBased { unit_type, unit_name: Some(unit_name) } => (unit_type, unit_name)
    );
    let unit_type = unit_type.to_string();

    let mut args = vec![
        "--fqin",
        // Tests may run in multiple parallel processes, therefore we need to ensure that
        // the FQINs created by the processes do not conflict with each other. This is
        // achieved by embedding the process ID into the FQIN, which is done by the
        // `sdv_telemetry_comms_stack_wrapper_test_publisher` binary, which replaces the
        // `__PROCESS_ID__` placeholder with its process ID.
        "vm:com.google.sdv.telemetry.tests.DataTunnelSubscriberTest__PROCESS_ID__/instance",
        "--unit-type",
        &unit_type,
        "--unit-name",
        &unit_name,
    ];
    match publisher_type {
        PublisherType::DataTunnel => args.extend(["data-tunnel", "--message-queue-size", "100"]),
        PublisherType::Rpc => args.extend(["rpc"]),
    }
    eprintln!("Spwaning publisher process with args: {args:?}");
    let mut child = Command::new("./sdv_telemetry_comms_stack_wrapper_test_publisher")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        // Print child process errors to the test's stderr.
        .stderr(Stdio::inherit())
        .args(args)
        .spawn()
        .unwrap();

    let mut child_stdout = BufReader::new(child.stdout.take().unwrap()).lines();
    let child_fqin = child_stdout
        .find_map(|line| {
            let line = line.unwrap();
            let (command, payload) = line.split_once(":")?;
            if command == "fqin" {
                Some(payload.to_owned())
            } else {
                None
            }
        })
        .unwrap();
    let child_fqin = ServiceFqin::from_str(&child_fqin)
        .with_context(|| format!("Failed to parse child FQIN '{child_fqin}'"))
        .unwrap();

    let mut publisher = TestPublisherProxy::new(child, child_fqin, child_stdout);
    publisher.register();
    publisher
}

// There is no way to create a `UnitIdentifier` in unit tests, other than by discovering a real
// service. Thus, we need to create a service in order to create a `MetadataGeneration` for
// testing.
pub(super) async fn create_metadata_generation(
    service_name: String,
) -> (TestPublisherProxy, MetadataGeneration) {
    let ctx = make_context().await;

    let unit_type = match_unwrap!(
        ServiceName::parse(&service_name).unwrap(),
        ServiceName::UnitTypeBased { unit_type, .. } => unit_type
    );

    let publisher =
        create_publisher_in_new_process(service_name.to_owned(), PublisherType::DataTunnel);
    let service_unit = discover_service(ctx, &unit_type).await;

    let metadata_generation = MetadataGeneration::new(
        MetadataServiceType::new(&service_unit.get_unit_metadata()),
        service_unit.get_unit_identifier(),
        Metadata { config_descriptor: None, message_descriptor: Int64Value::descriptor() },
    );

    (publisher, metadata_generation)
}
