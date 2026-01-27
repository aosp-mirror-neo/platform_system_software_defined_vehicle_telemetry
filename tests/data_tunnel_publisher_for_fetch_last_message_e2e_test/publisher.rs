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

//! A Data Tunnel publisher used in the `SdvE2ETelemetryDtFetchLastMessageTest`.
//! TODO: b/420890689 - Use the Simulator instead, once it supports simulation of DT publishers.

use anyhow::Context;
use clap::Parser;
use log::{error, info};
use protobuf::{Message, MessageFull};
use sdv_comms::id::ServiceFqin;
use sdv_comms::sd::UnitType;
use sdv_comms::ContextRef;
use sdv_proto_reflection_rs::message_metadata::MessageMetadata;
use sdv_telemetry_comms_stack_wrapper::data_tunnel_publisher::DataTunnelPublisher;
use sdv_telemetry_comms_utils::wait_for_sdv_agents_and_create_context;
use sdv_telemetry_dt_publisher_for_fetch_last_message_e2e_test_proto::publisher::Data;
use sdv_telemetry_utils::logging::{init_logger_for_samples_or_tests, validated_log_tag};
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::LazyLock;
use std::thread::sleep;
use std::time::Duration;

const LOG_TAG: &str = validated_log_tag("TelemetryTestDtFlmPub");

static SERVICE_FQIN: LazyLock<String> = LazyLock::new(|| {
    // Tests may want to run multiple instances of this publisher binary. Therefore, we need to
    // ensure that the FQINs created by the processes do not conflict with each other. This is
    // achieved by embedding the process ID into the FQIN.
    format!(
        "vm:com.google.sdv.telemetry.tests.DtPublisherForFetchLastMessageE2ETest{}/instance",
        std::process::id()
    )
});

const UNIT_TYPE: &str =
    "com.google.sdv.telemetry.tests.DtPublisherForFetchLastMessageE2ETest#Value";

const MESSAGE_COUNT: u32 = 1000;
const MESSAGE_SIZE: u32 = 10;

/// A Data Tunnel publisher used in the `SdvE2ETelemetryDtFetchLastMessageTest`.
#[derive(Parser, Debug)]
#[command()]
struct Args {
    /// Unit name to publish under.
    #[arg(short, long, default_value = "default")]
    unit_name: String,

    /// Values to publish
    values: Vec<u32>,
}

#[tokio::main]
async fn main() -> ExitCode {
    init_logger_for_samples_or_tests(LOG_TAG);

    wrapped_main()
        .await
        .inspect_err(|err| {
            eprintln!("{err:#?}");
            error!("{err:#?}");
        })
        .map_or_else(|_| ExitCode::FAILURE, |_| ExitCode::SUCCESS)
}

async fn create_dt_publisher(
    ctx: ContextRef,
    unit_type: &UnitType,
    unit_name: &str,
) -> anyhow::Result<DataTunnelPublisher> {
    let publisher_metadata = MessageMetadata {
        file_descriptors: vec![Data::descriptor().file_descriptor_proto().clone()],
        message_full_name: format!(".{}", Data::descriptor().full_name()),
        ..Default::default()
    };

    DataTunnelPublisher::register_topic(
        ctx,
        unit_type,
        unit_name,
        MESSAGE_COUNT,
        MESSAGE_SIZE,
        publisher_metadata.write_to_bytes()?,
    )
    .await
    .with_context(|| format!("Failed to register data tunnel publisher with unit type '{unit_type}' and unit name '{unit_name}'."))
}

async fn wrapped_main() -> anyhow::Result<()> {
    let args = Args::parse();

    info!("Setting up Data Tunnel publisher");
    let fqin = ServiceFqin::from_str(&SERVICE_FQIN)
        .with_context(|| format!("Failed to parse FQIN '{}'.", *SERVICE_FQIN))?;
    let ctx = wait_for_sdv_agents_and_create_context(fqin).await;

    let unit_type = UnitType::from_str(UNIT_TYPE)
        .with_context(|| format!("Failed to parse unit type '{UNIT_TYPE}'"))?;
    let dt_publisher = create_dt_publisher(ctx, &unit_type, &args.unit_name).await?;

    for value in args.values {
        info!("Publishing value {value}");
        let message = Data { value, ..Default::default() };
        dt_publisher
            .publish_length_delimited_proto(&message)
            .with_context(|| format!("Failed to write message: {message}"))?;
    }

    // Sleep forever and never return. This is necessary, so that the DT topic remains available.
    info!("Finished publishing values. Sleeping for eternity.");
    sleep(Duration::MAX);
    Ok(())
}
