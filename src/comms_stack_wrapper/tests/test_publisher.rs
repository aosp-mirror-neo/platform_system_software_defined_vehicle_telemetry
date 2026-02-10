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

//! Test-only binary that can be used to mimic a data tunnel publisher.

use anyhow::Context;
use clap::Parser;
use protobuf::well_known_types::wrappers::Int64Value;
use protobuf::Message;
use protobuf::MessageFull;
use sdv_comms::dt;
use sdv_comms::id::ServiceFqin;
use sdv_comms::sd::PublisherUnitMetadata;
use sdv_comms::sd::UnitMetadata;
use sdv_comms::sd::UnitRegistrationArgs;
use sdv_comms::sd::UnitType;
use sdv_proto_reflection_rs::message_metadata::MessageMetadata;
use sdv_telemetry_comms_utils::wait_for_sdv_agents_and_create_context;
use sdv_telemetry_utils::conversions::u32_into_usize;
use std::io;
use std::io::Read;
use std::slice;
use std::str::FromStr;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    fqin: String,
    #[arg(long)]
    unit_type: String,
    #[arg(long)]
    unit_name: String,
    #[arg(long)]
    message_queue_size: u32,
}

const MESSAGE_SIZE: u32 = 20;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    eprintln!("{args:?}");

    let fqin = ServiceFqin::from_str(&args.fqin).context("Failed to parse FQIN.")?;
    let unit_type = UnitType::from_str(&args.unit_type).context("Failed to parse Unit Type.")?;

    // The FQIN of the created context is actually silently updated with the correct VM name, so we
    // must not rely on `args.fqin`, but instead query it from the context via
    // `ctx.get_self_fqin()`.
    let ctx = wait_for_sdv_agents_and_create_context(fqin).await;
    println!("fqin:{}", ctx.get_self_fqin());

    let message_metadata = MessageMetadata {
        file_descriptors: vec![Int64Value::descriptor().file_descriptor_proto().to_owned()],
        message_full_name: format!(".{}", Int64Value::descriptor().full_name()),
        ..Default::default()
    }
    .write_to_bytes()
    .context("Failed to serialize message metadata")?;

    let create_unit_registration_args = move || -> UnitRegistrationArgs {
        let mut unit_registration_args = UnitRegistrationArgs::new();
        unit_registration_args.set_unit_type(&unit_type);
        unit_registration_args.set_unit_name(&args.unit_name);
        unit_registration_args.set_application_metadata(&message_metadata);

        let mut publisher_unit_metadata = PublisherUnitMetadata::new();
        publisher_unit_metadata.set_message_size(MESSAGE_SIZE);
        publisher_unit_metadata.set_message_count(args.message_queue_size);
        unit_registration_args.set_unit_metadata(&UnitMetadata::Publisher(publisher_unit_metadata));

        unit_registration_args
    };

    let mut publisher: Option<dt::Publisher> = None;
    let mut stdin = io::stdin();
    loop {
        let mut command: u8 = 0;
        if stdin.read_exact(slice::from_mut(&mut command)).is_err() {
            // If stdin is closed, exit.
            break;
        }
        match command {
            // register
            b'r' => {
                publisher = Some(
                    dt::Publisher::create(ctx, create_unit_registration_args())
                        .await
                        .context("Failed to register publisher")?,
                );
                println!("done");
            }
            // unregister
            b'u' => {
                drop(publisher.take());
                println!("done");
            }
            // message
            b'm' => {
                let mut value = 0;
                if stdin.read_exact(slice::from_mut(&mut value)).is_err() {
                    // If stdin is closed, exit.
                    break;
                }

                if let Some(publisher) = &mut publisher {
                    let mut message = vec![];
                    Int64Value { value: value.into(), ..Default::default() }
                        .write_length_delimited_to_vec(&mut message)
                        .context("Failed to serialize message")?;

                    assert!(message.len() <= u32_into_usize(MESSAGE_SIZE));
                    message.resize(u32_into_usize(MESSAGE_SIZE), 0);
                    publisher.write_messages(&message).context("Failed to write message")?;
                }
                println!("done");
            }
            command => {
                panic!("Unknown command: {} (0x{command:x})", command as char);
            }
        }
    }

    Ok(())
}
