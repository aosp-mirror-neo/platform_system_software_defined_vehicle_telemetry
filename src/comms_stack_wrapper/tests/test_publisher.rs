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
use clap::Subcommand;
use futures::FutureExt;
use parking_lot::Mutex;
use protobuf::well_known_types::wrappers::Int64Value;
use protobuf::Message;
use protobuf::MessageFull;
use sdv_comms::dt;
use sdv_comms::id::ServiceFqin;
use sdv_comms::rpc;
use sdv_comms::sd::PublisherUnitMetadata;
use sdv_comms::sd::ServerUnitMetadata;
use sdv_comms::sd::UnitMetadata;
use sdv_comms::sd::UnitRegistrationArgs;
use sdv_comms::sd::UnitType;
use sdv_proto_reflection_rs::message_metadata::MessageMetadata;
use sdv_status_rs::SdvStatus;
use sdv_status_rs::SdvStatusCode;
use sdv_telemetry_comms_utils::wait_for_sdv_agents_and_create_context;
use sdv_telemetry_utils::conversions::u32_into_usize;
use sdv_telemetry_utils::observable_value::ObservableValue;
use std::collections::HashMap;
use std::future;
use std::io;
use std::io::Read;
use std::slice;
use std::str::FromStr;
use std::sync::Arc;

const DT_MESSAGE_SIZE: u32 = 20;

#[derive(Parser, Debug)]
struct Args {
    /// FQIN of the service. If it contains `__PROCESS_ID__`, it will be replaced by the process ID.
    #[arg(long)]
    fqin: String,
    #[arg(long)]
    unit_type: String,
    #[arg(long)]
    unit_name: String,

    #[command(subcommand)]
    publisher_args: PublisherArgs,
}

#[derive(Subcommand, Debug)]
enum PublisherArgs {
    DataTunnel {
        #[arg(long)]
        message_queue_size: u32,
    },
    Rpc,
}

impl PublisherArgs {
    async fn register(
        &self,
        ctx: sdv_comms::ContextRef,
        mut unit_registration_args: UnitRegistrationArgs,
    ) -> anyhow::Result<Publisher> {
        match self {
            Self::DataTunnel { message_queue_size } => {
                let mut publisher_unit_metadata = PublisherUnitMetadata::new();
                publisher_unit_metadata.set_message_size(DT_MESSAGE_SIZE);
                publisher_unit_metadata.set_message_count(*message_queue_size);
                unit_registration_args
                    .set_unit_metadata(&UnitMetadata::Publisher(publisher_unit_metadata));

                Ok(Publisher::DataTunnel(
                    dt::Publisher::create(ctx, unit_registration_args)
                        .await
                        .context("Failed to register publisher")?,
                ))
            }
            Self::Rpc => {
                let mut server_unit_metadata = ServerUnitMetadata::new();
                server_unit_metadata.set_version(1);
                unit_registration_args
                    .set_unit_metadata(&UnitMetadata::Server(server_unit_metadata));

                let last_value = Arc::new(Mutex::new(ObservableValue::new()));
                let weak_last_value = Arc::downgrade(&last_value);

                Ok(Publisher::Rpc {
                    _server: rpc::Server::create_with_generic_handlers(
                        ctx,
                        unit_registration_args,
                        HashMap::from([(
                            "GetLatestMessage".to_owned(),
                            rpc::RpcMethod::Unary(Arc::new(move |_, _| {
                                future::ready(
                                    weak_last_value
                                        .upgrade()
                                        .ok_or_else(|| {
                                            SdvStatus::with_message(
                                                SdvStatusCode::Unavailable,
                                                "The RPC server has been unregistered.",
                                            )
                                        })
                                        .and_then(|last_value| {
                                            last_value.lock().get().cloned().ok_or_else(|| {
                                                SdvStatus::new(SdvStatusCode::NotFound)
                                            })
                                        }),
                                )
                                .boxed()
                            })),
                        )]),
                        &rpc::ServerOptions::default(),
                    )
                    .await
                    .context("Failed to register publisher")?,
                    last_value,
                })
            }
        }
    }
}

enum Publisher {
    DataTunnel(dt::Publisher),
    Rpc { _server: rpc::Server, last_value: Arc<Mutex<ObservableValue<Vec<u8>, 1>>> },
}

impl Publisher {
    fn publish(&self, value: i64) -> anyhow::Result<()> {
        match self {
            Self::DataTunnel(publisher) => {
                let mut message = vec![];
                Int64Value { value, ..Default::default() }
                    .write_length_delimited_to_vec(&mut message)
                    .context("Failed to serialize message")?;

                assert!(message.len() <= u32_into_usize(DT_MESSAGE_SIZE));
                message.resize(u32_into_usize(DT_MESSAGE_SIZE), 0);
                publisher.write_messages(&message).context("Failed to write message")?;
            }
            Self::Rpc { _server: _, last_value } => {
                let mut message = vec![];
                Int64Value { value, ..Default::default() }
                    .write_to_vec(&mut message)
                    .context("Failed to serialize message")?;

                last_value.lock().set(message);
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    eprintln!("{args:?}");
    let Args { fqin, unit_type, unit_name, publisher_args } = args;

    let fqin = fqin.replace("__PROCESS_ID__", &std::process::id().to_string());
    let fqin = ServiceFqin::from_str(&fqin).context("Failed to parse FQIN.")?;
    let unit_type = UnitType::from_str(&unit_type).context("Failed to parse Unit Type.")?;

    // The FQIN of the created context is actually silently updated with the correct VM name, so we
    // must not rely on `fqin`, but instead query it from the context via `ctx.get_self_fqin()`.
    let ctx = wait_for_sdv_agents_and_create_context(fqin).await;
    let fqin = ctx.get_self_fqin();
    println!("fqin:{fqin}");

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
        unit_registration_args.set_unit_name(&unit_name);
        unit_registration_args.set_application_metadata(&message_metadata);
        unit_registration_args
    };

    let mut publisher: Option<Publisher> = None;
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
                let unit_registration_args = create_unit_registration_args();
                publisher = Some(publisher_args.register(ctx, unit_registration_args).await?);
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
                    publisher.publish(value.into()).context("Failed to publish message")?;
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
