// Copyright 2023 Google LLC

//! Binary for testing SDV Configurable Publishers

use anyhow::Context;
use futures::stream::{self, BoxStream};
use futures::{FutureExt, StreamExt};
use log::{debug, error, info};
use protobuf::{Message, MessageFull};
use sample_publisher_proto::sample_publisher::{self, SampleMessage};
use sdv_comms::id::ServiceFqin;
use sdv_comms::sd::{ServerUnitMetadata, UnitMetadata, UnitRegistrationArgs, UnitType};
use sdv_comms::{rpc, ContextRef};
use sdv_proto_reflection_rs::message_metadata::MessageMetadata;
use sdv_status_rs::SdvResult;
use sdv_telemetry_utils::logging;
use std::collections::HashMap;
use std::future;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

const LOG_TAG: &str = logging::validated_log_tag("TelemetryRpcPub");

const SERVICE_FQIN: &str = "vm:com.google.sdv.telemetry.samples.RpcTestPublisher/instance";
const UNIT_TYPE: &str = "com.google.sdv.telemetry.samples.RpcTestPublisher#Test";
const UNIT_NAME: &str = "default";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // `wrapped_main` is used solely to make sure that any errors are logged via `error!` so that
    // they show up in logs.
    wrapped_main().await.inspect_err(|err| {
        error!("{err:?}");
    })
}

async fn wrapped_main() -> anyhow::Result<()> {
    sdv_telemetry_utils::logging::init_logger_for_samples_or_tests(LOG_TAG);

    let fqin = ServiceFqin::from_str(SERVICE_FQIN)
        .with_context(|| format!("Failed to parse FQIN '{SERVICE_FQIN}'."))?;
    let ctx = sdv_telemetry_comms_utils::wait_for_sdv_agents_and_create_context(fqin).await;

    info!("Creating publisher");
    let unit_type = UnitType::from_str(UNIT_TYPE)
        .with_context(|| format!("Failed to parse unit type '{UNIT_TYPE}'"))?;
    let _server = create_server(ctx, &unit_type, UNIT_NAME).await?;

    // Await pending future to prevent the process from exiting.
    future::pending::<()>().await;

    Ok(())
}

async fn create_server(
    ctx: ContextRef,
    unit_type: &UnitType,
    unit_name: &str,
) -> anyhow::Result<rpc::Server> {
    let server_metadata = MessageMetadata {
        file_descriptors: vec![sample_publisher::file_descriptor().proto().clone()],
        message_full_name: format!(".{}", SampleMessage::descriptor().full_name()),
        ..Default::default()
    };

    let mut server_unit_metadata = ServerUnitMetadata::new();
    server_unit_metadata.set_version(1);
    let mut unit_registration_args = UnitRegistrationArgs::new();
    unit_registration_args.set_unit_metadata(&UnitMetadata::Server(server_unit_metadata));
    unit_registration_args.set_unit_name(unit_name);
    unit_registration_args.set_unit_type(unit_type);
    unit_registration_args.set_application_metadata(
        &server_metadata.write_to_bytes().with_context(|| "Failed to serialize server metadata")?,
    );

    let create_subscription = rpc::RpcMethod::ServerStreaming(Arc::new(move |_, configuration| {
        future::ready(create_subscription(configuration)).boxed()
    }));
    let methods_map = HashMap::from([("CreateSubscription".to_owned(), create_subscription)]);
    Ok(rpc::Server::create_with_generic_handlers(
        ctx,
        unit_registration_args,
        methods_map,
        &rpc::ServerOptions::default(),
    )
    .await?)
}

fn create_subscription(_configuration: &[u8]) -> SdvResult<BoxStream<'static, SdvResult<Vec<u8>>>> {
    info!("Subscription started, publishing data");

    Ok(stream::unfold((), |_| {
        async {
            tokio::time::sleep(Duration::from_secs(1)).await;

            // Generate random data and send to subscriber.
            let random_value = rand::random::<u8>().into();
            let mut message = SampleMessage::new();
            message.sample_data = random_value;
            debug!("Sending message to subscriber: {}", &message);

            let message_bytes =
                message.write_to_bytes().expect("Message should be successfully serialized");
            Some((Ok(message_bytes), ()))
        }
    })
    .boxed())
}
